#!/usr/bin/env python3
"""
Pull the four datasets this analysis needs from NYC Open Data into ./raw as CSV.

    python scripts/fetch_data.py                # everything
    python scripts/fetch_data.py zones bic      # just those

Unauthenticated Socrata throttles hard. Get a free app token at
https://data.cityofnewyork.us/profile/edit/developer_settings and:

    export SOCRATA_APP_TOKEN=xxxxxxxx

The crash pulls are the big ones. Refuse-vehicle rows are filtered server-side so that
table stays small; the Crashes table is fetched only for the collision_ids we actually
need, in batches, rather than pulling 2.27M rows.
"""
import csv
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

RAW = Path(__file__).resolve().parent.parent / "raw"
TOKEN = os.environ.get("SOCRATA_APP_TOKEN")
PAGE = 50000

# The casing-safe refuse filter. Do not replace this with an equality test.
REFUSE_WHERE = (
    "upper(vehicle_type) like '%GARBAGE%' or upper(vehicle_type) like '%REFUSE%'"
)


def get(dataset, params, attempt=1):
    url = f"https://data.cityofnewyork.us/resource/{dataset}.csv?{urlencode(params)}"
    headers = {"Accept": "text/csv"}
    if TOKEN:
        headers["X-App-Token"] = TOKEN
    try:
        with urlopen(Request(url, headers=headers), timeout=180) as r:
            return r.read().decode("utf-8", "replace")
    except HTTPError as e:
        if e.code in (429, 502, 503, 504) and attempt <= 5:
            wait = 5 * attempt
            print(f"    {e.code}, waiting {wait}s (attempt {attempt})", flush=True)
            time.sleep(wait)
            return get(dataset, params, attempt + 1)
        raise


def paged(dataset, out_name, where=None, select=None, order=":id"):
    """Page through a dataset and write one CSV. Order by :id for a stable page window."""
    out = RAW / out_name
    RAW.mkdir(exist_ok=True)
    offset, total, header = 0, 0, None
    with out.open("w", newline="", encoding="utf-8") as fh:
        while True:
            params = {"$limit": PAGE, "$offset": offset, "$order": order}
            if where:
                params["$where"] = where
            if select:
                params["$select"] = select
            body = get(dataset, params).splitlines()
            if not body or len(body) <= 1:
                break
            if header is None:
                header = body[0]
                fh.write(header + "\n")
            fh.writelines(line + "\n" for line in body[1:])
            got = len(body) - 1
            total += got
            offset += PAGE
            print(f"    {total:,} rows", flush=True)
            if got < PAGE:
                break
            time.sleep(0.4)
    print(f"  -> {out} ({total:,} rows)")
    return total


def fetch_vehicles():
    print("Refuse-vehicle rows from Motor Vehicle Collisions - Vehicles (bm4k-52h4)")
    print("  NOTE: this table lags; expect nothing after 2026-05-04.")
    return paged("bm4k-52h4", "vehicles_refuse.csv", where=REFUSE_WHERE)


def fetch_crashes():
    """
    Crashes for the collision_ids we care about. Socrata 'in(...)' clauses have a length
    limit, so this batches. Run fetch_vehicles() first.
    """
    src = RAW / "vehicles_refuse.csv"
    if not src.exists():
        print("  vehicles_refuse.csv missing - run the 'vehicles' step first")
        return 0
    ids = sorted({r["collision_id"] for r in csv.DictReader(src.open())
                  if r.get("collision_id")})
    print(f"Crashes for {len(ids):,} refuse-involved collisions (h9gi-nx95)")
    RAW.mkdir(exist_ok=True)
    out = RAW / "crashes_refuse.csv"
    header_written, total = False, 0
    BATCH = 400
    with out.open("w", newline="", encoding="utf-8") as fh:
        for i in range(0, len(ids), BATCH):
            chunk = ids[i:i + BATCH]
            where = "collision_id in (" + ",".join(chunk) + ")"
            body = get("h9gi-nx95", {"$limit": 5000, "$where": where}).splitlines()
            if not body:
                continue
            if not header_written:
                fh.write(body[0] + "\n")
                header_written = True
            fh.writelines(l + "\n" for l in body[1:])
            total += len(body) - 1
            print(f"    {total:,} / {len(ids):,}", flush=True)
            time.sleep(0.4)
    print(f"  -> {out} ({total:,} rows)")
    return total


def fetch_bic():
    print("BIC Issued Violations (upii-frjc) - all 18,290 rows, current to last week")
    return paged("upii-frjc", "bic_violations.csv")


def fetch_zones():
    """GeoJSON, because the multipolygon is the point of this one."""
    print("DSNY Commercial Waste Zones (8ev8-jjxq) as GeoJSON")
    RAW.mkdir(exist_ok=True)
    url = "https://data.cityofnewyork.us/resource/8ev8-jjxq.geojson?$limit=100"
    headers = {"X-App-Token": TOKEN} if TOKEN else {}
    with urlopen(Request(url, headers=headers), timeout=120) as r:
        data = r.read()
    out = RAW / "cwz_zones.geojson"
    out.write_bytes(data)
    print(f"  -> {out} ({len(data):,} bytes)")
    return 1


STEPS = {
    "vehicles": fetch_vehicles,
    "crashes": fetch_crashes,
    "bic": fetch_bic,
    "zones": fetch_zones,
}

if __name__ == "__main__":
    wanted = sys.argv[1:] or ["vehicles", "crashes", "bic", "zones"]
    if not TOKEN:
        print("No SOCRATA_APP_TOKEN set - expect 429s on the crash pulls.\n")
    for name in wanted:
        if name not in STEPS:
            print(f"unknown step: {name} (choose from {', '.join(STEPS)})")
            continue
        STEPS[name]()
        print()
    print("Done. Next: python scripts/build_panel.py")
