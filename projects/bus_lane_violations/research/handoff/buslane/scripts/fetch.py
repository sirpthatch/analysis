#!/usr/bin/env python3
"""
Re-pull every aggregate in ../data/ straight from NYC Open Data, and
optionally pull row-level records for a corridor.

    python3 fetch.py                    # refresh all aggregates into ../data/
    python3 fetch.py --fy 2025          # same queries against the FY2025 file
    python3 fetch.py --rows "JAMAICA AVE" --out jamaica.csv

An app token is optional but lifts the anonymous rate limit:
    export SODA_APP_TOKEN=xxxxxxxx
Get one at https://data.cityofnewyork.us/profile/edit/developer_settings

Only dependency is `requests`.
"""

import argparse, csv, os, sys, time
import requests

DATASETS = {
    2025: "m5vz-tzqv",   # Parking Violations Issued - FY2025
    2026: "9mwx-gamw",   # Parking Violations Issued - FY2026
    2027: "pvqr-7yc4",   # Parking Violations Issued - FY2027 (in progress)
}

BASE = "https://data.cityofnewyork.us/resource/{}.json"
BUS_LANE = "upper(violation_description) like '%BUS LANE%'"
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, os.pardir, "data")


def session():
    s = requests.Session()
    tok = os.environ.get("SODA_APP_TOKEN")
    if tok:
        s.headers["X-App-Token"] = tok
    else:
        print("note: no SODA_APP_TOKEN set; you may hit HTTP 429", file=sys.stderr)
    return s


def soql(s, fourby, **params):
    """One SoQL call, with a single retry on 429 as the docs recommend."""
    url = BASE.format(fourby)
    q = {"$" + k: v for k, v in params.items()}
    for attempt in (1, 2):
        r = s.get(url, params=q, timeout=120)
        if r.status_code == 429:
            print("  429, waiting 30s then retrying once...", file=sys.stderr)
            time.sleep(30)
            continue
        r.raise_for_status()
        return r.json()
    raise SystemExit("rate limited twice; get an app token or try later")


def write_csv(rows, path, fields=None):
    if not rows:
        print(f"  no rows for {path}", file=sys.stderr)
        return
    fields = fields or list(rows[0].keys())
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})
    print(f"  wrote {len(rows):>6} rows -> {os.path.relpath(path)}")


def aggregates(s, fourby, fy):
    tag = f"fy{fy}"
    print("categories")
    write_csv(soql(s, fourby,
                   select="violation_description,count(*)",
                   where=BUS_LANE, group="violation_description"),
              os.path.join(DATA, f"violation_categories_{tag}.csv"))

    print("monthly by category")
    write_csv(soql(s, fourby,
                   select="date_trunc_ym(issue_date) as month,violation_description,count(*)",
                   where=BUS_LANE, group="month,violation_description",
                   order="month", limit="200"),
              os.path.join(DATA, f"monthly_by_category_{tag}.csv"))

    for label, desc in (("fixed", "BUS LANE VIOLATION"),
                        ("mobile", "MOBILE BUS LANE VIOLATION")):
        print(f"top locations ({label})")
        write_csv(soql(s, fourby,
                       select="street_name,count(*)",
                       where=f"violation_description='{desc}'",
                       group="street_name", order="count desc", limit="200"),
                  os.path.join(DATA, f"top_locations_{label}_{tag}.csv"))

    print("borough split")
    write_csv(soql(s, fourby,
                   select="violation_county,violation_description,count(*)",
                   where=BUS_LANE, group="violation_county,violation_description",
                   order="count desc", limit="100"),
              os.path.join(DATA, f"by_county_by_category_{tag}.csv"))


def rows_for(s, fourby, needle, out, page=50000):
    """Page through row-level records whose street_name matches `needle`."""
    where = (f"{BUS_LANE} and upper(street_name) like '%{needle.upper()}%'")
    offset, all_rows = 0, []
    while True:
        batch = soql(s, fourby, where=where, limit=str(page),
                     offset=str(offset), order="issue_date")
        all_rows.extend(batch)
        print(f"  +{len(batch)} (total {len(all_rows)})")
        if len(batch) < page:
            break
        offset += page
    fields = sorted({k for r in all_rows for k in r})
    write_csv(all_rows, out, fields)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fy", type=int, default=2026, choices=sorted(DATASETS))
    p.add_argument("--rows", help="street_name substring for a row-level pull")
    p.add_argument("--out", default="rows.csv")
    a = p.parse_args()

    fourby = DATASETS[a.fy]
    s = session()
    print(f"FY{a.fy} -> {fourby}")
    if a.rows:
        rows_for(s, fourby, a.rows, a.out)
    else:
        aggregates(s, fourby, a.fy)


if __name__ == "__main__":
    main()
