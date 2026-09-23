#!/usr/bin/env python3
"""
Starter analysis: NYC refuse-truck crashes across the 2023 side-guard deadline.

THIS SCRIPT HAS NOT BEEN RUN. It is a scaffold. No results are baked in anywhere
in this bundle except the two CSVs under data/, which came from direct SoQL
queries on 2026-09-22 and are documented in VERIFIED-DATA.md.

Usage:
    pip install requests pandas
    export SOCRATA_APP_TOKEN=...        # optional but raises the rate limit
    python analysis.py

Datasets:
    h9gi-nx95   Motor Vehicle Collisions - Crashes
    bm4k-52h4   Motor Vehicle Collisions - Vehicles
Join key: collision_id
"""

import os
import sys
import time

import pandas as pd
import requests

BASE = "https://data.cityofnewyork.us/resource"
CRASHES = "h9gi-nx95"
VEHICLES = "bm4k-52h4"

# Data begins usefully in 2016; 2014-2015 hold 1 record each (schema artifact).
START = "2016-01-01"

# Confirmed 2026-09-22: the crash table's most recent record is 2026-06-11.
# Re-check this on every run -- the reporting lag is the headline caveat.
KNOWN_MAX_CRASH_DATE = "2026-06-11"

APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from heavy_vehicle_codes import REFUSE, HEAVY, soql_in  # noqa: E402


def get(dataset, params, retries=3):
    """One SoQL request, with a single polite retry on 429."""
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    url = f"{BASE}/{dataset}.json"
    for attempt in range(retries):
        r = requests.get(url, params=params, headers=headers, timeout=120)
        if r.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"  429 rate limited, waiting {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"rate limited after {retries} attempts: {params}")


def get_all(dataset, select=None, where=None, order=None, page=50000):
    """Paged fetch. An explicit $order is required or paging repeats rows."""
    if order is None:
        order = ":id"
    rows, offset = [], 0
    while True:
        params = {"$limit": page, "$offset": offset, "$order": order}
        if select:
            params["$select"] = select
        if where:
            params["$where"] = where
        batch = get(dataset, params)
        rows.extend(batch)
        print(f"  {dataset}: {len(rows)} rows", file=sys.stderr)
        if len(batch) < page:
            return rows
        offset += page


def check_extent():
    """Step 0. Always establish the data extent before analysing it."""
    row = get(CRASHES, {"$select": "max(crash_date),min(crash_date),count(*)"})[0]
    print("\n=== Crash table extent ===")
    print(f"  rows: {row['count']}")
    print(f"  range: {row['min_crash_date'][:10]} to {row['max_crash_date'][:10]}")
    if row["max_crash_date"][:10] != KNOWN_MAX_CRASH_DATE:
        print(f"  NOTE: max date moved from the verified {KNOWN_MAX_CRASH_DATE}.")
    print("  The reporting lag here is a limitation to state in the post.")
    return row


def citywide_denominator():
    """
    Step 1, BLOCKING. Total crashes per year.

    The flat refuse-truck series means nothing without this. See NEXT-STEPS.md
    for the three outcomes and what each one implies for the story.
    """
    rows = get(CRASHES, {
        "$select": "date_trunc_y(crash_date) as yr,count(*) as n",
        "$group": "yr",
        "$order": "yr",
    })
    df = pd.DataFrame(rows)
    df["yr"] = df["yr"].str[:4].astype(int)
    df["n"] = df["n"].astype(int)
    return df.rename(columns={"n": "all_crashes"})


def heavy_by_year(codes):
    """Vehicle records per year for a given set of vehicle_type codes."""
    rows = get(VEHICLES, {
        "$select": "upper(vehicle_type) as vt,date_trunc_y(crash_date) as yr,"
                   "count(*) as n",
        "$where": f"upper(vehicle_type) in ({soql_in(codes)})",
        "$group": "vt,yr",
        "$order": "vt,yr",
    })
    df = pd.DataFrame(rows)
    df["yr"] = df["yr"].str[:4].astype(int)
    df["n"] = df["n"].astype(int)
    return df


def impact_vocabulary(code="GARBAGE OR REFUSE"):
    """
    Step 3. Enumerate the ACTUAL values before writing any side-impact filter.
    Do not guess these strings -- that is how you get a confident zero.
    """
    out = {}
    for col in ("point_of_impact", "pre_crash", "vehicle_damage"):
        rows = get(VEHICLES, {
            "$select": f"upper({col}) as v,count(*) as n",
            "$where": f"upper(vehicle_type)='{code}'",
            "$group": "v",
            "$order": "n desc",
            "$limit": 50,
        })
        out[col] = pd.DataFrame(rows)
    return out


def refuse_crashes_joined():
    """
    Step 5. Pull refuse-truck vehicle records and the crashes they belong to,
    then join locally -- Socrata has no cross-dataset join.
    """
    veh = pd.DataFrame(get_all(
        VEHICLES,
        select="unique_id,collision_id,crash_date,vehicle_type,vehicle_make,"
               "vehicle_year,state_registration,pre_crash,point_of_impact,"
               "vehicle_damage,contributing_factor_1",
        where=f"upper(vehicle_type) in ({soql_in(REFUSE)}) "
              f"and crash_date>='{START}'",
        order="unique_id",
    ))
    if veh.empty:
        raise SystemExit(
            "Zero rows. Before concluding the data isn't there: re-check the "
            "casing (upper() on BOTH sides) and the code spelling against "
            "data/vehicle_type_counts_2025plus.csv."
        )

    ids = sorted(veh["collision_id"].dropna().unique())
    print(f"\n  {len(veh)} refuse vehicle records, {len(ids)} distinct crashes",
          file=sys.stderr)

    # Page the crash lookup through in(...) clauses.
    chunks = []
    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]
        quoted = ",".join(f"'{x}'" for x in batch)
        chunks.extend(get(CRASHES, {
            "$select": "collision_id,crash_date,borough,zip_code,latitude,"
                       "longitude,on_street_name,number_of_cyclist_injured,"
                       "number_of_cyclist_killed,number_of_pedestrians_injured,"
                       "number_of_pedestrians_killed",
            "$where": f"collision_id in ({quoted})",
            "$limit": 50000,
        }))
    crash = pd.DataFrame(chunks)

    df = veh.merge(crash, on="collision_id", how="left",
                   suffixes=("_veh", "_crash"))
    for c in ("number_of_cyclist_injured", "number_of_cyclist_killed",
              "number_of_pedestrians_injured", "number_of_pedestrians_killed"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df["year"] = df["crash_date_veh"].str[:4].astype(int)
    return df


def main():
    check_extent()

    print("\n=== Step 1: citywide denominator (BLOCKING) ===")
    cw = citywide_denominator()
    print(cw.to_string(index=False))
    cw.to_csv("citywide_crashes_by_year.csv", index=False)

    print("\n=== Step 4: heavy vehicles by year ===")
    hv = heavy_by_year(HEAVY)
    print(hv.pivot(index="yr", columns="vt", values="n").fillna(0).to_string())
    hv.to_csv("heavy_vehicles_by_year.csv", index=False)

    print("\n=== Step 3: impact vocabulary (inspect before filtering) ===")
    for col, tbl in impact_vocabulary().items():
        print(f"\n-- {col} --")
        print(tbl.head(20).to_string(index=False))

    print("\n=== Step 5: refuse crashes, joined ===")
    df = refuse_crashes_joined()
    df.to_csv("refuse_crashes_joined.csv", index=False)

    harm = df.groupby("year").agg(
        crashes=("collision_id", "nunique"),
        cyclist_injured=("number_of_cyclist_injured", "sum"),
        cyclist_killed=("number_of_cyclist_killed", "sum"),
        ped_injured=("number_of_pedestrians_injured", "sum"),
        ped_killed=("number_of_pedestrians_killed", "sum"),
    ).reset_index()
    harm = harm.merge(cw, left_on="year", right_on="yr", how="left")
    harm["per_10k_crashes"] = (
        harm["crashes"] / harm["all_crashes"] * 10000).round(2)
    print(harm.drop(columns=["yr"]).to_string(index=False))
    harm.to_csv("refuse_harm_by_year.csv", index=False)

    print("""
NOT DONE YET -- see NEXT-STEPS.md:
  - Step 2, the hard one: separate DSNY municipal trucks from private carters.
    The side-guard rule only covers licensed trade waste vehicles. Until this is
    resolved the waiver framing does not cleanly attach to these numbers.
  - The side-impact narrowing, once the impact vocabulary above is known.
  - Confirm the 2016 series break is a schema change and not a reporting change.
""")


if __name__ == "__main__":
    main()
