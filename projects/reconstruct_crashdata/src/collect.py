"""Pull the source datasets to data/.

    python collect.py freshness   # how current each source is — run this first
    python collect.py all         # everything, in dependency order
    python collect.py socrata | trafficstat | centerline
    python collect.py all --refresh

Pulls are cached and skipped if already present, so an interrupted run can be
topped up by re-running. `--refresh` forces a re-fetch.
"""

import sys

import pandas as pd

import socrata
import trafficstat as ts
from constants import CENTERLINE, CRASHES, DATA_EXTERNAL, DATA_RAW, FREEZE_DATE


def freshness():
    """How far each source actually runs — the premise of the whole project."""
    r = socrata.query(CRASHES, select="count(*),max(crash_date)")[0]
    print("Socrata Motor Vehicle Collisions (h9gi-nx95)")
    print(f"  rows: {int(r['count']):,}   newest crash: {r['max_crash_date'][:10]}")
    print(f"  frozen since {FREEZE_DATE}" if r["max_crash_date"][:10] <= FREEZE_DATE
          else "  *** NO LONGER FROZEN — the gap may be closed, re-check the premise ***")
    print()
    print("NYPD TrafficStat")
    print(f"  data current through: {ts.as_of()}")
    print(f"  precincts available:  {len(ts.precincts())}")


def fetch_socrata(refresh=False):
    path = DATA_RAW / "socrata_crashes_2026.csv"
    if path.exists() and not refresh:
        print(f"Socrata crashes: cached ({path.name})")
        return pd.read_csv(path)
    print("Socrata crashes: fetching 2026 rows...")
    df = socrata.paged(CRASHES, where="crash_date >= '2026-01-01'")
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  {len(df):,} rows -> {path.name}")
    return df


def fetch_trafficstat(refresh=False, metric="Collisions"):
    path = DATA_RAW / "trafficstat_ytd_collisions.csv"
    if path.exists() and not refresh:
        print(f"TrafficStat: cached ({path.name})")
        return pd.read_csv(path)
    print("TrafficStat: pulling 79 precincts...")
    df = ts.all_incidents("YTD", metric, verbose=False)
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  {len(df):,} collisions -> {path.name}")
    return df


def fetch_centerline(refresh=False):
    path = DATA_EXTERNAL / "cscl_centerline.json"
    if path.exists() and not refresh:
        print(f"CSCL centerline: cached ({path.name})")
        return
    print("CSCL centerline: fetching...")
    df = socrata.paged(
        CENTERLINE,
        select="the_geom,physicalid,full_street_name,street_name,l_zip,r_zip,"
               "rw_type,borough_indicator,segment_type",
    )
    DATA_EXTERNAL.mkdir(parents=True, exist_ok=True)
    df.to_json(path, orient="records")
    print(f"  {len(df):,} segments -> {path.name}")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    refresh = "--refresh" in sys.argv
    cmd = args[0] if args else "freshness"

    if cmd == "freshness":
        freshness()
    elif cmd == "all":
        fetch_socrata(refresh)
        fetch_trafficstat(refresh)
        fetch_centerline(refresh)
    elif cmd == "socrata":
        fetch_socrata(refresh)
    elif cmd == "trafficstat":
        fetch_trafficstat(refresh)
    elif cmd == "centerline":
        fetch_centerline(refresh)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
