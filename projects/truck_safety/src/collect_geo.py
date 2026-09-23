"""Pull the location-level data research question 1 needs.

    python src/collect_geo.py trucks     # truck vehicle rows, 2016-present
    python src/collect_geo.py crashes    # ALL crash rows 2016-present, with lat/lon
    python src/collect_geo.py routes     # DOT truck route network (GeoJSON)
    python src/collect_geo.py all

Kept separate from `collect.py` because these are the big pulls: the crash
table is ~1.5M rows and is cached as parquet rather than CSV.  The full crash
table is pulled, not just the truck subset, because every question here is
comparative - "trucks crash off-route more often than what?" needs the what.
"""

import argparse
import json

import geopandas as gpd
import pandas as pd
import requests

import socrata
from constants import (
    BOROUGH_BOUNDARIES_ID,
    CRASHES_ID,
    EXTERNAL_FILES,
    RAW_FILES,
    SOCRATA_DOMAIN,
    START,
    TRUCK_ROUTES_ID,
    VEHICLES_ID,
)
from vehicle_codes import TRUCK_ALL, soql_in

CRASH_FIELDS = (
    "collision_id,crash_date,crash_time,borough,zip_code,latitude,longitude,"
    "on_street_name,number_of_persons_injured,number_of_persons_killed,"
    "number_of_cyclist_injured,number_of_cyclist_killed,"
    "number_of_pedestrians_injured,number_of_pedestrians_killed"
)

VEHICLE_FIELDS = (
    "unique_id,collision_id,crash_date,vehicle_type,vehicle_make,"
    "state_registration,pre_crash,point_of_impact,vehicle_damage"
)


def _cached_parquet(path, loader, refresh=False):
    if path.exists() and path.stat().st_size > 0 and not refresh:
        print(f"  have {path.name}")
        return pd.read_parquet(path)
    frame = loader()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    print(f"  got  {path.name} ({len(frame):,} rows)")
    return frame


def trucks(refresh=False):
    """Every vehicle row whose `vehicle_type` is on the curated truck list.

    Both tiers are pulled together and kept distinguishable by `vehicle_type`,
    so the headline can be run on the core list and the light-commercial tier
    reported as a sensitivity rather than re-fetched.
    """
    def loader():
        return socrata.fetch_all(
            VEHICLES_ID,
            select=VEHICLE_FIELDS,
            where=f"upper(vehicle_type) in ({soql_in(TRUCK_ALL)}) and crash_date>='{START}'",
            order="unique_id",
        )

    return _cached_parquet(RAW_FILES["truck_vehicles"], loader, refresh)


def crashes(refresh=False):
    """Every crash row since 2016, truck-involved or not.

    This is the denominator for every rate in the geography work: the share of
    truck crashes that happen off the truck network only means something next
    to the share of all crashes that do.
    """
    def loader():
        return socrata.fetch_all(
            CRASHES_ID,
            select=CRASH_FIELDS,
            where=f"crash_date>='{START}'",
            order="collision_id",
        )

    return _cached_parquet(RAW_FILES["all_crashes"], loader, refresh)


def routes(refresh=False):
    """The DOT truck route network, as GeoJSON.

    `jjja-shxy` carries a `routetype` distinguishing Through routes (long-haul
    movements crossing the city) from Local routes (access to a destination off
    the through network).  Both are part of the legal network; the distinction
    matters for interpretation, so it is preserved rather than dissolved.
    """
    return _geojson(TRUCK_ROUTES_ID, EXTERNAL_FILES["truck_routes"], refresh)


def _geojson(dataset_id, path, refresh=False):
    """Download a Socrata geospatial export to `path` and open it."""
    if path.exists() and path.stat().st_size > 0 and not refresh:
        print(f"  have {path.name}")
        return gpd.read_file(path)

    url = (f"https://{SOCRATA_DOMAIN}/api/geospatial/{dataset_id}"
           "?method=export&format=GeoJSON")
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(response.json()))
    frame = gpd.read_file(path)
    print(f"  got  {path.name} ({len(frame):,} features)")
    return frame


def boroughs(refresh=False):
    """Borough outlines, for map context only -- nothing is computed from them."""
    return _geojson(BOROUGH_BOUNDARIES_ID, EXTERNAL_FILES["boroughs"], refresh)


STEPS = {"trucks": trucks, "crashes": crashes, "routes": routes,
         "boroughs": boroughs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=list(STEPS) + ["all"])
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    for name in (list(STEPS) if args.step == "all" else [args.step]):
        print(f"\n=== {name} ===")
        STEPS[name](args.refresh)


if __name__ == "__main__":
    main()
