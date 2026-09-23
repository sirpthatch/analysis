"""Pull the collision data the side-guard question needs.

    python src/collect.py extent        # what the file covers - run this first
    python src/collect.py denominator   # citywide crashes per year (BLOCKING)
    python src/collect.py types         # vehicle_type vocabulary, by year
    python src/collect.py heavy         # heavy-vehicle records per year
    python src/collect.py impact        # point_of_impact / pre_crash vocabulary
    python src/collect.py refuse        # refuse vehicle rows + their crash rows
    python src/collect.py all

Pulls are cached as CSV under `data/raw/` and skipped if already present; pass
`--refresh` to re-fetch.  Nothing here narrows to side impacts: the value
vocabulary has to be enumerated (`impact`) before any such filter is written.
"""

import argparse

import pandas as pd

import socrata
from constants import (
    CRASHES_ID,
    RAW_FILES,
    START,
    VEHICLES_ID,
)
from vehicle_codes import HEAVY, REFUSE, soql_in

CRASH_FIELDS = (
    "collision_id,crash_date,crash_time,borough,zip_code,latitude,longitude,"
    "on_street_name,off_street_name,cross_street_name,"
    "number_of_persons_injured,number_of_persons_killed,"
    "number_of_cyclist_injured,number_of_cyclist_killed,"
    "number_of_pedestrians_injured,number_of_pedestrians_killed"
)

VEHICLE_FIELDS = (
    "unique_id,collision_id,crash_date,crash_time,vehicle_type,vehicle_make,"
    "vehicle_model,vehicle_year,state_registration,travel_direction,"
    "pre_crash,point_of_impact,vehicle_damage,contributing_factor_1"
)


def extent():
    """Step 0.  Establish what the data covers before analysing any of it.

    The maximum crash date is the single most important number here: the
    public file froze on 2026-06-11, which is why this project leans on the
    companion reconstruction for anything after that.
    """
    for dataset_id in (CRASHES_ID, VEHICLES_ID):
        meta = socrata.metadata(dataset_id)
        row = socrata.query(
            dataset_id, select="min(crash_date),max(crash_date),count(*)"
        ).iloc[0]
        print(f"\n{meta['name']} ({dataset_id})")
        print(f"  catalog updated  {meta['rows_updated_at']}")
        print(f"  rows             {int(row['count']):,}")
        print(
            f"  crash_date range {row['min_crash_date'][:10]} "
            f"to {row['max_crash_date'][:10]}"
        )


def denominator(refresh=False):
    """Step 1, BLOCKING.  Citywide crashes per year.

    The flat refuse-truck series means nothing without this.  If citywide
    crashes fell while refuse held flat, refuse trucks became relatively more
    dangerous; if both fell in step, there is no story.
    """

    def loader():
        frame = socrata.query(
            CRASHES_ID,
            select="date_trunc_y(crash_date) as yr,count(*) as n",
            group="yr",
            order="yr",
        )
        frame["yr"] = frame["yr"].str[:4].astype(int)
        return frame.rename(columns={"n": "all_crashes"})

    return socrata.cached_pull(RAW_FILES["citywide_crashes_by_year"], loader, refresh)


def vehicle_types(refresh=False):
    """The full `vehicle_type` vocabulary by year.

    `vehicle_type` is filthy - junk codes, duplicate concepts, and tens of
    thousands of blanks.  Pulling it by year also shows whether a category
    appeared or vanished mid-series, which would look like a trend.
    """

    def loader():
        return socrata.query(
            VEHICLES_ID,
            select="upper(vehicle_type) as vt,date_trunc_y(crash_date) as yr,count(*) as n",
            where=f"crash_date>='{START}'",
            group="vt,yr",
            order="n desc",
            limit=50_000,
        )

    return socrata.cached_pull(RAW_FILES["vehicle_types_by_year"], loader, refresh)


def heavy_by_year(refresh=False):
    """Heavy-vehicle records per year, refuse plus the rough control group."""

    def loader():
        return socrata.query(
            VEHICLES_ID,
            select="upper(vehicle_type) as vt,date_trunc_y(crash_date) as yr,count(*) as n",
            where=f"upper(vehicle_type) in ({soql_in(HEAVY)}) and crash_date>='{START}'",
            group="vt,yr",
            order="vt,yr",
        )

    return socrata.cached_pull(RAW_FILES["heavy_vehicles_by_year"], loader, refresh)


def impact_vocabulary(refresh=False):
    """Step 3.  Enumerate the ACTUAL values before writing a side-impact filter.

    Do not guess these strings - guessing is how you get a confident zero.
    """

    def loader():
        frames = []
        for column in ("point_of_impact", "pre_crash", "vehicle_damage"):
            frame = socrata.query(
                VEHICLES_ID,
                select=f"upper({column}) as value,count(*) as n",
                where=f"upper(vehicle_type) in ({soql_in(REFUSE)})",
                group="value",
                order="n desc",
                limit=200,
            )
            frame.insert(0, "column", column)
            frames.append(frame)
        return pd.concat(frames, ignore_index=True)

    return socrata.cached_pull(RAW_FILES["impact_vocabulary"], loader, refresh)


def refuse(refresh=False):
    """Step 5.  Refuse-truck vehicle rows, and the crash rows they belong to."""

    def vehicle_loader():
        frame = socrata.fetch_all(
            VEHICLES_ID,
            select=VEHICLE_FIELDS,
            where=f"upper(vehicle_type) in ({soql_in(REFUSE)}) and crash_date>='{START}'",
            order="unique_id",
        )
        if frame.empty:
            raise SystemExit(
                "Zero rows.  Before concluding the data isn't there: re-check "
                "the casing (upper() on BOTH sides) and the code spelling "
                "against data/raw/vehicle_types_by_year.csv."
            )
        return frame

    vehicles = socrata.cached_pull(
        RAW_FILES["refuse_vehicles"], vehicle_loader, refresh
    )

    def crash_loader():
        ids = sorted(vehicles["collision_id"].dropna().unique())
        print(f"  {len(vehicles):,} refuse vehicle rows, {len(ids):,} distinct crashes")
        return socrata.fetch_in_chunks(
            CRASHES_ID, "collision_id", ids, select=CRASH_FIELDS
        )

    crashes = socrata.cached_pull(RAW_FILES["refuse_crashes"], crash_loader, refresh)
    return vehicles, crashes


STEPS = {
    "extent": lambda refresh: extent(),
    "denominator": denominator,
    "types": vehicle_types,
    "heavy": heavy_by_year,
    "impact": impact_vocabulary,
    "refuse": refuse,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=list(STEPS) + ["all"])
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    steps = list(STEPS) if args.step == "all" else [args.step]
    for name in steps:
        print(f"\n=== {name} ===")
        STEPS[name](args.refresh)


if __name__ == "__main__":
    main()
