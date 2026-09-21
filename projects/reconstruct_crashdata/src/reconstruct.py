"""Build a Socrata-shaped crash table for the period the public file is missing.

The public crash file (`h9gi-nx95`) stops at 2026-06-11. NYPD's own TrafficStat
dashboard has kept publishing. This assembles the gap period from TrafficStat and
CSCL into a table shaped like Socrata's, so downstream work can treat the two
alike — with the honest caveat that several Socrata columns cannot be recovered
at all and are emitted as null rather than guessed.

    python reconstruct.py metrics    # pull each attribute metric (slow, cached)
    python reconstruct.py build      # assemble the gap table
    python reconstruct.py validate   # agreement against Socrata on the overlap

How the attributes work: TrafficStat's map report returns one point set per
*metric*, not per-record attributes. So "was anyone injured" is recovered by
pulling the "Injury Collisions" metric separately and joining its points back
onto the base collision set. The join key is (latitude, longitude, timestamp),
which measured 99.8% unique and matched 100% of injury and pedestrian subsets.
"""

import sys

import pandas as pd

import trafficstat as ts
from constants import DATA_PROCESSED, DATA_RAW, FREEZE_DATE

# TrafficStat metric -> the column it becomes.
#
# Two things about these that are not obvious from the metric names, both
# verified by set-containment against the base and injury sets:
#
# 1. Every flag is boolean, never a count. TrafficStat returns one row per
#    collision, so "Total Injuries" cannot yield Socrata's
#    number_of_persons_injured (measured ratio 1.00 against Socrata's true 1.35).
#
# 2. The mode metrics are subsets of INJURY collisions, not of all collisions.
#    "Col Pedestrian" is 100% contained in "Injury Collisions" — it means a
#    pedestrian was *hurt*, not that one was involved. Naming these
#    `pedestrian_involved` would misstate them by a wide margin: 5,675 injured
#    pedestrians against 57,373 collisions. Together the modes cover 94% of
#    injury collisions; the remainder are modes not pulled here (Other MV,
#    Stand-up Scooter, Off Road, Other Device).
#
# This makes them the closest available analogue to Socrata's
# number_of_pedestrians_injured / number_of_cyclist_injured / etc. — the same
# structure, as flags rather than counts.
ATTRIBUTE_METRICS = {
    "Injury Collisions": "injury",
    "Total Fatalities": "fatality",
    "Col Pedestrian": "injured_pedestrian",
    "Col Traditional Bicycle": "injured_cyclist",
    "Col E-bike": "injured_ebike_rider",
    "Col Motorcycle": "injured_motorcyclist",
    "Col Moped": "injured_moped_rider",
    "Col Car": "injured_car_occupant",
    "Col SUV": "injured_suv_occupant",
}

KEY = ["latitude", "longitude", "timestamp"]

# NYPD precinct number -> borough. Precincts nest inside boroughs by design, so
# this is exact, not an approximation. The 116th is Queens' newest precinct.
PRECINCT_BOROUGH = [
    (1, 34, "MANHATTAN"),
    (40, 52, "BRONX"),
    (60, 94, "BROOKLYN"),
    (100, 116, "QUEENS"),
    (120, 123, "STATEN ISLAND"),
]


def borough_for(precinct):
    """Borough from precinct number, matching Socrata's `borough` values."""
    try:
        n = int(precinct)
    except (TypeError, ValueError):
        return pd.NA
    for lo, hi, name in PRECINCT_BOROUGH:
        if lo <= n <= hi:
            return name
    return pd.NA


def _metric_path(metric):
    slug = metric.lower().replace(" ", "_").replace("-", "")
    return DATA_RAW / f"trafficstat_ytd_{slug}.csv"


def pull_metrics(metrics=None, refresh=False, pause=0.15):
    """Pull each attribute metric citywide, caching one CSV per metric.

    This is ~79 requests per metric, so it is slow and deliberately resumable:
    a metric already on disk is skipped unless `refresh` is set.
    """
    for metric in metrics or ATTRIBUTE_METRICS:
        path = _metric_path(metric)
        if path.exists() and not refresh:
            print(f"  {metric:<26} cached")
            continue
        print(f"  {metric:<26} pulling...", flush=True)
        df = ts.all_incidents("YTD", metric, pause=pause, verbose=False)
        df.to_csv(path, index=False)
        print(f"  {metric:<26} {len(df):>6,} rows -> {path.name}", flush=True)


def load_base():
    """The base collision set, restricted to the gap period."""
    df = pd.read_csv(DATA_RAW / "trafficstat_ytd_collisions.csv")
    df["crash_datetime"] = pd.to_datetime(df.timestamp, format="%m/%d/%y %I%p", errors="coerce")
    return df[df.crash_datetime > FREEZE_DATE].reset_index(drop=True)


def attach_attributes(base, metrics=None):
    """Join each metric's point set onto the base set as a boolean column."""
    out = base.copy()
    for metric, col in (metrics or ATTRIBUTE_METRICS).items():
        path = _metric_path(metric)
        if not path.exists():
            print(f"  WARNING {metric} not pulled; {col} will be null")
            out[col] = pd.NA
            continue
        m = pd.read_csv(path)
        flagged = set(map(tuple, m[KEY].round({"latitude": 7, "longitude": 7}).values))
        out[col] = [
            tuple(r) in flagged
            for r in out[KEY].round({"latitude": 7, "longitude": 7}).values
        ]
    return out


def build(geocode=True):
    """Assemble the gap table: TrafficStat base + attributes + CSCL geography."""
    df = attach_attributes(load_base())

    if geocode:
        import reverse_geocode as rg
        df = df.join(rg.reverse_geocode(df, rg.load_centerline()))

    # Shape it like Socrata's, so the two can be concatenated. Columns Socrata
    # has that cannot be recovered are emitted as null rather than invented:
    # contributing factors, per-record injury/fatality counts, and collision_id.
    out = pd.DataFrame({
        "crash_date": df.crash_datetime.dt.date,
        "crash_time": df.crash_datetime.dt.strftime("%H:%M"),
        "latitude": df.latitude,
        "longitude": df.longitude,
        "on_street_name": df.get("street_name_geocoded"),
        "zip_code": df.get("zip_geocoded"),
        "precinct": df.precinct,
        "collision_type": df.category,
        "roadway_type": df.get("roadway_type"),
        "geocode_dist_feet": df.get("geocode_dist_feet"),
        # False throughout the gap period: TrafficStat stopped populating the
        # hour in June 2026. Kept as a column so the absence is explicit rather
        # than implied by every value reading midnight.
        "time_is_known": df.crash_datetime.dt.hour != 0,
        "borough": df.precinct.map(borough_for),
        # Socrata's `location` is just the coordinate pair as a tuple string.
        "location": [
            f"({la}, {lo})" if pd.notna(la) else pd.NA
            for la, lo in zip(df.latitude, df.longitude)
        ],
        "source": "trafficstat",
    })
    for col in ATTRIBUTE_METRICS.values():
        out[col] = df[col]
    # Socrata columns with no recoverable analogue. Emitted so the shapes match,
    # null so nothing is silently invented.
    for col in ["number_of_persons_injured", "number_of_persons_killed",
                "contributing_factor_vehicle_1", "collision_id",
                "cross_street_name", "off_street_name", "vehicle_type_code1"]:
        out[col] = pd.NA

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    path = DATA_PROCESSED / "reconstructed_crashes_gap.csv"
    out.to_csv(path, index=False)
    print(f"{len(out):,} reconstructed crashes -> {path.name}")
    print(f"  period: {out.crash_date.min()} .. {out.crash_date.max()}")
    print(f"  street assigned: {out.on_street_name.notna().mean():.1%}")
    print(f"  zip assigned:    {out.zip_code.notna().mean():.1%}")
    print(f"  real time known: {out.time_is_known.mean():.1%}")
    return out


def validate():
    """Agreement between the two sources over the months both cover."""
    so = pd.read_csv(DATA_RAW / "socrata_crashes_2026.csv")
    so["dt"] = pd.to_datetime(so.crash_date)
    tsdf = pd.read_csv(DATA_RAW / "trafficstat_ytd_collisions.csv")
    tsdf["dt"] = pd.to_datetime(tsdf.timestamp, format="%m/%d/%y %I%p", errors="coerce")

    cut = pd.Timestamp(FREEZE_DATE)
    overlap = tsdf[tsdf.dt <= cut]
    print(f"Overlap 2026-01-01 .. {FREEZE_DATE} (both sources live)")
    print(f"  Socrata:     {len(so):>7,}")
    print(f"  TrafficStat: {len(overlap):>7,}")
    print(f"  ratio:       {len(overlap)/len(so):>7.3f}")

    a = so.groupby(so.dt.dt.to_period("M")).size()
    b = tsdf.groupby(tsdf.dt.dt.to_period("M")).size()
    cmp = pd.DataFrame({"socrata": a, "trafficstat": b})
    cmp["ratio"] = (cmp.trafficstat / cmp.socrata).round(3)
    print()
    print(cmp.to_string())
    return cmp


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "metrics":
        pull_metrics(refresh="--refresh" in sys.argv)
    elif cmd == "build":
        build()
    elif cmd == "validate":
        validate()
    else:
        print(__doc__)
        sys.exit(1)
