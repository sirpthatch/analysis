"""Turn raw TLC trip records into a tidy trip-speed table.

One row per usable trip, carrying the fields every research question needs:
when it happened, which zone it started and ended in, how fast it went, and
whether it paid the CBD toll.

    python src/speed.py --service yellow --start 2025-01 --end 2025-06
"""

import argparse

import pandas as pd

from constants import (
    CBD_BOUNDARY_ZONES,
    CBD_ZONES,
    CONTROL_ZONES_UPTOWN,
    MAX_TRIP_MILES,
    MAX_TRIP_MINUTES,
    MAX_TRIP_MPH,
    MIDTOWN_CORE_ZONES,
    MIDTOWN_EXTENDED_ZONES,
    MIN_TRIP_MILES,
    MIN_TRIP_MINUTES,
    MIN_TRIP_MPH,
    PROCESSED_DIR,
    UN_ZONES,
)
from tlc import month_range, trip_file

# Column names differ by service; normalise to pickup/dropoff/distance.
SERVICE_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance"),
    "fhvhv": ("pickup_datetime", "dropoff_datetime", "trip_miles"),
}


def load_month(service, year, month):
    """Read one monthly parquet into a normalised frame, or None if absent."""
    path = trip_file(service, year, month)
    if not path.exists():
        return None

    pickup_col, dropoff_col, distance_col = SERVICE_COLUMNS[service]
    columns = [pickup_col, dropoff_col, distance_col, "PULocationID", "DOLocationID"]

    frame = pd.read_parquet(path, columns=columns + _optional_columns(path, columns))
    return frame.rename(
        columns={
            pickup_col: "pickup",
            dropoff_col: "dropoff",
            distance_col: "miles",
            "PULocationID": "pu_zone",
            "DOLocationID": "do_zone",
        }
    )


def _optional_columns(path, required):
    """cbd_congestion_fee only exists from 2025; take it when the file has it."""
    import pyarrow.parquet as pq

    present = set(pq.ParquetFile(path).schema_arrow.names)
    return [
        c
        for c in ("cbd_congestion_fee", "congestion_surcharge")
        if c in present and c not in required
    ]


def add_speed(frame):
    """Attach trip duration and average speed, derived from the meter."""
    frame = frame.copy()
    frame["minutes"] = (frame["dropoff"] - frame["pickup"]).dt.total_seconds() / 60.0
    frame["mph"] = frame["miles"] / (frame["minutes"] / 60.0)
    return frame


DEFAULT_BOUNDS = {
    "miles": (MIN_TRIP_MILES, MAX_TRIP_MILES),
    "minutes": (MIN_TRIP_MINUTES, MAX_TRIP_MINUTES),
    "mph": (MIN_TRIP_MPH, MAX_TRIP_MPH),
}


def filter_plausible(frame, bounds=None):
    """Drop meter errors, idle vehicles, and highway/airport runs.

    Every bound is a judgement call that shapes the headline number, so
    `bounds` can be overridden to measure how much each one is worth.
    """
    bounds = bounds or DEFAULT_BOUNDS

    keep = pd.Series(True, index=frame.index)
    for column, (low, high) in bounds.items():
        keep &= frame[column].between(low, high)
    return frame[keep]


def drop_report(frame, bounds=None):
    """How many trips each bound removes, for the limitations section."""
    bounds = bounds or DEFAULT_BOUNDS

    rows = []
    for column, (low, high) in bounds.items():
        failed = ~frame[column].between(low, high)
        rows.append(
            {
                "bound": column,
                "range": f"{low}-{high}",
                "dropped": int(failed.sum()),
                "share": failed.mean(),
            }
        )
    return pd.DataFrame(rows)


# Geography flags, each meaning "this trip began and ended inside the area".
# Requiring both ends keeps the trip on that area's surface streets rather than
# letting it escape to the FDR or an airport.
GEOGRAPHIES = {
    "midtown_core": set(MIDTOWN_CORE_ZONES),
    "midtown_ext": set(MIDTOWN_EXTENDED_ZONES),
    "cbd": set(CBD_ZONES),
    "uptown": set(CONTROL_ZONES_UPTOWN),
    "cbd_boundary": set(CBD_BOUNDARY_ZONES),
    "un_core": set(UN_ZONES),
}


def tag_geography(frame):
    """Flag the areas a trip stayed wholly inside.

    Flags overlap by design - a midtown core trip is also a CBD trip - so each
    is its own boolean column rather than a single category.
    """
    frame = frame.copy()
    for name, zones in GEOGRAPHIES.items():
        inside = frame["pu_zone"].isin(zones) & frame["do_zone"].isin(zones)
        frame[name] = inside
    return frame


def add_time_keys(frame):
    """Calendar keys for the seasonal and event comparisons."""
    frame = frame.copy()
    pickup = frame["pickup"]
    frame["date"] = pickup.dt.date
    frame["year"] = pickup.dt.year
    frame["hour"] = pickup.dt.hour
    frame["dow"] = pickup.dt.dayofweek
    frame["iso_week"] = pickup.dt.isocalendar().week.astype("int16")
    return frame


def build_month(service, year, month):
    """Full pipeline for one month; returns None when the raw file is missing."""
    frame = load_month(service, year, month)
    if frame is None:
        return None

    frame = add_speed(frame)
    frame = filter_plausible(frame)
    frame = tag_geography(frame)
    frame = add_time_keys(frame)

    # Guard against stray rows stamped outside the file's own month.
    return frame[(frame["year"] == year) & (frame["pickup"].dt.month == month)]


def build(service, start, end, out_dir=None):
    """Write one processed parquet per month under data/processed/."""
    out_dir = out_dir or (PROCESSED_DIR / service)
    out_dir.mkdir(parents=True, exist_ok=True)

    written = []
    for year, month in month_range(start, end):
        frame = build_month(service, year, month)
        if frame is None:
            print(f"  skip {year:04d}-{month:02d}: no raw file")
            continue

        dest = out_dir / f"trips_{year:04d}-{month:02d}.parquet"
        frame.to_parquet(dest, index=False)
        print(
            f"  wrote {dest.name}: {len(frame):,} trips, mean {frame['mph'].mean():.2f} mph"
        )
        written.append(dest)

    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", default="yellow")
    parser.add_argument("--start", required=True, metavar="YYYY-MM")
    parser.add_argument("--end", required=True, metavar="YYYY-MM")
    args = parser.parse_args()

    build(args.service, args.start, args.end)


if __name__ == "__main__":
    main()
