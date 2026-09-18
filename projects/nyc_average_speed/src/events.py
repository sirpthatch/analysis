"""Permitted street events in Manhattan, as a proxy for road closures.

Source: NYC Permitted Event Information - Historical (Socrata bkfu-528j), the
Citywide Event Coordination permit feed.  It covers parades, street festivals,
film shoots, and demonstrations, and carries `street_closure_type` and
`police_precinct` - enough to say "this precinct lost road capacity that day".

There is no public NYPD feed for security closures (UNGA perimeters, motorcade
routes), so this understates event-driven closure.  See research/limitations.md.

    python src/events.py --start 2019-01-01
"""

import argparse

import pandas as pd
import requests

from constants import (
    EVENTS_FILE,
    EVENTS_URL,
    MIDTOWN_PRECINCTS,
    ROAD_CLOSURE_TYPES,
)

# Permit categories that can take street capacity.  The bulk of the feed is
# park sports permits, which have nothing to do with traffic.
ROAD_EVENT_TYPES = (
    "Street Event",
    "Production Event",
    "Parade",
    "Street Festival",
    "Single Block Festival",
    "Block Party",
    "Sidewalk Sale",
    "Theater Load in and Load Outs",
    "Plaza Event",
    "Plaza Partner Event",
    "Farmers Market",
    "Special Event",
)

PAGE_SIZE = 50000


def _quote_list(values):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def fetch(start, borough="Manhattan"):
    """Page through Socrata for road-affecting permits since `start`."""
    where = (
        f"event_borough='{borough}' AND start_date_time >= '{start}T00:00:00' "
        f"AND event_type in ({_quote_list(ROAD_EVENT_TYPES)})"
    )

    pages = []
    offset = 0
    while True:
        response = requests.get(
            EVENTS_URL,
            params={"$where": where, "$limit": PAGE_SIZE, "$offset": offset,
                    "$order": "start_date_time"},
            timeout=180,
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break

        pages.append(pd.DataFrame(page))
        offset += PAGE_SIZE
        print(f"  fetched {offset:,}...")

    return pd.concat(pages, ignore_index=True)


def _parse_precincts(value):
    """police_precinct arrives as a trailing-comma string like '14, 18,'."""
    if not isinstance(value, str):
        return []
    return [int(p) for p in value.replace(" ", "").split(",") if p.isdigit()]


def add_features(frame):
    """Normalise types and flag the midtown, capacity-taking subset."""
    frame = frame.copy()
    for column in ("start_date_time", "end_date_time"):
        frame[column] = pd.to_datetime(frame[column], errors="coerce")

    frame["precincts"] = frame["police_precinct"].map(_parse_precincts)
    frame["is_midtown"] = frame["precincts"].map(
        lambda ps: any(p in MIDTOWN_PRECINCTS for p in ps)
    )
    frame["takes_road"] = frame["street_closure_type"].isin(ROAD_CLOSURE_TYPES)
    frame["full_closure"] = frame["street_closure_type"] == "Full Street Closure"

    frame["date"] = frame["start_date_time"].dt.date
    return frame


def daily_midtown_closures(frame):
    """Collapse to one row per date: how much midtown street was spoken for."""
    midtown = frame[frame["is_midtown"]]
    return (
        midtown.groupby("date")
        .agg(
            events=("event_id", "size"),
            road_events=("takes_road", "sum"),
            full_closures=("full_closure", "sum"),
        )
        .reset_index()
    )


def build(start, dest=None):
    dest = dest or EVENTS_FILE
    dest.parent.mkdir(parents=True, exist_ok=True)

    frame = add_features(fetch(start))
    # `precincts` is a list column; store the parsed flags and drop it.
    frame.drop(columns=["precincts"]).to_parquet(dest, index=False)

    print(
        f"  wrote {dest.name}: {len(frame):,} permits, "
        f"{frame['is_midtown'].sum():,} midtown, "
        f"{(frame['is_midtown'] & frame['takes_road']).sum():,} taking road"
    )
    return dest


def load(dest=None):
    return pd.read_parquet(dest or EVENTS_FILE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2019-01-01", metavar="YYYY-MM-DD")
    args = parser.parse_args()

    build(args.start)


if __name__ == "__main__":
    main()
