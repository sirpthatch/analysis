"""Subway origin-destination matrix — the input for distance-based fare schemes.

`28vm-gjqr` (2026) and `jsu2-fbtj` (2024) estimate trips per O-D pair by month,
day of week and hour, "averaged by day of week over the calendar month". Summing
`estimated_average_ridership` over all seven days of the week and all hours
therefore gives one TYPICAL WEEK of trips for that month. That is the unit this
module returns.

The estimates are inferred from return swipes and scaled up to total
ridership; destinations are modelled, not observed. Good enough to price a
distance-based fare in aggregate, not to say anything about an individual trip.

Each day-of-week is pulled separately and paged ($order is required for stable
paging); ~22s per 50k-row page on 2026-09-28.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from . import collect

OD_DATASETS = {2024: "od_2024", 2026: "od_2026"}
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
PAGE = 50_000


def _pull_dow(year: int, month: int, dow: str, refresh: bool) -> list[dict]:
    path = collect.DATA_RAW / "od" / f"{year}-{month:02d}-{dow}.json"
    if path.exists() and not refresh:
        return json.loads(path.read_text())
    rows, offset = [], 0
    while True:
        page = collect.soql(OD_DATASETS[year], {
            "$select": "origin_station_complex_id AS o,destination_station_complex_id AS d,"
                       "sum(estimated_average_ridership) AS r",
            "$where": f"month={month} AND day_of_week='{dow}'",
            "$group": "o,d",
            "$order": "o,d",
            "$limit": str(PAGE),
            "$offset": str(offset),
        })
        rows += page
        if len(page) < PAGE:
            break
        offset += PAGE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows))
    return rows


def pair_week(year: int = 2026, month: int = 4, *, refresh: bool = False) -> pd.DataFrame:
    """Trips per O-D pair in a typical week of (year, month)."""
    with ThreadPoolExecutor(max_workers=3) as ex:
        parts = list(ex.map(lambda d: _pull_dow(year, month, d, refresh), DAYS))
    df = pd.DataFrame([r for p in parts for r in p])
    df["r"] = df["r"].astype(float)
    return (df.groupby(["o", "d"], as_index=False)["r"].sum()
              .rename(columns={"o": "origin_id", "d": "dest_id", "r": "weekly_trips"}))


def stations(year: int = 2026) -> pd.DataFrame:
    """Station complex id, name, borough and lat/long.

    Taken from one weekday hour of the HOURLY ridership dataset for that year and
    deduplicated client-side. The equivalent $group over the O-D dataset (10M
    rows/month) timed out on 2026-09-28. Complex ids are shared between the two.
    """
    path = collect.DATA_RAW / "od" / f"stations_{year}.json"
    if path.exists():
        rows = json.loads(path.read_text())
    else:
        key = "subway_2025" if year >= 2025 else "subway_2020_2024"
        rows = collect.soql(key, {
            "$select": "station_complex_id AS id,station_complex AS name,borough,"
                       "latitude AS lat,longitude AS lon",
            "$where": f"transit_timestamp >= '{year}-04-01T08:00:00' AND "
                      f"transit_timestamp < '{year}-04-01T09:00:00'",
            "$limit": "50000",
        })
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows))
    df = pd.DataFrame(rows).drop_duplicates("id")
    df[["lat", "lon"]] = df[["lat", "lon"]].astype(float)
    return df


def with_distance(pairs: pd.DataFrame, st: pd.DataFrame) -> pd.DataFrame:
    """Add straight-line origin->destination distance in miles (haversine).

    Straight-line, not track distance: it understates trips on indirect routes.
    For a fare-scheme sketch that is the honest simple choice; a zone scheme
    built on it inherits the same bias everywhere, so relative effects hold.
    """
    s = st.set_index("id")[["lat", "lon"]]
    o = s.reindex(pairs["origin_id"]).to_numpy()
    d = s.reindex(pairs["dest_id"]).to_numpy()
    lat1, lon1, lat2, lon2 = map(np.radians, (o[:, 0], o[:, 1], d[:, 0], d[:, 1]))
    a = (np.sin((lat2 - lat1) / 2) ** 2
         + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    out = pairs.copy()
    out["miles"] = 3958.8 * 2 * np.arcsin(np.sqrt(a))
    return out
