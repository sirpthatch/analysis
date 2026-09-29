"""Daily fare-class ridership, 2023 onward — the year-round view.

The headline comparison rests on two sixteen-day September windows. This module
pulls every day so the composition can be checked across the whole year
(additional_questions.md Q1), the student series can be lined up against the
school calendar (Q2), and ridership can be priced week by week (Q3).

Why one query per day: a month grouped by date_trunc_ymd(transit_timestamp) took
~5 minutes server-side on 2026-09-28; one day grouped by fare class alone takes
about a second. Each day is cached separately under data/raw/daily/ so an
interrupted pull resumes where it stopped.
"""

from __future__ import annotations

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from . import collect

START = "2023-01-01"
# Boundary between the two hourly datasets (same seam for subway and bus).
SEAM = dt.date(2025, 1, 1)

# Express bus routes carry the express fare ($7.00 -> $7.25), so they are pulled
# as their own series. Prefixes checked against the live route list 2026-09-28.
EXPRESS_BUS_WHERE = ("(upper(bus_route) like 'BM%' OR upper(bus_route) like 'BXM%' OR "
                     "upper(bus_route) like 'QM%' OR upper(bus_route) like 'SIM%' OR "
                     "upper(bus_route) like 'X%')")

# series -> (dataset before SEAM, dataset from SEAM, extra $where, cache dir)
SERIES = {
    "subway": ("subway_2020_2024", "subway_2025", None, "daily"),
    "bus": ("bus_2020_2024", "bus_2025", None, "daily_bus"),
    "bus_express": ("bus_2020_2024", "bus_2025", EXPRESS_BUS_WHERE, "daily_bus_express"),
}
DAILY_DIR = collect.DATA_RAW / SERIES["subway"][3]


def _pull_day(series: str, day: dt.date, refresh: bool) -> Path:
    before, after, extra, sub = SERIES[series]
    path = collect.DATA_RAW / sub / f"{day}.json"
    if path.exists() and not refresh:
        return path
    nxt = day + dt.timedelta(days=1)
    where = f"transit_timestamp >= '{day}' AND transit_timestamp < '{nxt}'"
    if extra:
        where += f" AND {extra}"
    rows = collect.soql(before if day < SEAM else after, {
        "$select": "fare_class_category,sum(ridership),sum(transfers)",
        "$where": where,
        "$group": "fare_class_category",
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows))
    return path


def pull(start: str = START, end: str | None = None, *, series: str = "subway",
         workers: int = 6, refresh: bool = False) -> int:
    """Cache every day in [start, end). `end` defaults to today. Returns days pulled."""
    d0 = dt.date.fromisoformat(start)
    d1 = dt.date.fromisoformat(end) if end else dt.date.today()
    days = [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days)]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda d: _pull_day(series, d, refresh), days))
    return len(days)


def load(series: str = "subway") -> pd.DataFrame:
    """Long table: date, fare_class_category, payment_method, ridership, transfers.

    Days with no rows (beyond the dataset's coverage end) are dropped.
    """
    frames = []
    for path in sorted((collect.DATA_RAW / SERIES[series][3]).glob("*.json")):
        rows = json.loads(path.read_text())
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.Timestamp(path.stem)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["ridership"] = df.pop("sum_ridership").astype(float)
    df["transfers"] = df.pop("sum_transfers").astype(float)
    # The bus data carries both 'OMNY - Fair Fare' and a stray 'OMNY - Fair Fares'.
    df["fare_class_category"] = df["fare_class_category"].replace(
        {"OMNY - Fair Fares": "OMNY - Fair Fare"})
    df = df.groupby(["date", "fare_class_category"], as_index=False)[
        ["ridership", "transfers"]].sum()
    df["payment_method"] = df["fare_class_category"].str.split(" - ").str[0].str.lower()
    return df[["date", "fare_class_category", "payment_method", "ridership", "transfers"]]


def wide(df: pd.DataFrame | None = None, series: str = "subway") -> pd.DataFrame:
    """date x fare_class_category ridership. Absent categories stay NaN, not 0."""
    df = load(series) if df is None else df
    return df.pivot_table(index="date", columns="fare_class_category",
                          values="ridership", aggfunc="sum")
