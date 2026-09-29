"""Socrata collection for the fare-cap analysis.

Design note, and it matters: `5wq4-mkjj` is 45.2M rows and `wujg-7c2s` is
larger. Downloading either is a mistake. Every pull here is a SERVER-SIDE
aggregate ($select with $group, bounded by $where on transit_timestamp), which
returns tens of rows instead of tens of millions.

Two hard-won constraints, both discovered by hitting them:

1. An unbounded aggregate over 45M rows TIMES OUT. Always bound
   transit_timestamp first. Never issue a $group without a $where.
2. Socrata rate-limits unauthenticated requests with HTTP 429. We retry once
   after a pause and then fail loudly rather than returning partial data. Set
   SOCRATA_APP_TOKEN in the environment to raise the limit.
3. Even a BOUNDED sixteen-day station x fare-class $group over `wujg-7c2s`
   exceeds the 120s read timeout (first live run, 2026-09-28). The same query
   over one day returns in under a second. Wide groupings therefore go through
   `grouped_by_day`, which issues one query per day and sums client-side.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

from . import constants as C

DATA_RAW = Path(__file__).resolve().parents[2] / "data" / "raw"
TIMEOUT = 120
RETRY_PAUSE = 20


class CollectionError(RuntimeError):
    """Raised when a pull fails in a way that would silently corrupt results."""


def _resolve(key: str) -> tuple[str, str]:
    spec = C.DATASETS[key]
    return spec["domain"], spec["id"]


def _headers() -> dict[str, str]:
    token = os.environ.get("SOCRATA_APP_TOKEN")
    return {"X-App-Token": token} if token else {}


def soql(key: str, params: dict[str, str], *, cache: str | None = None,
         refresh: bool = False) -> list[dict[str, Any]]:
    """Run one SoQL query. Caches to data/raw/<cache>.json when `cache` given.

    `params` keys are passed through verbatim, so use the '$select'/'$where'
    spellings. Text comparisons in `params` should already be wrapped in
    upper() by the caller — these datasets are inconsistently cased.
    """
    if cache:
        path = DATA_RAW / f"{cache}.json"
        if path.exists() and not refresh:
            return json.loads(path.read_text())

    domain, dsid = _resolve(key)
    url = f"https://{domain}/resource/{dsid}.json"

    for attempt in (1, 2):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == 1:
                time.sleep(RETRY_PAUSE)
                continue
            raise CollectionError(f"{key}: {type(exc).__name__} for {params!r}") from exc
        if resp.status_code == 429:
            if attempt == 1:
                time.sleep(RETRY_PAUSE)
                continue
            raise CollectionError(
                f"{key}: rate-limited twice (429). Set SOCRATA_APP_TOKEN and retry."
            )
        if resp.status_code != 200:
            raise CollectionError(
                f"{key}: HTTP {resp.status_code} for {params!r} -> {resp.text[:300]}"
            )
        rows = resp.json()
        break

    if cache:
        DATA_RAW.mkdir(parents=True, exist_ok=True)
        (DATA_RAW / f"{cache}.json").write_text(json.dumps(rows, indent=2))
    return rows


def _window_clause(year: int) -> str:
    start, end = C.WINDOWS[year]
    return f"transit_timestamp >= '{start}' AND transit_timestamp < '{end}'"


def fare_class_totals(year: int, *, refresh: bool = False) -> dict[str, float]:
    """Ridership by fare_class_category for one comparison window."""
    rows = soql(
        C.WINDOW_DATASET[year],
        {
            "$select": "fare_class_category,sum(ridership)",
            "$where": _window_clause(year),
            "$group": "fare_class_category",
        },
        cache=f"fare_class_{year}",
        refresh=refresh,
    )
    return {r["fare_class_category"]: float(r["sum_ridership"]) for r in rows}


def window_total(year: int, *, refresh: bool = False) -> dict[str, Any]:
    """Independent total for one window. Used to reconcile the fare-class split.

    This reconciliation is not ceremony. Running it is what revealed that a
    '>2026-09-01' filter with no upper bound silently covered sixteen days, not
    two, and that the resulting per-day ridership was implausible.
    """
    rows = soql(
        C.WINDOW_DATASET[year],
        {
            "$select": "count(*),sum(ridership),min(transit_timestamp),max(transit_timestamp)",
            "$where": _window_clause(year),
        },
        cache=f"window_total_{year}",
        refresh=refresh,
    )
    r = rows[0]
    return {
        "rows": int(r["count"]),
        "ridership": float(r["sum_ridership"]),
        "first": r["min_transit_timestamp"],
        "last": r["max_transit_timestamp"],
    }


def grouped_by_day(key: str, start: str, end: str, group: list[str], *,
                   cache: str | None = None, refresh: bool = False,
                   limit: int = 50_000) -> list[dict[str, Any]]:
    """sum(ridership) grouped by `group` over [start, end), one query per day.

    Results are summed client-side across days, so the output has the same
    shape as a single grouped query over the whole window. A day whose result
    hits `limit` raises rather than silently truncating.
    """
    if cache:
        path = DATA_RAW / f"{cache}.json"
        if path.exists() and not refresh:
            return json.loads(path.read_text())

    acc: dict[tuple, float] = defaultdict(float)
    day = dt.date.fromisoformat(start)
    stop = dt.date.fromisoformat(end)
    while day < stop:
        nxt = day + dt.timedelta(days=1)
        rows = soql(key, {
            "$select": ",".join(group) + ",sum(ridership)",
            "$where": f"transit_timestamp >= '{day}' AND transit_timestamp < '{nxt}'",
            "$group": ",".join(group),
            "$limit": str(limit),
        })
        if len(rows) >= limit:
            raise CollectionError(f"{key} {day}: hit $limit={limit}; results truncated")
        for r in rows:
            acc[tuple(r.get(g, "") for g in group)] += float(r["sum_ridership"])
        day = nxt

    out = [dict(zip(group, k), sum_ridership=str(v)) for k, v in acc.items()]
    if cache:
        DATA_RAW.mkdir(parents=True, exist_ok=True)
        (DATA_RAW / f"{cache}.json").write_text(json.dumps(out, indent=2))
    return out


def station_fare_class(year: int, *, refresh: bool = False) -> list[dict[str, Any]]:
    """Ridership by station complex AND fare class for one window.

    This is the load-bearing pull: for 2024 it yields each station's share of
    ridership on the 30-day unlimited pass, which is the revealed-preference
    map of riders who lost the monthly product. Chunked by day — the single
    sixteen-day query times out (see module docstring).
    """
    start, end = C.WINDOWS[year]
    rows = grouped_by_day(
        C.WINDOW_DATASET[year], start, end,
        ["station_complex_id", "station_complex", "borough", "fare_class_category"],
        cache=f"station_fare_class_{year}",
        refresh=refresh,
    )
    return [
        {
            "station_complex_id": r["station_complex_id"],
            "station_complex": r.get("station_complex", ""),
            "borough": r.get("borough", ""),
            "fare_class_category": r["fare_class_category"],
            "ridership": float(r["sum_ridership"]),
        }
        for r in rows
    ]


def fair_fares(*, refresh: bool = False) -> list[dict[str, Any]]:
    """Monthly Fair Fares enrollment. Citywide totals only — no geography."""
    rows = soql(
        "fair_fares_enrollees",
        {"$select": "month,total_fair_fares_enrollees", "$order": "month"},
        cache="fair_fares",
        refresh=refresh,
    )
    return [
        {"month": r["month"][:10],
         "enrollees": int(float(r["total_fair_fares_enrollees"]))}
        for r in rows
    ]


def collect_all(*, refresh: bool = False) -> None:
    for year in C.WINDOWS:
        window_total(year, refresh=refresh)
        fare_class_totals(year, refresh=refresh)
        station_fare_class(year, refresh=refresh)
    fair_fares(refresh=refresh)
