"""Thin Socrata client: paginated SoQL pulls, cached to CSV on disk.

Every helper here is idempotent in the same sense as the rest of the project -
a pull whose CSV already exists is skipped unless `refresh=True`, so a run can
be topped up without re-fetching what is already local.

Two habits this project keeps, both learned the hard way (see
`research/queries.md`): wrap text comparisons in `upper()` because casing is
inconsistent across these datasets, and keep `$where` clauses short, because
the read proxy rejects very long URLs with a 403 rather than a useful error.
"""

import time

import pandas as pd
import requests

from constants import SOCRATA_DOMAIN

PAGE_SIZE = 50_000
_MAX_RETRIES = 4


def resource_url(dataset_id):
    return f"https://{SOCRATA_DOMAIN}/resource/{dataset_id}.json"


def metadata(dataset_id):
    """Dataset name, last-updated timestamp, and field names."""
    response = _get(f"https://{SOCRATA_DOMAIN}/api/views/{dataset_id}.json", {})
    payload = response.json()
    return {
        "id": dataset_id,
        "name": payload.get("name"),
        "rows_updated_at": pd.to_datetime(payload.get("rowsUpdatedAt"), unit="s"),
        "columns": [column["fieldName"] for column in payload.get("columns", [])],
    }


def _get(url, params):
    """GET with a bounded backoff; Socrata rate-limits anonymous callers."""
    for attempt in range(_MAX_RETRIES):
        response = requests.get(url, params=params, timeout=120)
        if response.status_code != 429:
            response.raise_for_status()
            return response
        time.sleep(2**attempt)
    response.raise_for_status()
    return response


def query(dataset_id, select=None, where=None, group=None, order=None, limit=None):
    """Run one SoQL query and return a DataFrame.  No pagination."""
    params = {"$limit": limit or PAGE_SIZE}
    for key, value in (
        ("$select", select),
        ("$where", where),
        ("$group", group),
        ("$order", order),
    ):
        if value:
            params[key] = value
    return pd.DataFrame(_get(resource_url(dataset_id), params).json())


def fetch_all(dataset_id, select=None, where=None, order=None, page_size=PAGE_SIZE):
    """Page through a whole resource (or a filtered slice of one).

    Paging needs a stable sort or rows can repeat across pages; `:id` is the
    row identifier Socrata guarantees, so it is the default order.
    """
    frames, offset = [], 0
    while True:
        params = {"$limit": page_size, "$offset": offset, "$order": order or ":id"}
        if select:
            params["$select"] = select
        if where:
            params["$where"] = where

        page = _get(resource_url(dataset_id), params).json()
        if not page:
            break

        frames.append(pd.DataFrame(page))
        offset += page_size
        if len(page) < page_size:
            break

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_in_chunks(dataset_id, field, values, select=None, extra_where=None,
                    chunk_size=150):
    """Fetch rows whose `field` is in `values`, chunked to keep URLs short.

    Used to pull DOB permits for just the BBLs that carry affordable units,
    rather than the millions of permit rows citywide.
    """
    values = [str(value) for value in dict.fromkeys(values) if value]
    frames = []

    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        quoted = ",".join(f"'{value}'" for value in chunk)
        where = f"{field} in ({quoted})"
        if extra_where:
            where = f"({where}) AND ({extra_where})"
        frames.append(fetch_all(dataset_id, select=select, where=where))

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def cached_pull(path, loader, refresh=False):
    """Return the CSV at `path`, calling `loader()` to build it if missing."""
    if path.exists() and path.stat().st_size > 0 and not refresh:
        print(f"  have {path.name}")
        return pd.read_csv(path, dtype=str, low_memory=False)

    frame = loader()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    print(f"  got  {path.name} ({len(frame):,} rows)")
    return frame
