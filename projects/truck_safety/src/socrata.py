"""Thin Socrata client: SoQL queries and paginated pulls cached to CSV.

Two habits this project keeps, both inherited from the handoff bundle's
`queries.md` and both learned the hard way:

- Wrap **both sides** of every text comparison in `upper()`.  The casing in
  these columns is inconsistent, and a casing mismatch returns zero rows rather
  than an error.  A zero-row result is more often a casing artifact than a
  genuine absence.
- Always pass a stable `$order` when paging, or pages silently repeat rows.

An app token is optional but raises the anonymous rate limit; set
`SOCRATA_APP_TOKEN` in the environment to use one.
"""

import os
import time

import pandas as pd
import requests

from constants import SOCRATA_DOMAIN

PAGE_SIZE = 50_000
_MAX_RETRIES = 4

APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")


def resource_url(dataset_id):
    return f"https://{SOCRATA_DOMAIN}/resource/{dataset_id}.json"


def metadata(dataset_id):
    """Dataset name, last-updated timestamp, and field names."""
    payload = _get(f"https://{SOCRATA_DOMAIN}/api/views/{dataset_id}.json", {}).json()
    return {
        "id": dataset_id,
        "name": payload.get("name"),
        "rows_updated_at": pd.to_datetime(payload.get("rowsUpdatedAt"), unit="s"),
        "columns": [column["fieldName"] for column in payload.get("columns", [])],
    }


def _get(url, params):
    """GET with a bounded backoff; Socrata rate-limits anonymous callers."""
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    for attempt in range(_MAX_RETRIES):
        response = requests.get(url, params=params, headers=headers, timeout=120)
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
    """Page through a whole resource, or a filtered slice of one."""
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


def fetch_in_chunks(dataset_id, field, values, select=None, chunk_size=400):
    """Fetch rows whose `field` is in `values`, chunked to keep URLs short.

    Socrata has no cross-dataset join, so the crash rows behind a set of
    vehicle rows are pulled by `collision_id` and joined locally.
    """
    values = [str(value) for value in dict.fromkeys(values) if value]
    frames = []
    for start in range(0, len(values), chunk_size):
        quoted = ",".join(f"'{value}'" for value in values[start : start + chunk_size])
        frames.append(
            fetch_all(dataset_id, select=select, where=f"{field} in ({quoted})")
        )

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
