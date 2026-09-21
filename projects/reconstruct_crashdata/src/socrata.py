"""Thin Socrata client.

Two rules this project keeps to, both learned the hard way in the prior research
pass (see research/handoff/queries.md):

- Text fields in these datasets are inconsistently cased, so every text
  comparison wraps in `upper()`.
- The unauthenticated endpoint times out on large aggregations. Requests retry
  once with a longer timeout before giving up.
"""

import time

import pandas as pd
import requests

from constants import SOCRATA_DOMAIN

BASE = f"https://{SOCRATA_DOMAIN}"
DEFAULT_TIMEOUT = 60


def metadata(resource_id):
    """Catalog metadata for a dataset, including rowsUpdatedAt and column list."""
    r = requests.get(f"{BASE}/api/views/{resource_id}.json", timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()


def rows_updated_at(resource_id):
    """When the rows themselves last changed, as a UTC timestamp."""
    return pd.to_datetime(metadata(resource_id).get("rowsUpdatedAt"), unit="s", utc=True)


def query(resource_id, timeout=DEFAULT_TIMEOUT, retries=1, **params):
    """Run a SoQL query. Params are passed as `$select`, `$where`, etc.

    Pass SoQL clauses without the leading `$` — `query(rid, select="count(*)")`.
    """
    soql = {f"${k}": v for k, v in params.items() if v is not None}
    last = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(
                f"{BASE}/resource/{resource_id}.json",
                params=soql,
                timeout=timeout * (attempt + 1),
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as exc:
            last = exc
            if attempt < retries:
                time.sleep(2)
    raise last


def frame(resource_id, **params):
    """Same as `query`, as a DataFrame."""
    return pd.DataFrame(query(resource_id, **params))


def paged(resource_id, page_size=50000, **params):
    """Pull every row of a dataset, paging through the 1000-row default cap."""
    out, offset = [], 0
    while True:
        batch = query(resource_id, limit=page_size, offset=offset, timeout=120, **params)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return pd.DataFrame(out)
