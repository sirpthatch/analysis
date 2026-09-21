"""Client for NYPD TrafficStat (trafficstat.nypdonline.org).

TrafficStat is an Angular front end over a public JSON API. There is no
published documentation and no API key; the endpoints below were recovered from
the site's own bundle. Nothing here is authenticated or rate-limited as far as
the probing went, but it is an undocumented internal API — it can change or
disappear without notice, so re-verify before depending on it.

    POST /api/reports/{report_id}/data   body: [{"key":..., "values":[...]}]
    POST /api/filters/{filter_id}/data   body: {}
    GET  /api/visitors/count

Why this matters here: the Socrata crash file (`h9gi-nx95`) has been frozen
since 2026-06-11. TrafficStat's own pipeline was current to 2026-09-13 when this
was written, so it is the only public route to recent citywide collision
geography. See `research/trafficstat_api.md` for the caveats, which are
substantial — the records are much thinner than Socrata's.
"""

import json
import re
import time
import urllib.request

import pandas as pd

BASE = "https://trafficstat.nypdonline.org/api"

# Report IDs, from the site bundle's own report catalog.
REPORT_INCIDENT_MAP = "7d9951ab-45d8-41af-bcc7-10e2f1874de0"  # record-level, geocoded
REPORT_TRAFFICSTAT_BOOK = "b805fa11-d5d2-43f7-8c23-1649f5d387f1"  # the aggregate table
REPORT_ABOUT = "3ed925ee-caa8-4e9c-8d87-9e56fc8269f8"  # methodology notes

# Filter IDs.
FILTER_PRECINCT = "ec81a9e0-27d3-4c69-8a49-d02ce3236b69"
FILTER_PATROL_BOROUGH = "154dcf7a-0390-4a6d-a802-5b2fb58a9bad"
FILTER_METRIC = "33e40f74-63ad-4417-a3e3-cc194ee4041e"
FILTER_ASOF = "50d3635e-d92a-4b67-8aa7-b5eb78f5460d"

# The only time windows the API offers. There is no arbitrary date range and no
# access to prior years — YTD is as far back as it goes.
WINDOWS = ("WTD", "28D", "YTD")

# The API caps any single response at 5,000 records. Citywide 28D and YTD both
# hit it, so anything at that scale has to be pulled per precinct.
RECORD_CAP = 5000

_TAGS = re.compile(r"<[^>]+>")


def _post(path, body):
    req = urllib.request.Request(
        f"{BASE}/{path}",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def filter_options(filter_id):
    """The selectable values for one of the dashboard's filters."""
    return _post(f"filters/{filter_id}/data", {})


def as_of():
    """The date TrafficStat considers its data current through."""
    opts = filter_options(FILTER_ASOF)
    return opts[0]["label"].replace("As of ", "") if opts else None


def precincts():
    """The 79 precinct keys, zero-padded as the API expects ('001', '073')."""
    return [
        o["key"]
        for o in filter_options(FILTER_PRECINCT)
        if o["key"] != "Citywide" and not o["key"].startswith("PB")
    ]


def metrics():
    """Available metrics, as (group, key) — e.g. ('Fatalities', 'Fat Pedestrian')."""
    return [(o["groupBy"], o["key"]) for o in filter_options(FILTER_METRIC)]


def _filters(precinct, window, metric):
    return [
        {"key": "PRECINCTKey", "values": [precinct]},
        {"key": "BOROKey", "values": ["Citywide"]},
        {"key": "RECORDID", "values": [window]},
        {"key": "CrimeKey", "values": [metric]},
    ]


def _parse(rec, precinct, window, metric):
    """Expand one map marker into one row per collision.

    A marker is not always one collision. Where several occurred at the same
    coordinate, the API collapses them into a single marker whose `metric` field
    is the count and whose tooltip lists each occurrence as a
    (category, timestamp) pair. About a quarter of markers are collapses, so
    treating markers as records undercounts substantially.
    """
    lat = lon = None
    if rec.get("value") and "," in str(rec["value"]):
        lat, lon = (float(x) for x in str(rec["value"]).split(","))

    parts = [p.strip() for p in _TAGS.sub("|", rec.get("tooltipHtml") or "").split("|") if p.strip()]
    # parts[0] is the metric label; an "N Occurences" header follows on
    # collapsed markers. The remainder is (category, timestamp) pairs.
    label = parts[0] if parts else None
    rest = parts[1:]
    if rest and re.match(r"^\d+\s+Occurence", rest[0]):
        rest = rest[1:]

    pairs = [(rest[i], rest[i + 1]) for i in range(0, len(rest) - 1, 2)]
    if not pairs:
        pairs = [(rest[0] if rest else None, None)]

    return [
        {
            "latitude": lat,
            "longitude": lon,
            "metric_label": label,
            "category": cat,
            "timestamp": tstamp,
            "occurrences_at_point": rec.get("metric"),
            "precinct": precinct,
            "window": window,
            "metric": metric,
        }
        for cat, tstamp in pairs
    ]


def incidents(precinct="Citywide", window="YTD", metric="Collisions"):
    """Record-level geocoded incidents for one precinct and window.

    Returns one row per collision, expanding markers that collapse several at
    the same coordinate. `df.attrs["markers"]` is the pre-expansion count; if
    that equals RECORD_CAP the response was truncated, so split it by precinct
    rather than trusting the total.
    """
    if window not in WINDOWS:
        raise ValueError(f"window must be one of {WINDOWS}, got {window!r}")
    raw = _post(f"reports/{REPORT_INCIDENT_MAP}/data", _filters(precinct, window, metric))
    rows = [row for r in raw for row in _parse(r, precinct, window, metric)]
    df = pd.DataFrame(rows)
    # The cap applies to markers returned, not to the collisions they expand to.
    df.attrs["markers"] = len(raw)
    df.attrs["truncated"] = len(raw) >= RECORD_CAP
    # Each marker states its own occurrence count. Expansion recovers ~99% of
    # them; a handful of markers carry a malformed tooltip that yields fewer
    # pairs than claimed. Carry the gap rather than hiding it.
    df.attrs["claimed"] = sum((r.get("metric") or 1) for r in raw)
    df.attrs["unparsed"] = df.attrs["claimed"] - len(df)
    return df


def all_incidents(window="YTD", metric="Collisions", pause=0.3, verbose=True):
    """Every incident citywide, pulled precinct by precinct to clear the cap.

    Citywide in one call truncates at 5,000. Iterating the 79 precincts gets the
    real total; none of them individually came close to the cap when this was
    written, but `truncated` is still reported per precinct.
    """
    out = []
    for i, p in enumerate(precincts(), 1):
        df = incidents(p, window, metric)
        if df.attrs.get("truncated") and verbose:
            print(f"  WARNING precinct {p} hit the {RECORD_CAP}-record cap")
        if verbose:
            print(f"  [{i:>2}/79] precinct {p}: {len(df):>5} collisions "
                  f"({df.attrs['markers']} markers)")
        out.append(df)
        time.sleep(pause)
    return pd.concat(out, ignore_index=True)
