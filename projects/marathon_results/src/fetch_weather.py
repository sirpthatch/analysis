"""Stage 1 - fetch daily weather time series from the Open-Meteo archive.

IMPORTANT - quota model. Open-Meteo bills *weighted* units, not requests:

    weight ~= n_variables * n_days / 14

Free tier caps are 5,000 units/hour and 10,000 units/day. A single city over
1999-2026 therefore costs:

    legacy (3 vars)  ~ 2,163 units   ->  ~4 cities/day
    lean   (8 vars)  ~ 5,768 units   ->  ~1 city/day   (exceeds the hourly cap alone)
    full  (15 vars)  ~10,815 units   ->  ~1 city/day   (exceeds the hourly cap alone)

The existing 109-city/3-variable archive represents roughly 24 days of free-tier
quota. Expanding to thousands of cities is only practical with a commercial API key
(--api-key), which routes to the customer endpoint and lifts these caps.

This job is resumable: rerunning skips cities already present in the output, so it
can be left to accrete over many days.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent

FREE_URL = "https://archive-api.open-meteo.com/v1/archive"
PAID_URL = "https://customer-archive-api.open-meteo.com/v1/archive"

VARIABLE_SETS: dict[str, list[str]] = {
    # what data/weather_data_v2.csv already contains
    "legacy": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
    # adds the thermal-load variables the analysis actually wants
    "lean": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
             "dew_point_2m_mean", "wet_bulb_temperature_2m_mean",
             "precipitation_sum", "snowfall_sum", "wind_speed_10m_max"],
    "full": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
             "apparent_temperature_max", "apparent_temperature_mean",
             "dew_point_2m_mean", "relative_humidity_2m_mean",
             "wet_bulb_temperature_2m_mean", "precipitation_sum", "rain_sum",
             "snowfall_sum", "precipitation_hours", "wind_speed_10m_max",
             "shortwave_radiation_sum", "daylight_duration"],
}

RENAME = {
    "time": "date",
    "temperature_2m_max": "temp_max", "temperature_2m_min": "temp_min",
    "temperature_2m_mean": "temp_mean",
    "apparent_temperature_max": "apparent_temp_max",
    "apparent_temperature_mean": "apparent_temp_mean",
    "dew_point_2m_mean": "dew_point_mean",
    "relative_humidity_2m_mean": "humidity_mean",
    "wet_bulb_temperature_2m_mean": "wet_bulb_mean",
    "precipitation_sum": "precip_in", "rain_sum": "rain_in",
    "snowfall_sum": "snow_in", "precipitation_hours": "precip_hours",
    "wind_speed_10m_max": "wind_max", "shortwave_radiation_sum": "solar_mj",
    "daylight_duration": "daylight_s",
}


def bucket_for(city: str) -> str:
    """Partition name: first alphanumeric character of the city, else '_'."""
    c = str(city).strip().lower()
    return c[0] if c and c[0].isalnum() else "_"


def call_weight(n_vars: int, start: str, end: str) -> float:
    days = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days + 1
    return n_vars * days / 14.0


def fetch_city(lat: float, lng: float, daily: list[str], start: str, end: str,
               api_key: str | None, max_retries: int = 6, timeout: int = 300) -> dict:
    """One request -> the full daily series. Backs off on 429 like src/scraper."""
    url = PAID_URL if api_key else FREE_URL
    params = {
        "latitude": lat, "longitude": lng, "start_date": start, "end_date": end,
        "daily": daily, "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch", "wind_speed_unit": "mph", "timezone": "auto",
    }
    if api_key:
        params["apikey"] = api_key

    for attempt in range(max_retries):
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            reason = ""
            try:
                reason = resp.json().get("reason", "")
            except json.JSONDecodeError:
                pass
            # Hourly caps only clear on the hour; minute caps clear fast.
            wait = 3660 if "hour" in reason.lower() else min(60 * 2 ** attempt, 900)
            print(f"    429 ({reason or 'rate limited'}) - sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"Open-Meteo {resp.status_code}: {resp.text[:300]}")
    raise RuntimeError(f"gave up after {max_retries} retries")


def to_frame(payload: dict, city: str, state: str) -> pd.DataFrame:
    df = pd.DataFrame(payload["daily"]).rename(columns=RENAME)
    df.insert(0, "state", state)
    df.insert(0, "city", city)
    return df


def completed_cities(out_dir: Path) -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    for part in sorted(out_dir.glob("*/data.parquet")):
        d = pd.read_parquet(part, columns=["city", "state"]).drop_duplicates()
        done |= set(zip(d["city"], d["state"]))
    return done


def flush(buffers: dict[str, list[pd.DataFrame]], out: Path) -> None:
    """Append buffered cities into their partition, preserving prior contents."""
    for bkt, frames in list(buffers.items()):
        if not frames:
            continue
        path = out / bkt / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        new = pd.concat(frames, ignore_index=True)
        if path.exists():
            new = pd.concat([pd.read_parquet(path), new], ignore_index=True)
        new = new.drop_duplicates(subset=["city", "state", "date"], keep="last")
        new.to_parquet(path, index=False)
        buffers[bkt] = []


@click.command()
@click.option("--roster", default=str(ROOT / "data" / "city_roster.csv"),
              type=click.Path(exists=True, path_type=Path))
@click.option("--out", default=str(ROOT / "data" / "weather_daily"), type=click.Path(path_type=Path))
@click.option("--variables", default="lean", type=click.Choice(sorted(VARIABLE_SETS)))
@click.option("--start", default="1999-01-01")
@click.option("--end", default=None, help="Defaults to yesterday.")
@click.option("--limit", default=None, type=int, help="Stop after N new cities.")
@click.option("--api-key", default=None, envvar="OPENMETEO_API_KEY",
              help="Commercial key; lifts the free-tier caps.")
@click.option("--delay", default=1.0, type=float, help="Seconds between requests.")
def main(roster: Path, out: Path, variables: str, start: str, end: str | None,
         limit: int | None, api_key: str | None, delay: float) -> None:
    end = end or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    daily = VARIABLE_SETS[variables]
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    cities = pd.read_csv(roster).sort_values("priority")
    done = completed_cities(out)
    todo = cities[[(c, s) not in done for c, s in zip(cities["city"], cities["state"])]]
    if limit:
        todo = todo.head(limit)

    w = call_weight(len(daily), start, end)
    print(f"variable set '{variables}' ({len(daily)} vars), {start} -> {end}")
    print(f"~{w:,.0f} weighted units/city; free tier = 5,000/hr, 10,000/day"
          f"{' (bypassed: api key set)' if api_key else ''}")
    print(f"{len(done):,} cities already fetched, {len(todo):,} to go\n", flush=True)

    buffers: dict[str, list[pd.DataFrame]] = {}
    written = 0
    try:
        for i, row in enumerate(todo.itertuples(index=False), 1):
            city, state = row.city, row.state
            try:
                payload = fetch_city(row.lat, row.lng, daily, start, end, api_key)
            except RuntimeError as exc:
                print(f"  [{i}/{len(todo)}] {city}, {state}: FAILED - {exc}", flush=True)
                continue
            buffers.setdefault(bucket_for(city), []).append(to_frame(payload, city, state))
            written += 1
            print(f"  [{i}/{len(todo)}] {city}, {state} ok", flush=True)
            if written % 10 == 0:
                flush(buffers, out)
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\ninterrupted - flushing what we have")
    finally:
        flush(buffers, out)
        print(f"\ndone: {written:,} new cities -> {out}", flush=True)


if __name__ == "__main__":
    main()
