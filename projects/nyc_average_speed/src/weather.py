"""Hourly midtown weather from the Open-Meteo ERA5 archive.

Rain and snow are real confounders for traffic speed, so every trip gets joined
to the weather in the hour it started.  One point (Bryant Park) stands in for
all of midtown - at this scale the whole study area shares its weather.

Open-Meteo's free tier bills weighted units (~ n_variables * n_days / 14), and
one location over the full study period costs roughly 1,000 of a 10,000/day
budget, so no API key is needed.

    python src/weather.py --start 2019-01-01 --end 2026-07-31
"""

import argparse

import pandas as pd
import requests

from constants import MIDTOWN_LAT, MIDTOWN_LON, WEATHER_FILE, WEATHER_URL

HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "wind_speed_10m",
]

RENAME = {
    "time": "hour_ts",
    "temperature_2m": "temp_c",
    "precipitation": "precip_mm",
    "rain": "rain_mm",
    "snowfall": "snow_cm",
    "snow_depth": "snow_depth_m",
    "weather_code": "wmo_code",
    "wind_speed_10m": "wind_kmh",
}

# Precipitation thresholds for the categorical feature used in the models.
# Light rain barely moves traffic; heavy rain does.
WET_MM = 0.1
HEAVY_MM = 2.5


def fetch(start, end, lat=MIDTOWN_LAT, lon=MIDTOWN_LON):
    """Pull hourly weather for one point over [start, end], local time."""
    response = requests.get(
        WEATHER_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "America/New_York",
        },
        timeout=120,
    )
    response.raise_for_status()

    frame = pd.DataFrame(response.json()["hourly"]).rename(columns=RENAME)
    frame["hour_ts"] = pd.to_datetime(frame["hour_ts"])
    return frame


def add_features(frame):
    """Derive the handful of flags the speed models actually use."""
    frame = frame.copy()
    frame["is_wet"] = frame["precip_mm"] >= WET_MM
    frame["is_heavy_precip"] = frame["precip_mm"] >= HEAVY_MM
    frame["is_snowing"] = frame["snow_cm"] > 0
    frame["has_snow_cover"] = frame["snow_depth_m"] > 0

    # Join keys matching the trip table.
    frame["date"] = frame["hour_ts"].dt.date
    frame["hour"] = frame["hour_ts"].dt.hour
    return frame


def build(start, end, dest=None):
    dest = dest or WEATHER_FILE
    dest.parent.mkdir(parents=True, exist_ok=True)

    frame = add_features(fetch(start, end))
    frame.to_parquet(dest, index=False)

    wet_share = frame["is_wet"].mean()
    print(f"  wrote {dest.name}: {len(frame):,} hours, {wet_share:.1%} wet")
    return dest


def load(dest=None):
    """Read the cached hourly weather table."""
    return pd.read_parquet(dest or WEATHER_FILE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2019-01-01", metavar="YYYY-MM-DD")
    parser.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    args = parser.parse_args()

    build(args.start, args.end)


if __name__ == "__main__":
    main()
