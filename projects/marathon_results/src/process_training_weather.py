"""Stage 3 - attach training-window weather, race-day weather and runner identity.

Replaces both the ThreadPoolExecutor path in process_weather_features.py and the
standalone enrich_race_day_weather.py. Because Stage 2 already reduced weather to
one row per (city, state, date), the training-weather "join" is now a plain merge.

Two distinct geographies are in play and must not be conflated:
  * runner hometown (city, state)          -> training-window weather
  * race location  (race_location_city..)  -> race-day weather
"""
from __future__ import annotations

import glob
from pathlib import Path

import click
import numpy as np
import pandas as pd

from etl_config import WeatherConfig
from ledger import Ledger

ROOT = Path(__file__).resolve().parent.parent
RACE_DATA = ROOT / "data" / "joined_race_data_v3" / "global" / "data.parquet"
WINDOWS = ROOT / "data" / "city_window_features"
WEATHER = ROOT / "data" / "weather_daily"
RACE_LOCS = ROOT / "race_locations_normalized.csv"
IDENTITY = ROOT / "data" / "runner_identity.parquet"
OUT = ROOT / "data" / "training_weather" / "global" / "data.parquet"

RACE_DAY_COLS = ["temp_max", "temp_min", "temp_mean", "precip_in",
                 "wet_bulb_mean", "dew_point_mean", "wind_max"]


def _read_dir(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(pattern)
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def attach_identity(df: pd.DataFrame, led: Ledger) -> pd.DataFrame:
    if not IDENTITY.exists():
        print("  (no runner_identity.parquet - skipping; spec D will be unavailable)")
        df["runner_id"] = pd.NA
        return df
    ident = pd.read_parquet(IDENTITY).drop(columns=["name"])
    key = ["race", "date", "city", "state", "age", "sex", "time_minutes"]
    out = df.copy()
    out["time_minutes"] = out["time_minutes"].round(4)
    out = out.merge(ident, on=key, how="left")
    led.record("attach_runner_id", len(df), int(out["runner_id"].notna().sum()),
               "no identity match in enriched_raw_data.parquet (rows retained)")
    print(f"  runner_id attached to {out['runner_id'].notna().mean():.1%} of rows")
    return out


def attach_training_weather(df: pd.DataFrame, led: Ledger) -> pd.DataFrame:
    win = _read_dir(str(WINDOWS / "*" / "data.parquet"))
    before = len(df)
    out = df.merge(win, on=["city", "state", "date"], how="inner")
    led.record("training_weather_coverage", before, len(out),
               f"hometown has no weather in the {win[['city','state']].drop_duplicates().shape[0]}-city archive")
    return out


def attach_race_day(df: pd.DataFrame, led: Ledger) -> pd.DataFrame:
    locs = pd.read_csv(RACE_LOCS).rename(
        columns={"city": "race_location_city", "state": "race_location_state"})
    out = df.merge(locs.drop_duplicates("race"), on="race", how="left")
    print(f"  race location known for {out['race_location_city'].notna().mean():.1%} of rows")

    wx = _read_dir(str(WEATHER / "*" / "data.parquet"))
    keep = [c for c in RACE_DAY_COLS if c in wx.columns]
    wx = wx[["city", "state", "date"] + keep].rename(
        columns={c: f"race_day_{c}" for c in keep}
        | {"city": "race_location_city", "state": "race_location_state"})
    out = out.merge(wx, on=["race_location_city", "race_location_state", "date"], how="left")
    led.record("race_day_weather", len(out), int(out["race_day_temp_max"].notna().sum()),
               "race city not in the weather archive (rows retained)")
    print(f"  race-day weather on {out['race_day_temp_max'].notna().mean():.1%} of rows")
    return out


def add_mismatch(df: pd.DataFrame, cfg: WeatherConfig) -> pd.DataFrame:
    """How far race-day conditions departed from what the runner trained in."""
    for name in cfg.WINDOWS:
        tmean, tsd = f"{name}_temp_mean", f"{name}_temp_sd"
        if tmean not in df.columns:
            continue
        df[f"mismatch_temp_{name}"] = df["race_day_temp_max"] - df[tmean]
        # Standardized: 15F above your training mean means something different in a
        # low-variance climate (San Diego) than a high-variance one (Denver).
        df[f"mismatch_z_{name}"] = df[f"mismatch_temp_{name}"] / df[tsd].replace(0, np.nan)
        if "race_day_wet_bulb_mean" in df.columns and f"{name}_wet_bulb_mean" in df.columns:
            df[f"mismatch_wetbulb_{name}"] = (
                df["race_day_wet_bulb_mean"] - df[f"{name}_wet_bulb_mean"])
    return df


@click.command()
@click.option("--race-data", default=str(RACE_DATA), type=click.Path(exists=True, path_type=Path))
@click.option("--out", default=str(OUT), type=click.Path(path_type=Path))
def main(race_data: Path, out: Path) -> None:
    cfg = WeatherConfig()
    led = Ledger("stage3_training_weather")

    df = (pd.read_parquet(race_data) if str(race_data).endswith(".parquet")
          else pd.read_csv(race_data))
    print(f"race rows: {len(df):,}")
    for c in ("city", "state", "race"):
        df[c] = df[c].astype("string").str.lower().str.strip()
    # Parquet preserves datetimes; every weather key is an ISO date string.
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df = led.apply(df, df["city"].notna() & df["state"].notna(), "has_home_city",
                   "no hometown city/state on the record")

    print("\nattaching runner identity...")
    df = attach_identity(df, led)
    print("attaching training-window weather...")
    df = attach_training_weather(df, led)
    print("attaching race-day weather...")
    df = attach_race_day(df, led)
    df = add_mismatch(df, cfg)

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"\n{len(df):,} rows x {len(df.columns)} cols -> {out}")
    print(led.summary())
    led.write(out.parent.parent / "_ledger.csv")


if __name__ == "__main__":
    main()
