"""Migrate data/weather_data_v2.csv into the canonical data/weather_daily/ store.

The legacy CSV was fetched without an explicit `precipitation_unit`, so Open-Meteo
returned millimetres while WeatherConfig.PRECIP_THRESHOLD documented inches (see
TRAINING_WEATHER_PIPELINE.md 2.3). Verified: Boston 1999-2026 totals ~1,324/yr,
which is mm. This converts to inches so the whole store is in one unit system.

Temperatures were already requested in Fahrenheit and are carried through as-is.
Columns the legacy fetch never requested (wet bulb, dew point, wind) are absent and
stay absent - Stage 2 degrades to the variables actually present.
"""
from __future__ import annotations

from pathlib import Path

import click
import pandas as pd

from fetch_weather import bucket_for
from ledger import Ledger

ROOT = Path(__file__).resolve().parent.parent
MM_PER_INCH = 25.4


@click.command()
@click.option("--src", default=str(ROOT / "data" / "weather_data_v2.csv"),
              type=click.Path(exists=True, path_type=Path))
@click.option("--out", default=str(ROOT / "data" / "weather_daily"), type=click.Path(path_type=Path))
@click.option("--assume-mm/--assume-inch", default=True,
              help="Legacy precip units. Default mm (verified against station climatology).")
def main(src: Path, out: Path, assume_mm: bool) -> None:
    led = Ledger("stage1_migrate_legacy")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(src)
    print(f"read {len(df):,} rows, {df[['city','state']].drop_duplicates().shape[0]} cities")

    df["city"] = df["city"].str.lower().str.strip()
    df["state"] = df["state"].str.lower().str.strip()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    df["precip_in"] = df["precip"] / MM_PER_INCH if assume_mm else df["precip"]
    df = df.drop(columns=["precip"])

    before = len(df)
    df = df.drop_duplicates(subset=["city", "state", "date"], keep="last")
    led.record("dedup_city_date", before, len(df), "duplicate (city, state, date) rows")

    # Sanity: annual precip should land in a plausible US range once converted.
    yr = df.assign(y=df["date"].str[:4]).groupby(["city", "state", "y"])["precip_in"].sum()
    years = yr.index.get_level_values("y").astype(int)
    full_years = yr[(years >= 2000) & (years <= 2024)]
    print(f"annual precip inches - p05 {full_years.quantile(.05):.1f} "
          f"median {full_years.median():.1f} p95 {full_years.quantile(.95):.1f} "
          f"(US cities typically 5-65 in/yr)")

    df["bucket"] = df["city"].map(bucket_for)
    for bkt, g in df.groupby("bucket"):
        path = out / str(bkt) / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        g.drop(columns=["bucket"]).to_parquet(path, index=False)
    print(f"wrote {df['bucket'].nunique()} partitions -> {out}")
    print(led.summary())
    led.write(out.with_name(out.name + "_migrate_ledger.csv"))


if __name__ == "__main__":
    main()
