"""Stage 4 - build the analysis-ready panel.

Applies the sample filters, constructs outcomes and fixed-effect keys, and writes a
provenance ledger so the surviving sample can be audited for bias (research.md).

Every filter is recorded with rows in/out. Nothing is dropped silently.
"""
from __future__ import annotations

from pathlib import Path

import click
import numpy as np
import pandas as pd

from etl_config import PanelConfig, WeatherConfig
from ledger import Ledger

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "training_weather" / "global" / "data.parquet"
AGE_GRADE = ROOT / "data" / "age_graded_percentiles.csv"
OUT = ROOT / "data" / "analysis_panel.parquet"


def add_age_graded(df: pd.DataFrame) -> pd.DataFrame:
    """Robust z-score against the age/sex median: (t - p50) / (p75 - p25).

    From notebooks/feature_age_graded_percentiles.ipynb. Negative = faster than the
    age/sex median. The lookup has existed unused since February; this is its first
    consumer.
    """
    if not AGE_GRADE.exists():
        df["ag_z"] = np.nan
        return df
    ref = pd.read_csv(AGE_GRADE)[["age", "sex", "p25_time", "p50_time", "p75_time"]]
    ref["age"] = ref["age"].round().astype(int)
    out = df.merge(ref, left_on=["age_int", "sex"], right_on=["age", "sex"],
                   how="left", suffixes=("", "_ref"))
    iqr = (out["p75_time"] - out["p25_time"]).replace(0, np.nan)
    out["ag_z"] = (out["time_minutes"] - out["p50_time"]) / iqr
    return out.drop(columns=[c for c in ["age_ref", "p25_time", "p50_time", "p75_time"]
                             if c in out.columns])


@click.command()
@click.option("--src", default=str(SRC), type=click.Path(exists=True, path_type=Path))
@click.option("--out", default=str(OUT), type=click.Path(path_type=Path))
def main(src: Path, out: Path) -> None:
    cfg, wcfg = PanelConfig(), WeatherConfig()
    led = Ledger("stage4_analysis_panel")

    df = pd.read_parquet(src)
    print(f"input rows: {len(df):,}")

    # Sex is written lowercase by the scraper but every analysis filters on 'M'/'F',
    # which silently dropped every newly scraped record.
    df["sex"] = df["sex"].astype("string").str.upper().str.strip()

    df = led.apply(df, df["time_minutes"].between(cfg.MIN_TIME_MIN, cfg.MAX_TIME_MIN),
                   "plausible_finish", f"time outside {cfg.MIN_TIME_MIN:.0f}-{cfg.MAX_TIME_MIN:.0f} min")
    df = led.apply(df, df["age"].between(cfg.MIN_AGE, cfg.MAX_AGE),
                   "plausible_age", f"age outside {cfg.MIN_AGE}-{cfg.MAX_AGE}")
    df = led.apply(df, df["sex"].isin(["M", "F"]), "sex_present", "sex not M/F")

    for name in wcfg.WINDOWS:
        col = f"{name}_coverage"
        df = led.apply(df, df[col] >= wcfg.MIN_WINDOW_COVERAGE, f"{name}_window_coverage",
                       f"<{wcfg.MIN_WINDOW_COVERAGE:.0%} of the {name} window observed")

    df = led.apply(df, df["race_day_temp_max"].notna(), "race_day_weather",
                   "no race-day weather (needed for mismatch terms)")

    df["instance_id"] = df["race"].astype(str) + "|" + df["date"].astype(str)
    df["home_id"] = df["city"].astype(str) + "|" + df["state"].astype(str)
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["city_year"] = df["home_id"] + "|" + df["year"].astype(str)
    df["age_int"] = df["age"].round().astype(int)

    # Race instances must be big and diverse enough to identify anything within them.
    sz = df.groupby("instance_id")["time_minutes"].transform("size")
    nc = df.groupby("instance_id")["home_id"].transform("nunique")
    df = led.apply(df, (sz >= cfg.MIN_INSTANCE_FINISHERS) & (nc >= cfg.MIN_INSTANCE_CITIES),
                   "instance_support",
                   f"race instance has <{cfg.MIN_INSTANCE_FINISHERS} finishers "
                   f"or <{cfg.MIN_INSTANCE_CITIES} home cities")

    # A city FE needs several years of that city to be identified.
    ny = df.groupby("home_id")["year"].transform("nunique")
    df = led.apply(df, ny >= cfg.MIN_CITY_RACE_YEARS, "city_support",
                   f"home city appears in <{cfg.MIN_CITY_RACE_YEARS} race-years")

    df = add_age_graded(df)
    df["log_time"] = np.log(df["time_minutes"])

    # Boston-qualifying age brackets, the project's standard grouping.
    edges = [18, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 200]
    labels = ["18-34", "35-39", "40-44", "45-49", "50-54", "55-59",
              "60-64", "65-69", "70-74", "75-79", "80+"]
    df["age_bracket"] = pd.cut(df["age_int"], bins=edges, labels=labels, right=False)

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)

    print(f"\npanel: {len(df):,} rows x {len(df.columns)} cols -> {out}")
    print(f"  race instances : {df['instance_id'].nunique():,}")
    print(f"  home cities    : {df['home_id'].nunique():,}")
    print(f"  runners w/ id  : {df['runner_id'].notna().sum():,}")
    print(f"  years          : {df['year'].min()}-{df['year'].max()}")
    print(f"  ag_z available : {df['ag_z'].notna().mean():.1%}")
    print()
    print(led.summary())
    led.write(out.with_name(out.stem + "_ledger.csv"))


if __name__ == "__main__":
    main()
