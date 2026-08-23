"""Stage 0b - recover runner identity from the raw scrape dump.

`enriched_raw_data.parquet` carries a 100%-populated
`Last Name, First Name (Sex/Age)` column. `process_racedata.py` drops it when
building data/race_records/*.parquet, which is why no runner identity survives
downstream - and why runner fixed effects have never been possible.

This rebuilds the race_final join key from the raw dump alongside a `runner_id`,
producing a lookup that Stage 3 left-joins onto the race results. Keys that are
ambiguous within a race (two runners with identical name/city/age/sex/time) are
emitted with runner_id = NA rather than being guessed at.
"""
from __future__ import annotations

import re
from pathlib import Path

import click
import pandas as pd
import pyarrow.parquet as pq

from etl_utils import DateParser, TextNormalizer, TimeConverter
from ledger import Ledger

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "enriched_raw_data.parquet"
OUT = ROOT / "data" / "runner_identity.parquet"

NAME_RE = re.compile(r"^(.*?)\s*\(([MFmf])\s*(\d{1,3})\)\s*$")

RAW_COLS = ["Last Name, First Name (Sex/Age)", "Time", "Net Time", "date", "race",
            "City, State", "City, State, Country"]


def parse_name(series: pd.Series) -> pd.DataFrame:
    """'WENGROFF, NATALIE (F24)' -> name / sex / age."""
    ex = series.astype("string").str.extract(NAME_RE)
    ex.columns = ["name", "sex", "age"]
    ex["name"] = ex["name"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    ex["sex"] = ex["sex"].str.upper()
    ex["age"] = pd.to_numeric(ex["age"], errors="coerce")
    return ex


def parse_location(cs: pd.Series, csc: pd.Series) -> pd.DataFrame:
    """'Brooklyn, NY, USA' / 'Brooklyn, NY' -> city / state (lowercased)."""
    loc = cs.astype("string").fillna(csc.astype("string"))
    parts = loc.str.split(",")
    n = parts.str.len()
    city = parts.str[0].str.lower().str.strip()
    # State is the 2nd field; a trailing country ('USA') occupies the 3rd.
    state = parts.str[1].str.lower().str.strip().where(n >= 2)
    country = parts.str[2].str.lower().str.strip().where(n >= 3)
    # Non-US rows look like 'Brazil' (n == 1) or 'Ontario, Canada'.
    us = country.isna() | country.isin(["usa", "us", "u.s.a."])
    return pd.DataFrame({"city": city.where(us), "state": state.where(us)})


def build(raw_path: Path, led: Ledger) -> pd.DataFrame:
    pf = pq.ParquetFile(raw_path)
    frames = []
    for i in range(pf.metadata.num_row_groups):
        frames.append(pf.read_row_group(i, columns=RAW_COLS).to_pandas())
    raw = pd.concat(frames, ignore_index=True)
    print(f"raw rows: {len(raw):,}")

    df = parse_name(raw["Last Name, First Name (Sex/Age)"])
    df = pd.concat([df, parse_location(raw["City, State"], raw["City, State, Country"])], axis=1)
    # Apply the same race-name normalization process_join uses, so the keys line up.
    df["race"] = raw["race"].map(TextNormalizer.normalize_race_name)
    df["date"] = raw["date"].map(DateParser.parse_race_final_date)
    # race_final's `time` is the gun time column; net time is only sporadically present.
    # Round to avoid float-equality misses against race_final's stored minutes.
    df["time_minutes"] = raw["Time"].map(TimeConverter.hms_to_minutes).round(4)

    n = len(df)
    df = led.apply(df, df["name"].notna() & df["sex"].notna(), "parse_name",
                   "name did not match 'NAME (S##)'")
    df = led.apply(df, df["date"].notna(), "parse_date", "unparseable M_D_YY date")
    df = led.apply(df, df["time_minutes"].notna(), "parse_time", "unparseable finish time")
    df = led.apply(df, df["city"].notna() & df["state"].notna(), "has_home_city",
                   "no US city+state on the record (foreign or blank)")

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    # Identity = person, not entry. Hometown disambiguates common names; it is the
    # standard key for this data and is measured with error (see limitations).
    df["runner_id"] = df["name"] + "|" + df["city"] + "|" + df["state"]

    key = ["race", "date", "city", "state", "age", "sex", "time_minutes"]
    before = len(df)
    df = df.drop_duplicates(subset=key + ["runner_id"], keep="first")
    led.record("dedup_exact", before, len(df), "identical (key, runner_id) rows")

    # Where one join key maps to several runners we cannot tell them apart.
    dup = df.duplicated(subset=key, keep=False)
    df.loc[dup, "runner_id"] = pd.NA
    led.record("ambiguous_key", len(df), int((~dup).sum()),
               "join key maps to >1 runner_id; runner_id set to NA (rows retained)")
    df = df.drop_duplicates(subset=key, keep="first")

    print(f"\nlookup rows: {len(df):,}   runner_id resolved: {df['runner_id'].notna().sum():,}")
    return df[key + ["runner_id", "name"]]


@click.command()
@click.option("--raw", default=str(RAW), type=click.Path(exists=True, path_type=Path))
@click.option("--out", default=str(OUT), type=click.Path(path_type=Path))
def main(raw: Path, out: Path) -> None:
    led = Ledger("stage0b_runner_identity")
    df = build(raw, led)

    counts = df["runner_id"].value_counts()
    print(f"distinct runners: {len(counts):,}")
    for k in (2, 3, 5):
        sel = counts[counts >= k]
        print(f"  >={k} races: {len(sel):,} runners covering {sel.sum():,} rows")

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"\n-> {out}")
    print(led.summary())
    led.write(out.with_name(out.stem + "_ledger.csv"))


if __name__ == "__main__":
    main()
