"""Stage 0 - build the city roster used to drive weather ingest.

Ranks hometown cities by runner volume, attaches lat/lng, and cuts at a cumulative
coverage target. Cities that cannot be geocoded are recorded in the ledger rather
than silently dropped.
"""
from __future__ import annotations

from pathlib import Path

import click
import pandas as pd

from ledger import Ledger

ROOT = Path(__file__).resolve().parent.parent
RUNNER_COUNTS = ROOT / "data" / "city_state_runner_counts_v2.csv"
USCITIES = ROOT / "data" / "uscities.csv"
MAPPED = ROOT / "data" / "mapped_cities.csv"
OUT = ROOT / "data" / "city_roster.csv"


def load_runner_counts() -> pd.DataFrame:
    df = pd.read_csv(RUNNER_COUNTS)
    df = df[[c for c in df.columns if not c.startswith("Unnamed")]]
    df["city"] = df["city"].astype(str).str.lower().str.strip()
    df["state"] = df["state"].astype(str).str.lower().str.strip()
    return df.groupby(["city", "state"], as_index=False)["runner_count"].sum()


def load_geocodes() -> pd.DataFrame:
    """Prefer already-geocoded cities, fall back to the simplemaps gazetteer."""
    frames = []
    if MAPPED.exists():
        m = pd.read_csv(MAPPED)[["city", "state", "lat", "lng"]]
        frames.append(m)
    us = pd.read_csv(USCITIES, usecols=["city_ascii", "state_id", "lat", "lng", "population"])
    us = us.sort_values("population", ascending=False)          # keep the biggest homonym
    us["city"] = us["city_ascii"].str.lower().str.strip()
    us["state"] = us["state_id"].str.lower().str.strip()
    frames.append(us[["city", "state", "lat", "lng"]])
    geo = pd.concat(frames, ignore_index=True)
    geo["city"] = geo["city"].astype(str).str.lower().str.strip()
    geo["state"] = geo["state"].astype(str).str.lower().str.strip()
    return geo.drop_duplicates(subset=["city", "state"], keep="first")


@click.command()
@click.option("--target-coverage", default=0.72, type=float,
              help="Cumulative share of runner-rows the roster should cover.")
@click.option("--out", default=str(OUT), type=click.Path(path_type=Path))
def main(target_coverage: float, out: Path) -> None:
    led = Ledger("stage0_city_roster")

    counts = load_runner_counts()
    total = counts["runner_count"].sum()
    print(f"hometown cities: {len(counts):,}   runner-rows: {total:,}")

    geo = load_geocodes()
    roster = counts.merge(geo, on=["city", "state"], how="left")

    located = roster["lat"].notna()
    led.record("geocode", len(roster), int(located.sum()),
               f"no lat/lng in mapped_cities.csv or uscities.csv; "
               f"{roster.loc[~located, 'runner_count'].sum():,} runner-rows affected")
    roster = roster[located].copy()

    roster = roster.sort_values("runner_count", ascending=False).reset_index(drop=True)
    roster["cum_pct"] = roster["runner_count"].cumsum() / total
    roster["priority"] = roster.index + 1

    keep = roster["cum_pct"] <= target_coverage
    if keep.sum() < len(roster):
        keep.iloc[keep.sum()] = True          # include the city that crosses the target
    roster = led.apply(roster, keep, "coverage_target",
                       f"beyond cumulative coverage target {target_coverage:.0%}")

    out = Path(out)
    roster.to_csv(out, index=False)
    print(f"\nroster: {len(roster):,} cities covering "
          f"{roster['cum_pct'].iloc[-1]:.1%} of runner-rows -> {out}")
    print(led.summary())
    led.write(out.with_name(out.stem + "_ledger.csv"))


if __name__ == "__main__":
    main()
