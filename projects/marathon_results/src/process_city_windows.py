"""Stage 2 - per-city-day rolling training-window features.

The old featurizer (process_weather_features.py) re-sliced a city's whole weather
history once per (race, date, city) group under a thread pool. But the work is
O(city-days), not O(race-runners): the 90-day window ending on date d is a rolling
aggregate. Computing it once per city-day turns the downstream join into a plain
merge on (city, state, date) and makes new windows nearly free.

Window semantics match the old code exactly: [race_date - N, race_date), i.e.
pandas `closed='left'` on an offset window. Race day is excluded.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from etl_config import WeatherConfig
from process import EtlModule

# Optional columns: present only for cities fetched with the 'lean'/'full' sets.
OPTIONAL_MEANS = ["temp_mean", "wet_bulb_mean", "dew_point_mean",
                  "apparent_temp_mean", "wind_max", "solar_mj"]


class CityWindowEtlModule(EtlModule):
    """Daily weather -> one row per (city, state, date) of rolling window features."""

    output_format = "parquet"

    def __init__(self, config: WeatherConfig | None = None):
        self.config = config or WeatherConfig()

    def partition(self, file_paths: list[Path]) -> dict[Path, list[str]]:
        # Mirror the input partitioning: data/weather_daily/<bucket>/data.parquet
        return {p: [p.parent.name] for p in file_paths if p.suffix == ".parquet"}

    def verify_consistent(self, file_paths: list[Path]) -> None:
        # Partitions legitimately differ in optional columns depending on which
        # variable set a city was fetched with.
        return None

    def process_files(self, file_paths: list[Path]) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        for path in file_paths:
            if path.suffix != ".parquet":
                continue
            df = pd.read_parquet(path)
            frames = [self.featurize_city(g) for _, g in df.groupby(["city", "state"], sort=False)]
            frames = [f for f in frames if f is not None and len(f)]
            if frames:
                out[path.parent.name] = pd.concat(frames, ignore_index=True)
                print(f"  {path.parent.name}: {df[['city','state']].drop_duplicates().shape[0]} cities"
                      f" -> {len(out[path.parent.name]):,} city-days")
        return out

    # ------------------------------------------------------------------ helpers

    def _derive(self, d: pd.DataFrame) -> pd.DataFrame:
        """Add the per-day indicator columns the rolling aggregates sum over."""
        cfg = self.config
        if "temp_mean" not in d.columns:
            d["temp_mean"] = (d["temp_max"] + d["temp_min"]) / 2.0

        d["_observed"] = d["temp_max"].notna().astype(float)
        d["_wet"] = (d["precip_in"] >= cfg.PRECIP_THRESHOLD_IN).astype(float)
        d["_wet_weekend"] = (d["_wet"].astype(bool) & d.index.dayofweek.isin([5, 6])).astype(float)
        d["_heat"] = (d["temp_max"] >= cfg.HEAT_DAY_F).astype(float)
        d["_cold"] = (d["temp_max"] <= cfg.COLD_DAY_F).astype(float)
        d["_freeze"] = (d["temp_min"] <= cfg.FREEZE_F).astype(float)
        d["_hostile"] = (
            (d["precip_in"] >= cfg.HOSTILE_PRECIP_IN)
            | (d["temp_max"] <= cfg.HOSTILE_COLD_F)
            | (d["temp_max"] >= cfg.HOSTILE_HEAT_F)
        ).astype(float)
        # Missing days must not count as "not wet" / "not hostile".
        for c in ["_wet", "_wet_weekend", "_heat", "_cold", "_freeze", "_hostile"]:
            d.loc[d["temp_max"].isna(), c] = np.nan
        return d

    @staticmethod
    def _rolling_slope(roll_y, roll_iy, roll_i, roll_i2, n) -> pd.Series:
        """OLS slope of y on day index within each window, per day (7 * slope = degF/week).

        Uses raw sums so the whole thing stays vectorized:
            b = (Sum(iy) - Sum(i)Sum(y)/n) / (Sum(i^2) - Sum(i)^2/n)
        """
        num = roll_iy - roll_i * roll_y / n
        den = roll_i2 - roll_i ** 2 / n
        return (num / den.replace(0, np.nan)) * 7.0

    def featurize_city(self, g: pd.DataFrame) -> pd.DataFrame | None:
        cfg = self.config
        city, state = g["city"].iloc[0], g["state"].iloc[0]

        d = g.copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d.sort_values("date").drop_duplicates("date", keep="last").set_index("date")
        if len(d) < 2:
            return None

        # Complete daily grid over the city's own observed span; interior gaps become
        # NaN rows so n_days_observed reflects them honestly.
        d = d.reindex(pd.date_range(d.index.min(), d.index.max(), freq="D"))
        d["city"], d["state"] = city, state
        d = self._derive(d)

        d["_i"] = np.arange(len(d), dtype=float)
        d["_iy"] = d["_i"] * d["temp_mean"]
        d["_i2"] = d["_i"] ** 2

        means = ["temp_mean"] + [c for c in OPTIONAL_MEANS if c in g.columns and c != "temp_mean"]
        out = pd.DataFrame({"city": city, "state": state, "date": d.index})

        for name, days in cfg.WINDOWS.items():
            roll = d.rolling(f"{days}D", closed="left")
            # A count over an empty window is 0, not NaN; the aggregates below stay NaN.
            n_obs = roll["_observed"].sum().fillna(0.0)
            out[f"{name}_n_days_observed"] = n_obs.values
            out[f"{name}_coverage"] = (n_obs / days).values

            out[f"{name}_temp_min"] = roll["temp_min"].min().values
            out[f"{name}_temp_max"] = roll["temp_max"].max().values
            out[f"{name}_temp_median_min"] = roll["temp_min"].median().values
            out[f"{name}_temp_median_max"] = roll["temp_max"].median().values
            for c in means:
                out[f"{name}_{c}"] = roll[c].mean().values
            out[f"{name}_temp_sd"] = roll["temp_max"].std().values

            out[f"{name}_overall_precip"] = roll["precip_in"].sum().values
            out[f"{name}_days_of_precip"] = roll["_wet"].sum().values
            out[f"{name}_weekend_days_of_precip"] = roll["_wet_weekend"].sum().values
            out[f"{name}_heat_days"] = roll["_heat"].sum().values
            out[f"{name}_cold_days"] = roll["_cold"].sum().values
            out[f"{name}_freeze_days"] = roll["_freeze"].sum().values
            out[f"{name}_pct_days_hostile"] = (roll["_hostile"].sum() / n_obs.replace(0, np.nan)).values

            out[f"{name}_temp_trend"] = self._rolling_slope(
                roll["temp_mean"].sum(), roll["_iy"].sum(),
                roll["_i"].sum(), roll["_i2"].sum(), n_obs.replace(0, np.nan)).values

        # Acclimatization dose: exponentially weighted, shifted so race day is excluded.
        hl = cfg.ACCLIM_HALFLIFE_DAYS
        for c in ["temp_mean"] + [x for x in ("wet_bulb_mean", "dew_point_mean") if x in g.columns]:
            out[f"ewm_{c}_{hl}d"] = d[c].ewm(halflife=hl, ignore_na=True).mean().shift(1).values

        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        return out
