"""Tests for Stage 2 rolling training-window features."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from etl_config import WeatherConfig
from process_city_windows import CityWindowEtlModule


def make_city(n_days=200, start="2020-01-01", temp=50.0, precip=0.0):
    dates = pd.date_range(start, periods=n_days, freq="D")
    return pd.DataFrame({
        "city": "testville", "state": "ts",
        "date": dates.strftime("%Y-%m-%d"),
        "temp_max": np.full(n_days, temp + 10.0),
        "temp_min": np.full(n_days, temp - 10.0),
        "precip_in": np.full(n_days, precip),
    })


class TestWindowBounds:
    def test_full_window_is_ninety_days(self):
        out = CityWindowEtlModule().featurize_city(make_city())
        assert out.loc[out.index[-1], "full_n_days_observed"] == 90
        assert out.loc[out.index[-1], "peak_n_days_observed"] == 30

    def test_race_day_is_excluded(self):
        """Window is [race_date - N, race_date) - race day must not leak in."""
        d = make_city(n_days=120)
        # Make the final day wildly hot; it must not affect that day's own window.
        d.loc[d.index[-1], ["temp_max", "temp_min"]] = [300.0, 300.0]
        out = CityWindowEtlModule().featurize_city(d)
        assert out.loc[out.index[-1], "full_temp_max"] == 60.0

    def test_window_ramps_up_at_series_start(self):
        out = CityWindowEtlModule().featurize_city(make_city(n_days=200))
        assert out.loc[0, "full_n_days_observed"] == 0        # nothing before day 0
        assert out.loc[10, "full_n_days_observed"] == 10
        assert np.isnan(out.loc[0, "full_temp_mean"])         # but no aggregate exists

    def test_interior_gap_reduces_coverage(self):
        d = make_city(n_days=200)
        d = d.drop(d.index[100:120])                          # 20-day hole
        out = CityWindowEtlModule().featurize_city(d)
        assert len(out) == 200                                # grid is restored
        # Final day's window spans days 109-198; the gap removes days 109-119 (11 days).
        at = out.loc[out["date"] == d["date"].iloc[-1]]
        assert at["full_n_days_observed"].iloc[0] == 79
        assert at["full_coverage"].iloc[0] == pytest.approx(79 / 90)


class TestPrecipThreshold:
    def test_threshold_is_inches_not_millimetres(self):
        """Regression for the 0.2mm/0.2in unit bug: 0.1in must not count as a wet day."""
        d = make_city(n_days=120, precip=0.1)
        out = CityWindowEtlModule().featurize_city(d)
        assert out.loc[out.index[-1], "full_days_of_precip"] == 0

    def test_days_above_threshold_are_counted(self):
        d = make_city(n_days=120, precip=0.5)
        out = CityWindowEtlModule().featurize_city(d)
        assert out.loc[out.index[-1], "full_days_of_precip"] == 90

    def test_weekend_precip_counts_only_weekends(self):
        d = make_city(n_days=120, precip=0.5)
        out = CityWindowEtlModule().featurize_city(d)
        last = out.loc[out.index[-1]]
        assert last["full_weekend_days_of_precip"] == pytest.approx(26, abs=1)
        assert last["full_weekend_days_of_precip"] < last["full_days_of_precip"]

    def test_precip_total_sums_the_window(self):
        d = make_city(n_days=120, precip=0.25)
        out = CityWindowEtlModule().featurize_city(d)
        assert out.loc[out.index[-1], "full_overall_precip"] == pytest.approx(90 * 0.25)


class TestDerivedFeatures:
    def test_trend_is_zero_for_flat_series(self):
        out = CityWindowEtlModule().featurize_city(make_city(n_days=120))
        assert out.loc[out.index[-1], "full_temp_trend"] == pytest.approx(0.0, abs=1e-9)

    def test_trend_detects_warming(self):
        d = make_city(n_days=120)
        ramp = np.arange(120) * 1.0                            # +1 degF per day
        d["temp_max"] = 60.0 + ramp
        d["temp_min"] = 40.0 + ramp
        out = CityWindowEtlModule().featurize_city(d)
        assert out.loc[out.index[-1], "full_temp_trend"] == pytest.approx(7.0, rel=1e-6)

    def test_heat_and_cold_days(self):
        cfg = WeatherConfig()
        hot = make_city(n_days=120, temp=cfg.HEAT_DAY_F)       # temp_max = HEAT + 10
        out = CityWindowEtlModule().featurize_city(hot)
        assert out.loc[out.index[-1], "full_heat_days"] == 90
        assert out.loc[out.index[-1], "full_cold_days"] == 0

    def test_ewm_equals_mean_for_constant_series(self):
        out = CityWindowEtlModule().featurize_city(make_city(n_days=120, temp=50.0))
        assert out.loc[out.index[-1], "ewm_temp_mean_21d"] == pytest.approx(50.0)

    def test_ewm_weights_recent_days_more(self):
        d = make_city(n_days=120)
        d.loc[d.index[-10:], ["temp_max", "temp_min"]] = [100.0, 100.0]
        out = CityWindowEtlModule().featurize_city(d)
        last = out.loc[out.index[-1]]
        assert last["ewm_temp_mean_21d"] > last["full_temp_mean"]


class TestConfigDrivenWindows:
    def test_adding_a_window_requires_only_config(self):
        cfg = WeatherConfig()
        cfg.WINDOWS = {"taper": 14, "base": 180}
        out = CityWindowEtlModule(cfg).featurize_city(make_city(n_days=300))
        assert out.loc[out.index[-1], "taper_n_days_observed"] == 14
        assert out.loc[out.index[-1], "base_n_days_observed"] == 180
        assert "full_temp_mean" not in out.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
