"""Tests for the ledger and the Stage 4 panel filters."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from femodel import absorb, ols_cluster
from ledger import Ledger


class TestLedger:
    def test_rows_balance(self):
        """rows_in must equal rows_out + rows_dropped for every filter."""
        led = Ledger("t")
        df = pd.DataFrame({"x": range(100)})
        df = led.apply(df, df["x"] < 60, "under_60")
        df = led.apply(df, df["x"] % 2 == 0, "even")
        frame = led.to_frame()
        assert (frame["rows_in"] == frame["rows_out"] + frame["rows_dropped"]).all()

    def test_chain_is_continuous(self):
        """Each filter's rows_in must equal the previous filter's rows_out."""
        led = Ledger("t")
        df = pd.DataFrame({"x": range(100)})
        df = led.apply(df, df["x"] < 60, "a")
        df = led.apply(df, df["x"] < 30, "b")
        f = led.to_frame()
        assert f["rows_in"].iloc[1] == f["rows_out"].iloc[0]
        assert f["rows_out"].iloc[-1] == len(df)

    def test_pct_dropped(self):
        led = Ledger("t")
        df = pd.DataFrame({"x": range(100)})
        led.apply(df, df["x"] < 25, "quarter")
        assert led.to_frame()["pct_dropped"].iloc[0] == pytest.approx(75.0)

    def test_empty_input_does_not_divide_by_zero(self):
        led = Ledger("t")
        df = pd.DataFrame({"x": []})
        led.apply(df, df["x"] > 0, "none")
        assert led.to_frame()["pct_dropped"].iloc[0] == 0.0


class TestFixedEffects:
    def test_absorb_removes_group_means(self):
        rng = np.random.default_rng(0)
        g = rng.integers(0, 10, 500)
        df = pd.DataFrame({"y": rng.normal(size=500) + g * 5.0})
        out = absorb(df, ["y"], [g])
        assert out.groupby(g)["y"].mean().abs().max() < 1e-10

    def test_two_way_absorption_recovers_known_slope(self):
        rng = np.random.default_rng(1)
        n = 20000
        g, c = rng.integers(0, 40, n), rng.integers(0, 25, n)
        x = rng.normal(size=n) + 0.1 * g
        y = 2.5 * x + 3.0 * g + 1.5 * c + rng.normal(size=n)
        X = absorb(pd.DataFrame({"y": y, "x": x}), ["y", "x"], [g, c])
        b, se, info = ols_cluster(X["y"].to_numpy(), X[["x"]].to_numpy(), c,
                                  n_absorbed=40 + 25)
        assert b[0] == pytest.approx(2.5, abs=0.05)
        assert info["clusters"] == 25

    def test_cluster_se_exceeds_naive_se_when_correlated(self):
        """Within-cluster correlation must inflate the SE."""
        rng = np.random.default_rng(2)
        n, ncl = 5000, 20
        c = rng.integers(0, ncl, n)
        shock = rng.normal(scale=3.0, size=ncl)
        x = rng.normal(size=n)
        y = 1.0 * x + shock[c] + rng.normal(size=n)
        X = np.column_stack([x, np.ones(n)])
        _, se_cl, _ = ols_cluster(y, X, c)
        resid = y - X @ np.linalg.lstsq(X, y, rcond=None)[0]
        se_naive = np.sqrt(np.diag(np.linalg.pinv(X.T @ X) * resid.var()))
        assert se_cl[0] > se_naive[0]

    def test_absorbing_the_regressor_leaves_nothing(self):
        """A regressor constant within the FE is fully absorbed."""
        g = np.repeat(np.arange(50), 20)
        x = g.astype(float)
        out = absorb(pd.DataFrame({"x": x}), ["x"], [g])
        assert np.abs(out["x"].to_numpy()).max() < 1e-10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
