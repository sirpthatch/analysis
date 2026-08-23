"""Minimal fixed-effects OLS with cluster-robust standard errors.

statsmodels is not installed and the project convention is sklearn/numpy only, so
absorption is done by alternating projections (Frisch-Waugh-Lovell): iteratively
demean every column within each fixed effect until the changes stop mattering.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def absorb(df: pd.DataFrame, cols: list[str], fe_keys: list[np.ndarray],
           n_iter: int = 20, tol: float = 1e-8) -> pd.DataFrame:
    """Demean `cols` with respect to every fixed effect in `fe_keys`."""
    X = df[cols].astype("float64").copy()
    if not fe_keys:
        return X - X.mean()
    for _ in range(n_iter):
        before = X.to_numpy(copy=True)
        for key in fe_keys:
            X = X - X.groupby(key, observed=True).transform("mean")
        shift = np.nanmax(np.abs(X.to_numpy() - before))
        if shift < tol:
            break
    return X


def ols_cluster(y: np.ndarray, X: np.ndarray, cluster: np.ndarray,
                n_absorbed: int = 0) -> tuple[np.ndarray, np.ndarray, dict]:
    """OLS with one-way cluster-robust covariance.

    Returns (beta, se, info). `n_absorbed` counts fixed-effect parameters removed by
    `absorb`, so the residual degrees of freedom are not overstated.
    """
    XtX_inv = np.linalg.pinv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    resid = y - X @ beta

    n, k = X.shape
    codes = pd.factorize(cluster)[0]
    G = codes.max() + 1

    meat = np.zeros((k, k))
    order = np.argsort(codes, kind="stable")
    Xs, rs, cs = X[order], resid[order], codes[order]
    bounds = np.flatnonzero(np.diff(cs)) + 1
    for chunk_X, chunk_r in zip(np.split(Xs, bounds), np.split(rs, bounds)):
        u = chunk_X.T @ chunk_r
        meat += np.outer(u, u)

    dof = n - k - n_absorbed
    corr = (G / (G - 1)) * ((n - 1) / max(dof, 1))
    V = XtX_inv @ meat @ XtX_inv * corr
    se = np.sqrt(np.maximum(np.diag(V), 0))
    return beta, se, {"n": n, "k": k, "clusters": G, "dof": dof,
                      "r2_within": 1 - resid.var() / y.var() if y.var() else np.nan}


def tidy(names: list[str], beta: np.ndarray, se: np.ndarray, info: dict,
         spec: str, outcome: str, extra: dict | None = None) -> pd.DataFrame:
    t = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    out = pd.DataFrame({
        "spec": spec, "outcome": outcome, "term": names,
        "coef": beta, "se": se, "t": t,
        "ci_lo": beta - 1.96 * se, "ci_hi": beta + 1.96 * se,
        "n": info["n"], "clusters": info["clusters"],
    })
    for k, v in (extra or {}).items():
        out[k] = v
    return out


def stars(t: float) -> str:
    a = abs(t)
    return "***" if a >= 2.576 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""
