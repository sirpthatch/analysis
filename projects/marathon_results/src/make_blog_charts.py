"""Charts for the training-weather blog post.

Binscatter convention: the outcome and the regressor are both residualized on the
fixed effects, then the sample means are added back, so the x axis stays in real
degrees F while the picture shows the within-city / within-race relationship that
the regressions estimate.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from femodel import absorb

ROOT = Path(__file__).resolve().parent.parent
import click

SEASON = "spring"   # charts are built on spring races: the 90-day window is winter
SUF = "" if SEASON == "all" else f"_{SEASON}"
EST = ROOT / "data" / "estimates"
CELLS = EST / f"hometown_cells{SUF}.parquet"
LADDER = EST / f"hometown_quantile_ladder{SUF}.csv"
ESTIMATES = EST / f"hometown_weather_estimates{SUF}.csv"
OUT = ROOT / "blog"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#8a8985"
BLUE = "#2a78d6"
ORANGE = "#eb6834"
GRID = "#e6e5e1"

mpl.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "text.color": INK, "axes.labelcolor": INK2, "axes.edgecolor": GRID,
    "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "figure.dpi": 200,
})


def style(ax, title, subtitle, xlabel, ylabel):
    ax.set_title(title, fontsize=15, fontweight="600", color=INK, loc="left", pad=22)
    ax.text(0, 1.035, subtitle, transform=ax.transAxes, fontsize=10.5,
            color=INK2, ha="left", va="bottom")
    ax.set_xlabel(xlabel, fontsize=10.5)
    ax.set_ylabel(ylabel, fontsize=10.5)
    ax.tick_params(length=0, labelsize=10)
    ax.set_axisbelow(True)


def binscatter(cells, outcome, fe_cols, n_bins=12):
    sub = cells[[outcome, "temp"] + fe_cols].dropna()
    keys = [pd.factorize(sub[c])[0] for c in fe_cols]
    X = absorb(sub, [outcome, "temp"], keys)
    d = pd.DataFrame({"x": X["temp"] + sub["temp"].mean(),
                      "y": X[outcome] + sub[outcome].mean()})
    d["bin"] = pd.qcut(d["x"], n_bins, duplicates="drop")
    g = d.groupby("bin", observed=True).agg(x=("x", "mean"), y=("y", "mean"),
                                            n=("y", "size"), sd=("y", "std"))
    g["se"] = g["sd"] / np.sqrt(g["n"])
    return g


def chart_same_runner():
    """City FE vs runner FE - 'it is not you, it is who showed up'."""
    est = pd.read_csv(ESTIMATES)
    est = est[(est["term"] == "temp") & est["spec"].str.startswith("individual:")]
    # Both bars hold the race fixed and use the same 614,503 runner-races. They differ
    # ONLY in what else is absorbed: the runner's city, or the runner themselves.
    rows = [("Different runners,\nsame hometown", "individual:city FE", BLUE),
            ("The same runner,\ncompared to themselves", "individual:runner FE", ORANGE)]
    fig, ax = plt.subplots(figsize=(7.6, 4.6))
    ax.axhline(0, color=MUTED, linewidth=1, zorder=1)
    for i, (lab, spec, col) in enumerate(rows):
        r = est[est["spec"] == spec].iloc[0]
        ax.bar(i, r["coef"], width=0.5, color=col, zorder=2,
               edgecolor=SURFACE, linewidth=2)
        ax.errorbar(i, r["coef"], yerr=1.96 * r["se"], fmt="none", ecolor=INK2,
                    elinewidth=1.6, capsize=0, zorder=3)
        top = r["coef"] + 1.96 * r["se"] if r["coef"] >= 0 else r["coef"] - 1.96 * r["se"]
        ax.text(i, top + (0.04 if r["coef"] >= 0 else -0.04), f"{r['coef']:+.2f} min/°F",
                ha="center", va="bottom" if r["coef"] >= 0 else "top",
                fontsize=11, color=INK, fontweight="600")
    ax.set_xticks(range(len(rows)))
    ax.set_xticklabels([r[0] for r in rows], fontsize=10.5)
    style(ax, "Hold the runner constant and the penalty vanishes",
          "Effect on one runner's finish time — identical races in both bars",
          "", "Minutes added per °F")
    vals = [(r["coef"], r["se"]) for _, r in est.iterrows()]
    hi = max(c + 1.96 * s for c, s in vals); lo = min(c - 1.96 * s for c, s in vals)
    ax.set_ylim(min(lo, 0) - 0.18 * (hi - lo), max(hi, 0) + 0.22 * (hi - lo))
    ax.set_xlim(-0.75, 1.75)
    fig.tight_layout()
    fig.savefig(OUT / "03_same_runner.png", bbox_inches="tight")
    print("  03_same_runner.png")


def chart_turnout(cells):
    g = binscatter(cells, "log_n", ["home_id", "instance_id"])
    base = g["y"].iloc[0]
    pct = 100 * (np.exp(g["y"] - base) - 1)
    err = 100 * np.exp(g["y"] - base) * 1.96 * g["se"]
    fig, ax = plt.subplots(figsize=(7.6, 4.6))
    ax.axhline(0, color=MUTED, linewidth=1)
    ax.errorbar(g["x"], pct, yerr=err, fmt="o", color=ORANGE, markersize=8,
                linewidth=2, elinewidth=1.6, capsize=0,
                markeredgecolor=SURFACE, markeredgewidth=2, zorder=3)
    style(ax, "A mild winter, and a lot more of them show up",
          "Runners a city sends to a spring marathon, relative to its coldest winters",
          "Average temperature in the 90 days before the race (°F)",
          "Entrants vs. coldest bin (%)")
    ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    fig.tight_layout()
    fig.savefig(OUT / "01_turnout_vs_training_temp.png", bbox_inches="tight")
    print("  01_turnout_vs_training_temp.png")


def chart_ladder():
    lad = pd.read_csv(LADDER)
    lad = lad[lad["fe"] == "within-city"].copy()
    labels = {"p10": "Fastest 10%", "p25": "25th", "p50": "Median",
              "p75": "75th", "p90": "Slowest 10%"}
    lad["label"] = lad["quantile"].map(labels)
    x = np.arange(len(lad))
    fig, ax = plt.subplots(figsize=(7.6, 4.6))
    ax.axhline(0, color=MUTED, linewidth=1, zorder=1)
    ax.bar(x, lad["temp_coef"], width=0.6, color=BLUE, zorder=2,
           edgecolor=SURFACE, linewidth=2)
    ax.errorbar(x, lad["temp_coef"], yerr=1.96 * lad["temp_se"], fmt="none",
                ecolor=INK2, elinewidth=1.6, capsize=0, zorder=3)
    for xi, v, se in zip(x, lad["temp_coef"], lad["temp_se"]):
        top = v + 1.96 * se if v >= 0 else v - 1.96 * se
        ax.text(xi, top + (0.02 if v >= 0 else -0.02), f"{v:+.2f}",
                ha="center", va="bottom" if v >= 0 else "top",
                fontsize=10, color=INK, fontweight="600")
    ax.set_xticks(x)
    ax.set_xticklabels(lad["label"])
    style(ax, "The fast runners aren't slowing down at all",
          "Extra finish time per 1°F of milder winter, by place in the field",
          "", "Minutes added per °F")
    hi = (lad["temp_coef"] + 1.96 * lad["temp_se"]).max()
    lo = min(0.0, (lad["temp_coef"] - 1.96 * lad["temp_se"]).min())
    ax.set_ylim(lo - 0.12 * (hi - lo), hi + 0.18 * (hi - lo))
    fig.tight_layout()
    fig.savefig(OUT / "02_effect_by_finishing_position.png", bbox_inches="tight")
    print("  02_effect_by_finishing_position.png")


def main():
    OUT.mkdir(exist_ok=True)
    cells = pd.read_parquet(CELLS)
    print(f"cells: {len(cells):,}")
    chart_turnout(cells)
    chart_ladder()
    chart_same_runner()


if __name__ == "__main__":
    main()
