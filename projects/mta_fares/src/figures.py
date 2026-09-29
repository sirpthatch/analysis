#!/usr/bin/env python3
"""Render the article figures to article/lib/ as PNG.

    python src/figures.py

Reads only cached pulls (run src/collect.py and the timeseries/od pulls first).
Palette: the dataviz reference instance, first four categorical slots,
validated light-mode (adjacent CVD dE >= 9.1). Two slots sit below 3:1 contrast
on the surface, so every multi-series line is direct-labelled.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from farecap import evasion, proposals, revenue, schemes, timeseries  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "article" / "lib"
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
S1, S2, S3, S4 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": ["Helvetica Neue", "Arial", "DejaVu Sans"], "font.size": 10,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "grid.linestyle": "-",
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "lines.linewidth": 2, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlecolor": INK, "axes.titlelocation": "left",
})


def _save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> article/lib/{name}")


def _label_end(ax, x, y, text, color, dy=0):
    ax.annotate(text, (x, y), xytext=(6, dy), textcoords="offset points",
                va="center", fontsize=9, color=INK2)
    ax.plot([x], [y], "o", ms=5, color=color, mec=SURFACE, mew=1.5)


def fig_composition():
    w = timeseries.wide()
    grp = {
        "30-Day Unlimited": (["Metrocard - Unlimited 30-Day"], S1),
        "7-Day Unlimited": (["Metrocard - Unlimited 7-Day"], S2),
        "Seniors & Disability": (["Metrocard - Seniors & Disability", "OMNY - Seniors & Disability"], S3),
        "Students": (["Metrocard - Students", "OMNY - Students"], S4),
    }
    tot = w.sum(axis=1).resample("MS").sum()
    fig, ax = plt.subplots(figsize=(9, 4.6))
    offsets = {"30-Day Unlimited": 20, "7-Day Unlimited": 6}
    for label, (cols, c) in grp.items():
        s = w.reindex(columns=cols).sum(axis=1, min_count=1).fillna(0).resample("MS").sum() / tot * 100
        s = s[:"2026-08"]
        ax.plot(s.index, s.values, color=c, label=label)
        _label_end(ax, s.index[-1], s.values[-1], f"{label} {s.values[-1]:.2f}%", c,
                   dy=offsets.get(label, 0))
    for d, t in (("2024-09-01", "Sep 2024 window"), ("2026-01-04", "Passes retired\nJan 4 2026")):
        ax.axvline(pd.Timestamp(d), color=AXIS, lw=1)
        ax.text(pd.Timestamp(d), 11.2, " " + t, fontsize=8, color=MUTED, va="top")
    ax.set_ylim(0, 11.5)
    ax.set_ylabel("share of subway rides, %")
    ax.set_title("The monthly pass was fading for three years before it was retired")
    ax.legend(loc="upper center", ncol=4, frameon=False, fontsize=9, bbox_to_anchor=(0.5, -0.08))
    ax.set_xlim(right=pd.Timestamp("2027-06-01"))
    fig.text(0.01, -0.06, "Monthly share of subway entries by fare class, Jan 2023 – Aug 2026. "
             "Source: MTA Subway Hourly Ridership (wujg-7c2s, 5wq4-mkjj).", fontsize=8, color=MUTED)
    _save(fig, "fig1_fare_class_share.png")


def fig_revenue(days):
    chk = revenue.monthly_check(days)
    chk = chk.filter(regex="^(actual|model_)") / 1e6
    fig, ax = plt.subplots(figsize=(9, 4.4))
    x = chk.index
    ax.fill_between(x, chk["model_low"], chk["model_high"],
                    color=BLUE_RAMP[0], lw=0, label="model, low–high assumptions")
    ax.plot(x, chk["model_central"], color=S1, label="model, central")
    ax.plot(x, chk["actual"], color=S2, label="MTA reported farebox (actual)")
    _label_end(ax, x[-1], chk["actual"].iloc[-1], "actual", S2, dy=6)
    _label_end(ax, x[-1], chk["model_central"].iloc[-1], "model", S1, dy=-6)
    for d in chk.index[chk.index.month == 12]:
        ax.annotate("Dec", (d, chk.loc[d, "actual"]), xytext=(0, 6), textcoords="offset points",
                    ha="center", fontsize=7, color=MUTED)
    ax.set_ylim(0, None)
    ax.set_ylabel("$ millions per month")
    ax.set_title("Pricing every tap at its fare reproduces the MTA's farebox within ~2–4%")
    ax.legend(loc="lower right", frameon=False, fontsize=9)
    fig.text(0.01, -0.04, "NYCT + MTA Bus + SIR farebox revenue (Statement of Operations yg77-3tkj, Actual) vs "
             "paid subway, bus and paratransit entries priced by fare class. December actuals carry year-end adjustments.",
             fontsize=8, color=MUTED, wrap=True)
    _save(fig, "fig2_revenue_model_vs_actual.png")

    wk = revenue.weekly_revenue(days) / 1e6
    fig, ax = plt.subplots(figsize=(9, 4.0))
    ax.fill_between(wk.index, wk["total_low"], wk["total_high"], color=BLUE_RAMP[0], lw=0,
                    label="low–high assumptions")
    ax.plot(wk.index, wk["total_central"], color=S1, lw=1.6, label="all modes, central")
    ax.plot(wk.index, wk["subway"], color=S3, lw=1.6, label="subway only, central")
    _label_end(ax, wk.index[-1], wk["total_central"].iloc[-1], f"${wk['total_central'].iloc[-1]:.0f}M", S1)
    _label_end(ax, wk.index[-1], wk["subway"].iloc[-1], f"${wk['subway'].iloc[-1]:.0f}M", S3)
    ax.set_ylim(0, None)
    ax.set_ylabel("$ millions per week")
    ax.set_title("Modelled weekly fare revenue, subway and bus")
    ax.legend(loc="lower left", frameon=False, fontsize=9, ncol=3)
    _save(fig, "fig3_weekly_revenue.png")


def fig_evasion():
    # Start at 2021-Q1: the first quarter from which the subway rate, its margin of
    # error and the bus total are all published without gaps (subway 2020-Q2 was
    # not surveyed; subway MoE starts 2020-Q1; bus 'Total' starts 2020-Q4).
    r = evasion.rates()["2021":]
    fig, ax = plt.subplots(figsize=(9, 4.0))
    ax.plot(r.index, r["subway"] * 100, color=S1, label="subway (survey)")
    ax.fill_between(r.index, (r["subway"] - r["subway_moe"]) * 100, (r["subway"] + r["subway_moe"]) * 100,
                    color=BLUE_RAMP[0], lw=0)
    b = r["bus_total"].dropna()
    ax.plot(b.index, b * 100, color=S2, label="bus, all service (passenger counters)")
    _label_end(ax, r["subway"].dropna().index[-1], r["subway"].dropna().iloc[-1] * 100,
               f"subway {r['subway'].dropna().iloc[-1]:.1%}", S1)
    _label_end(ax, b.index[-1], b.iloc[-1] * 100, f"bus {b.iloc[-1]:.1%}", S2)
    ax.set_ylim(0, 60)
    ax.set_ylabel("share of riders not paying, %")
    ax.set_title("Fare evasion: subway fell to ~10% in late 2024; bus stays near half")
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    fig.text(0.01, -0.04, "MTA quarterly estimates, 2021-Q1 to 2026-Q2: subway 6kj3-ijvb (traffic-checker survey; "
             "shaded = margin of error), bus uv5h-dfhp (automated passenger counters, 'Total'). "
             "Sources: article/lib/fig4_sources.md", fontsize=8, color=MUTED)
    _save(fig, "fig4_fare_evasion.png")


def fig_distance():
    r = schemes.distance_fare()
    bo = r["by_origin"].dropna(subset=["borough"])
    bw = bo.groupby("borough").apply(
        lambda d: np.average(d["mean_price"], weights=d["weekly_trips"]), include_groups=False)
    chg = ((bw / 3.0 - 1) * 100).sort_values()
    fig, ax = plt.subplots(figsize=(7, 3.2))
    ax.barh(chg.index, chg.values, color=S1, height=0.55)
    for y, v in enumerate(chg.values):
        ax.text(v + (0.4 if v >= 0 else -0.4), y, f"{v:+.1f}%", va="center",
                ha="left" if v >= 0 else "right", fontsize=9, color=INK2)
    ax.axvline(0, color=AXIS, lw=1)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("average fare change vs flat \\$3.00, by origin borough, %")
    ax.tick_params(axis="y", length=0)
    p = r["band_prices"]
    ax.set_title("A revenue-neutral distance fare shifts cost to the outer boroughs")
    fig.text(0.01, -0.12, f"Bands (straight-line): <2 mi \\${p[0]:.2f}, 2–5 mi \\${p[1]:.2f}, "
             f"5–9 mi \\${p[2]:.2f}, >9 mi \\${p[3]:.2f}. Trips: MTA O-D estimate, typical week of April 2026 (28vm-gjqr). "
             "Staten Island not in O-D data.", fontsize=8, color=MUTED)
    ax.set_xlim(min(chg.min() - 3, -6), chg.max() + 4)
    _save(fig, "fig5_distance_fare_by_borough.png")


def fig_station_map():
    """30-Day Unlimited share of ALL rides by station, Sept 1-16 2024 (single panel).

    The share-within-MetroCard version (controls for OMNY adoption) is in
    output/station_30day_within_metrocard_2024.csv; it was dropped from the figure
    on request, so the caption carries the confound.
    """
    t = pd.read_csv(Path(__file__).resolve().parents[1] / "output" /
                    "station_30day_within_metrocard_2024.csv", dtype={"station_complex_id": str})
    st = pd.read_json(Path(__file__).resolve().parents[1] / "data" / "raw" / "od" / "stations_2026.json",
                      dtype={"id": str}).drop_duplicates("id")
    m = t.merge(st[["id", "lat", "lon"]], left_on="station_complex_id", right_on="id")
    fig, ax = plt.subplots(figsize=(6.4, 6.6))
    v = m["m30_all"]
    bins = np.quantile(v, np.linspace(0, 1, 6))
    idx = np.clip(np.digitize(v, bins[1:-1]), 0, 4)
    cols = np.array(BLUE_RAMP[1:6])[idx]
    order = np.argsort(v.to_numpy())
    ax.scatter(m["lon"].to_numpy()[order], m["lat"].to_numpy()[order],
               s=np.sqrt(m["total"].to_numpy()[order]) / 10, c=cols[order],
               edgecolors=SURFACE, linewidths=0.5)
    ax.set_aspect(1 / np.cos(np.radians(40.7)))
    ax.set_xticks([]); ax.set_yticks([]); ax.grid(False)
    ax.spines["bottom"].set_visible(False)
    for i in range(5):
        ax.scatter([], [], s=30, c=BLUE_RAMP[1 + i], label=f"{bins[i]:.1f}–{bins[i+1]:.1f}%")
    ax.legend(loc="upper left", frameon=False, fontsize=8,
              title="30-Day share of station's rides\n(quintiles)", title_fontsize=8)
    ax.set_title("Where monthly-pass riders boarded, Sept 1–16 2024", fontsize=12)
    fig.text(0.01, 0.02, "30-Day Unlimited rides as a share of all entries at each station. Dot area ∝ station "
             "ridership.\nStations with more OMNY users show lower shares partly for that reason. "
             "Source: MTA Subway Hourly Ridership (wujg-7c2s).", fontsize=8, color=MUTED)
    _save(fig, "fig6_station_30day_share_2024.png")


def fig_income_quintiles():
    """Proposal 1: average fare change by origin neighbourhood income, both schemes."""
    si = proposals.station_income()
    rows = {}
    for label, rel in (("distance fare", schemes.DISTANCE_RELATIVE),
                       ("inverse distance", schemes.INVERSE_DISTANCE_RELATIVE)):
        bo = schemes.distance_fare(relative=rel)["by_origin"].join(si[["lowmod_share"]]).dropna(
            subset=["lowmod_share"])
        q = pd.qcut(bo["lowmod_share"], 5, labels=False)
        rows[label] = bo.groupby(q).apply(
            lambda d: (np.average(d["mean_price"], weights=d["weekly_trips"]) / 3.0 - 1) * 100,
            include_groups=False)
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(8, 3.8))
    x = np.arange(5)
    wbar = 0.36
    for i, (col, c) in enumerate(((df.columns[0], S1), (df.columns[1], S2))):
        xs = x + (i - 0.5) * (wbar + 0.03)
        ax.bar(xs, df[col], width=wbar, color=c, label=col)
        for xi, v in zip(xs, df[col]):
            ax.text(xi, v + (0.3 if v >= 0 else -0.3), f"{v:+.1f}%", ha="center",
                    va="bottom" if v >= 0 else "top", fontsize=8, color=INK2)
    ax.axhline(0, color=AXIS, lw=1)
    ax.grid(axis="x", visible=False)
    ax.set_xticks(x, ["least\nlow-income", "2", "3", "4", "most\nlow-income"])
    ax.set_xlabel("origin station neighbourhood, quintile of low/moderate-income share")
    ax.set_ylabel("avg fare vs flat \\$3.00, %")
    ax.set_title("Inverse distance does favour lower-income neighbourhoods, by about 6%")
    ax.legend(loc="upper left", frameon=False, fontsize=9)
    ax.set_ylim(df.min().min() - 2.5, df.max().max() + 2.5)
    fig.text(0.01, -0.13, "Revenue-neutral bands on the April 2026 O-D matrix (28vm-gjqr). Income: HUD low/mod-income "
             "share of tracts within 0.5 mi (NYC qmcw-ur37, ACS 2016-20). Neighbourhood, not rider, income.",
             fontsize=8, color=MUTED)
    _save(fig, "fig7_distance_schemes_by_income.png")


def fig_caps():
    """Proposals 3-4: revenue change per cap scenario, range across assumptions."""
    g = pd.read_csv(Path(__file__).resolve().parents[1] / "output" / "p34_cap_grid.csv")
    agg = g.groupby("scenario", sort=False)["revenue_change_$M"].agg(["min", "max", "median"])
    agg = agg.iloc[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    y = np.arange(len(agg))
    ax.hlines(y, agg["min"], agg["max"], color=S1, lw=6, alpha=0.35)
    ax.plot(agg["median"], y, "o", color=S1, ms=7, mec=SURFACE, mew=1.5)
    for yi, (lo, hi) in enumerate(zip(agg["min"], agg["max"])):
        lo_s, hi_s = abs(round(hi)), abs(round(lo))
        ax.text(lo - 8, yi, f"\\${lo_s:,.0f}M – \\${hi_s:,.0f}M", va="center", ha="right",
                fontsize=8, color=INK2)
    ax.axvline(0, color=AXIS, lw=1)
    ax.set_yticks(y, [t.replace("$", r"\$") for t in agg.index])
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="y", visible=False)
    ax.set_xlim(agg["min"].min() * 1.45, 15)
    ax.set_xlabel("change in annual full-fare revenue vs today's \\$35 weekly cap, \\$M (static)")
    ax.set_title("What each cap would cost: a monthly cap is cheap; daily caps hinge on one unknown")
    fig.text(0.01, -0.05, "Synthetic full-fare rider population calibrated to observed 2026 trips and the 2024 unlimited-pass "
             "share. Bar: range across 3–6 trips/rider/week and, for daily caps, 0–25% same-day trip chaining; dot: median.",
             fontsize=8, color=MUTED)
    _save(fig, "fig8_cap_scenarios.png")


def fig_eligible_stations():
    """Golden-station eligibility: stations in >=51% low/moderate-income neighbourhoods.

    Eligible = HUD low/mod-income share of tracts within 0.5 mi >= 51% (the CDBG
    "CD-eligible" line), Staten Island excluded (SIR collects fares at only two
    stations). Colour: LMI share band; grey: not eligible. Size: daily entries,
    Sept 1-16 2026.
    """
    import json
    root = Path(__file__).resolve().parents[1]
    inc = proposals.station_income()
    st = pd.read_json(root / "data" / "raw" / "od" / "stations_2026.json",
                      dtype={"id": str}).drop_duplicates("id").set_index("id")
    raw = pd.DataFrame(json.loads((root / "data" / "raw" / "station_fare_class_2026.json").read_text()))
    daily = raw.assign(r=raw["sum_ridership"].astype(float)).groupby("station_complex_id")["r"].sum() / 16
    m = inc.join(st[["lat", "lon"]]).join(daily.rename("daily"))
    m = m[m["borough"] != "Staten Island"].dropna(subset=["lat", "lon", "daily"])
    elig = m["lowmod_share"] >= 0.51
    size = np.sqrt(m["daily"]) / 9

    fig, ax = plt.subplots(figsize=(9.0, 6.4))
    fig.subplots_adjust(top=0.97, bottom=0.08)
    ax.scatter(m.loc[~elig, "lon"], m.loc[~elig, "lat"], s=size[~elig], c="#d6d5cf",
               edgecolors=SURFACE, linewidths=0.4, label=f"not eligible ({(~elig).sum()})")
    bands = [(0.51, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 1.01)]
    ramp = [BLUE_RAMP[2], BLUE_RAMP[3], BLUE_RAMP[4], BLUE_RAMP[6]]
    for (lo, hi), c in zip(bands, ramp):
        sel = elig & (m["lowmod_share"] >= lo) & (m["lowmod_share"] < hi)
        hi_lbl = "100" if hi > 1 else f"{hi * 100:.0f}"
        ax.scatter(m.loc[sel, "lon"], m.loc[sel, "lat"], s=size[sel], c=c, edgecolors=SURFACE,
                   linewidths=0.5, label=f"{lo * 100:.0f}–{hi_lbl}% low/mod-income ({sel.sum()})")
    for name, dx, dy, ha in (("Flushing-Main St (7)", 0, 14, "center"),
                             ("Jackson Hts-Roosevelt Av/74 St-Broadway (7,E,F,M,R)", 75, -95, "left")):
        r = m[m["name"] == name].iloc[0]
        short = name.split(" (")[0].replace("/74 St-Broadway", "")
        ax.annotate(f"{short}\n{r['daily'] / 1e3:.0f}k entries/day", (r["lon"], r["lat"]),
                    xytext=(dx, dy), textcoords="offset points", fontsize=7.5, color=INK2,
                    ha=ha, va="center",
                    arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.6, shrinkA=0, shrinkB=3))
    ax.set_aspect(1 / np.cos(np.radians(40.7)))
    ax.set_xticks([]); ax.set_yticks([]); ax.grid(False)
    ax.spines["bottom"].set_visible(False)
    leg = ax.legend(loc="upper left", bbox_to_anchor=(1.0, 0.98), frameon=False, fontsize=8.5,
                    markerscale=0.8, title="Station neighbourhood\n(number of stations)", title_fontsize=8.5,
                    alignment="left")
    for h in leg.legend_handles:
        h.set_sizes([30])
    fig.suptitle(f"{int(elig.sum())} subway stations would be eligible to be golden", x=0.02, ha="left",
                 fontweight="bold", fontsize=12)
    fig.text(0.01, 0.02,
             "Eligible: HUD low/moderate-income share of census tracts within 0.5 mi of the station is at least 51%.\n"
             "Dot area ∝ daily entries (Sept 1–16 2026). Staten Island excluded. Sources: NYC Open Data qmcw-ur37\n"
             "(HUD LMISD, ACS 2016–20), Census 2023 tract Gazetteer, MTA Subway Hourly Ridership 5wq4-mkjj.",
             fontsize=7.5, color=MUTED)
    _save(fig, "fig9_golden_eligible_stations.png")


def main():
    print("figures:")
    fig_composition()
    days = {b: revenue.daily_revenue(b) for b in revenue.BAND}
    fig_revenue(days)
    fig_evasion()
    fig_distance()
    fig_station_map()
    fig_income_quintiles()
    fig_caps()
    fig_eligible_stations()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
