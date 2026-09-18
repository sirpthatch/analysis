"""Generate the article figures into article/lib/.

    python src/make_figures.py

Each figure answers one point in the article; nothing here is decorative.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import analysis as an
import charts as ch
import panel as pnl
from constants import BAND_ORDER, TIME_BAND_LABELS, UNGA_VIRTUAL_YEARS, unga_week

OUT = Path(__file__).parent.parent / "article" / "lib"
CLEAN_YEARS = [2022, 2023, 2024, 2025]

MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def load():
    panel = pnl.drop_outages(pnl.load_panel())
    panel["date"] = pd.to_datetime(panel["date"])
    panel["year"] = panel["date"].dt.year
    return panel


# --- 1. The daily curve - the article's opening hook -----------------------


def fig_daily_curve(panel):
    core = panel[panel["area"] == "midtown_core"]
    hourly = an.agg(core, ["hour"]).set_index("hour")

    fig, ax = plt.subplots(figsize=(9, 4.4))
    ax.plot(hourly.index, hourly["mph"], color=ch.SERIES[0], marker="o", zorder=3)

    walk = 3.1
    ax.axhline(walk, color=ch.INK_MUTED, lw=1.2, ls=(0, (4, 3)))
    ax.annotate(
        "brisk walking pace",
        xy=(1, walk),
        xytext=(0, 6),
        textcoords="offset points",
        fontsize=9,
        color=ch.INK_MUTED,
    )

    peak, trough = hourly["mph"].idxmax(), hourly["mph"].idxmin()
    for h, va in [(peak, -16), (trough, 10)]:
        ax.plot(h, hourly.loc[h, "mph"], "o", ms=8, color=ch.HIGHLIGHT, zorder=4)
        ax.annotate(
            f"{hourly.loc[h, 'mph']:.1f} mph at {h}:00",
            xy=(h, hourly.loc[h, "mph"]),
            xytext=(8, va),
            textcoords="offset points",
            fontsize=10,
            color=ch.HIGHLIGHT,
            fontweight="600",
        )

    ax.set(
        xlabel="hour of day",
        ylabel="mph",
        ylim=(0, 13),
        xticks=range(0, 24, 3),
        xticklabels=[f"{h}:00" for h in range(0, 24, 3)],
    )
    ax.set_title("Midtown traffic barely outpaces a walk for most of the working day")
    ch.subtitle(
        ax, "Average speed of taxi trips starting and ending in midtown, by hour"
    )
    ch.source(fig)
    ch.save(fig, OUT / "daily_curve.png")


# --- 2. The seasonal curve - RQ1 backbone ----------------------------------


def fig_seasonal_curve(panel):
    profile = an.seasonal_profile(panel, years=CLEAN_YEARS).set_index("iso_week")

    fig, ax = plt.subplots(figsize=(10.5, 4.6))
    ax.fill_between(
        profile.index,
        profile["mph"] - profile["sd"],
        profile["mph"] + profile["sd"],
        color=ch.SERIES[0],
        alpha=0.14,
        lw=0,
    )
    ax.plot(profile.index, profile["mph"], color=ch.SERIES[0], zorder=3)

    # Mark the two stories the article tells about this curve.
    for week, text, dy in [(38, "UNGA week", 20), (50, "December", 18)]:
        y = profile.loc[week, "mph"]
        ax.plot(week, y, "o", ms=8, color=ch.HIGHLIGHT, zorder=4)
        ax.annotate(
            text,
            xy=(week, y),
            xytext=(0, -dy),
            textcoords="offset points",
            ha="center",
            fontsize=10,
            color=ch.HIGHLIGHT,
            fontweight="600",
        )

    ticks = [1, 9, 18, 27, 35, 44, 52]
    ax.set(
        xlabel="week of year",
        ylabel="mph",
        ylim=(
            (profile["mph"] - profile["sd"]).min() - 0.15,
            (profile["mph"] + profile["sd"]).max() + 0.15,
        ),
        xticks=ticks,
        xticklabels=["Jan", "Mar", "May", "Jul", "Sep", "Nov", "Dec"],
    )
    ax.set_title("Midtown loses a third of its speed between January and December")
    ch.subtitle(
        ax,
        "Weekday speed by week of year, averaged over 2022–2025. Band shows ±1 sd across years",
    )
    ch.source(fig)
    ch.save(fig, OUT / "seasonal_curve.png")


# --- 3. Time bands over seven years - the counter-intuitive one ------------


def fig_band_trend(panel):
    by = pnl.by_band(panel, extra_keys=["year"], area="midtown_core")
    piv = by.pivot(index="year", columns="band", values="mph")[BAND_ORDER]

    fig, ax = plt.subplots(figsize=(10, 5))
    labels = []
    for slot, band in zip(ch.SERIES, BAND_ORDER):
        ax.plot(piv.index, piv[band], color=slot, marker="o", zorder=3)
        pct = (piv[band].iloc[-1] / piv[band].iloc[0] - 1) * 100
        name = TIME_BAND_LABELS[band].split(" (")[0]
        labels.append(
            (piv.index[-1], piv[band].iloc[-1], f"  {name}  {pct:+.0f}%", slot)
        )

    ax.set(xlabel="", ylabel="mph", xlim=(2018.8, 2027.9), ylim=(3.8, 11.4))
    ch.label_ends(ax, labels)
    ax.set_xticks(range(2019, 2027))
    ax.set_title("Midtown's quiet hours are disappearing faster than its rush hour")
    ch.subtitle(
        ax,
        "Weekday speed by time of day. Morning rush has held up twice as well "
        "as the evening and night",
    )
    ch.source(fig)
    ch.save(fig, OUT / "band_trend.png")


# --- 4. UNGA against neighbouring weeks - the natural experiment -----------


def fig_unga_natural_experiment(panel):
    nb = an.unga_vs_neighbours(panel)

    fig, ax = plt.subplots(figsize=(9, 4.6))
    colors = [ch.POS if v > 0 else ch.NEG for v in nb["pct"]]
    bars = ax.bar([str(y) for y in nb.index], nb["pct"], width=0.62, color=colors)
    ch.bar_gap_style(ax, bars)

    ax.axhline(0, color=ch.INK, lw=1)
    for x, v in zip(range(len(nb)), nb["pct"]):
        ax.annotate(
            f"{v:+.1f}%",
            xy=(x, v),
            xytext=(0, 7 if v > 0 else -16),
            textcoords="offset points",
            ha="center",
            fontsize=10,
            fontweight="600",
            color=ch.INK_SECONDARY,
        )

    virtual = [i for i, y in enumerate(nb.index) if y in UNGA_VIRTUAL_YEARS]
    if virtual:
        i = virtual[0]
        ax.annotate(
            "General Debate held\nby video — no motorcades",
            xy=(i, nb["pct"].iloc[i]),
            xytext=(0, 42),
            textcoords="offset points",
            ha="center",
            fontsize=9.5,
            color=ch.POS,
            fontweight="600",
        )

    ax.set(ylabel="speed vs surrounding weeks", ylim=(-28, 16))
    ax.set_yticks([-25, -20, -15, -10, -5, 0, 5, 10])
    ax.set_yticklabels([f"{v}%" for v in [-25, -20, -15, -10, -5, 0, 5, 10]])
    ax.set_title("When UNGA went virtual, midtown's traffic jam went with it")
    ch.subtitle(
        ax,
        "Midtown speed during UNGA week vs the three weeks either side, dry hours included",
    )
    ch.source(fig)
    ch.save(fig, OUT / "unga_natural_experiment.png")


# --- 5. Distance gradient - ordered, so an ordinal ramp ---------------------


def fig_unga_gradient(panel):
    areas = ["un_core", "midtown_core", "cbd", "uptown"]
    names = ["Beside the UN", "Midtown", "Below 60th St", "Above 60th St"]

    rows = []
    for area in areas:
        r = an.unga_vs_neighbours(panel, area=area)
        rows.append(r["pct"].rename(area))
    grid = pd.concat(rows, axis=1)

    inperson = [y for y in grid.index if y not in UNGA_VIRTUAL_YEARS]
    virtualy = [y for y in grid.index if y in UNGA_VIRTUAL_YEARS]

    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    x = np.arange(len(areas))

    for year in inperson:
        ax.plot(
            x,
            grid.loc[year, areas],
            color=ch.INK_MUTED,
            lw=1.1,
            alpha=0.55,
            marker="o",
            ms=4,
            zorder=2,
        )
    mean_ip = grid.loc[inperson, areas].mean()
    ax.plot(x, mean_ip, color=ch.NEG, lw=2.6, marker="o", ms=9, zorder=4)

    for year in virtualy:
        ax.plot(
            x,
            grid.loc[year, areas],
            color=ch.POS,
            lw=2.2,
            marker="o",
            ms=8,
            ls=(0, (5, 2)),
            zorder=3,
        )
    mean_v = grid.loc[virtualy, areas].mean()

    ax.axhline(0, color=ch.INK, lw=1, zorder=1)
    # Value labels sit below the in-person line, clear of the virtual line above.
    for xi, v in zip(x, mean_ip):
        ax.annotate(
            f"{v:.0f}%",
            xy=(xi, v),
            xytext=(0, -19),
            textcoords="offset points",
            ha="center",
            fontsize=10.5,
            color=ch.NEG,
            fontweight="600",
        )

    ax.set(
        xticks=x,
        xlim=(-0.35, 4.9),
        ylabel="speed vs surrounding weeks",
        ylim=(-40, 14),
    )
    ch.label_ends(
        ax,
        [
            (x[-1], mean_ip.iloc[-1], "  in-person years", ch.NEG),
            (x[-1], mean_v.iloc[-1], "  2020–21 (virtual)", ch.POS),
        ],
        min_gap_frac=0.12,
    )
    ax.set_xticklabels(names)
    ax.set_yticks([-40, -30, -20, -10, 0, 10])
    ax.set_yticklabels([f"{v}%" for v in [-40, -30, -20, -10, 0, 10]])
    ax.set_title("You can see the UN security perimeter in the traffic data")
    ch.subtitle(
        ax,
        "UNGA-week slowdown by distance from UN headquarters. Faint lines are individual years",
    )
    ch.source(fig)
    ch.save(fig, OUT / "unga_gradient.png")


# --- 6. What is actually the slowest week -----------------------------------


def fig_slowest_weeks(panel):
    year = 2024
    weekly = an.week_table(panel)
    weekly = weekly[weekly["iso_year"] == year].sort_values("iso_week")
    uw = pd.Timestamp(unga_week(year)[0]).isocalendar().week

    ranked = weekly.nsmallest(10, "mph").copy()
    ranked["label"] = [
        pd.Timestamp.fromisocalendar(int(y), int(w), 1).strftime("%-d %b")
        for y, w in zip(ranked["iso_year"], ranked["iso_week"])
    ]
    ranked = ranked.iloc[::-1]
    colors = [ch.HIGHLIGHT if w == uw else ch.SERIES[0] for w in ranked["iso_week"]]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars = ax.barh(ranked["label"], ranked["mph"], height=0.62, color=colors)
    ch.bar_gap_style(ax, bars)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", visible=True)

    for y_i, (v, w) in enumerate(zip(ranked["mph"], ranked["iso_week"])):
        txt = f"{v:.2f}" + ("   ← UNGA week" if w == uw else "")
        ax.annotate(
            txt,
            xy=(v, y_i),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9.5,
            color=ch.HIGHLIGHT if w == uw else ch.INK_SECONDARY,
            fontweight="600" if w == uw else "normal",
        )

    ax.set(xlabel="mph", xlim=(0, 6.4))
    ax.set_title("Across midtown, December beats UNGA week")
    ch.subtitle(
        ax,
        f"Ten slowest weeks of {year}, midtown core weekday speed. Beside the UN itself, "
        "UNGA ranks first every year",
    )
    ch.source(fig)
    ch.save(fig, OUT / "slowest_weeks.png")


# --- 7. Congestion pricing: speed against a control ------------------------


def fig_congestion_speed(panel):
    did = an.did_table(panel, months=range(1, 8))

    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for color, area, name in [
        (ch.PAIR[0], "cbd", "Below 60th St (tolled)"),
        (ch.PAIR[1], "uptown", "Above 60th St (no toll)"),
    ]:
        ax.plot(did.index, did[area], color=color, marker="o", zorder=3)
        ch.label_end(ax, did.index[-1], did[area].iloc[-1], f"  {name}", color)

    ax.axvline(2025, color=ch.INK_MUTED, lw=1.2, ls=(0, (4, 3)))
    ax.annotate(
        "congestion pricing\nbegins Jan 2025",
        xy=(2025, 11.0),
        xytext=(-8, 0),
        textcoords="offset points",
        ha="right",
        fontsize=9.5,
        color=ch.INK_SECONDARY,
    )

    ax.set(ylabel="mph", xlim=(2018.8, 2028.6), ylim=(7, 11.6))
    ax.set_xticks(range(2019, 2027))
    ax.set_title("Congestion pricing bought midtown about 2% — for one year")
    ch.subtitle(ax, "January–July speed, tolled zone vs the control above 60th St")
    ch.source(fig)
    ch.save(fig, OUT / "congestion_speed.png")


# --- 8. Congestion pricing: volume went the other way ----------------------


def fig_congestion_volume(panel):
    jj = panel[panel["date"].dt.month <= 7]
    counts = jj.groupby([jj["date"].dt.year, "area"])["trips"].sum().unstack()
    idx = (counts / counts.loc[2019]) * 100

    # 2019 is the index base and 2020-21 are pandemic years; showing them would
    # compress the comparison the article actually makes into invisibility.
    idx = idx.loc[2022:]

    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for color, area, name in [
        (ch.PAIR[0], "cbd", "Below 60th St (tolled)"),
        (ch.PAIR[1], "uptown", "Above 60th St (no toll)"),
    ]:
        ax.plot(idx.index, idx[area], color=color, marker="o", zorder=3)

    ax.axvline(2025, color=ch.INK_MUTED, lw=1.2, ls=(0, (4, 3)), zorder=1)
    ax.annotate(
        "toll begins",
        xy=(2025, 34.2),
        xytext=(-8, 0),
        textcoords="offset points",
        ha="right",
        fontsize=9.5,
        color=ch.INK_SECONDARY,
    )

    lo, hi = idx.loc[2024, "cbd"], idx.loc[2025, "cbd"]
    ax.annotate(
        "",
        xy=(2025, hi),
        xytext=(2024, lo),
        arrowprops=dict(arrowstyle="->", color=ch.HIGHLIGHT, lw=2.2),
    )
    ax.annotate(
        f"taxi trips in the tolled\nzone rose {(hi / lo - 1) * 100:.0f}%",
        xy=(2024.45, (lo + hi) / 2),
        xytext=(0, 26),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color=ch.HIGHLIGHT,
        fontweight="600",
    )

    ax.set(
        ylabel="trips, % of 2019 level",
        xlim=(2021.8, 2028.2),
        ylim=(32, 56),
    )
    ax.set_xticks(range(2022, 2027))
    ch.label_ends(
        ax,
        [
            (
                idx.index[-1],
                idx["cbd"].iloc[-1],
                "  Below 60th St (tolled)",
                ch.PAIR[0],
            ),
            (
                idx.index[-1],
                idx["uptown"].iloc[-1],
                "  Above 60th St (no toll)",
                ch.PAIR[1],
            ),
        ],
        min_gap_frac=0.09,
    )
    ax.set_title("Taxi trips in the tolled zone went up, not down")
    ch.subtitle(
        ax,
        "January–July yellow taxi volume as a share of its 2019 level. "
        "Pandemic years omitted",
    )
    ch.source(fig)
    ch.save(fig, OUT / "congestion_volume.png")


def main():
    ch.apply_style()
    OUT.mkdir(parents=True, exist_ok=True)
    panel = load()
    print(f"panel: {len(panel):,} rows")

    fig_daily_curve(panel)
    fig_seasonal_curve(panel)
    fig_band_trend(panel)
    fig_unga_natural_experiment(panel)
    fig_unga_gradient(panel)
    fig_slowest_weeks(panel)
    fig_congestion_speed(panel)
    fig_congestion_volume(panel)


if __name__ == "__main__":
    main()
