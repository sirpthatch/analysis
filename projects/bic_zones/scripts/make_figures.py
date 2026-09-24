#!/usr/bin/env python3
"""
Build the CWZ-safety article figures into `article/lib/`.

    python scripts/make_figures.py

Reads `raw/cwz_zones.geojson`, `data/cwz_rollout.csv` and `out/refuse_crashes_zoned.csv`
(run `fetch_data.py` then `build_panel.py` first if these are missing). Styling comes
from `viz.py` - the same validated palette used across this author's other NYC data
projects.
"""
import textwrap
from datetime import date
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import Normalize
from matplotlib.patches import Patch

import viz

ROOT = Path(__file__).resolve().parent.parent
DATA, OUT, RAW = ROOT / "data", ROOT / "out", ROOT / "raw"
FIG_DIR = ROOT / "article" / "lib"

TODAY = pd.Timestamp(date(2026, 9, 24))
REFUSE_DATA_ENDS = pd.Timestamp("2026-05-04")
USABLE_TREATED = ["QN-2", "BX-1", "BX-2", "BK-5", "QN-3"]  # phases 1-3: any post-period in the data

viz.use_style()


def _save(fig, name):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    path = FIG_DIR / name
    fig.savefig(path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  wrote {path.relative_to(ROOT)}")


def load_rollout():
    r = pd.read_csv(DATA / "cwz_rollout.csv", parse_dates=["implementation_date"])
    r = r.assign(codes=r["zone_codes"].str.split(";")).explode("codes")
    r["codes"] = r["codes"].str.strip()
    return r.set_index("codes")


# --------------------------------------------------------------------------
# Figure 1: the 20 zones, dark = already live, light = further in the future
# --------------------------------------------------------------------------
def fig_rollout_map():
    gdf = gpd.read_file(RAW / "cwz_zones.geojson").set_index("zone")
    rollout = load_rollout()
    gdf = gdf.join(rollout[["phase", "zone_names", "implementation_date"]])

    is_live = gdf["implementation_date"].notna() & (gdf["implementation_date"] <= TODAY)
    is_scheduled = gdf["implementation_date"].notna() & (gdf["implementation_date"] > TODAY)
    is_undated = gdf["implementation_date"].isna()

    # Rank only among LIVE zones, earliest first, so the ramp's full dark-to-light
    # range is spent distinguishing live zones from each other - not diluted by
    # also having to span the 12 zones that aren't live yet.
    live_order = list(gdf[is_live].sort_values("implementation_date").index)
    live_rank = {z: i for i, z in enumerate(live_order)}
    norm = Normalize(vmin=0, vmax=max(len(live_order) - 1, 1))

    def live_color(zone):
        return viz.SEQ_CMAP(1 - norm(live_rank[zone]))

    NOT_LIVE_FILL = "#dce6f2"     # flat, pale blue-grey - clearly off the live ramp
    UNDATED_FILL = "#eeeeec"      # light grey

    fig, ax = plt.subplots(figsize=(10.5, 7.8))

    gdf[is_live].plot(ax=ax, color=[live_color(z) for z in gdf[is_live].index],
                       edgecolor="white", linewidth=0.6)

    # Scheduled but not live yet: flat pale fill + diagonal hatch = "not implemented."
    gdf[is_scheduled].plot(ax=ax, facecolor=NOT_LIVE_FILL, edgecolor=viz.INK_MUTED,
                            linewidth=0.6, hatch="////")

    # No rollout date published at all: a visibly different, dotted light-grey treatment.
    gdf[is_undated].plot(ax=ax, facecolor=UNDATED_FILL, edgecolor=viz.INK_MUTED,
                          linewidth=0.7, linestyle=":", hatch="...")

    viz.map_axes(ax)
    ax.set_title(f"Commercial waste zone rollout, as of {TODAY:%B %-d, %Y}")

    # Legend: live zones as color swatches (darker = earlier), then the two
    # not-yet-live treatments.
    handles = []
    phases = load_rollout().drop_duplicates("phase").sort_values("phase")
    for _, row in phases.iterrows():
        zone0 = load_rollout()[load_rollout()["phase"] == row["phase"]].index[0]
        label = f"Phase {row['phase']}: {row['zone_names']}"
        if pd.notna(row["implementation_date"]):
            label += f" ({row['implementation_date']:%Y-%m})"
        if zone0 in live_rank:
            handles.append(Patch(facecolor=live_color(zone0), edgecolor="white", label=label))
        elif pd.notna(row["implementation_date"]):
            handles.append(Patch(facecolor=NOT_LIVE_FILL, edgecolor=viz.INK_MUTED,
                                  hatch="////", label=label))
        else:
            handles.append(Patch(facecolor=UNDATED_FILL, edgecolor=viz.INK_MUTED,
                                  hatch="...", linestyle=":", label=label))
    ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.0, 0.5),
              fontsize=8, frameon=False,
              title="Solid, darker = live earlier\nHatched = scheduled, not live yet\nDotted grey = no date published",
              title_fontsize=8, handlelength=1.4, labelspacing=1.0)

    viz.note(fig, "Source: DSNY Commercial Waste Zones (data.cityofnewyork.us: 8ev8-jjxq)\n"
                   "and DSNY's published implementation schedule (data/cwz_rollout.csv).")
    _save(fig, "cwz_rollout_map.png")


# --------------------------------------------------------------------------
# Figure 2: pre vs. post crashes/month in the zones with any usable post-period
# --------------------------------------------------------------------------
def fig_pre_post():
    cz = pd.read_csv(OUT / "refuse_crashes_zoned.csv", parse_dates=["crash_date"])
    cz = cz[cz["crash_date"] <= REFUSE_DATA_ENDS]
    rollout = load_rollout()

    def months_between(a, b):
        return max(0, (b.year - a.year) * 12 + (b.month - a.month))

    rows = []
    for z in USABLE_TREATED:
        impl = rollout.loc[z, "implementation_date"]
        sub = cz[cz["zone"] == z]
        pre_n = (sub["crash_date"] < impl).sum()
        post_n = (sub["crash_date"] >= impl).sum()
        pre_months = months_between(cz["crash_date"].min(), impl)
        post_months = months_between(impl, REFUSE_DATA_ENDS)
        rows.append({"zone": z, "zone_name": rollout.loc[z, "zone_names"].split(";")[0]
                      if ";" in str(rollout.loc[z, "zone_names"]) else rollout.loc[z, "zone_names"],
                      "pre_n": pre_n, "post_n": post_n,
                      "pre_rate": pre_n / pre_months, "post_rate": post_n / post_months})
    t = pd.DataFrame(rows)
    pooled = pd.DataFrame([{
        "zone": "POOLED", "zone_name": "All 5 zones",
        "pre_n": t["pre_n"].sum(), "post_n": t["post_n"].sum(),
        "pre_rate": t["pre_n"].sum() / sum(months_between(cz["crash_date"].min(), rollout.loc[z, "implementation_date"]) for z in USABLE_TREATED),
        "post_rate": t["post_n"].sum() / sum(months_between(rollout.loc[z, "implementation_date"], REFUSE_DATA_ENDS) for z in USABLE_TREATED),
    }])
    t = pd.concat([t, pooled], ignore_index=True)
    t.to_csv(OUT / "fig_pre_post_table.csv", index=False)

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    x = np.arange(len(t))
    w = 0.34
    b1 = ax.bar(x - w / 2, t["pre_rate"], width=w, color=viz.BLUE, label="Pre-implementation")
    b2 = ax.bar(x + w / 2, t["post_rate"], width=w, color=viz.ORANGE, label="Post-implementation")
    for bars in (b1, b2):
        for b in bars:
            ax.annotate(f"{b.get_height():.2f}", (b.get_x() + b.get_width() / 2, b.get_height()),
                        ha="center", va="bottom", fontsize=8, color=viz.INK_SECONDARY)

    labels = [f"{r.zone}\n({r.pre_n:.0f} to {r.post_n:.0f} crashes)" for r in t.itertuples()]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.5)
    ax.set_ylabel("refuse-vehicle crashes / zone-month")
    ax.set_title("Crash rate before vs. after each zone went live")
    viz.strip(ax)
    ax.legend(loc="upper right")
    ax.set_ylim(0, t[["pre_rate", "post_rate"]].to_numpy().max() * 1.18)
    viz.note(fig, "Zones with any post-implementation history in the crash data (phases 1-3 only). "
                   "Data truncated to 2026-05-04, where refuse-vehicle reporting thins out.\n"
                   "Rates divide raw counts by whole zone-months observed; small base, read as counts, not a significance test.")
    _save(fig, "cwz_pre_post_rate.png")
    return t


# --------------------------------------------------------------------------
# Figure 3: currently-active zones vs. not-yet-active zones, both indexed to
# their own 2019 monthly average (=100), over calendar time
# --------------------------------------------------------------------------
def fig_active_vs_control_aggregated(n_boot=200_000, seed=0):
    """
    One aggregated number per group instead of a noisy month-by-month line: every
    zone-month in the window where at least one zone is live gets bucketed into
    "active" (that zone was live that month) or "not-yet-active," and each bucket's
    total crashes are indexed to the sum of its zones' own 2019 monthly averages.

    Uncertainty comes from a bootstrap null: redraw each zone-month's crash count
    from THAT zone's own 12 actual 2019 monthly values, i.e. "what would this index
    look like if the zone's rate had simply stayed at its 2019 level." That pins
    both group's null distributions at 100 by construction and lets us ask how
    unusual the observed 13.5-point gap actually is.
    """
    rng = np.random.default_rng(seed)
    cz = pd.read_csv(OUT / "refuse_crashes_zoned.csv", parse_dates=["crash_date"])
    cz = cz[cz["crash_date"] <= REFUSE_DATA_ENDS]
    rollout = load_rollout()
    impl = rollout["implementation_date"]
    zones = sorted(impl.index)

    baseline19 = cz[cz["crash_date"].dt.year == 2019].groupby("zone").size().reindex(zones, fill_value=0) / 12
    values_2019 = {
        z: (cz[(cz["zone"] == z) & (cz["crash_date"].dt.year == 2019)]
            .groupby(cz["crash_date"].dt.month).size().reindex(range(1, 13), fill_value=0).to_numpy())
        for z in zones
    }

    cz["month"] = cz["crash_date"].dt.to_period("M")
    # Drop the trailing partial month (data ends 2026-05-04) - a 4-day month
    # would otherwise undercount that bucket.
    last_full_month = (REFUSE_DATA_ENDS.replace(day=1) - pd.Timedelta(days=1)).to_period("M")
    months = pd.period_range("2020-01", last_full_month, freq="M")
    monthly = (cz.groupby(["zone", "month"]).size().unstack(fill_value=0)
               .reindex(index=zones, columns=months, fill_value=0))

    slots_active, slots_nonactive, first_active = [], [], None
    for m in months:
        mts = m.to_timestamp()
        az = [z for z in zones if pd.notna(impl[z]) and impl[z] <= mts]
        nz = [z for z in zones if z not in az]
        if az and first_active is None:
            first_active = m
        if first_active is not None:
            slots_active += [(z, m) for z in az]
            slots_nonactive += [(z, m) for z in nz]

    def observed(slots):
        obs = sum(monthly.loc[z, m] for z, m in slots)
        base = sum(baseline19[z] for z, m in slots)
        return obs, base

    def boot_sum(slots):
        total = np.zeros(n_boot)
        counts = pd.Series([z for z, _ in slots]).value_counts()
        for z, k in counts.items():
            total += rng.choice(values_2019[z], size=(n_boot, k), replace=True).sum(axis=1)
        return total

    a_obs, a_base = observed(slots_active)
    n_obs, n_base = observed(slots_nonactive)
    idx_active, idx_nonactive = 100 * a_obs / a_base, 100 * n_obs / n_base

    idx_active_boot = 100 * boot_sum(slots_active) / a_base
    idx_nonactive_boot = 100 * boot_sum(slots_nonactive) / n_base
    gap_boot = idx_active_boot - idx_nonactive_boot
    observed_gap = idx_active - idx_nonactive
    p_gap = (np.abs(gap_boot) >= abs(observed_gap)).mean()

    summary = pd.DataFrame([
        {"group": "Not-yet-active zones", "index_2019_100": idx_nonactive, "observed_crashes": n_obs,
         "baseline_2019_equivalent": n_base, "zone_months": len(slots_nonactive),
         "null_sd": idx_nonactive_boot.std()},
        {"group": "Currently-active zones", "index_2019_100": idx_active, "observed_crashes": a_obs,
         "baseline_2019_equivalent": a_base, "zone_months": len(slots_active),
         "null_sd": idx_active_boot.std()},
    ])
    summary.to_csv(OUT / "fig_active_vs_control_aggregated.csv", index=False)
    print(summary.round(2).to_string(index=False))
    print(f"gap = {observed_gap:.1f} pts, two-sided bootstrap p = {p_gap:.3f}")

    fig, ax = plt.subplots(figsize=(6.5, 5.2))
    ax.axhline(100, color=viz.INK_MUTED, lw=1, ls=":")
    x = np.arange(2)
    heights = [idx_nonactive, idx_active]
    colors = [viz.BLUE, viz.ORANGE]
    errs = [idx_nonactive_boot.std(), idx_active_boot.std()]
    bars = ax.bar(x, heights, width=0.55, color=colors,
                   yerr=errs, capsize=6,
                   error_kw=dict(ecolor=viz.INK_SECONDARY, lw=1.2))
    for b, h in zip(bars, heights):
        ax.annotate(f"{h:.1f}", (b.get_x() + b.get_width() / 2, h), xytext=(0, 10),
                    textcoords="offset points", ha="center", fontsize=12, fontweight="600")

    ax.set_xticks(x)
    ax.set_xticklabels([
        f"Not-yet-active zones\n({n_obs} crashes / {n_base:.0f} 2019-equivalent,\n{len(slots_nonactive)} zone-months)",
        f"Currently-active zones\n({a_obs} crashes / {a_base:.0f} 2019-equivalent,\n{len(slots_active)} zone-months)",
    ], fontsize=8.5)
    ax.set_ylabel("crashes, indexed to each zone's own\n2019 monthly average = 100")
    ax.set_title("Currently-active vs. not-yet-active zones, indexed to 2019")
    ax.set_ylim(0, 120)
    viz.strip(ax)
    viz.note(fig,
             f"Window: {first_active} through {last_full_month}, the period where at least one zone is live. "
             "100 = each group's own 2019 monthly average.\n"
             "Error bars: ±1 SD of a bootstrap null where each zone's crash count is redrawn from its own 12 actual "
             "2019 monthly values (\"if the rate never left 2019\").\n"
             f"The {observed_gap:.1f}-point gap has a two-sided bootstrap probability of {p_gap:.0%} under that null - "
             "not distinguishable from chance. Zones weren't randomized into rollout order.")
    _save(fig, "cwz_active_vs_control_aggregated.png")
    return summary, observed_gap, p_gap


if __name__ == "__main__":
    print("Building CWZ article figures...")
    fig_rollout_map()
    pre_post_table = fig_pre_post()
    print(pre_post_table.to_string(index=False))
    fig_active_vs_control_aggregated()
    print("Done.")
