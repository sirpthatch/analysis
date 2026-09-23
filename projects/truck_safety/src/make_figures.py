"""Build the article figures into `article/lib/`.

    python src/make_figures.py

Every figure reads the cached pulls, so it regenerates from scratch after
`collect.py` / `collect_geo.py` / `routes.py`.  Styling comes from `viz.py`:
one validated palette, no second y-axis, direct labels on every chart because
these render as static images with no hover layer.
"""

import re

import geopandas as gpd
import numpy as np
import pandas as pd
from matplotlib.colors import TwoSlopeNorm
import matplotlib.pyplot as plt

import viz
from vehicle_codes import TRUCK_CORE
from constants import (
    EXTERNAL_FILES,
    PROCESSED_DIR,
    RAW_FILES,
    ROUTE_BUFFER_M,
    _PROJECT_ROOT,
)

FIG_DIR = _PROJECT_ROOT / "article" / "lib"
METRIC_CRS = 32618

# Chassis models built specifically for refuse collection.  Licensees fill in
# `vehicle_body_type` loosely -- only 5 front-end loaders are coded in a 7,101
# vehicle fleet -- so refuse vehicles are identified by chassis too.
REFUSE_CHASSIS = re.compile(
    r"^(LR|LEU|LE|MR|MRU|TERRAPRO|ACX|WX|WXLL|XPEDITOR|LET|LDT|PB520|520|CONDOR)"
)
REFUSE_BODIES = ["Rear End Loader", "Front End Loader", "Side Loader", "Dual Bin"]
BELOW_THRESHOLD = ["< 6,000 lbs.", "6,001 - 10,000 lbs."]


def _save(fig, name):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    path = FIG_DIR / name
    fig.savefig(path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  wrote {path.relative_to(_PROJECT_ROOT)}")


# --------------------------------------------------------------------------
# Truck routes
# --------------------------------------------------------------------------

def _crash_points():
    points = pd.read_parquet(PROCESSED_DIR / "crash_points.parquet")
    geo = gpd.GeoDataFrame(
        points,
        geometry=gpd.points_from_xy(points.longitude, points.latitude),
        crs=4326,
    ).to_crs(METRIC_CRS)
    geo["x"], geo["y"] = geo.geometry.x, geo.geometry.y
    return geo


def fig_route_share():
    """On-route share, trucks against everything else."""
    shares = pd.read_csv(PROCESSED_DIR / "route_share.csv")
    order = ["Truck (core)", "Truck (incl. light commercial)", "No truck involved"]
    labels = ["Heavy trucks", "Trucks incl. light commercial", "No truck involved"]
    sub = shares.set_index("group").loc[order]

    fig, ax = plt.subplots(figsize=(8, 3))
    viz.strip(ax, keep_grid="x")
    bars = ax.barh(range(len(sub)), sub[f"on_route_{ROUTE_BUFFER_M}m"],
                   color=[viz.BLUE, viz.BLUE, viz.ORANGE], height=0.6)
    ax.set_yticks(range(len(sub)), labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 75)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.set_title(f"Crashes within {ROUTE_BUFFER_M} m of a designated truck route")

    for rect, value, n in zip(bars, sub[f"on_route_{ROUTE_BUFFER_M}m"], sub["crashes"]):
        ax.text(value + 1.2, rect.get_y() + rect.get_height() / 2,
                f"{value:.1f}%   (n={n:,})", va="center", fontsize=9, color=viz.INK)

    viz.note(fig, "NYPD collision data 2016–2026, geocoded crashes only. The gap holds "
                  "at 15 m and 50 m too. Groups overlap: heavy trucks are a subset of "
                  "the second bar.")
    return fig


def fig_truck_share_map():
    """Where trucks are over-represented in crashes."""
    geo = _crash_points()
    boroughs = gpd.read_file(EXTERNAL_FILES["boroughs"]).to_crs(METRIC_CRS)
    citywide = geo.is_truck_core.mean()

    fig, ax = plt.subplots(figsize=(8.8, 7.0), layout="constrained")
    viz.map_axes(ax)
    boroughs.plot(ax=ax, facecolor="#f2f1ed", edgecolor="#ffffff", linewidth=1.2)
    hb = ax.hexbin(geo.x, geo.y, C=geo.is_truck_core.astype(float),
                   reduce_C_function=np.mean, gridsize=95, mincnt=40,
                   cmap=viz.DIV_CMAP,
                   norm=TwoSlopeNorm(vmin=0, vcenter=citywide, vmax=0.25),
                   linewidths=0.0)

    bar = fig.colorbar(hb, ax=ax, shrink=0.78, pad=0.01)
    bar.set_label("Share of crashes involving a truck", fontsize=9, color=viz.INK_SECONDARY)
    bar.outline.set_visible(False)
    bar.ax.tick_params(labelsize=8, color=viz.GRID)
    bar.ax.axhline(citywide, color=viz.INK, linewidth=1.2)
    bar.ax.text(-0.35, citywide, f"citywide {citywide:.1%}", va="center", ha="right",
                fontsize=8, color=viz.INK, transform=bar.ax.get_yaxis_transform())
    bar.ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0%}")

    ax.set_title("Where trucks are over-represented in crashes")
    viz.note(fig, "Red = trucks involved in a higher share of crashes than the citywide "
                  "average; blue = lower. Hexes with fewer than 40 crashes omitted.", y=0.035)
    return fig


def fig_off_route_map():
    """Truck crashes away from the designated network."""
    geo = _crash_points()
    truck = geo[geo.is_truck_core]
    off = truck[truck.dist_m > ROUTE_BUFFER_M]
    boroughs = gpd.read_file(EXTERNAL_FILES["boroughs"]).to_crs(METRIC_CRS)
    routes = gpd.read_file(EXTERNAL_FILES["truck_routes"]).to_crs(METRIC_CRS)

    fig, ax = plt.subplots(figsize=(8.8, 7.0), layout="constrained")
    viz.map_axes(ax)
    boroughs.plot(ax=ax, facecolor="#f2f1ed", edgecolor="#ffffff", linewidth=1.2)
    routes.plot(ax=ax, color="#c9c8c2", linewidth=0.45)
    hb = ax.hexbin(off.x, off.y, gridsize=95, bins="log",
                   cmap=viz.SEQ_CMAP_ORANGE, mincnt=1, linewidths=0.0)

    bar = fig.colorbar(hb, ax=ax, shrink=0.78, pad=0.01)
    bar.set_label("Off-route truck crashes per hex (log scale)", fontsize=9,
                  color=viz.INK_SECONDARY)
    bar.outline.set_visible(False)
    bar.ax.tick_params(labelsize=8, color=viz.GRID)

    ax.set_title(f"Truck crashes more than {ROUTE_BUFFER_M} m from any truck route")
    viz.note(fig, f"{len(off):,} crashes — {len(off)/len(truck):.0%} of all truck crashes. "
                  "Grey lines are the designated network. Leaving it to reach a "
                  "destination is permitted, so off-route is not off-limits.", y=0.035)
    return fig


def fig_truck_overrepresentation():
    """Trucks' share of crashes against their share of the harm.

    One series -- trucks' share of each outcome -- so one colour and no legend.
    The crash share doubles as the reference line: every bar above it is
    over-representation.
    """
    vehicles = pd.read_parquet(RAW_FILES["truck_vehicles"])
    vehicles["vt"] = vehicles.vehicle_type.fillna("").str.upper()
    core = set(vehicles.loc[vehicles.vt.isin(TRUCK_CORE), "collision_id"])

    crashes = pd.read_parquet(RAW_FILES["all_crashes"])
    crashes["yr"] = crashes.crash_date.str[:4].astype(int)
    crashes = crashes[crashes.yr <= 2025]          # complete years only
    crashes["truck"] = crashes.collision_id.isin(core)
    for column in ("number_of_persons_killed", "number_of_cyclist_killed",
                   "number_of_pedestrians_killed"):
        crashes[column] = pd.to_numeric(crashes[column], errors="coerce").fillna(0)

    truck = crashes[crashes.truck]
    rows = [
        ("Share of all crashes", crashes.truck.mean() * 100),
        ("Share of pedestrian deaths",
         truck.number_of_pedestrians_killed.sum() / crashes.number_of_pedestrians_killed.sum() * 100),
        ("Share of all traffic deaths",
         truck.number_of_persons_killed.sum() / crashes.number_of_persons_killed.sum() * 100),
        ("Share of cyclist deaths",
         truck.number_of_cyclist_killed.sum() / crashes.number_of_cyclist_killed.sum() * 100),
    ]
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    baseline = values[0]

    fig, ax = plt.subplots(figsize=(8.4, 3.2))
    viz.strip(ax, keep_grid="x")
    bars = ax.barh(range(len(rows)), values, color=viz.BLUE, height=0.62)
    ax.set_yticks(range(len(rows)), labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, 36)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    # The dashed line extends from the first bar, which is already labelled --
    # no second annotation needed, and it would land on the axis ticks.
    ax.axvline(baseline, color=viz.INK_MUTED, linewidth=1.2, linestyle=(0, (4, 3)),
               zorder=0)
    ax.set_title("Trucks are in one crash in thirteen — and one cyclist death in four")

    for rect, value in zip(bars, values):
        ax.text(value + 0.6, rect.get_y() + rect.get_height() / 2, f"{value:.1f}%",
                va="center", fontsize=9, color=viz.INK)

    viz.note(fig, "NYPD collision data, complete years 2016–2025. 118,860 truck-involved "
                  "crashes of 1,504,722. Dashed line marks the 7.9% crash share; bars "
                  "beyond it are over-representation. Involvement is not fault — the "
                  "crash record does not say which vehicle was responsible.")
    fig.tight_layout()
    return fig


def fig_refuse_divergence():
    """Everything fell; refuse trucks fell less. Two panels, never two y-axes.

    Panel one indexes both series to 2016 = 100, which is the only honest way
    to put a count of 230,000 and a count of 552 on one scale. Panel two shows
    what that divergence produces: refuse crashes as a rate of all crashes.
    """
    harm = pd.read_csv(PROCESSED_DIR / "refuse_harm_by_year.csv")
    harm = harm[harm.year <= 2025]          # 2026 is a partial year
    harm["idx_all"] = harm.all_crashes / harm.all_crashes.iloc[0] * 100
    harm["idx_refuse"] = harm.crashes / harm.crashes.iloc[0] * 100

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2))

    ax = viz.strip(axes[0])
    ax.plot(harm.year, harm.idx_refuse, color=viz.BLUE, marker="o",
            label="Crashes involving a refuse truck")
    ax.plot(harm.year, harm.idx_all, color=viz.ORANGE, marker="o",
            label="All NYC crashes")
    ax.axhline(100, color=viz.GRID, linewidth=1.0, zorder=0)
    ax.set_ylim(0, 155)
    ax.set_title("Both fell — but not by the same amount")
    ax.set_ylabel("Indexed, 2016 = 100", fontsize=9)
    ax.legend(loc="lower left", fontsize=8.5)

    for series, colour, offset in ((harm.idx_refuse, viz.BLUE, 11),
                                   (harm.idx_all, viz.ORANGE, -16)):
        ax.annotate(f"{series.iloc[-1]:.0f}", (2025, series.iloc[-1]),
                    textcoords="offset points", xytext=(-6, offset),
                    ha="right", fontsize=9, color=colour, fontweight="600")

    ax = viz.strip(axes[1])
    ax.plot(harm.year, harm.per_10k_crashes, color=viz.BLUE, marker="o")
    ax.set_ylim(0, 58)
    ax.set_title("So refuse trucks became a bigger share of what is left")
    ax.set_ylabel("Refuse crashes per 10,000 city crashes", fontsize=9)
    for year in (2016, 2025):
        row = harm[harm.year == year].iloc[0]
        ax.annotate(f"{row.per_10k_crashes:.0f}", (year, row.per_10k_crashes),
                    textcoords="offset points",
                    xytext=(8, -4) if year == 2016 else (-8, 8),
                    ha="left" if year == 2016 else "right",
                    fontsize=9, color=viz.INK, fontweight="600")

    for ax in axes:
        ax.set_xticks(range(2016, 2026, 2))

    viz.note(fig, "NYPD collision data, complete years 2016–2025. Refuse = NYPD "
                  "vehicle_type 'GARBAGE OR REFUSE', which mixes private carters, DSNY "
                  "municipal trucks and some street sweepers — the crash record cannot "
                  "separate them. Absolute refuse crashes fell 27% (552 to 402); the "
                  "rise is relative.")
    fig.tight_layout()
    return fig


# --------------------------------------------------------------------------
# Side guards
# --------------------------------------------------------------------------

def _bic_covered():
    """BIC fleet, restricted to the vehicles the rule covers (>10,000 lbs)."""
    fleet = pd.read_csv(RAW_FILES["bic_fleet"], dtype=str)
    fleet = fleet[~fleet.vehicle_gross_vehicle_weight.isin(BELOW_THRESHOLD)
                  & fleet.vehicle_gross_vehicle_weight.notna()].copy()

    model = fleet.vehicle_model.fillna("").str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
    is_refuse = model.str.match(REFUSE_CHASSIS) | fleet.vehicle_body_type.isin(REFUSE_BODIES)

    fleet["group"] = np.where(is_refuse, "Refuse collection",
                     np.where(fleet.vehicle_body_type == "Dump Truck", "Dump truck",
                     np.where(fleet.vehicle_body_type == "Roll-off Truck", "Roll-off",
                     np.where(fleet.vehicle_body_type == "Tractor", "Tractor",
                     np.where(fleet.vehicle_body_type == "Truck", "Truck (unspecified)",
                              "Other commercial")))))
    fleet["guarded"] = fleet.vehicle_has_side_guard == "Yes"
    return fleet


def fig_sideguard_by_type():
    """The compliance story: high where it is visible, low where the volume is."""
    fleet = _bic_covered()
    table = fleet.groupby("group")["guarded"].agg(n="size", yes="sum")
    table["pct"] = table.yes / table.n * 100
    table["without"] = table.n - table.yes
    table = table.sort_values("pct", ascending=True)

    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))

    ax = viz.strip(axes[0], keep_grid="x")
    bars = ax.barh(range(len(table)), table["pct"], color=viz.BLUE, height=0.62)
    ax.set_yticks(range(len(table)), table.index, fontsize=9)
    ax.set_xlim(0, 118)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.set_title("Share with a side guard")
    for rect, value in zip(bars, table["pct"]):
        ax.text(value + 2, rect.get_y() + rect.get_height() / 2, f"{value:.1f}%",
                va="center", fontsize=9, color=viz.INK)

    ax = viz.strip(axes[1], keep_grid="x")
    bars = ax.barh(range(len(table)), table["without"], color=viz.ORANGE, height=0.62)
    ax.set_yticks(range(len(table)), [""] * len(table))
    ax.set_xlim(0, table["without"].max() * 1.28)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:,.0f}")
    ax.set_title("Vehicles with no side guard")
    for rect, value, n in zip(bars, table["without"], table["n"]):
        ax.text(value + table["without"].max() * 0.02,
                rect.get_y() + rect.get_height() / 2, f"{value:,}  of {n:,}",
                va="center", fontsize=9, color=viz.INK)

    viz.note(fig, "BIC Licensees and Registrants Fleet Information, exported 2026-09-21; "
                  "vehicles over the rule's 10,000 lb threshold. Side-guard status is "
                  "reported by the licensee. Refuse collection identified by chassis "
                  "model as well as body type.")
    fig.tight_layout()
    return fig


def fig_sideguard_by_weight():
    """The same fleet cut by weight — a much weaker pattern than by type."""
    fleet = pd.read_csv(RAW_FILES["bic_fleet"], dtype=str)
    order = ["6,001 - 10,000 lbs.", "10,001 - 14,000 lbs.", "14,001 - 16,000 lbs.",
             "16,001 - 19,500 lbs.", "19,501 - 26,000 lbs.", "26,001 - 33,000 lbs.",
             "33,001 lbs. or greater"]
    labels = ["6,001–10,000", "10,001–14,000", "14,001–16,000", "16,001–19,500",
              "19,501–26,000", "26,001–33,000", "33,001 +"]
    sub = fleet[fleet.vehicle_gross_vehicle_weight.isin(order)]
    table = sub.groupby("vehicle_gross_vehicle_weight")["vehicle_has_side_guard"].agg(
        n="size", yes=lambda s: (s == "Yes").sum()).reindex(order)
    table["pct"] = table.yes / table.n * 100

    fig, ax = plt.subplots(figsize=(8, 3.4))
    viz.strip(ax)
    bars = ax.bar(range(len(table)), table["pct"], color=viz.BLUE, width=0.62)
    ax.set_xticks(range(len(table)), labels, fontsize=8, rotation=20, ha="right")
    ax.set_ylim(0, 85)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.set_xlabel("Gross vehicle weight rating (lbs)")
    ax.set_title("Share with a side guard, by vehicle weight")
    for rect, value, n in zip(bars, table["pct"], table["n"]):
        ax.text(rect.get_x() + rect.get_width() / 2, value + 2,
                f"{value:.0f}%\nn={n:,}", ha="center", fontsize=8, color=viz.INK)

    viz.note(fig, "Weight alone predicts compliance poorly — the heaviest band is "
                  "43.1%, in the middle of the range. Vehicle type separates far more "
                  "sharply. Vehicles under 10,000 lbs are outside the rule.")
    fig.tight_layout()
    return fig


def main():
    viz.use_style()
    print("=== truck routes ===")
    _save(fig_truck_overrepresentation(), "truck_overrepresentation.png")
    _save(fig_route_share(), "route_on_off_share.png")
    _save(fig_truck_share_map(), "truck_share_map.png")
    _save(fig_off_route_map(), "off_route_map.png")
    print("=== side guards ===")
    _save(fig_refuse_divergence(), "refuse_divergence.png")
    _save(fig_sideguard_by_type(), "sideguard_by_type.png")
    _save(fig_sideguard_by_weight(), "sideguard_by_weight.png")


if __name__ == "__main__":
    main()
