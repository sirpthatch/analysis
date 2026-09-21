"""Figures for the writeup. Outputs to article/lib/.

    python make_figures.py column_status
    python make_figures.py all
"""

import sys

import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.colors import LinearSegmentedColormap, PowerNorm
from matplotlib.patches import FancyBboxPatch

from constants import DATA_PROCESSED, PROJECT_ROOT

OUT = PROJECT_ROOT / "article" / "lib"

# Chart chrome, from the design system's light mode.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"

# The three states are ordinal — full fidelity, partial, none — so they take a
# one-hue ramp rather than a status palette: the reader sees the order in the
# color. (A good/serious/critical status palette was the obvious first choice
# and fails CVD validation: green vs red is deltaE 4.1 under deuteranopia.)
RAMP = ["#104281", "#2a78d6", "#86b6ef"]

GROUPS = [
    {
        "label": "RECOVERED",
        "glyph": "●",
        "note": "Straight from TrafficStat, full fidelity",
        "color": RAMP[0],
        "header_ink": "#ffffff",
        "items": [
            ("crash_date", ""),
            ("latitude", ""),
            ("longitude", ""),
            ("location", ""),
        ],
    },
    {
        "label": "RECREATED",
        "glyph": "◐",
        "note": "Joined or derived · injury fields are boolean flags",
        "color": RAMP[1],
        "header_ink": "#ffffff",
        "items": [
            ("on_street_name", "street centerline · 88.2%"),
            ("zip_code", "street centerline · 95.0%"),
            ("borough", "from precinct · exact"),
            ("number_of_persons_injured", ""),
            ("number_of_persons_killed", ""),
            ("number_of_pedestrians_injured", ""),
            ("number_of_cyclist_injured", ""),
            ("number_of_motorist_injured", ""),
        ],
    },
    {
        "label": "UNRECOVERABLE",
        "glyph": "○",
        "note": "Present in the output, always null",
        "color": RAMP[2],
        "header_ink": INK,
        "items": [
            ("crash_time", "0% populated after June"),
            ("cross_street_name", ""),
            ("off_street_name", ""),
            ("number_of_pedestrians_killed", ""),
            ("number_of_cyclist_killed", ""),
            ("number_of_motorist_killed", ""),
            ("contributing_factor_vehicle_1", ""),
            ("contributing_factor_vehicle_2", ""),
            ("contributing_factor_vehicle_3", ""),
            ("contributing_factor_vehicle_4", ""),
            ("contributing_factor_vehicle_5", ""),
            ("vehicle_type_code1", ""),
            ("vehicle_type_code2", ""),
            ("vehicle_type_code_3", ""),
            ("vehicle_type_code_4", ""),
            ("vehicle_type_code_5", ""),
            ("collision_id", ""),
        ],
    },
]


def column_status():
    """Every Socrata column, grouped by whether the reconstruction recovers it."""
    fig, ax = plt.subplots(figsize=(14, 7.62), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 7.62)
    ax.axis("off")

    ax.text(0.45, 7.34, "What survives the reconstruction",
            fontsize=21, fontweight="bold", color=INK, ha="left", va="top")
    ax.text(0.45, 6.92,
            "All 29 columns of NYC's Motor Vehicle Collisions file (h9gi-nx95), "
            "rebuilt from NYPD TrafficStat for June 12 \u2013 September 13, 2026",
            fontsize=11.5, color=INK_2, ha="left", va="top")

    # Geometry, both dimensions checked against the content rather than guessed:
    # horizontally 0.45 + 2*(4.2+0.38) + 4.2 = 13.81 < 14 (a first pass clipped
    # the third card); vertically the 17-row card runs to
    # 6.48 - 0.66 - (17*0.285 + 0.2) - 0.04 = 0.74, clear of the footer at 0.45.
    col_w, gap, x0 = 4.2, 0.38, 0.45
    top, row_h, head_h = 6.48, 0.285, 0.66

    for g, group in enumerate(GROUPS):
        x = x0 + g * (col_w + gap)

        ax.add_patch(FancyBboxPatch(
            (x, top - head_h), col_w, head_h,
            boxstyle="round,pad=0,rounding_size=0.07",
            facecolor=group["color"], edgecolor="none", zorder=2))
        ax.text(x + 0.17, top - 0.25, group["glyph"],
                fontsize=11.5, color=group["header_ink"], va="center", zorder=3)
        ax.text(x + 0.46, top - 0.25, " ".join(group["label"]),
                fontsize=11, fontweight="bold", color=group["header_ink"],
                va="center", zorder=3)
        ax.text(x + col_w - 0.17, top - 0.25, str(len(group["items"])),
                fontsize=14, fontweight="bold", color=group["header_ink"],
                va="center", ha="right", zorder=3)
        ax.text(x + 0.17, top - 0.51, group["note"],
                fontsize=8.4, color=group["header_ink"], va="center",
                alpha=0.9, zorder=3)

        body_h = len(group["items"]) * row_h + 0.2
        ax.add_patch(FancyBboxPatch(
            (x, top - head_h - body_h - 0.04), col_w, body_h,
            boxstyle="round,pad=0,rounding_size=0.07",
            facecolor=group["color"], alpha=0.07, edgecolor="none", zorder=1))

        y = top - head_h - 0.24
        for name, note in group["items"]:
            ax.text(x + 0.17, y, name, fontsize=9.0, color=INK,
                    family="DejaVu Sans Mono", va="center", zorder=3)
            if note:
                ax.text(x + col_w - 0.17, y, note, fontsize=8.0, color=INK_2,
                        va="center", ha="right", style="italic", zorder=3)
            y -= row_h

    ax.text(0.45, 0.45,
            "Recreated fields are boolean flags, not counts: TrafficStat returns one row per collision, "
            "so \u201cwas anyone injured\u201d survives and \u201chow many\u201d does not.",
            fontsize=9.4, color=INK_2, ha="left", va="center")
    ax.text(0.45, 0.20,
            "Percentages are agreement with the official file over the five months both sources cover.",
            fontsize=9.4, color=INK_2, ha="left", va="center")

    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "column_status.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    total = sum(len(g["items"]) for g in GROUPS)
    print(f"{path.relative_to(PROJECT_ROOT)}  ({total} columns)")
    return path


# The documented sequential blue ramp, 100 -> 700. Sequential encoding gets the
# full range: the lightest step means "near zero" and may recede toward surface.
BLUE_RAMP = [
    "#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7",
    "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b",
]
CRASH_CMAP = LinearSegmentedColormap.from_list("crash_blue", BLUE_RAMP)


def _basemap(ax, centerline):
    """NYC street network, recessive — it orients the reader, nothing more."""
    segs = []
    for geom in centerline.geometry:
        if geom.geom_type == "LineString":
            segs.append(list(geom.coords))
        else:
            segs.extend(list(g.coords) for g in geom.geoms)
    ax.add_collection(LineCollection(
        segs, linewidths=0.10, colors="#c9c8c3", alpha=0.55, zorder=1))


def crash_map():
    """Where the crashes the public never saw actually happened."""
    import geopandas as gpd

    import reverse_geocode as rg

    d = pd.read_csv(DATA_PROCESSED / "reconstructed_crashes_gap.csv")
    pts = rg.to_points(d)                      # projected to EPSG:2263, feet
    cl = rg.load_centerline()

    fig = plt.figure(figsize=(15.5, 9.4), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    # Panel aspect is matched to the city's own: NYC's bounding box is 152,560 x
    # 148,581 ft (1.03:1), so a much wider panel would just render dead space
    # either side of an equal-aspect map.
    gs = fig.add_gridspec(1, 2, width_ratios=[1.15, 1],
                          left=0.022, right=0.978, top=0.815, bottom=0.088,
                          wspace=0.12)

    # ---- map -------------------------------------------------------------
    ax = fig.add_subplot(gs[0, 0])
    ax.set_facecolor(SURFACE)
    _basemap(ax, cl)
    # Counts are heavily skewed — median hex 10, max 86 — so a linear ramp puts
    # half the city in the bottom eighth of the scale and washes the structure
    # out. Log over-corrects the other way: counts of 1-3 read as substantial and
    # the whole city goes dark. A square-root power norm sits between the two and
    # keeps both ends legible. The colorbar is labelled in real counts.
    hb = ax.hexbin(pts.geometry.x, pts.geometry.y, gridsize=62, mincnt=1,
                   cmap=CRASH_CMAP, linewidths=0, zorder=2,
                   norm=PowerNorm(gamma=0.5))
    ax.set_aspect("equal")
    ax.axis("off")
    minx, miny, maxx, maxy = cl.total_bounds
    padx, pady = (maxx - minx) * 0.012, (maxy - miny) * 0.012
    ax.set_xlim(minx - padx, maxx + padx)
    ax.set_ylim(miny - pady, maxy + pady)

    # Upper-left of the panel is open water and New Jersey — the one reliably
    # empty area, so the legend goes there rather than over Staten Island.
    cax = ax.inset_axes([0.028, 0.760, 0.30, 0.019])
    cb = fig.colorbar(hb, cax=cax, orientation="horizontal",
                      ticks=[1, 10, 30, 60, 86])
    cb.outline.set_visible(False)
    cb.ax.set_xticklabels(["1", "10", "30", "60", "86"])
    cb.ax.tick_params(labelsize=8.5, colors=INK_2, length=0, pad=3)
    cb.set_label("crashes per hex", fontsize=8.8, color=INK_2, labelpad=5)

    # Labels sit in open space beside each borough, not on top of it: over the
    # hex mass they were unreadable whatever the ink.
    for name, (lon, lat) in {
        "BRONX": (-73.762, 40.898), "MANHATTAN": (-74.035, 40.742),
        "QUEENS": (-73.788, 40.648), "BROOKLYN": (-74.062, 40.600),
        "STATEN ISLAND": (-74.222, 40.648),
    }.items():
        pt = gpd.GeoSeries.from_xy([lon], [lat], crs="EPSG:4326").to_crs("EPSG:2263")
        ax.text(pt.x[0], pt.y[0], name, fontsize=8.8, color=INK_2,
                ha="center", va="center", zorder=4,
                path_effects=[pe.withStroke(linewidth=2.6, foreground=SURFACE)])

    # ---- ranked streets --------------------------------------------------
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor(SURFACE)
    top = d.on_street_name.value_counts().head(16).sort_values()
    bars = ax2.barh(range(len(top)), top.values, height=0.72,
                    color="#2a78d6", zorder=2)
    ax2.set_yticks(range(len(top)))
    ax2.set_yticklabels([t.title() for t in top.index], fontsize=9.4, color=INK)
    ax2.set_xlim(0, top.max() * 1.16)
    ax2.set_xlabel("crashes, June 12 \u2013 September 13", fontsize=9.2, color=INK_2)
    ax2.tick_params(axis="x", labelsize=8.8, colors=INK_2, length=0)
    ax2.tick_params(axis="y", length=0)
    for sp in ("top", "right", "left", "bottom"):
        ax2.spines[sp].set_visible(False)
    ax2.grid(axis="x", color="#e6e5e1", lw=0.7, zorder=0)
    ax2.set_axisbelow(True)
    for b, v in zip(bars, top.values):
        ax2.text(v + top.max() * 0.018, b.get_y() + b.get_height() / 2, f"{v}",
                 va="center", fontsize=8.8, color=INK_2)
    ax2.set_title("Worst 16 corridors", fontsize=12, fontweight="bold",
                  color=INK, loc="left", pad=11)
    ax.set_title("Citywide density", fontsize=12, fontweight="bold",
                 color=INK, loc="left", pad=11)

    # ---- titles ----------------------------------------------------------
    fig.text(0.022, 0.962, "The crashes New York couldn\u2019t see",
             fontsize=22, fontweight="bold", color=INK, ha="left", va="top")
    fig.text(0.022, 0.917,
             f"{len(d):,} collisions between June 12 and September 13, 2026, reconstructed from NYPD TrafficStat "
             f"after the city\u2019s public crash file stopped updating",
             fontsize=11.5, color=INK_2, ha="left", va="top")
    fig.text(0.022, 0.030,
             "Reconstructed from NYPD TrafficStat; street names matched to the city street centerline (88.2% agreement). "
             "Highway crashes are positioned to the nearest mile marker.",
             fontsize=9.0, color=INK_2, ha="left", va="center")

    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "crash_map.png"
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    print(f"{path.relative_to(PROJECT_ROOT)}  ({len(d):,} crashes)")
    return path


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "column_status":
        column_status()
    elif cmd == "crash_map":
        crash_map()
    elif cmd == "all":
        column_status()
        crash_map()
    else:
        print(__doc__)
        sys.exit(1)
