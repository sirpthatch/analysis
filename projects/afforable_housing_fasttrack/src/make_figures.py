"""Regenerate every article figure into `article/lib/`.

    python src/make_figures.py
    python src/make_figures.py --only fast_track_map
"""

import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

import charts
import fasttrack as ft
from constants import (
    BOROUGH_NAMES,
    COMMUNITY_DISTRICTS,
    EXTERNAL_FILES,
    FAST_TRACK_LIST_SIZE,
    MAP_CRS,
    to_hpd_district,
)

FIGURE_DIR = ft.PROCESSED_DIR.parent.parent / "article" / "lib"

# Community district names.  DCP's boundary file carries only the numeric
# code, and a map keyed "BK-10" tells a reader nothing - the whole point of
# the finding is *which neighbourhoods* these are.
DISTRICT_NAMES = {
    "MN-02": "Greenwich Village & SoHo",
    "MN-05": "Midtown",
    "MN-07": "Upper West Side",
    "MN-08": "Upper East Side",
    "BK-10": "Bay Ridge",
    "BK-12": "Borough Park",
    "BK-18": "Canarsie",
    "QN-05": "Ridgewood",
    "QN-10": "Howard Beach",
    "QN-11": "Bayside",
    "QN-13": "Queens Village",
    "SI-02": "Mid-Island",
    "SI-03": "South Shore",
    "BK-11": "Bensonhurst & Bath Beach",
    "QN-03": "Jackson Heights",
    "QN-04": "Elmhurst & Corona",
    "QN-09": "Richmond Hill & Woodhaven",
}


def load_districts(table=None):
    """District polygons joined to the ranking, in a projected CRS.

    Socrata serves this layer tagged EPSG:4326 while the coordinates are
    already NY State Plane feet, so reprojecting it on the declared CRS sends
    every vertex to infinity.  Detect it by magnitude and override the tag
    rather than transform.
    """
    shapes = gpd.read_file(EXTERNAL_FILES["community_district_geom"])
    shapes = shapes[shapes["commntydst"].isin(COMMUNITY_DISTRICTS)].copy()
    shapes["district"] = shapes["commntydst"].map(to_hpd_district)

    if abs(shapes.total_bounds).max() > 360:
        shapes = shapes.set_crs(MAP_CRS, allow_override=True)
    shapes = shapes.to_crs(MAP_CRS)

    if table is None:
        table = ft.rank_districts()
    return shapes.merge(table.reset_index(), on="district", how="left")


def fast_track_map(table=None, path=None):
    """The 59 community districts, with the projected twelve picked out."""
    districts = load_districts(table)
    selected = districts[districts["on_fast_track"]].sort_values("rate_rank")

    # The axis is off, so the title cannot hang off it - it is drawn on the
    # figure, and the map gets the space that is left.
    fig = plt.figure(figsize=(13, 8.6))
    ax = fig.add_axes([0.01, 0.02, 0.62, 0.82])
    fig.text(
        0.01, 0.965,
        "The twelve community districts projected for the affordable housing fast track",
        ha="left", va="top", fontsize=16, fontweight="600", color=charts.INK,
    )
    fig.text(
        0.01, 0.915,
        "Lowest rate of affordable housing development, July 2021 - June 2026, under the "
        "methodology the City Planning Commission adopted in April 2026.\n"
        "Reverse-engineered from HPD, DOB and DCP open data. DCP publishes the official "
        "list by October 1, 2026.",
        ha="left", va="top", fontsize=10, color=charts.INK_SECONDARY, linespacing=1.6,
    )

    districts.plot(
        ax=ax, color=charts.CONTEXT_FILL, edgecolor=charts.CONTEXT_EDGE, linewidth=0.9
    )
    selected.plot(
        ax=ax, color=charts.HIGHLIGHT, edgecolor=charts.HIGHLIGHT_EDGE, linewidth=1.0
    )

    # A numbered marker rather than a name on the polygon: the Manhattan
    # districts are far too narrow to carry "Upper West Side" without
    # colliding, and the numbers double as the rate ranking.
    for _, row in selected.iterrows():
        point = row.geometry.representative_point()
        ax.plot(
            point.x, point.y, marker="o", markersize=14,
            color="#ffffff", markeredgecolor=charts.HIGHLIGHT_EDGE,
            markeredgewidth=1.2, zorder=5,
        )
        ax.text(
            point.x, point.y, str(int(row["rate_rank"])), ha="center", va="center",
            fontsize=8, fontweight="600", color=charts.HIGHLIGHT_EDGE, zorder=6,
        )

    ax.set_axis_off()
    ax.set_aspect("equal")

    _draw_key(fig, selected)
    # The legend sits under the key rather than in the map's bottom-left
    # corner, which Staten Island occupies.
    fig.legend(
        handles=[
            Patch(facecolor=charts.HIGHLIGHT, edgecolor=charts.HIGHLIGHT_EDGE,
                  label="Projected fast-track district"),
            Patch(facecolor=charts.CONTEXT_FILL, edgecolor="#d8d5cc",
                  label="Other community district"),
        ],
        loc="upper left", bbox_to_anchor=(0.655, 0.235), frameon=False, fontsize=10,
    )

    fig.text(
        0.655, 0.125,
        "* Rate not measurable from public data. The rule counts an affordable unit\n"
        "only if its building can be matched to a Department of Buildings construction\n"
        "permit. Every unit these two districts produced this cycle fails that match:\n"
        "Bay Ridge's because HPD redacts the address of small owner-occupied homes,\n"
        "Midtown's because its one project is missing its building identifier. Both\n"
        "belong in the twelve under every reading tested, but both built more than\n"
        "zero. Every other district on the list is understated the same way, by less.",
        ha="left", va="top", fontsize=8.5, color=charts.INK_MUTED, linespacing=1.7,
    )

    return _save(fig, path or FIGURE_DIR / "fast_track_map.png")


# --- Comparison map: what counting existing stock would change -------------
# Three categories, so the first three validated categorical slots, which are
# the ones that clear the all-pairs gates a choropleth needs.  Every district
# is also numbered and keyed, so identity never rests on colour alone - which
# is what the contrast WARN on the aqua slot obliges.
MEMBERSHIP_COLOURS = {
    "both": ("#2a78d6", "#1c5cab", "On both lists"),
    "adopted_rule_only": ("#eb6834", "#b8461d", "Only under the adopted rule"),
    "stock_adjusted_only": ("#1baf7a", "#0f7a54", "Only if existing stock counted"),
}
MEMBERSHIP_ORDER = ("both", "adopted_rule_only", "stock_adjusted_only")

KEY_HEADINGS = {
    "both": "On the list either way",
    "adopted_rule_only": "Listed only because existing stock is ignored",
    "stock_adjusted_only": "Would be listed if existing stock counted",
}


def stock_comparison_map(combined=None, path=None):
    """Which districts the list gains and loses if existing stock counts."""
    if combined is None:
        combined = ft.rank_with_existing_stock()
    districts = load_districts(combined)

    order = {name: n for n, name in enumerate(MEMBERSHIP_ORDER)}
    marked = districts[districts["list_membership"] != "neither"].copy()
    marked["group_order"] = marked["list_membership"].map(order)
    marked = marked.sort_values(["group_order", "stock_adjusted_rank"])
    marked["label"] = range(1, len(marked) + 1)

    fig = plt.figure(figsize=(13.5, 9.6))
    ax = fig.add_axes([0.005, 0.02, 0.58, 0.83])

    fig.text(
        0.01, 0.972,
        "Counting the affordable housing a district already has would replace "
        "five of the twelve",
        ha="left", va="top", fontsize=15.5, fontweight="600", color=charts.INK,
    )
    fig.text(
        0.01, 0.928,
        "The adopted rule divides new affordable units by a district's total housing stock, and "
        "ignores the affordable\nhousing already standing there. Rulemaking commenters asked for "
        "that to change; the Commission declined.\nBelow, the same cycle ranked both ways.",
        ha="left", va="top", fontsize=10, color=charts.INK_SECONDARY, linespacing=1.6,
    )

    districts.plot(ax=ax, color=charts.CONTEXT_FILL,
                   edgecolor=charts.CONTEXT_EDGE, linewidth=0.9)
    for group, (fill, edge, _) in MEMBERSHIP_COLOURS.items():
        part = marked[marked["list_membership"] == group]
        if not part.empty:
            part.plot(ax=ax, color=fill, edgecolor=edge, linewidth=1.0)

    for _, row in marked.iterrows():
        point = row.geometry.representative_point()
        edge = MEMBERSHIP_COLOURS[row["list_membership"]][1]
        ax.plot(point.x, point.y, marker="o", markersize=14, color="#ffffff",
                markeredgecolor=edge, markeredgewidth=1.2, zorder=5)
        ax.text(point.x, point.y, str(int(row["label"])), ha="center", va="center",
                fontsize=8, fontweight="600", color=edge, zorder=6)

    ax.set_axis_off()
    ax.set_aspect("equal")

    _draw_comparison_key(fig, marked)
    return _save(fig, path or FIGURE_DIR / "stock_comparison_map.png")


def _draw_comparison_key(fig, marked):
    """Three grouped sections, each row showing both rates side by side."""
    left, y = 0.60, 0.865
    fig.text(0.885, y, "adopted", ha="right", va="center", fontsize=8,
             color=charts.INK_MUTED, family="monospace")
    fig.text(0.985, y, "with stock", ha="right", va="center", fontsize=8,
             color=charts.INK_MUTED, family="monospace")
    y -= 0.030

    for group in MEMBERSHIP_ORDER:
        rows = marked[marked["list_membership"] == group]
        if rows.empty:
            continue
        fill, edge, _ = MEMBERSHIP_COLOURS[group]

        fig.text(left, y, "\u25a0", ha="left", va="center", fontsize=11, color=fill)
        fig.text(left + 0.022, y, KEY_HEADINGS[group], ha="left", va="center",
                 fontsize=9.5, fontweight="600", color=charts.INK)
        y -= 0.036

        for _, row in rows.iterrows():
            district = row["district"]
            fig.text(left + 0.012, y, f"{int(row['label']):>2}", ha="left", va="center",
                     fontsize=9, fontweight="600", color=edge, family="monospace")
            fig.text(left + 0.045, y, district, ha="left", va="center", fontsize=9,
                     color=charts.INK_SECONDARY, family="monospace")
            fig.text(left + 0.105, y, DISTRICT_NAMES.get(district, ""), ha="left",
                     va="center", fontsize=9.5, color=charts.INK)
            fig.text(0.885, y, f"{row['rate_pct']:.3f}%", ha="right", va="center",
                     fontsize=9, color=charts.INK_SECONDARY, family="monospace")
            fig.text(0.985, y, f"{row['stock_adjusted_pct']:.3f}%", ha="right",
                     va="center", fontsize=9, color=charts.INK_SECONDARY,
                     family="monospace")
            y -= 0.0335
        y -= 0.014

    fig.text(
        0.60, y - 0.005,
        "Existing stock counted here is NYCHA public housing plus HPD-counted units created or\n"
        "preserved before this cycle. It leaves out Mitchell-Lama, HDFC co-ops, older LIHTC\n"
        "properties and rent-stabilized private housing, none of which publish a per-district\n"
        "count - so every district's stock is understated, the outer boroughs' most of all.",
        ha="left", va="top", fontsize=8.5, color=charts.INK_MUTED, linespacing=1.7,
    )


def _draw_key(fig, selected):
    """The numbered key, listing the twelve in rank order with their rates.

    Rows whose rate is an artifact of the permit data rather than a
    measurement carry an asterisk, keyed to the note beneath.
    """
    fig.text(
        0.655, 0.80, "Ranked by rate of affordable housing development",
        ha="left", va="top", fontsize=10, fontweight="600", color=charts.INK,
    )
    suspect = ft.rate_is_artifact(selected.set_index("district"))

    for offset, (district, row) in enumerate(selected.set_index("district").iterrows()):
        y = 0.755 - offset * 0.0455
        marked = bool(suspect[district])
        fig.text(
            0.655, y, f"{int(row['rate_rank']):>2}", ha="left", va="center",
            fontsize=9.5, fontweight="600", color=charts.HIGHLIGHT_EDGE,
            family="monospace",
        )
        fig.text(
            0.685, y, district, ha="left", va="center", fontsize=9.5,
            color=charts.INK_SECONDARY, family="monospace",
        )
        fig.text(
            0.745, y, DISTRICT_NAMES.get(district, ""), ha="left", va="center",
            fontsize=10, color=charts.INK,
        )
        # The asterisk hangs outside the right-aligned rate so the decimal
        # points still line up down the column.
        fig.text(
            0.963, y, f"{row['rate_pct']:.3f}%", ha="right", va="center",
            fontsize=9.5,
            color=charts.HIGHLIGHT_EDGE if marked else charts.INK_SECONDARY,
            family="monospace",
        )
        if marked:
            fig.text(
                0.966, y, "*", ha="left", va="center", fontsize=12,
                fontweight="600", color=charts.HIGHLIGHT_EDGE, family="monospace",
            )


def _save(fig, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)
    print(f"  wrote {path.relative_to(path.parent.parent.parent)}")
    return path


FIGURES = {
    "fast_track_map": fast_track_map,
    "stock_comparison_map": stock_comparison_map,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=list(FIGURES), help="render one figure")
    args = parser.parse_args()

    charts.apply_style()
    for name in [args.only] if args.only else FIGURES:
        FIGURES[name]()


if __name__ == "__main__":
    main()
