"""Chart styling for the article figures.

Palette and mark specs follow the project's data-viz conventions.  Every
categorical palette below was run through the validator rather than chosen by
eye; the notes on each record what it passed.

Figures are static PNGs for Substack, so there is no hover layer - which makes
direct labelling mandatory rather than optional wherever a series would
otherwise be identified by colour alone.
"""

import matplotlib as mpl
import matplotlib.pyplot as plt

# --- Surfaces and ink ------------------------------------------------------
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a887f"
GRID = "#e6e5e0"

# --- Categorical: fixed order, never cycled --------------------------------
# Validated light, adjacent pairs: worst CVD dE 9.1, normal-vision dE 19.6.
# Three slots sit under 3:1 on the light surface, so the relief rule applies -
# these are only ever used with visible direct labels.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]

# Two-series comparisons clear every check outright (CVD dE 24.7).
PAIR = ["#2a78d6", "#eb6834"]

# --- Ordinal: one hue, light to dark ---------------------------------------
# For genuinely ordered categories - distance from the UN, for instance, is a
# magnitude, not an identity, so a categorical palette would be the wrong
# encoding regardless of whether it validated.
ORDINAL_4 = ["#86b6ef", "#3987e5", "#256abf", "#104281"]

# --- Diverging: warm/cool poles, neutral grey midpoint ---------------------
POS = "#2a78d6"
NEG = "#e34948"
NEUTRAL = "#f0efec"

HIGHLIGHT = "#e34948"


def apply_style():
    """Recessive axes, thin marks, text in ink rather than series colour."""
    mpl.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "axes.edgecolor": GRID,
            "axes.labelcolor": INK_SECONDARY,
            "axes.titlecolor": INK,
            "axes.titlesize": 13,
            "axes.titleweight": "600",
            "axes.titlelocation": "left",
            "axes.titlepad": 30,
            "axes.labelsize": 10,
            "axes.linewidth": 0.8,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": INK_SECONDARY,
            "ytick.color": INK_SECONDARY,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "xtick.major.size": 0,
            "ytick.major.size": 0,
            "legend.frameon": False,
            "legend.fontsize": 9.5,
            "legend.labelcolor": INK_SECONDARY,
            "lines.linewidth": 2.0,
            "lines.markersize": 4.5,
            "font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
            "font.size": 10,
            "figure.dpi": 110,
            "savefig.dpi": 200,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.28,
        }
    )


def subtitle(ax, text):
    """A deck line under the title, in secondary ink."""
    ax.annotate(
        text,
        xy=(0, 1.0),
        xycoords="axes fraction",
        xytext=(0, 20),
        textcoords="offset points",
        fontsize=10,
        color=INK_SECONDARY,
        va="bottom",
        ha="left",
    )


def source(fig, text="Source: NYC TLC yellow taxi trip records, 2019–2026"):
    """Provenance line, bottom left."""
    fig.text(0.0, -0.02, text, fontsize=8, color=INK_MUTED, ha="left", va="top")


def label_end(ax, x, y, text, color, dx=6, dy=0, **kw):
    """Direct label at a line's end - identity without relying on colour."""
    ax.annotate(
        text,
        xy=(x, y),
        xytext=(dx, dy),
        textcoords="offset points",
        color=color,
        fontsize=9.5,
        va="center",
        fontweight="600",
        **kw,
    )


def label_ends(ax, items, min_gap_frac=0.055, dx=6):
    """Place end-of-line labels, pushing apart any that would collide.

    Lines that converge - midday and evening speeds, for instance - would
    otherwise stack their labels on top of each other.
    """
    lo, hi = ax.get_ylim()
    min_gap = (hi - lo) * min_gap_frac

    items = sorted(items, key=lambda it: it[1])
    placed = []
    for x, y, text, color in items:
        target = y
        if placed and target - placed[-1] < min_gap:
            target = placed[-1] + min_gap
        placed.append(target)
        # Anchor the text at the ADJUSTED height, not the raw one.
        ax.annotate(
            text,
            xy=(x, target),
            xytext=(dx, 0),
            textcoords="offset points",
            color=color,
            fontsize=9.5,
            va="center",
            fontweight="600",
            annotation_clip=False,
            xycoords="data",
        )
        # Leader line back to the series, so a pushed label still reads as its own.
        if abs(target - y) > (hi - lo) * 0.008:
            ax.annotate(
                "",
                xy=(x, y),
                xytext=(x, target),
                xycoords="data",
                textcoords="data",
                arrowprops=dict(arrowstyle="-", color=color, lw=0.9, alpha=0.55),
            )


def bar_gap_style(ax, bars, zorder=3):
    """Bars sit above the grid, with a hairline surface gap between neighbours.

    Rounded data-ends were tried and abandoned: matplotlib's FancyBboxPatch
    rounds in data units, so on a horizontal bar chart - where x is mph and y is
    a category index - the corner radius is wildly different on each axis and
    the marks come out spiked. Square ends with a clean gap read better than a
    broken flourish.
    """
    for b in bars:
        b.set_zorder(zorder)
        b.set_linewidth(0.0)
    ax.set_axisbelow(True)


def save(fig, path):
    fig.savefig(path)
    plt.close(fig)
    print(f"  wrote {path}")
