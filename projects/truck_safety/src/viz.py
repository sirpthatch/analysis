"""Shared chart styling: one palette, one set of type and grid rules.

Colours are the validated reference palette (categorical slots 1 and 2, the
blue sequential ramp, and the blue/red diverging pair with a grey midpoint).
The categorical pair was checked with the palette validator before use:
worst-pair CVD Delta E 24.7, normal-vision 33.6, both clear of the floors, and
both slots clear 3:1 against the light surface.

Figures here are static, so there is no hover layer; every chart therefore
carries direct labels or an adjacent table rather than relying on a tooltip to
recover a value.
"""

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a8881"
GRID = "#e6e5e1"

# Categorical slots 1 and 2.
BLUE = "#2a78d6"
ORANGE = "#eb6834"

# Sequential: one hue, light to dark.
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SEQ_CMAP = LinearSegmentedColormap.from_list("urbancalc_blue", BLUE_RAMP)

# Diverging: two poles that read as opposite, neutral grey midpoint.
DIVERGING = ["#0d366b", "#256abf", "#6da7ec", "#cde2fb", "#f0efec",
             "#f6c9c8", "#ea8a89", "#d9534f", "#a32b28"]
DIV_CMAP = LinearSegmentedColormap.from_list("urbancalc_div", DIVERGING)


def use_style():
    """Apply the project's matplotlib defaults."""
    mpl.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "font.size": 10,
        "text.color": INK,
        "axes.labelcolor": INK_SECONDARY,
        "axes.edgecolor": GRID,
        "axes.linewidth": 1.0,
        "axes.titlesize": 13,
        "axes.titleweight": "600",
        "axes.titlecolor": INK,
        "axes.titlelocation": "left",
        "axes.titlepad": 10,
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.color": INK_SECONDARY,
        "ytick.color": INK_SECONDARY,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.frameon": False,
        "legend.fontsize": 9,
        "lines.linewidth": 2.0,
        "lines.markersize": 8,
        "figure.dpi": 120,
    })


def strip(ax, keep_grid="y"):
    """Recede the frame: no top/right spines, grid on one axis only."""
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(axis=keep_grid)
    if keep_grid == "y":
        ax.grid(axis="x", visible=False)
    else:
        ax.grid(axis="y", visible=False)
    return ax


def map_axes(ax):
    """A map panel: no frame, no ticks, equal aspect."""
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(False)
    return ax


def note(fig, text, y=-0.01):
    """A source/caveat line under a figure, in muted ink."""
    fig.text(0.0, y, text, ha="left", va="top", fontsize=8, color=INK_MUTED)


# Second sequential context (the skill's rule: the next categorical hue, as its
# own one-hue ramp). Used only where a blue ramp is already on the page.
ORANGE_RAMP = ["#fde3d6", "#f9c3a8", "#f4a07a", "#eb6834", "#c44e22", "#953a19", "#672711"]
SEQ_CMAP_ORANGE = LinearSegmentedColormap.from_list("urbancalc_orange", ORANGE_RAMP)
