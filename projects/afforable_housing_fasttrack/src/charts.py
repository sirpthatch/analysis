"""Chart styling for the article figures.

Palette and mark specs follow the project's data-viz conventions.  The
categorical values below were run through the validator rather than chosen by
eye; the notes record what each passed.

Figures are static PNGs for Substack, so there is no hover layer - which makes
direct labelling mandatory rather than optional wherever something would
otherwise be identified by colour alone.
"""

import matplotlib as mpl

# --- Surfaces and ink ------------------------------------------------------
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a887f"
GRID = "#e6e5e0"

# --- Categorical: fixed order, never cycled --------------------------------
# Validated light, adjacent pairs: worst CVD dE 9.1, normal-vision dE 19.6.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
PAIR = ["#2a78d6", "#eb6834"]

# --- Focus / context -------------------------------------------------------
# The fast-track map is not a two-category comparison - it is one set of
# districts against the rest of the city.  So the twelve carry the one strong
# hue and everything else recedes to a near-surface neutral, rather than both
# groups taking a categorical slot and competing for attention.
HIGHLIGHT = "#e34948"
HIGHLIGHT_EDGE = "#a82a29"
CONTEXT_FILL = "#eceae4"
CONTEXT_EDGE = "#ffffff"

# Measured on the light surface: the highlight clears the mark floor at
# 3.85:1 and its edge at 6.76:1.  The two fills separate at CVD dE 31.2
# (deutan) and normal-vision dE 36.6, well clear of the targets.
#
# The validator FAILs the context fill on the lightness band, the chroma floor
# and contrast (1.17:1), and that is expected rather than ignored: those checks
# score categorical *marks*, and the context fill is a recessive ground - the
# unselected districts are the page, not a second series.  The map also
# direct-labels every highlighted district, so identity never rests on colour
# alone either way.

# --- Ordinal: one hue, light to dark ---------------------------------------
ORDINAL_4 = ["#86b6ef", "#3987e5", "#256abf", "#104281"]


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
        }
    )


def title(ax, headline, standfirst=None):
    """Left-aligned headline with an optional explanatory line beneath it."""
    ax.set_title(headline, loc="left", pad=30 if standfirst else 14)
    if standfirst:
        ax.text(
            0, 1.035, standfirst, transform=ax.transAxes, ha="left", va="bottom",
            fontsize=10, color=INK_SECONDARY,
        )
