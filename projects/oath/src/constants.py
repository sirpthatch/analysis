from pathlib import Path

_CONSTANTS_DIR = Path(__file__).parent
_PROJECT_ROOT = _CONSTANTS_DIR.parent

GEOTRACT_FILE = _PROJECT_ROOT / "data/external/tl_2025_36_tract/tl_2025_36_tract.shp"
GEOBLOCK_FILE = _PROJECT_ROOT / "data/external/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp"