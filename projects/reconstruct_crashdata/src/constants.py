"""Dataset identifiers and shared constants.

Every ID here was verified against the live catalog on 2026-09-21.
"""

from pathlib import Path

SOCRATA_DOMAIN = "data.cityofnewyork.us"

# Motor Vehicle Collisions - Crashes. The dataset this project reconstructs.
# Public rows stop at 2026-06-11 despite the city continuing to log crashes.
CRASHES = "h9gi-nx95"

# Citywide Street Centerline (CSCL). Street name and per-side ZIP on every
# segment — the reference layer for recovering the fields TrafficStat omits.
CENTERLINE = "inkn-q76z"

# The date the public crash file stops. Everything after this is the gap.
FREEZE_DATE = "2026-06-11"

BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_EXTERNAL = PROJECT_ROOT / "data" / "external"
