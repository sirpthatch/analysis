"""
Heavy-vehicle vehicle_type codes for NYC Motor Vehicle Collisions - Vehicles
(bm4k-52h4).

STATUS: starting point, NOT curated. Derived from the top-30 frequency query run
2026-09-22 (see data/vehicle_type_counts_2025plus.csv). Re-run queries.md section 3
with $limit=200 over the full 2016-present period before trusting this list.

All values are UPPERCASE because every comparison must be wrapped in upper() on
both sides -- the raw column is inconsistently cased.
"""

# The rule's actual target: licensed trade waste vehicles. NYPD does not
# distinguish private carters from DSNY municipal trucks.
REFUSE = [
    "GARBAGE OR REFUSE",
]

# Other heavy vehicles, useful as a comparison group. Side guards are not
# required on most of these, which makes them a rough control.
OTHER_HEAVY = [
    "BOX TRUCK",
    "TRACTOR TRUCK DIESEL",
    "TRACTOR TRUCK GASOLINE",
    "DUMP",
    "FLAT BED",
    "TOW TRUCK / WRECKER",
]

# Light vehicles, for the denominator / sanity checks.
LIGHT = [
    "SEDAN",
    "STATION WAGON/SPORT UTILITY VEHICLE",
    "4 DR SEDAN",
    "2 DR SEDAN",
    "PICK-UP TRUCK",
    "TAXI",
    "VAN",
    "CONVERTIBLE",
]

# Vulnerable road users, to identify the crashes that matter here.
VRU = [
    "BIKE",
    "BICY",      # duplicate concept -- collapse with BIKE
    "E-BIKE",
    "E-SCOOTER",
    "MOPED",
    "MOTORCYCLE",
]

# Junk / ambiguous codes seen in the top 30. Do not silently drop these --
# 29,436 records since 2025 have a BLANK vehicle_type, which is larger than
# every heavy-vehicle category combined. Quantify what you exclude.
JUNK = [
    "",           # blank: 29,436 records since 2025-01-01
    "PK",
    "DELV",
    "UN/C",
    "STANDING S",
    "CARRY ALL",
]

HEAVY = REFUSE + OTHER_HEAVY


def soql_in(codes):
    """Render a list of codes as a SoQL in(...) clause value."""
    return ",".join("'" + c.replace("'", "''") + "'" for c in codes)


if __name__ == "__main__":
    print("upper(vehicle_type) in (" + soql_in(HEAVY) + ")")
