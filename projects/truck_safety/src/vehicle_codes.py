"""
Heavy-vehicle vehicle_type codes for NYC Motor Vehicle Collisions - Vehicles
(bm4k-52h4).

STATUS: heavy-vehicle codes re-queried over the full 2016-present period on
2026-09-22 (`python src/collect.py types`, output in
`data/raw/vehicle_types_by_year.csv`). The VRU, LIGHT and JUNK lists below are
still the uncurated starting point inherited from the handoff bundle.

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
    "BICY",  # duplicate concept -- collapse with BIKE
    "E-BIKE",
    "E-SCOOTER",
    "MOPED",
    "MOTORCYCLE",
]

# Junk / ambiguous codes seen in the top 30. Do not silently drop these --
# 29,436 records since 2025 have a BLANK vehicle_type, which is larger than
# every heavy-vehicle category combined. Quantify what you exclude.
JUNK = [
    "",  # blank: 29,436 records since 2025-01-01
    "PK",
    "DELV",
    "UN/C",
    "STANDING S",
    "CARRY ALL",
]

HEAVY = REFUSE + OTHER_HEAVY

# point_of_impact values, ENUMERATED not guessed -- the full vocabulary for
# GARBAGE OR REFUSE rows is 19 values; see data/raw/impact_vocabulary.csv.
# Side guards address lateral impacts: the doors and the quarter panels.
SIDE_IMPACT = [
    "LEFT SIDE DOORS",
    "RIGHT SIDE DOORS",
    "LEFT FRONT QUARTER PANEL",
    "RIGHT FRONT QUARTER PANEL",
    "LEFT REAR QUARTER PANEL",
    "RIGHT REAR QUARTER PANEL",
]

# The narrowest reading: a guard sits between the axles, so the doors are the
# impact location it most directly covers. Quarter panels straddle the ends of
# the guard. Report both and say which is which.
SIDE_IMPACT_STRICT = [
    "LEFT SIDE DOORS",
    "RIGHT SIDE DOORS",
]


def soql_in(codes):
    """Render a list of codes as a SoQL in(...) clause value."""
    return ",".join("'" + c.replace("'", "''") + "'" for c in codes)


if __name__ == "__main__":
    print("upper(vehicle_type) in (" + soql_in(HEAVY) + ")")


# ---------------------------------------------------------------------------
# Trucks, for research question 1.
#
# Curated 2026-09-22 from the full 2016-present vocabulary
# (`data/raw/vehicle_types_by_year.csv`: 2,165 distinct codes, 2.82M rows).
# PUBLISH THIS LIST IN THE POST - the definition of "truck" moves the headline
# number and a reader cannot check it otherwise.
#
# Two artifacts in the raw column shaped these lists:
#
#  1. Some records carry `vehicle_type` TRUNCATED TO 5 CHARACTERS -- TRACT,
#     BOX T, TOW T, GARBA, FIRET, SANIT, FLAT.  These are the same vehicles as
#     their spelled-out siblings and are folded in below.  Missing them
#     undercounts; they are small but systematic.
#  2. Free-text entry produces a very long tail.  121 codes cover 99.7% of all
#     rows; the remaining ~2,000 codes cover 0.3%.  Anything below is excluded
#     by omission -- quantify that exclusion rather than ignoring it.

# Waste/refuse codes within TRUCK_CORE, broken out on their own so callers can
# ask "how many of these truck crashes were a waste truck specifically" without
# re-deriving the list.  Broader than REFUSE above -- REFUSE is the exact-match
# list the older side-guard pull (`collect.py refuse`) queries Socrata with, and
# predates the 2026-09-22 truncation curation, so it misses GARBAGE TR / GARBA /
# SANITATION / SANIT.  Undercounts by omission; not reconciled here.
TRUCK_CORE_WASTE = [
    "GARBAGE OR REFUSE", "GARBAGE TR", "GARBA", "SANITATION", "SANIT",
]

# Unambiguous heavy goods vehicles.  Most are over the 10,000 lb GVWR line the
# side-guard rule uses, though NYPD records no weight so that is an inference
# from the vehicle class, not a measured fact.
TRUCK_CORE = [
    "BOX TRUCK", "BOX T",
    "TRACTOR TRUCK DIESEL", "TRACTOR TRUCK GASOLINE", "TRACT", "TRAC",
    "DUMP", "DUMP TRUCK",
    "FLAT BED", "FLAT", "FLAT RACK",
    "GARBAGE OR REFUSE", "GARBAGE TR", "GARBA", "SANITATION", "SANIT",
    "TOW TRUCK / WRECKER", "TOW TRUCK", "TOW T",
    "TANKER",
    "CONCRETE MIXER",
    "CHASSIS CAB",
    "REFRIGERATED VAN",
    "ARMORED TRUCK",
    "BEVERAGE TRUCK",
    "STAKE OR RACK",
    "LIFT BOOM",
    "TRUCK",
    "LARGE COM VEH(6 OR MORE TIRES)",
    "MULTI-WHEELED VEHICLE",
    "TRAILER", "TRAIL",
    "OPEN BODY",
    "BULK AGRICULTURE",
    "SNOW PLOW",
    "HOPPER",
]

# Light commercial and pickups.  Reported as a SEPARATE sensitivity tier, not
# folded into the headline: most sit under the rule's weight line, and
# PICK-UP TRUCK alone is 79,491 rows -- large enough to swing any total it
# joins.
TRUCK_LIGHT = [
    "PICK-UP TRUCK", "PICK UP TR", "PICK UP", "PICK",
    "PICKUP WITH MOUNTED CAMPER",
    "SMALL COM VEH(4 TIRES)",
    "VAN",
    "DELIVERY", "DELIV", "DELV",
    "USPS", "POSTA",
    "LUNCH WAGON",
    "UTILITY", "UTILI", "UTIL",
]

# Deliberately EXCLUDED from both, and why.  Buses and emergency vehicles are
# heavy but are not goods vehicles and are outside the trade-waste rule
# entirely; forklifts and similar are off-road plant that reach the street
# only incidentally.
NOT_TRUCKS = [
    "BUS", "SCHOOL BUS", "SCHOO", "MTA BUS",           # transit
    "AMBULANCE", "AMBUL", "AMBU", "AMB",               # emergency
    "FIRE TRUCK", "FIRETRUCK", "FIRET", "FIRE", "FDNY",
    "FDNY TRUCK", "FDNY FIRE", "FDNY AMBUL", "POLI",
    "FORKLIFT", "FORKL", "FORK", "PALLET",             # off-road plant
    "MOTORIZED HOME", "RV", "VAN CAMPER",              # recreational
]

TRUCK_ALL = TRUCK_CORE + TRUCK_LIGHT
