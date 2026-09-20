"""Paths, dataset IDs, cycle dates, and community-district geography.

Everything here that names a live dataset or a rule parameter is sourced in
`research/research-log.md`; the rule itself is summarized in `CLAUDE.md`.
"""

from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent

DATA_DIR = _PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"

# --- The rule's measurement cycle ------------------------------------------
# Five years, July 1 - June 30.  The first cycle closed 6/30/2026 and DCP must
# publish the twelve lowest-rate districts no later than 10/1/2026.
CYCLE_START = "2021-07-01"
CYCLE_END = "2026-06-30"

# The denominator is the prior Decennial Census count plus Net New Housing
# Units through the cycle start, so net completions are credited for calendar
# 2020 and 2021.  Both are full calendar years in `dbdt-5s7j` and the cycle
# starts mid-2021, so this overshoots slightly - see `build_denominator`.
DENOMINATOR_COMPLETION_YEARS = ("comp2020", "comp2021")

FAST_TRACK_LIST_SIZE = 12

# --- Socrata datasets ------------------------------------------------------
SOCRATA_DOMAIN = "data.cityofnewyork.us"

# HPD, Affordable Housing Production by Building.  Source of the rule's
# "Start Date as publicly reported by HPD" and the unit counts.
HPD_PRODUCTION_ID = "hg8x-zxpr"

# DCP, Housing Database by Community District.  Source of the denominator:
# `cenunits20` (2020 Census housing units) plus per-year net completions.
DCP_HOUSING_CD_ID = "dbdt-5s7j"

# DCP, Housing Database Project Level Files.  Carries `datepermit` per DOB job
# already geocoded to BBL - the cleanest read on the rule's second milestone,
# but it lags (last refreshed 2026-03-16), so DOB is queried directly too.
DCP_HOUSING_PROJECT_ID = "br6q-ssj3"

# DCP, Housing Database by 2020 CDTA.  CDTAs are built from whole census
# tracts and are labeled "equivalent" or "approximation" per community
# district.  Same pipeline, different geography - which makes it the only
# independent read available on the denominator, and it disagrees with the
# CD file on two of the four districts flagged as duplicates.
DCP_HOUSING_CDTA_ID = "48dt-mn3z"

# DOB permits.  Post-2021 new construction files in DOB NOW; the legacy BIS
# table still carries jobs filed before the cutover, so both are needed to
# cover the cycle.
# NYCHA, Development Data Book.  Public housing apartments per development -
# the largest single block of existing deeply affordable stock, and the only
# part of it with a clean per-district count in open data.
NYCHA_DEVELOPMENTS_ID = "evjd-dqpz"

DOB_NOW_PERMITS_ID = "rbx6-tga4"
DOB_BIS_PERMITS_ID = "ipu4-2q9a"

RAW_FILES = {
    "hpd_production": RAW_DIR / "hpd_affordable_production.csv",
    "dcp_housing_cd": RAW_DIR / "dcp_housing_db_by_cd.csv",
    "dcp_housing_project": RAW_DIR / "dcp_housing_db_projects.csv",
    "dcp_housing_cdta": RAW_DIR / "dcp_housing_db_by_cdta.csv",
    "dob_now_permits": RAW_DIR / "dob_now_approved_permits.csv",
    "dob_bis_permits": RAW_DIR / "dob_bis_permit_issuance.csv",
    "nycha_developments": RAW_DIR / "nycha_development_data_book.csv",
}

EXTERNAL_FILES = {
    "community_district_geom": EXTERNAL_DIR / "community_districts.geojson",
}

# --- Geography -------------------------------------------------------------
# HPD writes community districts as "BK-05"; DCP writes them as "301".
BOROUGH_CODE_TO_PREFIX = {"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"}
PREFIX_TO_BOROUGH_CODE = {v: k for k, v in BOROUGH_CODE_TO_PREFIX.items()}

# The 59 real community districts.  `dbdt-5s7j` also carries "joint interest
# areas" - Central Park (164), the airports, Rikers, the large cemeteries -
# which have near-zero housing stock and would rank first on any rate if left
# in.  Whitelisting the 59 is safer than blacklisting the ones seen so far.
DISTRICTS_PER_BOROUGH = {"1": 12, "2": 12, "3": 18, "4": 14, "5": 3}

COMMUNITY_DISTRICTS = tuple(
    f"{boro}{district:02d}"
    for boro, count in DISTRICTS_PER_BOROUGH.items()
    for district in range(1, count + 1)
)

BOROUGH_NAMES = {
    "MN": "Manhattan",
    "BX": "Bronx",
    "BK": "Brooklyn",
    "QN": "Queens",
    "SI": "Staten Island",
}


def to_hpd_district(code):
    """'301' -> 'BK-01'.  Returns None for joint interest areas."""
    code = str(code).strip()
    if code not in COMMUNITY_DISTRICTS:
        return None
    return f"{BOROUGH_CODE_TO_PREFIX[code[0]]}-{code[1:]}"


def to_dcp_district(label):
    """'BK-01' -> '301'.  Returns None for anything unrecognized."""
    label = str(label).strip().upper()
    prefix, _, number = label.partition("-")
    if prefix not in PREFIX_TO_BOROUGH_CODE or not number.isdigit():
        return None
    code = f"{PREFIX_TO_BOROUGH_CODE[prefix]}{int(number):02d}"
    return code if code in COMMUNITY_DISTRICTS else None

# --- Mapping ---------------------------------------------------------------
# NY State Plane Long Island (feet).  Boundaries come out of Socrata in
# EPSG:4326, which draws the city noticeably sheared at this latitude.
MAP_CRS = "EPSG:2263"
