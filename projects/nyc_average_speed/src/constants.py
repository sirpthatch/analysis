"""Paths, zone definitions, and event calendars for the midtown speed analysis."""

from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent

DATA_DIR = _PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"

# TLC trip record data is published as monthly parquet on CloudFront.
# Index page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TLC_TRIP_URL = (
    TLC_BASE_URL + "/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet"
)
TAXI_ZONE_LOOKUP_URL = TLC_BASE_URL + "/misc/taxi_zone_lookup.csv"
TAXI_ZONE_SHAPE_URL = TLC_BASE_URL + "/misc/taxi_zones.zip"

TAXI_ZONE_LOOKUP_FILE = EXTERNAL_DIR / "taxi_zone_lookup.csv"
TAXI_ZONE_SHAPE_FILE = EXTERNAL_DIR / "taxi_zones" / "taxi_zones.shp"

# Services with usable trip-level timing. "yellow" is the long history (street
# hails, midtown-heavy); "fhvhv" is the modern volume (Uber/Lyft) but only from
# 2019 and with a different distance definition.  See research/plan.md.
SERVICES = ("yellow", "green", "fhvhv")

# --- Geography -------------------------------------------------------------
# Taxi zone LocationIDs.  Names from taxi_zone_lookup.csv.

MIDTOWN_CORE_ZONES = {
    161: "Midtown Center",
    162: "Midtown East",
    163: "Midtown North",
    164: "Midtown South",
    230: "Times Sq/Theatre District",
    100: "Garment District",
}

# Midtown core plus the ring that shares its street grid and congestion.
MIDTOWN_EXTENDED_ZONES = {
    **MIDTOWN_CORE_ZONES,
    48: "Clinton East",
    50: "Clinton West",
    170: "Murray Hill",
    186: "Penn Station/Madison Sq West",
    229: "Sutton Place/Turtle Bay North",
    233: "UN/Turtle Bay South",
}

# Zones adjacent to UN headquarters - where UNGA street closures bite hardest.
# Tagged as its own area so RQ2 can ask whether the effect is sharp and local
# or spread across midtown.
UN_ZONES = {233: "UN/Turtle Bay South", 229: "Sutton Place/Turtle Bay North"}

# Manhattan zones inside the CBD tolling zone - the treated group for the
# congestion pricing question.
#
# CBD = Central Business District, MTA's term for the tolled area in its
# Central Business District Tolling Program: Manhattan south of 60th Street.
#
# Verified 2026-09-18 geometrically: each Manhattan zone polygon was split on a
# line from W 60th/Hudson River to E 60th/East River and scored by the share of
# its area lying south.  The split is unusually clean - 41 zones are 100% south,
# 3 straddle the line, and the rest are 0% - so the boundary is far less
# approximate than first assumed.
#
# Governor's/Ellis/Liberty Island (103-105) are south of the line but have no
# road connection to Manhattan, so they are excluded from a surface-street
# analysis.
CBD_ZONES = {
    4,
    12,
    13,
    45,
    48,
    50,
    68,
    79,
    87,
    88,
    90,
    100,
    107,
    113,
    114,
    125,
    137,
    144,
    148,
    158,
    161,
    162,
    163,
    164,
    170,
    186,
    209,
    211,
    224,
    229,
    230,
    231,
    232,
    233,
    234,
    246,
    249,
    261,
}

# Manhattan above 60th St - the control group.  Excludes parks and islands with
# no ordinary street grid (Central Park, Highbridge Park, Inwood Hill Park,
# Randalls Island, Marble Hill) and the boundary zones below.
CONTROL_ZONES_UPTOWN = {
    24,
    41,
    42,
    74,
    75,
    116,
    127,
    151,
    152,
    166,
    236,
    238,
    239,
    243,
    244,
    262,
    263,
}

# Zones straddling or hugging 60th St.  Kept out of BOTH groups so neither is
# contaminated - but interesting in their own right: if drivers began ending
# trips just north of the line to dodge the toll, it shows up here first.
CBD_BOUNDARY_ZONES = {
    140: "Lenox Hill East",  # 8% south of 60th
    141: "Lenox Hill West",  # 11%
    142: "Lincoln Square East",  # 24%
    143: "Lincoln Square West",  # 26%
    202: "Roosevelt Island",  # 30%
    237: "Upper East Side South",  # 14%
}

# --- Event calendar --------------------------------------------------------
# CBD tolling (congestion pricing) went live 2025-01-05.
CONGESTION_PRICING_START = "2025-01-05"

# UNGA General Debate windows, verified 2026-09-18 against un.org and the Dag
# Hammarskjold Library research guide (research.un.org/en/docs/ga/generaldebate).
#
# Under GA rules the debate opens on the Tuesday of the fourth week of September
# and runs nine working days, which in practice means Tuesday-Saturday plus a
# trailing Monday or Tuesday.  Dates below are (first day, last day) inclusive;
# the gap days in between are not sitting days but the delegations are still in
# town, so for traffic purposes the span is what matters.
UNGA_GENERAL_DEBATE = {
    2019: ("2019-09-24", "2019-09-30"),  # 74th
    2020: ("2020-09-22", "2020-09-29"),  # 75th - held virtually, see note below
    2021: ("2021-09-21", "2021-09-27"),  # 76th - hybrid
    2022: ("2022-09-20", "2022-09-26"),  # 77th
    2023: ("2023-09-19", "2023-09-26"),  # 78th
    2024: ("2024-09-24", "2024-09-30"),  # 79th
    2025: ("2025-09-23", "2025-09-29"),  # 80th
    2026: ("2026-09-22", "2026-09-28"),  # 81st - beyond our data (ends 2026-07)
}

# The 75th session (2020) ran on pre-recorded video statements with almost no
# delegations present, and the 76th (2021) was hybrid with reduced attendance.
# They are the closest thing to a natural control for RQ2: UNGA on the calendar,
# but no motorcades.  Treat them as such rather than dropping them.
UNGA_VIRTUAL_YEARS = {2020, 2021}


def unga_week(year):
    """The Monday-Sunday week containing the General Debate opening.

    Heads of state arrive over the preceding weekend and motorcades run through
    the working week, so the calendar week is the right unit for a traffic
    comparison - and it sidesteps the ambiguity in when the debate formally ends.
    """
    import datetime as _dt

    start = _dt.date.fromisoformat(UNGA_GENERAL_DEBATE[year][0])
    monday = start - _dt.timedelta(days=start.weekday())
    return monday, monday + _dt.timedelta(days=6)


# --- Trip filtering --------------------------------------------------------
# Bounds for a plausible in-borough surface street trip.  Trips outside these
# are meter errors, airport runs, or vehicles left on the clock.
MIN_TRIP_MILES = 0.2
MAX_TRIP_MILES = 10.0
MIN_TRIP_MINUTES = 1.0
MAX_TRIP_MINUTES = 90.0
MIN_TRIP_MPH = 1.0
MAX_TRIP_MPH = 40.0


# --- Supporting data -------------------------------------------------------
# Bryant Park, as a single stand-in point for midtown weather.
MIDTOWN_LAT = 40.7536
MIDTOWN_LON = -73.9832

WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_FILE = PROCESSED_DIR / "weather_hourly.parquet"

# NYC Permitted Event Information - Historical (Socrata). Covers 2007-present
# and carries street_closure_type and police_precinct.
EVENTS_URL = "https://data.cityofnewyork.us/resource/bkfu-528j.json"
EVENTS_FILE = PROCESSED_DIR / "permitted_events.parquet"

# Midtown NYPD precincts, for joining precinct-coded events to the study area.
MIDTOWN_PRECINCTS = {
    10: "Chelsea",
    13: "Gramercy",
    14: "Midtown South",
    17: "Turtle Bay/UN",
    18: "Midtown North",
    20: "Upper West Side South",
}

# Closure types that actually take road capacity away from traffic.
ROAD_CLOSURE_TYPES = {
    "Full Street Closure",
    "Sidewalk and Street Closure",
    "Curb Lane Only",
    "Sidewalk and Curb Lane Closure",
}


# --- Data quality ----------------------------------------------------------
# Days where the TLC source file itself is missing most of its trips.  Found by
# scanning for days below half the surrounding 15-day median, then ruling out
# snow and holidays against the weather archive: each date below was dry, mild
# and ordinary, yet retains 2-4% of normal volume across every taxi zone at once.
#
# Four of the five fall inside UNGA weeks, so leaving them in would have
# silently corrupted RQ2.  Excluded from all analysis.
TLC_OUTAGE_DATES = {
    "2022-09-18": "2.6% of normal citywide; dry, 17C, no holiday",
    "2023-09-21": "37.9% of normal; dry",
    "2023-09-22": "1.9% of normal; dry",
    "2023-09-23": "3.2% of normal",
    "2023-09-24": "2.6% of normal",
}

# Real low-traffic days, kept but worth knowing about: blizzards and holidays
# confirmed against the weather archive.  Listed so nobody re-investigates them.
KNOWN_LOW_TRAFFIC_DATES = {
    "2020-11-26": "Thanksgiving",
    "2020-12-17": "12cm snow",
    "2020-12-25": "Christmas",
    "2021-02-01": "blizzard, 27cm",
    "2021-02-02": "blizzard aftermath",
    "2021-02-07": "snow",
    "2022-01-29": "blizzard, 13cm",
    "2022-12-25": "Christmas",
    "2023-12-25": "Christmas",
    "2026-01-25": "blizzard, 19cm",
    "2026-01-26": "blizzard aftermath",
    "2026-02-23": "snow, 15cm",
}


# --- Time bands ------------------------------------------------------------
# Hour-of-day is too thin for a daily series in the midtown core (the median 4am
# hour has 7 trips), so the analysis groups hours into bands.  Boundaries follow
# the shape of the speed curve: the overnight plateau, the morning descent into
# the midday floor, the evening trough, and the night recovery.
#
# Each entry is (first hour, last hour inclusive).
TIME_BANDS = {
    "overnight": (0, 6),  # 12am-7am
    "morning": (7, 9),  # 7am-10am
    "midday": (10, 15),  # 10am-4pm
    "evening": (16, 19),  # 4pm-8pm
    "night": (20, 23),  # 8pm-12am
}

# Plain-English labels for charts and the article.
TIME_BAND_LABELS = {
    "overnight": "Overnight (12-7am)",
    "morning": "Morning (7-10am)",
    "midday": "Midday (10am-4pm)",
    "evening": "Evening (4-8pm)",
    "night": "Night (8pm-12am)",
}

BAND_ORDER = list(TIME_BANDS)


def band_of(hour):
    """Map an hour of day to its band name."""
    for name, (low, high) in TIME_BANDS.items():
        if low <= hour <= high:
            return name
    raise ValueError(f"hour out of range: {hour}")
