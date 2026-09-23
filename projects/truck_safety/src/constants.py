"""Paths, dataset IDs, and the dates the side-guard question turns on.

Every dataset ID here was confirmed against the live catalog; the confirmation
run and its timestamps live in `research/research-log.md`.  Figures inherited
from the handoff bundle are in `research/handoff/truck-sideguards/VERIFIED-DATA.md`
and are re-queried rather than trusted.
"""

from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent

DATA_DIR = _PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"

# --- Socrata datasets ------------------------------------------------------
SOCRATA_DOMAIN = "data.cityofnewyork.us"

# NYPD Motor Vehicle Collisions.  One crash row per collision; one vehicle row
# per vehicle involved.  Join on `collision_id`.
CRASHES_ID = "h9gi-nx95"
VEHICLES_ID = "bm4k-52h4"
PERSONS_ID = "f55k-p6yu"

# DSNY Commercial Waste Zones - the geography the trade-waste rule applies in,
# and the most promising handle on separating carters from DSNY's own fleet.
WASTE_ZONES_ID = "8ev8-jjxq"

# BIC, Licensees and Registrants Fleet Information.  Every active vehicle of an
# approved BIC licensee or registrant, with make/model/year, body type, GVWR
# band and -- critically -- `vehicle_has_side_guard` per vehicle.  This is the
# waiver-adjacent data NEXT-STEPS.md assumed required a FOIL.
BIC_FLEET_ID = "n84m-kx4j"

# DOT, New York City Truck Routes - the network commercial vehicles are
# required to use.  Research question 1 asks how often truck crashes happen
# off it.
TRUCK_ROUTES_ID = "jjja-shxy"

# Buffer applied to the route centrelines before testing whether a crash sits
# on the network.  Crash coordinates are geocoded to the roadbed and routes are
# centrelines, so a crash on a route can land tens of feet off it.  30 m is
# about half a wide NYC roadway plus geocoding slop; `analysis_routes.py`
# reports the sensitivity across 15/30/50 m rather than resting on one value.
ROUTE_BUFFER_M = 30

# --- The rule's timeline ---------------------------------------------------
# Side guards mandated on heavy-duty trade waste vehicles in 2015, with
# companies given until 2023 to comply.  BIC grants exemptions where the
# design or operation of the vehicle makes installation impractical.
RULE_ADOPTED_YEAR = 2015
COMPLIANCE_DEADLINE_YEAR = 2023

# The vehicle file carries 1 record in each of 2014 and 2015 - a reporting
# schema artifact, not an event.  The usable series starts in 2016.
SERIES_START_YEAR = 2016
START = "2016-01-01"

# --- The companion reconstruction -----------------------------------------
# The public crash file stopped updating on 2026-06-11.  `../reconstruct_crashdata`
# rebuilds 2026-06-12 through 2026-09-13 from NYPD TrafficStat.  Treat it as a
# parallel NYPD-sourced series, not as recovered Socrata rows.
RECONSTRUCTION_CSV = (
    _PROJECT_ROOT.parent
    / "reconstruct_crashdata"
    / "data"
    / "processed"
    / "reconstructed_crashes_gap.csv"
)

RAW_FILES = {
    "citywide_crashes_by_year": RAW_DIR / "citywide_crashes_by_year.csv",
    "vehicle_types_by_year": RAW_DIR / "vehicle_types_by_year.csv",
    "heavy_vehicles_by_year": RAW_DIR / "heavy_vehicles_by_year.csv",
    "refuse_vehicles": RAW_DIR / "refuse_vehicles.csv",
    "refuse_crashes": RAW_DIR / "refuse_crashes.csv",
    "impact_vocabulary": RAW_DIR / "impact_vocabulary.csv",
    "truck_vehicles": RAW_DIR / "truck_vehicles.parquet",
    "bic_fleet": RAW_DIR / "bic_fleet.csv",
    "all_crashes": RAW_DIR / "all_crashes.parquet",
}

EXTERNAL_FILES = {
    "truck_routes": EXTERNAL_DIR / "truck_routes.geojson",
    "boroughs": EXTERNAL_DIR / "borough_boundaries.geojson",
}

# DCP borough boundaries (water areas clipped), for map context only.
BOROUGH_BOUNDARIES_ID = "gthc-hcne"
