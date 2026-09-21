"""Reverse geocoding NYC collision coordinates to street name and ZIP.

Socrata's crash file carries `on_street_name`, `cross_street_name` and
`zip_code`; TrafficStat carries none of them. They can be recovered from
coordinates by joining to DCP's Citywide Street Centerline (CSCL, `inkn-q76z`),
which has `full_street_name` and left/right ZIP on every segment.

An important distinction: Socrata's street fields come off the police report as
the officer wrote them. This produces *nearest street centerline*, which is a
different thing that usually agrees. `validate_against_socrata()` measures how
often, so the disagreement rate is a known quantity rather than an assumption.

Distances are computed in EPSG:2263 (NY State Plane Long Island, feet), not in
degrees — a nearest-neighbour search in lat/lon is wrong at this latitude.
"""

import re

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from constants import DATA_EXTERNAL

WGS84 = "EPSG:4326"
NY_FEET = "EPSG:2263"

# CSCL RW_TYPE. Roads trucks are banned from are type 2 (highway) only in part —
# parkway status isn't a CSCL field, so this classifies form, not legality.
RW_TYPE = {
    "1": "Street", "2": "Highway", "3": "Bridge", "4": "Tunnel",
    "5": "Boardwalk", "6": "Path/Trail", "7": "Step Street", "8": "Driveway",
    "9": "Ramp", "10": "Alley", "11": "Unknown", "12": "Non-physical segment",
    "13": "U-Turn", "14": "Ferry Route",
}

# Segment types that aren't real roadway and shouldn't win a nearest-match.
EXCLUDE_RW_TYPES = {"6", "7", "12", "13", "14"}


def load_centerline(path=None, drop_non_roadway=True):
    """CSCL as a projected GeoDataFrame, ready for a nearest join."""
    path = path or DATA_EXTERNAL / "cscl_centerline.json"
    df = pd.read_json(path)
    if drop_non_roadway:
        df = df[~df.rw_type.astype(str).isin(EXCLUDE_RW_TYPES)]
    gdf = gpd.GeoDataFrame(
        df.drop(columns=["the_geom"]),
        geometry=[shape(g) for g in df.the_geom],
        crs=WGS84,
    )
    gdf["rw_type_label"] = gdf.rw_type.astype(str).map(RW_TYPE)
    # CSCL serves l_zip as a number and r_zip as a string, so a raw comparison
    # of the two is always unequal. Normalize both to bare digit strings.
    for col in ("l_zip", "r_zip"):
        gdf[col] = (
            gdf[col].astype("string").str.strip()
            .str.replace(r"\.0$", "", regex=True).replace({"": pd.NA, "nan": pd.NA})
        )
    return gdf.to_crs(NY_FEET)


def to_points(df, lat="latitude", lon="longitude"):
    """Coordinate columns to a projected point GeoDataFrame, nulls dropped."""
    ok = df[df[lat].notna() & df[lon].notna() & (df[lat] != 0)].copy()
    return gpd.GeoDataFrame(
        ok, geometry=gpd.points_from_xy(ok[lon], ok[lat]), crs=WGS84
    ).to_crs(NY_FEET)


def reverse_geocode(df, centerline=None, lat="latitude", lon="longitude", max_feet=300):
    """Attach nearest-segment street name, ZIP and roadway type to each point.

    `max_feet` caps how far a point may be from a segment and still be labelled;
    beyond it the fields come back null rather than guessing. Points on water,
    in parks or snapped to a highway mile marker are the usual offenders.

    ZIP: CSCL stores it per side of the street (`l_zip` / `r_zip`). Deciding the
    side needs the offset direction, which is more precision than this warrants,
    so where the two genuinely differ the left value is taken and
    `zip_ambiguous` is set. They agree on the large majority of segments.
    """
    cl = centerline if centerline is not None else load_centerline()
    pts = to_points(df, lat, lon)

    joined = gpd.sjoin_nearest(
        pts, cl[["geometry", "full_street_name", "l_zip", "r_zip", "rw_type_label", "physicalid"]],
        how="left", distance_col="dist_feet",
    )
    # sjoin_nearest emits one row per tie; keep the single closest per point.
    joined = joined[~joined.index.duplicated(keep="first")]

    far = joined.dist_feet > max_feet
    joined.loc[far, ["full_street_name", "l_zip", "r_zip", "rw_type_label"]] = None

    out = pd.DataFrame(index=df.index)
    out["street_name_geocoded"] = joined.full_street_name
    out["zip_geocoded"] = joined.l_zip
    out["zip_ambiguous"] = (joined.l_zip != joined.r_zip) & joined.l_zip.notna()
    out["roadway_type"] = joined.rw_type_label
    out["geocode_dist_feet"] = joined.dist_feet
    out["segment_id"] = joined.physicalid
    return out


def normalize_street(s):
    """Normalize a street name enough to compare two sources fairly.

    Two traps worth naming, both of which silently deflate a match rate:

    - Socrata pads a house number ahead of the street ("215       VAN PELT AVE",
      "82-66     BROADWAY"). The number must be stripped — but NYC streets are
      themselves numbered ("64 RD", "2 AVE"), so a bare leading-digit rule eats
      the street name. The padding is the discriminator: a house number is
      followed by two or more spaces, a numbered street by one.
    - Suffix substitution needs word boundaries, or ROAD->RD turns BROADWAY into
      BRDWAY.
    """
    if pd.isna(s):
        return None
    s = str(s).upper().strip()
    s = re.sub(r"^\d+(-\d+)?\s{2,}", "", s)   # padded house number only
    s = re.sub(r"^\d+-\d+\s+", "", s)         # Queens-style, unambiguous
    for a, b in [
        ("AVENUE", "AVE"), ("STREET", "ST"), ("ROAD", "RD"), ("PLACE", "PL"),
        ("BOULEVARD", "BLVD"), ("PARKWAY", "PKWY"), ("EXPRESSWAY", "EXPY"),
        ("DRIVE", "DR"), ("TURNPIKE", "TPKE"), ("HIGHWAY", "HWY"),
        ("NORTH", "N"), ("SOUTH", "S"), ("EAST", "E"), ("WEST", "W"),
    ]:
        s = re.sub(rf"\b{a}\b", b, s)
    return " ".join(s.split())


def validate_against_socrata(socrata_df, centerline=None, sample=None, seed=0):
    """Measure agreement between geocoded values and Socrata's recorded ones.

    Socrata rows carry coordinates *and* officer-written street names and ZIPs,
    so geocoding their own coordinates and comparing is a direct read on how far
    this method can be trusted before applying it to TrafficStat.
    """
    df = socrata_df[
        socrata_df.latitude.notna() & (socrata_df.latitude != 0)
    ].copy()
    if sample:
        df = df.sample(min(sample, len(df)), random_state=seed)

    got = reverse_geocode(df, centerline)
    df = df.join(got)

    df["street_match"] = df.apply(
        lambda r: (
            normalize_street(r.street_name_geocoded) == normalize_street(r.on_street_name)
            if pd.notna(r.on_street_name) and pd.notna(r.street_name_geocoded)
            else None
        ), axis=1)
    # At an intersection the nearest segment may legitimately be any of the
    # streets named on the report, so the fair test is whether the geocoded name
    # appears anywhere in the record's on/cross/off fields.
    def _any_match(r):
        if pd.isna(r.street_name_geocoded):
            return None
        recorded = {
            normalize_street(r.on_street_name),
            normalize_street(r.cross_street_name),
            normalize_street(r.off_street_name),
        } - {None}
        if not recorded:
            return None
        return normalize_street(r.street_name_geocoded) in recorded

    df["street_match_any"] = df.apply(_any_match, axis=1)
    df["zip_match"] = df.apply(
        lambda r: (
            str(r.zip_geocoded).split(".")[0] == str(r.zip_code).split(".")[0]
            if pd.notna(r.zip_code) and pd.notna(r.zip_geocoded)
            else None
        ), axis=1)
    return df
