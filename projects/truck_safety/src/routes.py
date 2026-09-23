"""Research question 1: where truck crashes happen, and how many are off-route.

    python src/routes.py

Reads the pulls from `src/collect_geo.py` and writes to `data/processed/`:

    crash_points.parquet        every geocoded crash since 2016, flagged for
                                truck involvement, with its distance to the
                                nearest truck route and that route's type
    route_share.csv             on-route share by vehicle group and threshold
    truck_crashes_by_year.csv   the yearly series, truck and citywide

Method note: NYPD geocodes a crash to the roadbed and DOT publishes routes as
centrelines, so a crash genuinely on a truck route still lands some distance
from the line.  "On-route" is therefore a distance threshold, not a boolean
fact about the data, and every number here is reported across 15 / 30 / 50 m
so the reader can see how much the choice matters.
"""

import geopandas as gpd
import numpy as np
import pandas as pd

from constants import (
    EXTERNAL_FILES,
    PROCESSED_DIR,
    RAW_FILES,
    ROUTE_BUFFER_M,
    SERIES_START_YEAR,
)
from vehicle_codes import TRUCK_CORE, TRUCK_CORE_WASTE, TRUCK_LIGHT

# Metric CRS for NYC.  Distances below are metres in UTM zone 18N.
METRIC_CRS = 32618

THRESHOLDS_M = (15, 30, 50)

# Points further than this from any route are off-route by any reading; the
# join stops looking there rather than searching the whole network.
MAX_SEARCH_M = 1000

NYC_BBOX = {"lat": (40.4, 41.0), "lon": (-74.3, -73.6)}

HARM_COLUMNS = [
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
]


def truck_collision_ids():
    """Collision ids carrying at least one truck, split by tier plus waste.

    A crash is truck-involved if any vehicle row in it is on the list -- the
    unit is the crash, not the vehicle, so a crash with two box trucks counts
    once.  `waste` is a subset of `core` (TRUCK_CORE_WASTE below), broken out
    so the waste-truck share can be read on the exact same geocoded
    denominator as the truck share, rather than against the separate citywide
    (non-geocoded) count `refuse_harm_by_year.csv` uses.
    """
    vehicles = pd.read_parquet(RAW_FILES["truck_vehicles"])
    vehicles["vt"] = vehicles["vehicle_type"].fillna("").str.upper()

    core = set(vehicles.loc[vehicles["vt"].isin(TRUCK_CORE), "collision_id"])
    light = set(vehicles.loc[vehicles["vt"].isin(TRUCK_LIGHT), "collision_id"])
    waste = set(vehicles.loc[vehicles["vt"].isin(TRUCK_CORE_WASTE), "collision_id"])
    return core, light, waste


def crash_points():
    """All geocoded crashes since 2016 as points, flagged for truck involvement."""
    crashes = pd.read_parquet(RAW_FILES["all_crashes"])
    total = len(crashes)

    for column in ("latitude", "longitude"):
        crashes[column] = pd.to_numeric(crashes[column], errors="coerce")
    for column in HARM_COLUMNS:
        crashes[column] = pd.to_numeric(crashes[column], errors="coerce").fillna(0)

    located = crashes[
        crashes["latitude"].between(*NYC_BBOX["lat"])
        & crashes["longitude"].between(*NYC_BBOX["lon"])
    ].copy()
    print(f"  {len(located):,} of {total:,} crashes have usable coordinates "
          f"({len(located) / total:.1%})")

    core, light, waste = truck_collision_ids()
    located["is_truck_core"] = located["collision_id"].isin(core)
    located["is_truck_light"] = located["collision_id"].isin(light)
    located["is_truck_waste"] = located["collision_id"].isin(waste)
    located["year"] = located["crash_date"].str[:4].astype(int)

    # Geocoding bias check: if truck crashes are located at a different rate
    # from the rest, every share computed downstream is skewed.
    crashes["is_truck_core"] = crashes["collision_id"].isin(core)
    rate = crashes.groupby("is_truck_core").apply(
        lambda frame: frame["latitude"].between(*NYC_BBOX["lat"]).mean(),
        include_groups=False,
    )
    print(f"  geocoded share -- truck crashes {rate.get(True, float('nan')):.1%}, "
          f"other crashes {rate.get(False, float('nan')):.1%}")

    return gpd.GeoDataFrame(
        located,
        geometry=gpd.points_from_xy(located["longitude"], located["latitude"]),
        crs=4326,
    ).to_crs(METRIC_CRS)


def with_route_distance(points):
    """Attach the distance to the nearest truck route, and that route's type."""
    routes = gpd.read_file(EXTERNAL_FILES["truck_routes"]).to_crs(METRIC_CRS)
    routes = routes[["geometry", "routetype", "street"]].rename(
        columns={"street": "route_street"}
    )

    joined = gpd.sjoin_nearest(
        points, routes, how="left", distance_col="dist_m", max_distance=MAX_SEARCH_M
    )
    # A tie puts one crash on several rows; keep its nearest route only.
    joined = (
        joined.sort_values("dist_m")
        .drop_duplicates("collision_id")
        .drop(columns=["index_right"])
    )
    joined["dist_m"] = joined["dist_m"].fillna(np.inf)
    return joined


def route_share(frame):
    """On-route share for each vehicle group, at each distance threshold."""
    groups = {
        "Truck (core)": frame["is_truck_core"],
        "Truck (incl. light commercial)": frame["is_truck_core"] | frame["is_truck_light"],
        "No truck involved": ~(frame["is_truck_core"] | frame["is_truck_light"]),
        "All crashes": pd.Series(True, index=frame.index),
    }

    rows = []
    for name, mask in groups.items():
        subset = frame[mask]
        row = {"group": name, "crashes": len(subset)}
        for threshold in THRESHOLDS_M:
            row[f"on_route_{threshold}m"] = round(
                (subset["dist_m"] <= threshold).mean() * 100, 1
            )
        rows.append(row)
    return pd.DataFrame(rows)


def by_borough(frame):
    """On-route share by borough, trucks against everything else."""
    frame = frame[frame["borough"].notna()].copy()
    frame["on_route"] = frame["dist_m"] <= ROUTE_BUFFER_M

    out = frame.groupby(["borough", "is_truck_core"])["on_route"].agg(["mean", "size"])
    out = out.unstack("is_truck_core")
    out.columns = ["other_share", "truck_share", "other_n", "truck_n"]
    out["other_share"] = (out["other_share"] * 100).round(1)
    out["truck_share"] = (out["truck_share"] * 100).round(1)
    out["gap_pp"] = (out["truck_share"] - out["other_share"]).round(1)
    return out.reset_index()


def by_year(frame):
    """Truck-involved crashes per year, against the citywide count."""
    truck = frame[frame["is_truck_core"]]

    out = frame.groupby("year").agg(
        all_crashes=("collision_id", "size"),
        truck_crashes=("is_truck_core", "sum"),
        waste_crashes=("is_truck_waste", "sum"),
    )
    out["truck_cyclist_injured"] = truck.groupby("year")["number_of_cyclist_injured"].sum().astype(int)
    out["truck_ped_injured"] = truck.groupby("year")["number_of_pedestrians_injured"].sum().astype(int)
    out["truck_killed"] = truck.groupby("year")["number_of_persons_killed"].sum().astype(int)
    out["truck_share_pct"] = (out["truck_crashes"] / out["all_crashes"] * 100).round(2)
    out["waste_share_pct"] = (out["waste_crashes"] / out["all_crashes"] * 100).round(3)
    out["truck_off_route_pct"] = (
        truck[truck["dist_m"] > ROUTE_BUFFER_M].groupby("year").size()
        / truck.groupby("year").size() * 100
    ).round(1)
    return out.reset_index()


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("=== building crash points ===")
    points = with_route_distance(crash_points())
    keep = [
        "collision_id", "crash_date", "year", "borough", "on_street_name",
        "latitude", "longitude", "is_truck_core", "is_truck_light",
        "is_truck_waste", "dist_m", "routetype", "route_street", *HARM_COLUMNS,
    ]
    points[keep].to_parquet(PROCESSED_DIR / "crash_points.parquet", index=False)
    print(f"  wrote crash_points.parquet ({len(points):,} rows)")

    shares = route_share(points)
    shares.to_csv(PROCESSED_DIR / "route_share.csv", index=False)
    print("\n=== On-route share, by vehicle group and threshold ===")
    print(shares.to_string(index=False))

    boroughs = by_borough(points)
    boroughs.to_csv(PROCESSED_DIR / "route_share_by_borough.csv", index=False)
    print(f"\n=== On-route share by borough (within {ROUTE_BUFFER_M} m) ===")
    print(boroughs.to_string(index=False))

    years = by_year(points)
    years.to_csv(PROCESSED_DIR / "truck_crashes_by_year.csv", index=False)
    print("\n=== Truck-involved crashes by year ===")
    print(years.to_string(index=False))


if __name__ == "__main__":
    main()
