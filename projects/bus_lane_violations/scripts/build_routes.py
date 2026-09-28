#!/usr/bin/env python3
"""
Build one polyline per bus route (per direction) from the MTA static GTFS
feeds in data/raw/gtfs/*.zip, keeping the shape used by the most trips.

    curl -o data/raw/gtfs/gtfs_<b>.zip https://rrgtfsfeeds.s3.amazonaws.com/gtfs_<b>.zip
        for b in bx b m q si busco
    python3 scripts/build_routes.py

Writes data/raw/route_shapes.json: {route_id: [[[lat, lon], ...], ...]}.
"""
import glob
import json
import os
import zipfile

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, os.pardir, "data", "raw")


def main():
    trips, shapes = [], []
    for path in sorted(glob.glob(os.path.join(RAW, "gtfs", "gtfs_*.zip"))):
        with zipfile.ZipFile(path) as z:
            t = pd.read_csv(z.open("trips.txt"), dtype=str,
                            usecols=["route_id", "direction_id", "shape_id"])
            s = pd.read_csv(z.open("shapes.txt"),
                            dtype={"shape_id": str},
                            usecols=["shape_id", "shape_pt_lat", "shape_pt_lon",
                                     "shape_pt_sequence"])
        trips.append(t)
        shapes.append(s)
    trips = pd.concat(trips).dropna(subset=["shape_id"])
    shapes = pd.concat(shapes).drop_duplicates(["shape_id", "shape_pt_sequence"])

    best = (trips.groupby(["route_id", "direction_id", "shape_id"]).size()
            .rename("n").reset_index()
            .sort_values("n", ascending=False)
            .drop_duplicates(["route_id", "direction_id"]))
    shapes = shapes[shapes["shape_id"].isin(best["shape_id"])].sort_values(
        ["shape_id", "shape_pt_sequence"])
    pts = {sid: g[["shape_pt_lat", "shape_pt_lon"]].round(5).values.tolist()
           for sid, g in shapes.groupby("shape_id")}

    out = {}
    for route, g in best.groupby("route_id"):
        out[route] = [pts[s] for s in g["shape_id"] if s in pts]
    path = os.path.join(RAW, "route_shapes.json")
    with open(path, "w") as fh:
        json.dump(out, fh)
    print(f"{len(out)} routes -> {os.path.relpath(path)}")


if __name__ == "__main__":
    main()
