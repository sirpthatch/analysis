#!/usr/bin/env python3
"""
Download the MTA Bus Automated Camera Enforcement violations file
(data.ny.gov kh8p-hcbm, ~6.5M rows) and convert it to parquet.

    python3 scripts/fetch_ace.py            # download + convert
    python3 scripts/fetch_ace.py --convert  # re-convert an existing CSV

Writes data/raw/ace_violations.csv and data/raw/ace_violations.parquet
(both gitignored).
"""
import argparse
import os
import sys

import pandas as pd
import requests

URL = "https://data.ny.gov/api/views/kh8p-hcbm/rows.csv?accessType=DOWNLOAD"
HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, os.pardir, "data", "raw")
CSV = os.path.join(RAW, "ace_violations.csv")
PARQUET = os.path.join(RAW, "ace_violations.parquet")

COLUMNS = {
    "Violation ID": "violation_id",
    "Vehicle ID": "vehicle_id",
    "First Occurrence": "first_occurrence",
    "Last Occurrence": "last_occurrence",
    "Violation Status": "violation_status",
    "Violation Type": "violation_type",
    "Bus Route ID": "bus_route_id",
    "Violation Latitude": "lat",
    "Violation Longitude": "lon",
    "Stop ID": "stop_id",
    "Stop Name": "stop_name",
}


def download():
    os.makedirs(RAW, exist_ok=True)
    with requests.get(URL, stream=True, timeout=600) as r:
        r.raise_for_status()
        n = 0
        with open(CSV + ".part", "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                n += len(chunk)
                if n % (100 << 20) < (1 << 20):
                    print(f"  {n >> 20} MB", file=sys.stderr, flush=True)
    os.replace(CSV + ".part", CSV)
    print(f"downloaded {n >> 20} MB -> {CSV}")


def convert():
    df = pd.read_csv(CSV, usecols=list(COLUMNS), dtype=str).rename(columns=COLUMNS)
    fmt = "%m/%d/%Y %I:%M:%S %p"
    for c in ("first_occurrence", "last_occurrence"):
        df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce")
    for c in ("lat", "lon"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("violation_status", "violation_type", "bus_route_id", "stop_id", "stop_name"):
        df[c] = df[c].astype("category")
    df.to_parquet(PARQUET, index=False)
    print(f"{len(df):,} rows -> {PARQUET}")
    print("unparsed first_occurrence:", df["first_occurrence"].isna().sum())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--convert", action="store_true")
    a = p.parse_args()
    if not a.convert:
        download()
    convert()


if __name__ == "__main__":
    main()
