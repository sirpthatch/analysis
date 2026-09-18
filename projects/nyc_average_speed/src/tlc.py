"""Fetch TLC trip record data and the taxi zone reference files.

Downloads are idempotent: an existing file of nonzero size is left alone, so a
partial month range can be topped up by re-running.

    python src/tlc.py zones
    python src/tlc.py trips --service yellow --start 2024-01 --end 2025-12
"""

import argparse
import sys
import zipfile

import requests

from constants import (
    EXTERNAL_DIR,
    RAW_DIR,
    TAXI_ZONE_LOOKUP_FILE,
    TAXI_ZONE_LOOKUP_URL,
    TAXI_ZONE_SHAPE_FILE,
    TAXI_ZONE_SHAPE_URL,
    TLC_TRIP_URL,
)


def _download(url, dest, chunk=1 << 20):
    """Stream `url` to `dest`, writing to a .part file until complete."""
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(part, "wb") as handle:
            for block in response.iter_content(chunk_size=chunk):
                handle.write(block)

    part.rename(dest)
    return dest


def month_range(start, end):
    """Yield (year, month) tuples from 'YYYY-MM' start through end, inclusive."""
    start_year, start_month = (int(part) for part in start.split("-"))
    end_year, end_month = (int(part) for part in end.split("-"))

    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        yield year, month
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def trip_file(service, year, month):
    return RAW_DIR / service / f"{service}_tripdata_{year:04d}-{month:02d}.parquet"


def fetch_trips(service, start, end):
    """Download every monthly parquet in the range; return the local paths."""
    paths = []
    for year, month in month_range(start, end):
        dest = trip_file(service, year, month)
        existed = dest.exists()
        url = TLC_TRIP_URL.format(service=service, year=year, month=month)
        try:
            _download(url, dest)
        except requests.HTTPError as err:
            # Months beyond the publication lag simply do not exist yet.
            print(f"  skip {year:04d}-{month:02d}: {err}", file=sys.stderr)
            continue
        print(f"  {'have' if existed else 'got '} {dest.name}")
        paths.append(dest)
    return paths


def fetch_zones():
    """Download the taxi zone lookup table and the zone shapefile."""
    _download(TAXI_ZONE_LOOKUP_URL, TAXI_ZONE_LOOKUP_FILE)
    print(f"  have {TAXI_ZONE_LOOKUP_FILE.name}")

    if not TAXI_ZONE_SHAPE_FILE.exists():
        archive = EXTERNAL_DIR / "taxi_zones.zip"
        _download(TAXI_ZONE_SHAPE_URL, archive)
        # The archive already contains a taxi_zones/ folder, so extract to
        # EXTERNAL_DIR rather than nesting it a second time.
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(EXTERNAL_DIR)
        archive.unlink()
    print(f"  have {TAXI_ZONE_SHAPE_FILE.name}")

    return TAXI_ZONE_LOOKUP_FILE, TAXI_ZONE_SHAPE_FILE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("zones", help="download taxi zone lookup + shapefile")

    trips = sub.add_parser("trips", help="download monthly trip record parquet")
    trips.add_argument("--service", default="yellow", help="yellow, green, or fhvhv")
    trips.add_argument("--start", required=True, metavar="YYYY-MM")
    trips.add_argument("--end", required=True, metavar="YYYY-MM")

    args = parser.parse_args()

    if args.command == "zones":
        fetch_zones()
    else:
        fetch_trips(args.service, args.start, args.end)


if __name__ == "__main__":
    main()
