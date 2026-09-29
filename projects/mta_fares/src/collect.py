#!/usr/bin/env python3
"""Cache every aggregate pull the analysis needs.

    python src/collect.py all            # all pulls, using cache where present
    python src/collect.py all --refresh  # re-query everything, ignore cache
    python src/collect.py windows        # just the reconciliation totals

Nothing here downloads a full dataset. Every pull is a server-side aggregate;
see farecap/collect.py for why that is not optional at 45M rows.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from farecap import collect, constants as C  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("target", choices=["all", "windows", "fareclass", "stations", "fairfares"])
    p.add_argument("--refresh", action="store_true", help="ignore cache, re-query")
    args = p.parse_args()

    try:
        if args.target in ("all", "windows"):
            for year in sorted(C.WINDOWS):
                t = collect.window_total(year, refresh=args.refresh)
                print(f"window {year}: {t['rows']:,} rows, "
                      f"{t['ridership']:,.0f} rides, {t['first']} .. {t['last']}")

        if args.target in ("all", "fareclass"):
            for year in sorted(C.WINDOWS):
                d = collect.fare_class_totals(year, refresh=args.refresh)
                print(f"fare classes {year}: {len(d)} categories")

        if args.target in ("all", "stations"):
            for year in sorted(C.WINDOWS):
                rows = collect.station_fare_class(year, refresh=args.refresh)
                print(f"station x fare class {year}: {len(rows):,} rows")

        if args.target in ("all", "fairfares"):
            ff = collect.fair_fares(refresh=args.refresh)
            print(f"fair fares: {len(ff)} months, {ff[0]['month']} .. {ff[-1]['month']}")
    except collect.CollectionError as exc:
        print(f"COLLECTION FAILED: {exc}", file=sys.stderr)
        return 1

    print(f"\ncached under {collect.DATA_RAW}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
