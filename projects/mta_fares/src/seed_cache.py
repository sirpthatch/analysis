#!/usr/bin/env python3
"""Copy the committed fixtures into data/raw/ so the analysis runs offline.

    python src/seed_cache.py

Seeded pulls are then used by collect.py's cache. Pass --refresh to any
collect/analyze command to overwrite them with live data.
"""

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
DEST = ROOT / "data" / "raw"


def main() -> int:
    DEST.mkdir(parents=True, exist_ok=True)
    seeded = []
    for src in sorted(FIXTURES.glob("*.json")):
        shutil.copy2(src, DEST / src.name)
        seeded.append(src.name)
    if not seeded:
        print("no fixtures found", file=sys.stderr)
        return 1
    for name in seeded:
        print(f"seeded {name}")
    print(f"\n{len(seeded)} fixtures -> {DEST}")
    print("Daily/bus/O-D series are not fixtures; see research/additional-questions.md.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
