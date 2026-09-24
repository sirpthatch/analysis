#!/usr/bin/env python3
"""
Normalize, join, and build the zone-month panel.

    python scripts/build_panel.py

Inputs (from fetch_data.py):
    raw/vehicles_refuse.csv    one row per refuse vehicle involved in a crash
    raw/crashes_refuse.csv     crash-level records for those collision_ids
    raw/cwz_zones.geojson      zone polygons
    data/cwz_rollout.csv       implementation dates per zone

Outputs:
    out/refuse_crashes_zoned.csv   one row per crash, with zone + months-since-implementation
    out/panel_zone_month.csv       zone x month counts, the thing you actually model

What this deliberately does NOT do: decide whether a truck was municipal DSNY or a private
carter. That separation is the analytical crux and there is no clean field for it. See the
municipal_share_warning printed at the end.
"""
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW, OUT, DATA = ROOT / "raw", ROOT / "out", ROOT / "data"

# NYC bounding box. Non-null latitude is not the same as valid latitude -
# the crash feed carries 0.0 and other junk coordinates.
LAT_MIN, LAT_MAX = 40.45, 40.95
LON_MIN, LON_MAX = -74.30, -73.68

# Crash data ends here; anything implemented near or after this has no post-period.
CRASH_DATA_ENDS = date(2026, 6, 11)
REFUSE_VEHICLE_DATA_ENDS = date(2026, 5, 4)


def norm_vehicle_type(raw):
    """All ten observed spellings collapse to one label. See data/refuse_vehicle_variants.csv."""
    u = (raw or "").upper()
    return "GARBAGE_OR_REFUSE" if ("GARBAGE" in u or "REFUSE" in u) else None


def load_rollout():
    rows = list(csv.DictReader((DATA / "cwz_rollout.csv").open()))
    by_zone = {}
    for r in rows:
        impl = r["implementation_date"].strip()
        d = datetime.strptime(impl, "%Y-%m-%d").date() if impl else None
        for code in [c.strip() for c in r["zone_codes"].split(";") if c.strip()]:
            by_zone[code] = {
                "phase": r["phase"],
                "implementation_date": d,
                "treatment_status": r["treatment_status"],
            }
    return by_zone


def point_in_ring(x, y, ring):
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        if (y1 > y) != (y2 > y):
            t = (y - y1) / (y2 - y1)
            if x < x1 + t * (x2 - x1):
                inside = not inside
    return inside


def point_in_polygon(x, y, polygon):
    """polygon = [outer_ring, hole, hole, ...]"""
    if not polygon or not point_in_ring(x, y, polygon[0]):
        return False
    return not any(point_in_ring(x, y, h) for h in polygon[1:])


def load_zones():
    """Returns [(zone_code, zone_name, [polygon, ...], bbox)]. Pure stdlib, no geopandas."""
    gj = json.loads((RAW / "cwz_zones.geojson").read_text())
    zones = []
    for f in gj["features"]:
        p = f.get("properties", {})
        geom = f.get("geometry") or {}
        polys = []
        if geom.get("type") == "MultiPolygon":
            polys = geom["coordinates"]
        elif geom.get("type") == "Polygon":
            polys = [geom["coordinates"]]
        xs = [pt[0] for poly in polys for ring in poly for pt in ring]
        ys = [pt[1] for poly in polys for ring in poly for pt in ring]
        if not xs:
            continue
        zones.append((p.get("zone"), p.get("zone_name"), polys,
                      (min(xs), min(ys), max(xs), max(ys))))
    return zones


def assign_zone(lon, lat, zones):
    for code, name, polys, (x0, y0, x1, y1) in zones:
        if not (x0 <= lon <= x1 and y0 <= lat <= y1):
            continue
        if any(point_in_polygon(lon, lat, poly) for poly in polys):
            return code, name
    return None, None


def months_between(a, b):
    return (b.year - a.year) * 12 + (b.month - a.month)


def main():
    for f in ["vehicles_refuse.csv", "crashes_refuse.csv", "cwz_zones.geojson"]:
        if not (RAW / f).exists():
            sys.exit(f"missing raw/{f} - run scripts/fetch_data.py first")

    OUT.mkdir(exist_ok=True)
    rollout = load_rollout()
    zones = load_zones()
    print(f"loaded {len(zones)} zone polygons, {len(rollout)} zone implementation dates")

    # Vehicle side: normalize casing, keep make for the municipal-vs-private question.
    veh = defaultdict(list)
    spellings = Counter()
    for r in csv.DictReader((RAW / "vehicles_refuse.csv").open()):
        if norm_vehicle_type(r.get("vehicle_type")):
            spellings[r.get("vehicle_type")] += 1
            veh[r["collision_id"]].append({
                "vehicle_make": (r.get("vehicle_make") or "").strip().upper(),
                "vehicle_year": r.get("vehicle_year") or "",
                "state_registration": (r.get("state_registration") or "").strip().upper(),
            })
    print(f"{sum(spellings.values()):,} refuse-vehicle rows across "
          f"{len(spellings)} raw spellings, {len(veh):,} distinct collisions")

    rows, dropped_geo, dropped_zone = [], 0, 0
    for c in csv.DictReader((RAW / "crashes_refuse.csv").open()):
        cid = c.get("collision_id")
        if cid not in veh:
            continue
        try:
            lat, lon = float(c.get("latitude") or "nan"), float(c.get("longitude") or "nan")
        except ValueError:
            dropped_geo += 1
            continue
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            dropped_geo += 1
            continue
        cd = (c.get("crash_date") or "")[:10]
        if not cd:
            continue
        d = datetime.strptime(cd, "%Y-%m-%d").date()
        zcode, zname = assign_zone(lon, lat, zones)
        if not zcode:
            dropped_zone += 1
        meta = rollout.get(zcode or "", {})
        impl = meta.get("implementation_date")
        makes = ";".join(sorted({v["vehicle_make"] for v in veh[cid] if v["vehicle_make"]}))
        rows.append({
            "collision_id": cid,
            "crash_date": cd,
            "month": cd[:7],
            "borough": c.get("borough") or "",
            "latitude": lat,
            "longitude": lon,
            "zone": zcode or "",
            "zone_name": zname or "",
            "implementation_date": impl.isoformat() if impl else "",
            "treatment_status": meta.get("treatment_status", ""),
            "months_since_implementation": months_between(impl, d) if impl else "",
            "post_implementation": int(bool(impl and d >= impl)) if impl else "",
            "refuse_vehicles_in_crash": len(veh[cid]),
            "vehicle_makes": makes,
            "persons_injured": c.get("number_of_persons_injured") or "",
            "persons_killed": c.get("number_of_persons_killed") or "",
            "cyclist_injured": c.get("number_of_cyclist_injured") or "",
            "cyclist_killed": c.get("number_of_cyclist_killed") or "",
            "pedestrians_injured": c.get("number_of_pedestrians_injured") or "",
            "pedestrians_killed": c.get("number_of_pedestrians_killed") or "",
        })

    crash_out = OUT / "refuse_crashes_zoned.csv"
    with crash_out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"-> {crash_out} ({len(rows):,} crashes; "
          f"{dropped_geo:,} dropped for bad coordinates, {dropped_zone:,} fell outside every zone)")

    # Zone x month panel.
    panel = Counter()
    killed = Counter()
    for r in rows:
        key = (r["zone"], r["zone_name"], r["month"], r["treatment_status"],
               r["implementation_date"])
        panel[key] += 1
        try:
            killed[key] += int(r["persons_killed"] or 0)
        except ValueError:
            pass
    panel_out = OUT / "panel_zone_month.csv"
    with panel_out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["zone", "zone_name", "month", "treatment_status",
                    "implementation_date", "crashes", "persons_killed"])
        for k in sorted(panel):
            w.writerow(list(k) + [panel[k], killed[k]])
    print(f"-> {panel_out} ({len(panel):,} zone-months)")

    # The honest caveats, printed every run so they don't get forgotten.
    top_makes = Counter(m for r in rows for m in r["vehicle_makes"].split(";") if m)
    print("\n--- read this before modelling ---")
    print(f"Crash data ends {CRASH_DATA_ENDS}; refuse-vehicle rows effectively end "
          f"{REFUSE_VEHICLE_DATA_ENDS}. Truncate to the earlier date.")
    print("Only phases 1-3 have any post-period: QN-2 (2025-01-03), BX-1/BX-2 (2025-12-01), "
          "BK-5/QN-3 (2026-03-01). Phase 4 landed 10 days before the data ends - not usable.")
    print("MUNICIPAL SHARE WARNING: vehicle_type does not separate DSNY trucks from private "
          "carters, and only private carters are zoned. Top vehicle_make values below - use "
          "them to build a municipal-vs-private heuristic, and disclose the residual.")
    for make, n in top_makes.most_common(12):
        print(f"    {make:<28} {n:>6,}")
    print("Small numbers: ~400 refuse crashes a year citywide across 20 zones. Pool phases 1-3, "
          "report counts and rates, skip significance theatre.")


if __name__ == "__main__":
    main()
