# NYC truck safety and the side-guard waiver

New York required side guards on heavy-duty trade waste vehicles in 2015, with a
2023 compliance deadline, then exempted a large share of the fleet by waiver.
This project asks whether the waiver process neutralized the rule — whether the
deadline shows up in the crash data at all.

## The finding that unblocked the project

The inherited pitch rested on refuse trucks appearing in NYPD crash records at a
flat rate every year — 398 in 2023, 406 in 2024, 402 in 2025. Flat counts mean
nothing without the denominator, and the denominator moved a lot.

**Citywide crashes fell 63% from 2016 to 2025. Refuse-truck records fell 27%.**

Normalized, refuse trucks went from 24.0 to 47.0 per 10,000 citywide crashes —
roughly doubling in relative terms, with no kink at the 2023 deadline:

| Year | 2016 | 2019 | 2020 | 2022 | **2023** | 2024 | 2025 |
|---|---|---|---|---|---|---|---|
| Refuse per 10k crashes | 24.0 | 34.5 | 34.1 | 41.3 | **41.2** | 44.5 | 47.0 |

Every other heavy-vehicle class — box trucks, tractor trailers, dumps — is flat
or falling on the same denominator over the same period. The rise is specific to
refuse trucks.

**Two catches.** The harm counts inside those crashes are single digits per year.
Cyclists injured in side-impact refuse crashes run 0–5 annually; killed, 0–1. A
40% fatality reduction against a base of one death a year is undetectable. The
argument has to rest on the crash-rate series, not the death counts. And
`GARBAGE OR REFUSE` does not map onto the fleet the BIC waiver covers — it
includes DSNY trucks, which were bound by the same deadline through a different
agency, and excludes non-refuse trade waste vehicles that BIC does cover. See
[`research/limitations.md`](research/limitations.md) — read it before writing
anything.

Full provenance for every figure above is in
[`research/research-log.md`](research/research-log.md).

## Research question 1 — trucks citywide

`notebooks/truck_crash_profile.ipynb` profiles truck involvement and maps it
against the DOT truck route network.

- **110,445 truck-involved crashes since 2016**, 7.2%–8.3% of all crashes, a
  band that does not move across the decade. Refuse trucks rose sharply as a
  share of crashes over the same period — trucks as a whole did not.
- **62.9% of truck crashes are on the designated truck route network**, against
  47.0% of crashes with no truck involved. The 16-point gap holds at 15 m, 30 m
  and 50 m.
- **37% of truck crashes happen off the network.** Widest gap in Queens
  (+14.2pp); Staten Island is the one borough where truck crashes are *less*
  on-route than everyone else's.
- Trucks are over-represented in Hunts Point and the South Bronx, the
  Brooklyn–Queens industrial belt, and the crossing approaches — not in the
  densest-crash parts of Midtown.

**Off-route is not the same as illegal** — trucks may leave the network to reach
a destination, and nothing in the crash record says where they were going.

## Structure

```
truck_safety/
├── spec_initialsetup.md   # objective and research questions
├── research/
│   ├── research-log.md    # what was queried, when, and what came back
│   ├── limitations.md     # what these numbers cannot support
│   └── handoff/           # the inherited research-idea bundle, unmodified
├── src/                   # Socrata client, collection, analysis
├── data/
│   ├── raw/               # source pulls, immutable
│   └── processed/         # the yearly series
├── notebooks/             # exploration
└── article/               # writeup
    └── lib/               # assets
```

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running it

```bash
source venv/bin/activate

python src/collect.py extent        # is the file still frozen? run this first
python src/collect.py all           # crash/vehicle aggregates, cached under data/raw/
python src/analysis.py              # the refuse-truck yearly series

python src/collect_geo.py all       # truck rows, all crash rows, routes, boroughs
python src/routes.py                # nearest-route distances and on/off-route shares

jupyter lab notebooks/truck_crash_profile.ipynb   # the maps
```

`data/` is gitignored; the above rebuilds it. Pulls are cached as CSV and
skipped if present, so an interrupted run can be topped up; `--refresh`
re-fetches.

## Sources

| Source | ID | Role |
|---|---|---|
| Motor Vehicle Collisions – Crashes | `h9gi-nx95` | Crash-level records, the denominator |
| Motor Vehicle Collisions – Vehicles | `bm4k-52h4` | `vehicle_type`, `point_of_impact` |
| Motor Vehicle Collisions – Person | `f55k-p6yu` | Per-person injury detail (not yet used) |
| DSNY Commercial Waste Zones | `8ev8-jjxq` | Carter-versus-DSNY separation (not yet used) |
| New York City Truck Routes | `jjja-shxy` | The designated network, for on/off-route |
| Borough Boundaries | `gthc-hcne` | Map context only |

Every link cited in the pitch is in
[`research/handoff/truck-sideguards/SOURCES.md`](research/handoff/truck-sideguards/SOURCES.md).

**The public crash file stopped updating on 2026-06-11.** The companion
[`../reconstruct_crashdata`](../reconstruct_crashdata) project rebuilds
2026-06-12 → 2026-09-13 from NYPD TrafficStat, but that reconstruction carries no
vehicle type, so it can extend the denominator and the citywide cyclist and
pedestrian series — not the refuse-truck series.
