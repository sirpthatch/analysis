# Reconstructing NYC's Missing Crash Data

New York City's public crash file — [Motor Vehicle Collisions – Crashes](https://data.cityofnewyork.us/d/h9gi-nx95),
the dataset behind most street-safety reporting in the city — **stopped updating
on June 11, 2026**. NYPD kept recording crashes. The public just stopped seeing
them.

This project reconstructs the missing period from NYPD's own TrafficStat
dashboard, which has been publishing current data the whole time, and validates
the result against the months where both sources overlap.

**The reconstruction agrees with Socrata to within 1.1% over five overlapping
months**, and independently reproduces a crash total reported through an entirely
separate route to within 0.2%.

## What this produces

`data/processed/reconstructed_crashes_gap.csv` — every collision NYPD recorded
between 2026-06-12 and 2026-09-13, shaped like a Socrata crash row:

| Recovered | Method | Fidelity |
|---|---|---|
| date, coordinates, precinct | TrafficStat directly | full |
| collision type | TrafficStat | `UNKNOWN` on ~51% (NYPD's own gap) |
| street name | nearest CSCL centerline | 88.2% agreement |
| ZIP code | CSCL per-side ZIP | 95.0% agreement |
| injury / fatality / per-mode injury | one TrafficStat metric per flag | boolean, not counts; within 0.1pp of Socrata |
| time of day | TrafficStat | **absent for the entire gap period** — see below |

**Not recoverable, emitted as null:** contributing factors, per-record injury and
fatality counts, `collision_id`, cross/off street names. No source exposes them.

**Time of day is absent for the whole gap period.** TrafficStat carries a real
hour for ~95% of January–April records, 12.5% of May, and **none at all from
June onward** — the field degraded a month *before* the public file froze. Any
hour-of-day analysis of the gap is impossible, and the timing of that
degradation is itself worth chasing.

See [`research/socrata_gap_reconstruction.md`](research/socrata_gap_reconstruction.md)
for the full assessment and [`research/limitations.md`](research/limitations.md)
for what the result cannot support.

## Structure

```
reconstruct_crashdata/
├── research/          # Assessment, methods, limitations
├── notebooks/         # Validation and exploration
├── src/               # Collection, reconstruction, reverse geocoding
├── data/
│   ├── raw/           # Source pulls, immutable
│   ├── processed/     # The reconstructed dataset
│   └── external/      # CSCL centerline
└── article/           # Writeup
    └── lib/           # Assets
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
cd src

python collect.py freshness      # is the file still frozen? run this first
python collect.py all            # Socrata + TrafficStat + CSCL centerline
python reconstruct.py metrics    # attribute pulls (~700 requests, cached)
python reconstruct.py build      # assemble the gap table
python reconstruct.py validate   # agreement on the overlap period
```

`data/` is gitignored; the above rebuilds it. Metric pulls are cached per metric,
so an interrupted run resumes.

## Sources

| Source | ID | Role |
|---|---|---|
| Motor Vehicle Collisions – Crashes | `h9gi-nx95` | The frozen file being reconstructed |
| NYPD TrafficStat | undocumented JSON API | Current crash records |
| Citywide Street Centerline (CSCL) | `inkn-q76z` | Street name and ZIP reference |

TrafficStat has no published API. The endpoints were recovered from the site's
own JavaScript and are documented in
[`research/trafficstat_api.md`](research/trafficstat_api.md) — they can change
without notice.
