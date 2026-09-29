# OMNY Fare Cap — what replaced the 30-Day Unlimited MetroCard

Quantifying the rider population affected when the MTA retired the monthly
unlimited pass in January 2026 and replaced it with a weekly-only fare cap.

Read `CLAUDE.md` first for the story, the state of play, and the open items.

## Status

Scoped 2026-09-28; first live run the same evening. All 11 re-verification tests
pass against the live APIs, both windows reconcile exactly, the station verdict is
DISPERSED (narrowly, and confounded — see CLAUDE.md), and the follow-up questions
in `additional_questions.md` are answered in `research/additional-questions.md`.

## Layout

```
CLAUDE.md                  handoff brief — story, facts, open items, gotchas
README.md                  this file
requirements.txt
src/
  collect.py               CLI: cache the aggregate pulls
  analyze.py               CLI: composition, cap arithmetic, station tables
  seed_cache.py            CLI: copy fixtures into data/raw/ for offline work
  farecap/
    constants.py           verified dataset ids, fare schedule, recorded figures
    collect.py             Socrata access — server-side aggregates only
    fareclass.py           fare-class composition, shares, cap arithmetic
    stations.py            per-station monthly-pass share + flatness verdict
    timeseries.py          daily fare-class series, subway / bus / express bus (Q1-Q4)
    revenue.py             weekly revenue model vs MTA farebox actuals (Q3)
    evasion.py             fare evasion rates and fare-equivalent value (Q4)
    od.py                  origin-destination matrix + station coordinates (Q5)
    schemes.py             monthly cap, distance fare, off-peak fare sketches (Q5)
    proposals.py           spec_fare_proposals.md: income test, synthetic riders, caps, float
  figures.py               CLI: render article/lib/*.png
research/
  research-log.md          sourced detail, every claim with its provenance
  queries.md               exact API calls, copy-pasteable
  plan.md                  phased plan
  limitations.md           what the data cannot support
  sources.md               news and document sources with dates
  additional-questions.md  findings for additional_questions.md Q1-Q5
  fare-proposals.md        modelled impact of spec_fare_proposals.md 1-4
  docs/                    MTA data dictionaries and fare documents (PDF + text)
  handoff/                 scoping-session fixtures, preserved
tests/
  test_verified_figures.py re-checks recorded figures against the live APIs
  fixtures/                verbatim responses from the scoping session
notebooks/
article/
  outline.md               draft structure
  lib/                     figures (python src/figures.py)
data/raw/                  gitignored; rebuild with src/collect.py
output/                    gitignored; CSVs from src/analyze.py
```

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Running it

Offline, against the committed fixtures — works with no network:

```bash
python src/seed_cache.py
python src/analyze.py --composition --arithmetic
```

That reproduces the headline composition table, the shares, and the cap arithmetic.

On a networked machine, do this first:

```bash
export SOCRATA_APP_TOKEN=...        # optional, raises the rate limit
python src/collect.py all --refresh # re-query everything
pytest tests/                       # confirm the recorded figures still hold
python src/analyze.py --all         # adds the station-level analysis
```

`pytest` hitting the network is deliberate: the tests are the re-verification step, not
a unit-test suite. A failure most likely means the MTA restated its ridership
estimates, which belongs in `research/research-log.md`.

## The one query that decides the project

```bash
python src/analyze.py --stations
```

It prints a verdict: `DISPERSED` means 30-day-pass share varies by station and the map
is worth building; `FLAT` means it doesn't, there is no geography, and the piece is a
citywide arithmetic note. Run it before investing in anything else.

## Data sources

| Dataset | Id | Domain |
|---|---|---|
| MTA Subway Hourly Ridership: Beginning 2025 | `5wq4-mkjj` | data.ny.gov |
| MTA Subway Hourly Ridership: 2020-2024 | `wujg-7c2s` | data.ny.gov |
| Fair Fares Enrollees (HRA) | `3tw8-6si8` | data.cityofnewyork.us |
| MTA Bus Hourly Ridership: 2020-2024 / Beginning 2025 | `kv7t-n8in` / `gxb3-akrn` | data.ny.gov |
| MTA NYCT Subway Fare Evasion / Bus Fare Evasion | `6kj3-ijvb` / `uv5h-dfhp` | data.ny.gov |
| MTA Statement of Operations | `yg77-3tkj` | data.ny.gov |
| MTA Daily Ridership and Traffic | `sayj-mze2` | data.ny.gov |
| MTA Subway O-D Ridership Estimate: 2024 / Beginning 2026 | `jsu2-fbtj` / `28vm-gjqr` | data.ny.gov |

Remaining unverified ids in `constants.DATASETS`.

## The follow-up questions

The daily series and O-D matrix are large and not committed. Pull them, then:

```bash
python src/analyze.py --questions   # Q1-Q5 tables -> output/
python src/figures.py               # article/lib/*.png
```

Pull commands are in `research/additional-questions.md` § Reproduce.

## The fare proposals

Needs two reference files in `data/external/` (gitignored):

```bash
mkdir -p data/external
curl -sG "https://data.cityofnewyork.us/resource/qmcw-ur37.json" --data-urlencode '$limit=5000' \
  -o data/external/cdbg_tracts_qmcw-ur37.json
curl -s -o data/external/2023_gaz_tracts_36.txt \
  https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_gaz_tracts_36.txt
python src/analyze.py --proposals   # ~2 min -> output/p1_*, p2_*, p34_*
python src/figures.py               # adds fig7, fig8
```
