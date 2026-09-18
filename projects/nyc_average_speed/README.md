# NYC Midtown Average Speed

How fast does traffic actually move on midtown Manhattan's surface streets, how
does that change over the year, and what did UNGA week and congestion pricing do
to it?

Source data is the NYC TLC trip record data (yellow taxi and high-volume
for-hire vehicles), which gives a per-trip distance and duration — enough to
reconstruct an average speed for every trip that began and ended in midtown.

See [`spec_initial.md`](spec_initial.md) for the objective and research
questions, and [`research/plan.md`](research/plan.md) for the working plan.

## Structure

```
nyc_average_speed/
├── research/      # Plan, notes, and sources gathered along the way
├── notebooks/     # Jupyter notebooks for exploration
├── src/           # Data collection and the codified speed pipeline
├── data/
│   ├── raw/       # Original TLC parquet, immutable
│   ├── processed/ # Filtered trip-speed tables
│   └── external/  # Taxi zone lookup and shapefile
└── article/       # Final writeup
    └── lib/       # Writeup assets (images, etc.)
```

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Collecting data

```bash
source venv/bin/activate

# Taxi zone lookup table + zone shapefile (small, do this first)
python src/tlc.py zones

# Monthly trip records (~70MB each for yellow; 2019-01..2026-07 is ~6GB)
python src/tlc.py trips --service yellow --start 2019-01 --end 2026-07

# Confounders: hourly midtown weather, and permitted street events
python src/weather.py --start 2019-01-01 --end 2026-07-31
python src/events.py --start 2019-01-01
```

Downloads are idempotent — a month already on disk is skipped, so an
interrupted range can be topped up by re-running the same command.

## Building the analysis panel

```bash
python src/panel.py --service yellow --start 2019-01 --end 2026-07
```

Seven years of yellow taxi is ~250M trips, too many to hold at once, so
`panel.py` streams month by month and writes two compact tables to
`data/processed/`:

- **`speed_panel.parquet`** — one row per date × hour × area, carrying summed
  `miles` and `minutes` (so distance-weighted speed can be recomputed at any
  grouping), trip counts, and the hourly weather joined on.
- **`zone_pairs.parquet`** — month × area × origin × destination counts, which
  is what the composition-drift check in Phase 2 runs on.

Areas are `midtown_core`, `midtown_ext`, `cbd`, `uptown` and `cbd_boundary`;
each flag means the trip both started *and* ended inside that area.

`cbd` is the **Central Business District** — MTA's term for the area tolled
by its Central Business District Tolling Program, Manhattan south of 60th
Street. It is the treated group for the congestion pricing question, with
`uptown` as the control and `cbd_boundary` the zones straddling 60th St,
held out of both.

### Drilling down to individual trips

```bash
python src/speed.py --service yellow --start 2024-09 --end 2024-09
```

Writes the full per-trip table for those months to
`data/processed/<service>/`. Only needed for work the panel cannot answer —
filter sensitivity, distribution shapes. Filter bounds live in
`src/constants.py` and can be overridden per call via
`speed.filter_plausible(frame, bounds)`.

## Analysis

Notebooks, in the order they were built:

| Notebook | Phase |
|---|---|
| `profile_tlc.ipynb` | Data profiling — filters, sample size, composition drift, source outages |
| `seasonality.ipynb` | RQ1: how speed moves over the year |
| `unga.ipynb` | RQ2: the UNGA-week effect |
| `congestion_pricing.ipynb` | RQ3: the 2025 CBD toll |

Findings are written up in `research/`: `profile_findings.md`, then one file per
research question. `research/limitations.md` is the running list of what the
data cannot support.

Shared analysis helpers live in `src/analysis.py` so the notebooks stay thin and
the methods are testable.

## The article

`article/midtown.speed.md`, with figures in `article/lib/`.

```bash
python src/make_figures.py
```

Regenerates every figure. Chart styling and the validated palette are in
`src/charts.py`.

## Usage

```bash
source venv/bin/activate
jupyter notebook
```
