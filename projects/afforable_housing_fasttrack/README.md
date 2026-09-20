# NYC Affordable Housing Fast Track — Projecting the Twelve

The November 2025 charter amendment (Question 2) strips City Council approval
out of the land-use process for affordable housing in the twelve community
districts with the lowest **rate** of affordable housing development over the
preceding five years. The City Planning Commission adopted the methodology by
rule in April 2026, the first measurement cycle closed June 30, 2026, and DCP
must publish the list of twelve no later than **October 1, 2026**.

This project applies the adopted rule to public data to project that list
before it appears, and asks what the rule's choice of a *rate* — units built
over total housing stock, with no credit for affordable stock a district
already has — does to the result compared with the *count* ranking most
coverage assumes.

See [`spec_initialsetup.md`](spec_initialsetup.md) for the objective and
research questions, [`CLAUDE.md`](CLAUDE.md) for the rule as precisely as it
has been pinned down, and [`research/`](research/) for sourcing.

## Structure

```
afforable_housing_fasttrack/
├── research/          # Rule text, sources, plan, notes, limitations
│   └── handoff/       # Prior-session data pulls, kept for provenance
├── notebooks/         # Jupyter notebooks for exploration
├── src/               # Data collection and the codified ranking
├── data/
│   ├── raw/           # Socrata pulls, immutable
│   ├── processed/     # Ranking tables
│   └── external/      # Census and other non-Socrata reference data
└── article/           # Final writeup
    └── lib/           # Writeup assets (images, etc.)
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
cd src

# What each source is, and how far past the cycle end it actually runs
python collect.py freshness

# Everything, in dependency order (HPD first - the DOB and DCP project
# pulls are keyed off its BBLs and BINs)
python collect.py all
```

Pulls are cached as CSV under `data/raw/` and skipped if already present, so
an interrupted run can be topped up by re-running. `--refresh` forces a
re-fetch.

| Source | Dataset | Role |
|---|---|---|
| HPD Affordable Housing Production by Building | `hg8x-zxpr` | Numerator: affordable units and the rule's HPD "Start Date" |
| DOB NOW: Build – Approved Permits | `rbx6-tga4` | The rule's second milestone, current to this week |
| DOB Permit Issuance (legacy BIS) | `ipu4-2q9a` | Same, for jobs filed before the DOB NOW cutover |
| DCP Housing Database by Community District | `dbdt-5s7j` | Denominator: 2020 Census units + net new units |
| DCP Housing Database Project Level Files | `br6q-ssj3` | Cross-check on permit dates, already geocoded |
| DCP Housing Database by 2020 CDTA | `48dt-mn3z` | Independent read on the denominator |
| Community district boundaries | `dbdt-5s7j` (geojson) | Map geometry, keyed to the same district codes |

## Building the ranking

```bash
python rank.py
python rank.py --sensitivity     # alternative readings of the rule
python rank.py --understatement  # could undercounting alone explain the list?
```

Writes `data/processed/fast_track_ranking.csv` — one row per community
district with the numerator, the denominator, the rate, the rule's rate rank,
and the count rank the rate rank is being compared against.

The rule as implemented in `src/fasttrack.py`:

- **Numerator** — affordable dwelling units in buildings with *both* an
  HPD-reported Start Date and a DOB construction permit by cycle end, at least
  one of the two falling inside the cycle, excluding units in existing
  residential buildings under a preservation program.
- **Denominator** — 2020 Decennial Census housing units plus net new housing
  units through the cycle start.
- **Ranking** — numerator over denominator, ascending, twelve lowest, all 59
  community districts pooled. Joint interest areas (Central Park, the
  airports, Rikers, the cemeteries) are excluded; left in, their near-zero
  denominators would take the top of the list.

Every one of those steps involves a judgment call where the public data does
not line up exactly with the rule's language. Those calls are listed in
`research/limitations.md` and varied systematically in `rank.py --sensitivity`.

## Figures

```bash
python make_figures.py
python make_figures.py --only fast_track_map
```

Regenerates every figure into `article/lib/`. Chart styling and the validated
palette are in `src/charts.py`.

- **`fast_track_map.png`** — all 59 community districts outlined, with the
  projected twelve filled and numbered by rate rank.
- **`stock_comparison_map.png`** — the same map ranked both ways, showing
  which districts the list gains and loses if a district's *existing*
  affordable housing counts toward the measure. See
  `research/stock_adjusted_counterfactual.md`.

## Analysis

| Notebook | Phase |
|---|---|
| `profile_sources.ipynb` | Data profiling — coverage, permit match rates, the missing quarter |
| `ranking.ipynb` | The projected list, and rate vs. count |

`research/process_timeline.md` covers what being on the list does to a
project: the land-use clock goes from about seven months to about three, and
City Council review is removed entirely.

`research/definitions.md` pins down what "affordable" means here — the rule
defers to HPD, HPD counts toward a mayoral housing plan, and that plan's
income bands run up to 165% of AMI (about $252,000 for a three-person
household in 2026). Worth reading before quoting any unit count.

Findings are written up in `research/`. `research/limitations.md` is the
running list of what the data cannot support.

## Usage

```bash
source venv/bin/activate
jupyter notebook
```
