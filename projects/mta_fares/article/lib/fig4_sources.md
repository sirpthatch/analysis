# Figure 4 — sources for every number

`fig4_fare_evasion.png` · rendered by `fig_evasion()` in `src/figures.py`
(`python src/figures.py`) from `farecap.evasion.rates()`.

## What is plotted

Quarterly estimated **share of riders who enter without paying**, 2021-Q1 to
2026-Q2 (22 quarters, no gaps):

- **Subway** (blue line) — MTA NYCT subway fare evasion rate; shaded band is the
  MTA's published margin of error (95% confidence).
- **Bus, all service** (orange line) — the dataset's `Total` row: local/limited,
  Select Bus Service and express combined, as published by the MTA.

Both values are published by the MTA as fractions (e.g. `0.102`) and plotted ×100.
Nothing is modelled, smoothed or adjusted.

## Sources

| | Subway | Bus |
|---|---|---|
| Dataset | MTA NYCT Subway Fare Evasion: Beginning 2018 | MTA Bus Fare Evasion: Beginning 2019 |
| Publisher | Metropolitan Transportation Authority, via data.ny.gov | same |
| Dataset ID | `6kj3-ijvb` | `uv5h-dfhp` |
| URL | https://data.ny.gov/d/6kj3-ijvb | https://data.ny.gov/d/uv5h-dfhp |
| Columns used | `time_period`, `fare_evasion`, `margin_of_error` | `time_period`, `trip_type` (= `Total`), `fare_evasion` |
| Data last updated (portal metadata `rowsUpdatedAt`) | 2026-07-24 | 2026-07-24 |
| Retrieved | 2026-09-28 | 2026-09-28 |
| Cached copy | `data/raw/evasion_subway.json` | `data/raw/evasion_bus.json` |
| Methodology document | MTA, "MTA NYCT Subway Fare Evasion Overview" (dataset attachment), saved as `research/docs/MTA_SubwayFareEvasion_Overview.pdf` | MTA, "MTA Bus Fare Evasion Overview" (dataset attachment), saved as `research/docs/MTA_BusFareEvasion_Overview.pdf` |

Exact API calls (each returns the full dataset):

```
https://data.ny.gov/resource/6kj3-ijvb.json?$limit=1000
https://data.ny.gov/resource/uv5h-dfhp.json?$limit=1000
```

## How each series is measured (MTA's own description)

**Subway — traffic-checker survey.** "The fare evasion rate is the estimated
percentage of riders who illegally enter the system. It is estimated using surveys
conducted by traffic checkers and by applying stratified sampling." The sample is
"about 0.03 percent of the 2.1 million combinations per quarter of unique control
areas (970) and unique hours (2,160)"; the margin of error gives a 95 percent
confidence interval. *(Subway Overview, "General Description" and "Data Collection
Methodology")*

**Bus — passenger counters vs. fare taps.** "Since Q4 2020, the BFE rate has been
estimated using Automated Passenger Counter (APC) and Automated Fare Collection
(AFC) data." For each route it is "the difference between the number of boardings
and paid fares divided by boardings … To get the systemwide BFE rate, each route's
BFE rate is weighted by its total paid ridership." The MTA describes the method
as follows: "Unlike previous methods, it is calculated based on actual data, not a
survey." *(Bus Overview, "General Description", "Data Collection Methodology" and
"Calculation")* The dataset publishes no margin of error for bus, so the orange
line has no band.

The two series measure evasion differently (sampled observation vs. sensor counts),
so compare each line with itself over time. The gap between the two lines is a
real difference in scale, but part of it may come from the different methods.

## Why the figure starts in 2021-Q1

1. **Subway method change.** The current subway method "was first used in Q1 2021,
   with a slight change made in Q4 2023. This method is not apples-to-apples
   comparable to the previous rates calculated using the previous methodology."
   *(Subway Overview)* Earlier quarters in the dataset (2018–2020) use the old
   method.
2. **Gaps before 2021.** Subway 2020-Q2 is blank (no survey during the pandemic);
   subway margins of error are not published before 2020-Q1; the bus `Total` row
   starts in 2020-Q4, when the bus method moved from surveys to counters ("Fare
   evasion was not reported in Q2 or Q3 2020, as checkers were not deployed due to
   the COVID-19 pandemic." — Bus Overview).

From 2021-Q1 on, both plotted series are complete and each uses a single
methodology throughout (apart from the "slight change" to subway sampling in
2023-Q4, which the MTA did not flag as a break).

## The plotted values

| Quarter | Subway | Subway margin of error | Subway 95% interval (shaded) | Bus, all service |
|---|---|---|---|---|
| 2021-Q1 | 11.3% | ±0.9% | 10.4% – 12.2% | 24.9% |
| 2021-Q2 | 10.6% | ±0.8% | 9.8% – 11.4% | 24.7% |
| 2021-Q3 | 9.2% | ±1.1% | 8.1% – 10.3% | 26.0% |
| 2021-Q4 | 9.8% | ±1.6% | 8.2% – 11.4% | 30.0% |
| 2022-Q1 | 12.5% | ±1.5% | 11.0% – 14.0% | 33.6% |
| 2022-Q2 | 12.2% | ±1.2% | 11.0% – 13.4% | 34.2% |
| 2022-Q3 | 13.4% | ±1.2% | 12.2% – 14.6% | 35.9% |
| 2022-Q4 | 13.5% | ±1.6% | 11.9% – 15.1% | 39.3% |
| 2023-Q1 | 11.1% | ±0.9% | 10.2% – 12.0% | 40.5% |
| 2023-Q2 | 12.3% | ±1.1% | 11.2% – 13.4% | 42.1% |
| 2023-Q3 | 14.0% | ±1.2% | 12.8% – 15.2% | 43.7% |
| 2023-Q4 | 13.3% | ±1.0% | 12.3% – 14.3% | 47.3% |
| 2024-Q1 | 13.6% | ±0.9% | 12.7% – 14.5% | 48.9% |
| 2024-Q2 | 14.0% | ±1.1% | 12.9% – 15.1% | 50.6% |
| 2024-Q3 | 13.1% | ±1.1% | 12.0% – 14.2% | 49.2% |
| 2024-Q4 | 10.4% | ±0.8% | 9.6% – 11.2% | 46.0% |
| 2025-Q1 | 9.8% | ±0.9% | 8.9% – 10.7% | 44.9% |
| 2025-Q2 | 9.9% | ±0.9% | 9.0% – 10.8% | 45.1% |
| 2025-Q3 | 11.0% | ±0.8% | 10.2% – 11.8% | 44.1% |
| 2025-Q4 | 10.1% | ±0.8% | 9.3% – 10.9% | 48.9% |
| 2026-Q1 | 10.6% | ±1.0% | 9.6% – 11.6% | 48.2% |
| 2026-Q2 | 10.2% | ±1.0% | 9.2% – 11.2% | 48.5% |

## Numbers that appear on the figure

- **"subway 10.2%"** — subway rate, 2026-Q2.
- **"bus 48.5%"** — bus `Total`, 2026-Q2.
- **Title, "subway fell to ~10% in late 2024"** — 13.1% in 2024-Q3 to
  10.4% in 2024-Q4; every quarter since has been between 9.8%
  and 11.0%. The 2024-Q3 → Q4 drop (2.7 points)
  is larger than either quarter's margin of error
  (±1.1%, ±0.8%).
- **Title, "bus stays near half"** — bus `Total` has been between
  44.1% and 50.6% since 2023-Q4, peaking at
  50.6% in 2024-Q2.
