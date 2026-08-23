# Training-Weather Pipeline Design

**Question.** Does the weather a runner trained in — the weather in their hometown over the 90 days
and 30 days before race day — affect their marathon result?

> **Status: implemented and run.** See `RESULTS_training_weather.md` for findings and
> `./run_pipeline.sh` to reproduce. Two estimates in this document turned out to be wrong
> and are corrected inline below: the Stage 1 fetch cost (section 2.1 / Stage 1) and the
> availability of runner identity (Stage 0b, which proved *better* than assumed).

This document reviews what the project already has, identifies what blocks the question, and
specifies the pipeline to answer it. Numbers below were measured against the current data, not
estimated.

---

## 1. Review: what already exists

The project is further along than the file layout suggests. A 90/30-day hometown-weather featurizer
already runs end to end.

**Current chain**

```
data/race_records/*.parquet (1,335 files)  ─┐
data/marathon_results_dedup.csv (619k)     ─┴─> src/process_join.py (JoinEtlModule)
                                                  └─> data/joined_race_data_v2/global/data.csv
                                                        2,120,259 rows
                                                        race,date,city,state,age,sex,time_minutes,join_key,source
                                                          └─> src/process_weather_features.py
                                                                (90d "full" + 30d "peak" windows,
                                                                 keyed on runner hometown)
                                                                └─> data/featurized_race_data_v3.csv
                                                                      1,857,106 rows, 21 cols
                                                                      └─> enrich_race_day_weather.py
                                                                            └─> ..._v3_with_raceday_weather.csv
                                                                                  1,857,106 rows, 26 cols
```

`src/process.py` provides a small build system (`EtlModule` + `click` CLI): partitions are
directories under `--out`, and an existing partition directory is skipped rather than rebuilt.
`src/etl_config.py` holds `WeatherConfig(FULL_TRAINING_DAYS=90, PEAK_TRAINING_DAYS=30,
MIN_WEATHER_RECORDS=5, PRECIP_THRESHOLD=0.2, EARLIEST_WEATHER_DATE=2000-01-01)`.

**Existing training-weather features** (7 metrics × 2 windows, window is `[race_date - N, race_date)`):
`{full,peak}_temp_min`, `_temp_max`, `_temp_median_min`, `_temp_median_max`, `_overall_precip`,
`_overall_days_of_precip`, `_overall_weekend_days_of_precip`.

So the featurization concept in `research.md` is already built. What is missing is the coverage,
the physiologically relevant variables, and — mainly — an identification strategy that separates
"training weather" from "which city you live in."

---

## 2. What blocks the question

### 2.1 Weather covers 109 cities, capping the sample at 25%

`data/weather_data_v2.csv` has 1,058,210 daily rows but only **109 distinct (city, state)** pairs.
Measured against `data/city_state_runner_counts_v2.csv` (7,409,199 runner-rows):

| Cities fetched | Share of runner-rows covered |
|---|---|
| **109 (today)** | **25.1%** |
| 250 | 38.3% |
| 500 | 48.3% |
| 1,000 | 59.7% |
| 2,000 | 71.5% |
| 3,466 (`top_training_cities_80pct.csv`) | 80.1% |

This is the single largest constraint. It also caps home-city cardinality at 109, which is what
starves the interaction terms in §5.

Coverage is not uniform: all 109 cities start 1999-01-01, but 41 stop at 2025-03-01 and
`st. george, ut` stops at **2014-03-14** (4,311 missing days). Any panel assumed to be balanced
will silently drop city-years.

### 2.2 Only three weather variables, and none of them is heat stress

Only `temp_max`, `temp_min`, `precip` were ever fetched. Marathon performance responds to *thermal
load* — wet-bulb / dew point / solar radiation — not dry-bulb max. I confirmed the Open-Meteo
archive returns all of these in the **same single request**, at no extra cost:

```
temperature_2m_max/min/mean, apparent_temperature_max/mean, dew_point_2m_mean,
relative_humidity_2m_mean, wet_bulb_temperature_2m_mean, precipitation_sum,
rain_sum, snowfall_sum, precipitation_hours, wind_speed_10m_max,
shortwave_radiation_sum, daylight_duration
```

Since §2.1 requires re-fetching anyway, the extra variables are free.

### 2.3 The precipitation threshold is wrong by a factor of 25

`WeatherConfig.PRECIP_THRESHOLD = 0.2` is commented `(inches)`, but the stored `precip` is in
**millimetres** — the existing fetch omits `precipitation_unit`, and Open-Meteo defaults to mm.
Verified: Boston 2019 sums to 1,324.2 (mm ≈ 52 in; the value is nonsense as inches).

So every `*_days_of_precip` and `*_weekend_days_of_precip` column in v3 counts days with
**> 0.2 mm** — any measurable trace — not 0.2 in (5.08 mm). Those columns need recomputation with an
explicit unit.

This is fixable by arithmetic (mm ÷ 25.4) and needs **no refetch** — which is what allowed the
pipeline to run in full despite the quota limit above. Confirmed after conversion: annual city
totals sit at p05 7.6″ / median 36.9″ / p95 65.3″. Measured effect: 32.3 → 12.7 wet days per 90.

### 2.4 The featurizer re-derives every window per race-date group

`_process_race_date_group` (`src/process_weather_features.py:245`) slices the hometown's full
weather history once per (race, date, city) group, under a `ThreadPoolExecutor`. The window bounds,
the `>5 records` gate and the `0.2` threshold are hardcoded there, duplicating the config-driven
`_get_training_windows` / `_extract_weather_features` (lines 315, 346) that only the unit tests
reach. Adding a third window means editing three code paths.

The work is inherently O(city-days), not O(race-runners): for a given city, the 90-day window
ending on date *d* is a rolling aggregate. Restructuring to precompute it once (§4, Stage 2)
collapses ~8,500 grouped slices into one vectorized pass over 1.07M city-day rows, and makes new
windows nearly free.

### 2.5 There is no identification strategy — this is the real problem

**Training weather is almost entirely a restatement of "which city do you live in."** Measured on
the current v3 data (1.42M rows passing basic quality filters):

- Cross-sectional SD of 90-day training temp: **12.65 °F**
- SD *within* a (home city, race) cell, across years: **2.47 °F** (median of 12,168 cells)

So ~96% of the variance in "training weather" is fixed city climate. A naive regression of finish
time on training temperature is a regression on **city identity**, and cities differ in runner
quality, altitude, income, running culture and — critically — who self-selects into travelling to a
marathon. That estimate cannot be read causally.

The good news: the residual variation is small but the sample is huge, and there is a second,
stronger source of variation. Within a single **race instance** (same race, same day, same course,
same weather, same field), runners arrive from many different home climates:

- 6,516 race instances; median 33 runners and 10 distinct home cities
- 1,633 instances with ≥100 runners and ≥5 home cities → **1,286,731 rows (90.6%)** retained
- Within-race-instance SD of 90-day training temp: **6.02 °F** — half the total cross-sectional SD

A race-instance fixed effect therefore absorbs course, field, date and race-day conditions exactly,
while leaving substantial training-weather variation to work with. Combining that with a home-city
fixed effect removes the selection confound and leaves the year-to-year weather shock.

**Feasibility smoke test.** I ran the two-way FE spec below on the existing v3 data (N = 1,048,872;
1,013 instances; 109 cities; alternating-projection demeaning, SEs clustered on home city):

| term | coef (min) | SE | t |
|---|---|---|---|
| 90-day training temp (°F) | **+0.320** | 0.082 | 3.93 |
| training temp × race-day temp | −0.0003 | 0.0019 | −0.16 |
| age (centered) | +0.535 | 0.044 | 12.28 |
| age² | +0.0375 | 0.0011 | 32.95 |
| male | −28.66 | 0.476 | −60.17 |

A 1-SD warmer training block (2.9 °F within-transform) costs roughly **0.9 minutes**. The design
works and the machinery is sound. The interaction is null here, but with only 109 home cities that
term is badly under-powered — which is exactly what §2.1 fixes.

---

## 3. Design decisions

| Decision | Choice | Why |
|---|---|---|
| Window definition | `[race_date − N, race_date)`, N ∈ {90, 30}, race day excluded | Matches existing behaviour and the brief; race day is a separate covariate, not training |
| Geographic key | `(city, state)` lowercased — unchanged | Already the key everywhere; `join_key` convention preserved |
| Window computation | Per-city rolling aggregates precomputed once, then a plain merge | O(city-days); new windows become a config change |
| Storage | Parquet for tables > 50 MB, CSV kept only for small reference tables | v3 CSV is 313 MB and reparsed on every notebook run |
| Outcome | `time_minutes` primary; age-graded z and `log(time)` as secondaries | Existing analyses use minutes; the age-grading lookup already exists but has no consumer |
| Identification | Two-way FE: race-instance × home-city | Only spec that separates weather from city (§2.5) |
| Provenance | A filter ledger written by every stage | `research.md` asks for this; today skip counts are only printed |

---

## 4. Pipeline

Six stages. Stages 0–3 are ETL under the existing `EtlModule` framework; Stages 4–5 are the
analysis layer. Each stage writes `_ledger.csv` alongside its output.

```
Stage 0  build city roster            src/build_city_roster.py       → data/city_roster.csv
Stage 0b recover runner identity      src/process_racedata.py (fix)  → data/race_final/ (+ runner_id)
Stage 1  fetch daily weather          src/fetch_weather.py           → data/weather_daily/
Stage 2  rolling window features      src/process_city_windows.py    → data/city_window_features/
Stage 3  join to race results         src/process_training_weather.py→ data/training_weather/
Stage 4  analysis-ready panel         src/build_analysis_panel.py    → data/analysis_panel.parquet
Stage 5  estimation                   src/estimate_training_effect.py→ data/estimates/
```

### Stage 0 — City roster

Rank `data/city_state_runner_counts_v2.csv` by `runner_count`, join `data/uscities.csv` on
lowercased `(city_ascii, state_id)` for `lat`/`lng`, cut at a cumulative-coverage target.

Reuses the geocode join already written in `Map_Cities.ipynb` cells 0–2; this stage is that
notebook code promoted to a module.

**Output** `data/city_roster.csv`: `city, state, runner_count, cum_pct, lat, lng, priority`

**Target: 2,000 cities → 71.5% of runner-rows** (up from 25.1%).

> **Correction (measured after this document was written).** Open-Meteo bills *weighted units*,
> not requests: `n_variables × n_days / 14`, against caps of 5,000/hour and 10,000/day. One city
> over 1999–2026 costs ~2,163 units at 3 variables and ~10,815 at 15 — a single full-range
> 15-variable request exhausts the hourly quota outright. The existing 109-city archive represents
> ~24 days of free-tier quota, and 2,000 cities would take **~15 months**, not 8 hours. The roster
> and fetcher are built and resumable; running them at scale requires a commercial API key
> (`--api-key`). The pipeline therefore runs on the existing 109 cities. See
> `RESULTS_training_weather.md` for the consequences.

Ordering by `priority` means an interrupted run still yields the most valuable cities.

Unresolvable names go to the ledger with `reason=no_geocode` rather than being dropped silently —
this is the material that `analyze_missing_race_cities.py` currently surfaces by hand.

### Stage 0b — Recover runner identity

`enriched_raw_data.parquet` (5,268,288 rows, the pre-normalization scrape dump) carries a
`Last Name, First Name (Sex/Age)` column that is **100% populated**. `src/process_racedata.py`
discards it when building `data/race_records/*.parquet`, which is why runner identity is absent
everywhere downstream. Retaining it costs nothing and unlocks the strongest available spec.

Measured on the raw dump, using `City, State, Country` (72.1% non-null) backfilled with
`City, State` (2.8%) for a 74.9% location rate, and a person key of
`lower(name minus the "(M42)" suffix) | lower(city, state)`:

| | runners | rows | share of located rows |
|---|---|---|---|
| ≥ 2 races | 552,231 | 1,668,525 | **42.3%** |
| ≥ 3 races | 223,105 | 1,010,273 | 25.6% |

**1.67M runner-races are attached to a repeat runner.** That supports a runner fixed effect, which
controls for ability, experience and motivation directly — the confounds Spec (A) can only address
at city level.

Work: parse the `(Sex/Age)` suffix off the name, build `runner_id`, carry it through
`process_racedata.py` → `process_join.py` → the panel. Fix the `time_to_minutes` /
`_time_to_minutes` `AttributeError` at `src/process_racedata.py:35` in the same pass, since that
path currently cannot rerun at all.

### Stage 1 — Daily weather ingest

Rewrite of `fetch_weather_incremental` (`Map_Cities.ipynb` cell 9) as a module.

- One request per city for `1999-01-01 → today`, all 15 daily variables (§2.2)
- **Explicit units**: `temperature_unit=fahrenheit`, `precipitation_unit=inch`, `wind_speed_unit=mph`,
  `timezone=auto`. Fixes §2.3 at the source.
- Resume by reading completed `(city, state)` from the output, as today
- Exponential backoff on 429 mirroring `src/scraper/scraper.py:161-191`, rather than the current
  "sleep 60 then raise"
- **Partition by first letter of city** → `data/weather_daily/<a-z>/data.parquet`, so `EtlModule`'s
  skip-existing semantics give free incremental resume at partition granularity

**Output** `data/weather_daily/<bucket>/data.parquet`:
`city, state, date, temp_max, temp_min, temp_mean, apparent_temp_max, apparent_temp_mean,
dew_point_mean, humidity_mean, wet_bulb_mean, precip_in, rain_in, snow_in, precip_hours,
wind_max, solar_mj, daylight_s`

Backfilling the existing 109 cities with the new variables is part of this stage — the old
`weather_data_v2.csv` is retained only until Stage 2 is validated against it.

### Stage 2 — Per-city rolling window features (the architectural change)

Instead of slicing each hometown's history once per race-date group, compute every window once per
city-day. For each city, on a complete daily date index:

```python
for name, days in config.WINDOWS.items():        # {"full": 90, "peak": 30}
    roll = city_df.rolling(f"{days}D", closed="left")   # [d-N, d) — excludes race day
```

`closed="left"` reproduces the current `inclusive='left'` semantics exactly.

**Feature set per window** (existing 7, plus the additions the question actually needs):

*Carried over* — `temp_min`, `temp_max`, `temp_median_min`, `temp_median_max`, `overall_precip`,
`days_of_precip`, `weekend_days_of_precip` (the last two now on a correct inch threshold).

*Thermal load* — `temp_mean`, `wet_bulb_mean`, `dew_point_mean`, `apparent_temp_mean`,
`heat_days` (days with `temp_max ≥ 80 °F`), `cold_days` (`temp_max ≤ 40 °F`),
`freeze_days` (`temp_min ≤ 32 °F`), `solar_mean`.

*Acclimatization dose* — heat acclimatization is driven by recent, repeated exposure, so add an
**exponentially weighted mean** of `wet_bulb_mean` with a 21-day half-life (`ewm_wetbulb_21d`) and
the same for `temp_mean`. This is the physiologically correct shape and is not expressible as a
flat window average.

*Trajectory* — `temp_trend` (OLS slope of daily mean temp across the window, °F/week). A block that
warms into race day is a different stimulus from one that cools, even at identical means.

*Disruption* — `pct_days_hostile`: share of days with `precip ≥ 0.2 in` **or** `temp_max ≤ 25 °F`
**or** `temp_max ≥ 90 °F`. A proxy for sessions likely missed or moved indoors, which is the actual
causal channel from weather to fitness.

*Coverage* — `n_days_observed` per window, carried forward so Stage 4 can filter on data
sufficiency instead of the current hardcoded `> 5` gate.

**Output** `data/city_window_features/<bucket>/data.parquet`, keyed `city, state, date` — one row
per city-day, ~1.07M rows for 109 cities and ~20M for 2,000.

This is what makes new windows cheap: adding a 14-day taper window or a 180-day base window is one
entry in `WeatherConfig.WINDOWS`.

### Stage 3 — Join to race results

`data/joined_race_data_v2/global/data.csv` ⋈ Stage 2 on `(city, state, date)` — a straight merge,
replacing the entire `ThreadPoolExecutor` path. Then fold in `enrich_race_day_weather.py` as the
second half of the same stage: `race_locations_normalized.csv` gives `race_location_city/state`,
and Stage 1's daily table gives race-day conditions for the **race** city.

Race-day columns extend to `race_day_wet_bulb`, `race_day_dew_point`, `race_day_apparent_max`,
`race_day_wind_max` alongside the existing temp/precip.

**Mismatch features** — computed here because they need both sides. These operationalize
`research.md`'s "if a race has dramatically different weather than the weather people train with":

```
mismatch_temp      = race_day_temp_max     − full_temp_mean
mismatch_wetbulb   = race_day_wet_bulb     − full_wet_bulb_mean
mismatch_temp_peak = race_day_temp_max     − peak_temp_mean
mismatch_z         = mismatch_temp / (within-city SD of daily temp_max over the window)
```

`mismatch_z` matters: 15 °F above what you trained in means something different in San Diego
(low variance, unadapted) than in Denver (high variance, adapted).

**Output** `data/training_weather/global/data.parquet`

### Stage 4 — Analysis panel

Applies filters, builds the outcome and the FE keys, and writes the ledger that `research.md` asks
for.

Filters, each logged with a row count:

| Filter | Rule |
|---|---|
| Plausible finish | `120 ≤ time_minutes ≤ 420` |
| Plausible age | `18 ≤ age ≤ 85` (v3 contains `age = 3225`) |
| Sex present | `sex ∈ {M, F}` — **normalize case first**: the scraper writes `m`/`f`, every notebook filters `M`/`F`, so new records are currently dropped silently |
| Window coverage | `n_days_observed ≥ 0.8 × window_days` for both windows |
| Race-day weather | present (needed for mismatch terms) |
| Instance size | `≥ 100 finishers` and `≥ 5 distinct home cities` per race instance |
| City support | home city appears in `≥ 3` distinct race-years |

The instance filter retains **90.6%** of quality-filtered rows, so it is cheap.

Outcomes: `time_minutes` (primary), `log_time`, and `ag_z` — the robust z-score
`(time − p50) / (p75 − p25)` from `notebooks/feature_age_graded_percentiles.ipynb` via
`data/age_graded_percentiles.csv`. That lookup exists and has never been used; this is its consumer.

Keys: `instance_id = race|date`, `home_id = city|state`, `city_year = city|state|year`.

**Output** `data/analysis_panel.parquet` + `data/analysis_panel_ledger.csv`
(`stage, filter, rows_in, rows_out, rows_dropped, reason`)

### Stage 5 — Estimation

Three estimands, reported side by side. Reporting all three is the point — the gap between them
*is* the selection effect, and it is itself a result.

**(A) Causal core — two-way FE.** Absorbs race instance and home city; identified off year-to-year
weather shocks in a runner's hometown.

```
time_igc = α_g(race×date) + γ_c(home city) + β·TrainWeather_ct
           + δ·(TrainWeather_ct × RaceDayWeather_g)
           + f(age) + sex + ε
```

`RaceDayWeather_g` has no main effect: it is collinear with the instance FE and correctly absorbed.
δ is the acclimatization hypothesis — does training hot protect you on a hot race day? Cluster SEs
on home city (109 today, ~2,000 after Stage 1; the current count is thin for cluster inference,
which is a further reason to run Stage 1 first).

**(B) Descriptive — "hardy cities."** Race-instance FE only, home-city FE replaced by city
*coefficients*. This answers `research.md`'s "toughest town" / "most resilient city" questions
directly: a city's coefficient is its runners' average time relative to everyone else in the same
races on the same days. Explicitly labelled **confounded by selection** — it measures who travels
from that city as much as how they trained.

**(C) Heterogeneity.** Spec (A) interacted with age bracket (reuse the Boston-qualifying brackets
already standard in the research notebooks), sex, and a fast/slow split at the within-instance 25th
percentile. Answers "how does weather hardiness change with age/sex" and "do top runners respond
differently."

**(D) Strongest spec — runner FE.** Available once Stage 0b lands, on the ~1.67M rows with a
repeat runner. Replaces the home-city FE with a runner FE:

```
time_igr = α_g(race×date) + θ_r(runner) + β·TrainWeather_ct + δ·(TrainWeather × RaceDayWeather_g)
           + f(age) + ε
```

θ_r absorbs the home city (runners rarely move within the sample), so β is identified off the same
person racing under different training conditions — the cleanest read available on this data. Note
the sample shifts toward frequent racers, so (D) and (A) answer slightly different questions; report
both.

Implementation: alternating-projection demeaning + `numpy.linalg.lstsq` + cluster-robust sandwich
SEs — about 40 lines, already validated in the smoke test above, and consistent with the project's
sklearn-only convention. `statsmodels` is not installed; adding `pyfixest` would be cleaner but is a
new dependency, so the hand-rolled version is the default unless you want the dependency.

---

## 5. Config changes

`src/etl_config.py`:

```python
@dataclass
class WeatherConfig:
    WINDOWS: dict[str, int] = field(default_factory=lambda: {"full": 90, "peak": 30})
    MIN_WINDOW_COVERAGE: float = 0.8        # replaces MIN_WEATHER_RECORDS = 5
    PRECIP_THRESHOLD_IN: float = 0.2        # renamed — was ambiguous, data was mm
    HEAT_DAY_F: float = 80.0
    COLD_DAY_F: float = 40.0
    ACCLIM_HALFLIFE_DAYS: int = 21
    EARLIEST_WEATHER_DATE: datetime = datetime(1999, 1, 1)   # matches actual data

@dataclass
class PanelConfig:
    MIN_TIME_MIN: float = 120.0
    MAX_TIME_MIN: float = 420.0
    MIN_AGE: int = 18
    MAX_AGE: int = 85
    MIN_INSTANCE_FINISHERS: int = 100
    MIN_INSTANCE_CITIES: int = 5
    MIN_CITY_RACE_YEARS: int = 3
```

Note `EARLIEST_WEATHER_DATE` is currently `2000-01-01` while the data starts `1999-01-01`; the
derived `min_race_date` gate (`process_weather_features.py:123`) therefore discards ~15 months of
usable 2000-season races.

---

## 6. Files

**New**
```
src/build_city_roster.py           Stage 0
src/fetch_weather.py               Stage 1  (promotes Map_Cities.ipynb cells 4/9)
src/process_city_windows.py        Stage 2  (the rolling-window core)
src/process_training_weather.py    Stage 3  (absorbs enrich_race_day_weather.py)
src/build_analysis_panel.py        Stage 4
src/estimate_training_effect.py    Stage 5
src/ledger.py                      shared filter-provenance writer
tests/test_process_city_windows.py
tests/test_build_analysis_panel.py
notebooks/research_training_weather.ipynb
```

**Modified**
```
src/etl_config.py                  window dict, panel config, unit fix
src/process_racedata.py            retain runner name -> runner_id; fix _time_to_minutes typo
src/process_join.py                carry runner_id through the unified schema
src/etl_utils.py                   add TextNormalizer.normalize_sex  (m/f → M/F)
tests/test_process_weather_features.py   currently fails — TestFileTypeDetection asserts the old
                                         loose _is_race_file/_is_weather_file behaviour
```

**Retired**
```
src/process_race_weather_join.py   dead — process_files ends in `return 1/0`
src/process_weather_features.py    superseded by Stages 2-3; keep until parity is verified
enrich_race_day_weather.py         folded into Stage 3
```

---

## 7. Verification

**Unit** — extend the existing pytest suite (`tests/README.md` conventions, `sys.path` bootstrap as
in `tests/test_process_join.py:14`):
- Rolling windows on a synthetic 200-day city series: assert `[d−90, d)` bounds, race day excluded,
  `n_days_observed` correct across a deliberate gap
- `days_of_precip` with values straddling 0.2 in — the §2.3 regression test
- EWM acclimatization reduces to the flat mean when half-life → ∞
- Ledger row counts sum: `rows_in == rows_out + rows_dropped` at every stage

**Parity** — run Stage 2+3 restricted to the current 109 cities and the 90/30 windows, then compare
against `data/featurized_race_data_v3.csv` row for row. Temp features must match to floating-point
tolerance. The precip-day counts will **not** match, and shouldn't — that is the §2.3 fix. Any other
divergence is a bug.

**Coverage** — after Stage 1, assert covered runner-rows ≥ 70% of 7,409,199 (vs 1,858,843 today) and
that no city has an unflagged gap like `st. george, ut`.

**End-to-end**
```
.venv/bin/python src/build_city_roster.py --target-coverage 0.72
.venv/bin/python src/fetch_weather.py --roster data/city_roster.csv --out data/weather_daily
.venv/bin/python src/process.py ingest --src data/weather_daily --out data/city_window_features \
    --name process_city_windows.CityWindowEtlModule
.venv/bin/python src/process.py ingest --src data/training_weather_input --out data/training_weather \
    --name process_training_weather.TrainingWeatherEtlModule
.venv/bin/python src/build_analysis_panel.py
.venv/bin/python src/estimate_training_effect.py --spec all
pytest tests/ -v
```

**Statistical sanity** — before reading any coefficient:
1. Reproduce the smoke test (§2.5): 90-day training temp ≈ +0.32 min/°F, t ≈ 3.9 on the 109-city
   sample. A large move after expanding to 2,000 cities is a signal to investigate, not to celebrate.
2. Placebo: replace the training window with the **90 days *after*** the race. A non-zero
   coefficient means the FE structure is leaking city-season effects.
3. Report (A) and (B) together. If they disagree sharply — likely — the difference is the selection
   effect and belongs in the writeup, not in a footnote.

---

## 8. Sequencing

| # | Work | Unblocks | Rough effort |
|---|---|---|---|
| 1 | Stage 0 + Stage 1 (2,000 cities, 15 vars, correct units) | Everything; 25% → 72% coverage | ~1 day code, ~8 h unattended fetch |
| 1b | Stage 0b recover `runner_id` from the raw parquet | Spec (D), the strongest estimate | ~0.5 day |
| 2 | Stage 2 rolling windows + parity test | Cheap new windows; kills the O(race-groups) path | ~1 day |
| 3 | Stage 3 join + mismatch features | The mismatch hypothesis | ~0.5 day |
| 4 | Stage 4 panel + ledger | Reproducible filtering; `research.md`'s bias audit | ~0.5 day |
| 5 | Stage 5 estimation (A/B/C) | The answer | ~1 day |

Stage 1's fetch is the long pole and has no dependencies — start it first and build Stages 2–4
against the existing 109 cities while it runs.

---

## 9. Known limitations to carry into the writeup

- **Runner identity is a fuzzy key.** Spec (D) matches on `name|city`, which conflates homonyms and
  breaks on name changes and relocation. It is the standard approach in this literature, but the
  runner FE is measured with error and β from (D) should be read alongside (A), not instead of it.
- **Hometown is a point-in-time self-report**, recorded at the race. Runners move; the 90-day window
  is attributed to the city they listed on race day. Unmeasurable here, and it attenuates β toward
  zero.
- **Daily aggregates, not session weather.** `race_day_temp_max` is a daily max, typically well above
  what an 8 a.m. start actually experienced. Adding hourly data for race mornings only (~8,500
  requests) would sharpen the race-day side considerably, and is a natural follow-on.
- **Training weather is not training.** Weather constrains training; it does not determine it.
  Treadmills, indoor tracks and travel all break the link, and none are observed.
- **Selection into racing is not addressed by any spec here.** Spec (A) removes the *level* effect of
  living in a given city, but not the possibility that a bad winter changes *who* from that city
  shows up. If harsh training weather deters marginal runners, β is biased toward "bad weather makes
  you faster." A first-stage check on finisher counts per (city, race-year) against training weather
  would size this; it belongs in Stage 5 as a diagnostic.
