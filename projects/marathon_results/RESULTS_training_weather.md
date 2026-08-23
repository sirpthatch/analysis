# Does training weather affect marathon performance?

Results from the pipeline specified in `TRAINING_WEATHER_PIPELINE.md`, run end to end
on 2026-08-21. Reproduce with `./run_pipeline.sh` (~1m45s from a clean tree).

**Panel:** 1,003,814 runner-races | 953 race instances | 109 home cities | 2000–2023
**Training weather:** mean temperature in the runner's hometown over the 90 days
before race day, `[race_date − 90, race_date)`. Race day itself is excluded.

---

## Headline

**The apparent effect of training weather is composition, not physiology.**

Controlling for home city, a warmer 90-day training block predicts a slower race:
**+0.33 minutes per °F** (t = 3.03). But controlling for *the runner* instead, the
effect disappears: **−0.03 min/°F** (t = −0.29). On the identical sample the two
specifications give +0.379 and −0.027, and the city-FE estimate falls **outside** the
runner-FE confidence interval — so this is not a power problem.

What generates the gap is turnout. A warmer training season brings **1.7% more
finishers per °F** out of a given city (t = 4.86). The extra entrants are
disproportionately slower, which raises the city's average time in warm years without
any individual running worse.

| Specification | Fixed effects | Coef (min/°F) | SE | t | N |
|---|---|---|---|---|---|
| **A** causal core | race-instance × home-city | **+0.325** | 0.107 | 3.03 | 1,003,814 |
| A, 30-day peak window | race-instance × home-city | +0.216 | 0.085 | 2.53 | 1,003,814 |
| A, age-graded outcome | race-instance × home-city | +0.0041 z | 0.0013 | 3.08 | 999,774 |
| A, restricted to D's sample | race-instance × home-city | +0.379 | 0.099 | 3.83 | 614,503 |
| **D** runner FE | race-instance × **runner** | **−0.027** | 0.093 | −0.29 | 614,503 |
| *Placebo:* post-race weather | race-instance × home-city | +0.036 | 0.071 | 0.50 | 1,003,814 |
| *Selection:* log finishers | race-instance × home-city | +0.0172 | 0.0035 | 4.86 | 44,857 |

Spec D is identified off 99,009 runners with within-runner variation in training
temperature (median SD 2.32 °F).

### The placebo passes

Weather in the 90 days **after** the race — which cannot affect it — gives +0.036
min/°F, t = 0.50. The fixed-effect structure is not leaking city-season effects, so
spec A's +0.325 is a real feature of the data. It is just not a training effect.

---

## Reading the specifications against each other

The three estimates answer three different questions, and reporting only one would
have been misleading:

- **Spec A (+0.33)** — "in years when a city's spring was warmer, were that city's
  marathon results slower?" **Yes.** But the runners are not the same people.
- **Spec D (−0.03)** — "when the *same runner* trained through a warmer block, did
  they race slower?" **No, and the effect is bounded near zero.**
- **Selection (+1.7%/°F)** — "did a warmer training season change who showed up?"
  **Yes**, and in the direction that manufactures spec A's result.

Anyone stopping at spec A would report that warm training costs ~0.8 minutes per
1-SD (2.5 °F) warmer block. That number is real but not causal, and the pipeline
was built to catch exactly this.

### Acclimatization

The interaction between training temperature and race-day temperature — does training
hot protect you when race day is hot? — is null in spec A (+0.0009, t = 0.62) and in
spec D (−0.00003, t = −0.01). The 30-day window shows a marginal +0.0027 (t = 1.92)
in the *wrong* direction for acclimatization, and it does not survive the runner FE.
**No support for a training-climate acclimatization effect in this data.** With only
109 home cities the interaction is the least well-powered term in the model; this is
the result most likely to change with broader weather coverage.

---

## Spec B — "hardy cities" (descriptive, confounded)

Average finish time relative to everyone else in the same race on the same day, after
age and sex. **Negative = faster.** Cities with ≥500 runner-races.

| Fastest | min | | Slowest | min |
|---|---|---|---|---|
| boulder, co | −25.7 | | sacramento, ca | +19.4 |
| minneapolis, mn | −14.8 | | myrtle beach, sc | +14.8 |
| cambridge, ma | −14.1 | | vancouver, wa | +12.3 |
| boston, ma | −13.4 | | detroit, mi | +12.2 |
| hermosa beach, ca | −13.3 | | riverside, ca | +10.6 |
| seattle, wa | −12.2 | | los angeles, ca | +7.1 |

This answers `research.md`'s "toughest town" question, but it is **selection, not
training**. Boulder's −25.7 minutes reflects who lives in and travels from Boulder.
Note the pattern: cities that *host* a big accessible marathon (Sacramento/CIM,
Myrtle Beach, Detroit) rank slowest, because their local field is broad while
out-of-towners self-select as committed. That is the composition mechanism visible
directly. Full table: `data/estimates/spec_B_city_effects.csv`.

## Spec C — heterogeneity

All within the spec A frame, so these inherit the composition confound and should be
read as descriptive.

| Dimension | Group | min/°F | t |
|---|---|---|---|
| Sex | Male | +0.337 | 2.84 |
| Sex | Female | +0.299 | 3.04 |
| Ability | Fastest quartile | +0.140 | 4.79 |
| Ability | Rest of field | +0.102 | 1.64 |
| Age | 18–34 | +0.378 | 3.44 |
| Age | 35–39 | +0.428 | 2.95 |
| Age | 55–59 | +0.126 | 0.87 |
| Age | 60–64 | −0.079 | −0.44 |

The apparent effect weakens with age and is near zero past 55 — consistent with the
composition story, since marginal newcomers drawn out by a mild season skew young.

---

## What changed in the pipeline

Five substantive corrections, all verified:

**1. The join key was destroying 72% of runners.** `process_join.py` deduplicated on
`race__date__city__state` — a *location* key, not a record key. Every runner from the
same hometown in the same race collapsed into one row: 7,411,671 race_final records
became 2,052,583. The key now includes age, sex and finish time, and the joined table
holds **7,660,166** rows (was 2,120,259). Regression tests added
(`test_create_join_key_distinguishes_runners_in_same_city`).

**2. The precipitation threshold was wrong by 25×.** `PRECIP_THRESHOLD = 0.2` was
documented as inches, but the stored data was millimetres (Open-Meteo's default, since
the original fetch omitted `precipitation_unit`). Confirmed against climatology:
converting to inches puts annual city totals at p05 7.6″ / median 36.9″ / p95 65.3″,
the correct US range. Every `days_of_precip` column had been counting any measurable
trace — **32.3 of 90 days**, versus **12.7** at a true 0.2″ threshold. Fixed by
conversion; no refetch needed.

**3. Runner identity recovered.** `enriched_raw_data.parquet` carries a 100%-populated
name column that `process_racedata.py` discarded. Rebuilt into a `runner_id` lookup:
2,949,246 resolved rows, 462,524 runners with ≥2 races. This is what makes spec D —
the finding above — possible at all.

**4. Windows are now rolling per city-day.** The old featurizer re-sliced each
hometown's full history once per (race, date, city) group under a thread pool. Since
the 90-day window ending on date *d* is a rolling aggregate, Stage 2 computes it once
per city-day: **all 109 cities in ~4 seconds**, and the downstream join is a plain
merge. Adding a window is now one entry in `WeatherConfig.WINDOWS`.
**Parity verified**: temperatures and precipitation totals match the old
`featurized_race_data_v3.csv` to floating point (max |diff| 1.4e-14).

**5. Sex-case normalization.** The scraper writes `m`/`f`; every analysis filtered on
`M`/`F`, silently dropping newly scraped records. Normalized in Stage 4.

---

## The Stage 1 coverage estimate in the design doc was wrong

The design assumed one Open-Meteo request = one call unit and estimated ~8 hours to
fetch 2,000 cities. **Open-Meteo bills weighted units**: `n_variables × n_days / 14`,
against caps of 5,000/hour and 10,000/day. A single city over 1999–2026 costs:

| Variable set | Units/city | Free-tier rate |
|---|---|---|
| legacy (3 vars) | ~2,163 | ~4 cities/day |
| lean (8 vars) | ~5,768 | ~1 city/day (exceeds the hourly cap alone) |
| full (15 vars) | ~10,815 | ~1 city/day |

The existing 109-city archive represents roughly **24 days** of free-tier quota.
Reaching 2,000 cities would take **~15 months** on the free tier, not 8 hours. One
15-variable full-range request exhausted the hourly quota outright during testing.

**What this means for the results above.** The analysis runs on the 109 cities already
held, so the panel is 1.0M runner-races rather than the ~3M that 72% coverage would
give. The consequences are specific:

- 109 clusters is thin for cluster-robust inference. Spec A and D are far enough apart
  that the conclusion is not delicate, but the standard errors are the weakest part.
- The training × race-day interaction is the least-powered term and is where broader
  coverage would most change the answer.
- No thermal-load variables. Only `temp_max`, `temp_min`, `precip` were ever fetched —
  no wet-bulb or dew point, which are what actually drive heat stress. Those require a
  refetch, unlike the precip fix.

`src/fetch_weather.py` is written, quota-aware and resumable, with `--api-key` routing
to the commercial endpoint. `data/city_roster.csv` holds the prioritized 3,706-city
target list (72% of runner-rows). Running it is a decision about spending money on an
API key, so it is left unrun — see "Next" below.

---

## Limitations

- **Runner identity is a fuzzy key** (`name|city`). Homonyms merge; name changes and
  relocation split. Spec D's runner FE is therefore measured with error, which biases
  it *toward* spec A — so the true within-runner effect is, if anything, even closer to
  zero than −0.027.
- **Hometown is self-reported at the race** and attributed to the whole 90-day window.
  Runners move; this attenuates all estimates toward zero.
- **Daily aggregates, not session weather.** `race_day_temp_max` is a daily maximum,
  typically well above what an 8 a.m. start experienced.
- **Weather constrains training; it does not determine it.** Treadmills, indoor tracks
  and travel all break the link and none are observed.
- **Spec D changes the estimand**, not just the controls: it is identified off frequent
  racers, who are not a random sample of the field.

## Next

1. **Decide on Open-Meteo capacity.** A commercial key unlocks the 3,706-city roster
   and the thermal-load variables in one pass. Alternatively NOAA GHCN-Daily is free
   and bulk-downloadable, but has no humidity — a station-mapping project in itself.
2. **Hourly race-morning weather** (~8,500 requests, cheap under the weight formula)
   would replace daily maxima with what runners actually ran in.
3. **Push the selection result further.** The turnout finding is interesting on its own
   — weather shaping who enters a marathon — and the pipeline already computes it.

## Pre-existing issue, untouched

`tests/test_iterator.py` has 16 failures unrelated to this work: `CheckpointIterator.process`
calls `process_func(key, records)` while the tests supply single-argument functions.
`src/iterator.py` and its tests are unmodified here. The 49 tests covering this
pipeline pass.
