# Phase 2 — Profiling findings

Yellow taxi, 2019-01 → 2026-07. 91 monthly files, 5.2GB raw, collapsed to a
330,311-row date × hour × area panel.

*Terms:* **CBD** = Central Business District, MTA's name for the tolled area
in its Central Business District Tolling Program — Manhattan south of 60th
Street. It is the treated group for congestion pricing; **uptown** (above
60th) is the control. Midtown sits inside the CBD. Every number below is distance-weighted
(total miles ÷ total hours) unless it says otherwise.

**Headline for Phase 3:** the data supports the analysis. The one threat that
could have invalidated it — composition drift — is measured and negligible. One
real problem surfaced that nobody was looking for: five days of TLC source
outage, four of them inside UNGA weeks.

---

## 0. The data-quality problem, found by accident

Looking for the thinnest days in the panel turned up 2022-09-18 with **18 trips**
in the midtown core against a 3,550 median, and 2023-09-22 with 19. Those are
not traffic patterns.

They are not a pipeline bug either — the loss is in TLC's own file. The
September 2022 parquet holds 4,229 citywide trips for the 18th against ~126,000
for the 16th. Every taxi zone collapses at once.

### Ruling out the obvious alternative: did taxis just avoid midtown?

A fair challenge, since four of these days sit in UNGA weeks when midtown really
is locked down. If drivers were avoiding midtown, volumes *outside* midtown
would hold up. Three tests say otherwise.

**1. Airports.** JFK is 15 miles from midtown, in another borough. No avoidance
behaviour reaches it.

| | 2022-09-16 | 2022-09-18 | 2023-09-20 | 2023-09-22 |
|---|---|---|---|---|
| JFK pickups | 5,924 | **21** | 4,878 | **23** |
| LaGuardia pickups | 3,372 | **25** | 3,815 | **24** |

Staten Island and Newark pickups go to **zero**. Queens drops from ~10,000 to
149. This is citywide, not midtown.

**2. The hourly pattern — the decisive one.** The feed does not decline, it
falls off a cliff at one specific hour and stays flat:

| 2022-09-17 hour | rows | | 2023-09-21 hour | rows |
|---|---|---|---|---|
| 13 | 6,615 | | 11 | 5,359 |
| 14 | 5,805 | | 12 | 5,589 |
| **15** | **180** | | **13** | **742** |
| 16 | 196 | | 14 | 150 |

Normal all morning, vertical drop at 3pm (2022) and 1pm (2023), flat for 3–4
days, then instant full recovery. Drivers across five boroughs do not uniformly
stop at 3:00pm and resume at midnight three days later.

**3. Vendor mix shifts.** Normal split is VendorID 1 at 25.5%, VendorID 2 at
74.5%. During the 2023 outage it flips to 59% / 41% — one vendor's upload
pipeline failed harder than the other's. VendorID is a payment-terminal
provider, not a geography; avoidance behaviour cannot move it.

**Conclusion:** partial upload failure, ~3–5% of records surviving and skewed by
vendor. Not a representative sample, correctly excluded.

Scanning every day against its surrounding 15-day median, then ruling out snow
and holidays against the weather archive, splits 17 anomalous days cleanly:

| Kind | Days | Evidence |
|---|---|---|
| Genuine low traffic | 12 | Blizzards (27cm on 2021-02-01), Christmas, Thanksgiving — all confirmed in the weather data |
| **Source outage** | **5** | Dry, mild, no holiday, yet 2–4% of normal volume across all zones simultaneously |

The five outages: **2022-09-18, 2023-09-21, 2023-09-22, 2023-09-23, 2023-09-24.**

**Four of the five fall inside UNGA weeks.** Left in, they would have quietly
corrupted the single question the project most wants to answer. They are now
listed in `constants.TLC_OUTAGE_DATES` and dropped by `panel.drop_outages()`;
`panel.detect_outages()` re-runs the scan when the data extends.

**Cost, and how to recover it.** The outage is *bounded*: it begins mid-afternoon
on 2023-09-21, so **Mon 18, Tue 19 and Wed 20 September 2023 are fully clean**.
Comparing Mon–Wed blocks instead of full weeks puts 2023 back in the analysis:

> **UNGA 2023: 4.15 mph, rank 1 of 52 Mon–Wed blocks, year median 5.40,
> gap −1.24.** The slowest such block of the entire year.

That is consistent with every other year and *stronger* than the rank-4 figure
the contaminated full-week calculation produced. Phase 3 should run RQ2 both
ways — full weeks for 2019/2021/2022/2024/2025, and a Mon–Wed variant that
includes 2023 — and report the two together.

Every other year is unaffected by the exclusion (2022-09-18 was a Sunday,
already outside the weekday analysis).

---

## 1. Volume: the decline is real, and midtown fell hardest

Midtown core trips, indexed to 2019, same months (Jan–Jul) throughout so the
partial 2026 year is comparable:

| Year | Midtown core | CBD | Uptown |
|---|---|---|---|
| 2019 | 1.00 | 1.00 | 1.00 |
| 2020 | 0.31 | 0.33 | 0.40 |
| 2021 | 0.17 | 0.24 | 0.37 |
| 2022 | 0.33 | 0.40 | 0.41 |
| 2023 | 0.37 | 0.39 | 0.37 |
| 2024 | 0.39 | 0.42 | 0.40 |
| 2025 | 0.42 | 0.49 | 0.46 |
| 2026 | 0.34 | 0.43 | 0.44 |

Yellow taxi never recovered. Midtown peaked at 42% of its 2019 self in 2025 and
**fell back to 34% in 2026** — the decline is ongoing, not a COVID scar that is
healing. Midtown consistently sits below the CBD and uptown, so whatever is
eroding yellow taxi is eroding it fastest exactly where we are looking.

**Consequence for RQ3:** any volume claim has to be de-trended, and the trend is
not monotonic. A naive "fewer rides after congestion pricing" reading would be
wrong in sign — CBD volume *rose* 0.42 → 0.49 across the 2025 introduction.

## 2. Speed: midtown is getting steadily slower

Distance-weighted mph, Jan–Jul each year:

| Year | Midtown core | CBD | Uptown | Boundary |
|---|---|---|---|---|
| 2019 | 6.18 | 8.62 | 10.66 | 7.98 |
| 2020 | 6.88 | 9.25 | 10.93 | 8.37 |
| 2021 | 8.66 | 10.34 | 10.63 | 8.69 |
| 2022 | 6.92 | 9.30 | 10.42 | 8.14 |
| 2023 | 5.95 | 8.62 | 10.28 | 7.81 |
| 2024 | 5.51 | 8.18 | 10.18 | 7.53 |
| 2025 | 5.45 | 8.41 | 10.25 | 7.49 |
| 2026 | **4.94** | 7.82 | 9.85 | 7.05 |

Midtown in 2026 is **20% slower than in 2019** and slower than any year on
record here. Uptown moved 10.66 → 9.85 over the same span (−8%), so this is a
midtown story, not a citywide one.

## 3. Filter sensitivity: only one bound is load-bearing

Against a 5.038 mph default (2024-09 midtown core), moving each bound:

| Variant | Δ mph | Trips kept |
|---|---|---|
| mph cap 25 / 60 | ∓0.001 | 91.6% |
| mph floor 0.5 | −0.027 | 91.9% |
| miles max 5 / 20 | −0.014 / +0.027 | 91.5% / 91.7% |
| minutes max 45 | +0.016 | 91.4% |
| minutes min 2 | −0.009 | 90.9% |
| **miles min 0.5** | **+0.117** | **78.5%** |

Every bound except the minimum trip distance moves the headline by less than
0.03 mph. Raising the minimum from 0.2 to 0.5 miles drops 13% of trips and adds
0.12 mph, because sub-half-mile hops are both numerous and slow.

**Verdict:** the filters are safe. The 0.2-mile floor is the only judgement call
worth disclosing, and it is conservative — a stricter floor would make midtown
look *faster*, not slower.

Pathologies in raw midtown data: 4.3% zero distance, 5.4% under 1 mph, 0.10%
over 40 mph, 0.008% non-positive duration. Filters remove ~8.4% in total.

## 4. Sample size: weekly is safe, hourly-by-day is not

| Area | Median trips/day | p05 | Min |
|---|---|---|---|
| midtown_core | 3,550 | 454 | 18 |
| midtown_ext | 11,020 | 2,237 | 90 |
| cbd | 44,470 | 8,174 | 703 |
| uptown | 7,539 | 3,152 | 174 |

By hour of day the core thins to a **median of 7 trips at 4am** and 225 at 2pm.

**Rule for Phase 3:** daily and weekly cuts are fine on the core. Anything at
hour × day resolution needs midtown extended, or needs pooling across weeks.
Overnight hours cannot carry a daily series at all.

## 4b. Time bands — the fix for the sample-size limit

Hour-of-day is too thin for a daily series, so hours group into five bands
(`constants.TIME_BANDS`, aggregated by `panel.by_band()`). Sample per day,
midtown core:

| Band | Median | p05 | Usable at |
|---|---|---|---|
| Overnight (12–6am) | 138 | 14 | weekly, or midtown extended |
| Morning (6–10am) | 457 | 50 | daily |
| Midday (10am–4pm) | 1,261 | 206 | daily |
| Evening (4–8pm) | 1,063 | 120 | daily |
| Night (8pm–12am) | 618 | 38 | daily |

Speed over the whole period, and how tightly each band holds together:

| Band | mph | Hour range within band | Spread |
|---|---|---|---|
| Overnight | 10.00 | 8.96 – 11.59 | 2.63 |
| Morning | 6.76 | 5.59 – 10.19 | **4.60** |
| Midday | 5.23 | 5.01 – 5.56 | 0.54 |
| Evening | 5.18 | 4.90 – 5.69 | 0.79 |
| Night | 6.91 | 6.60 – 7.68 | 1.07 |

Midday and evening are near-flat, so their band averages are meaningful.
**Morning is not** — 6am still behaves like overnight (10.19 mph) while 9am is
already at 5.59, so the 6.76 average describes no hour that actually exists.
Moving 6am into overnight halves that spread to 2.47 at no cost to overnight.
Left as specified for now; one line in `constants.TIME_BANDS` changes it.

**Midday 5.23 vs evening 5.18** is the measured form of "midtown has a rush
*day*, not a rush hour".

### The decline is not uniform across the day

| Band | 2019 → 2026 | Change |
|---|---|---|
| Night (8pm–12am) | 7.33 → 5.98 | **−18.3%** |
| Evening | 5.32 → 4.39 | −17.4% |
| Overnight | 10.48 → 8.78 | −16.3% |
| Midday | 5.23 → 4.38 | −16.2% |
| Morning (6–10am) | 6.58 → 5.94 | **−9.8%** |

Morning rush is twice as resilient as every other part of the day. Midtown's
*quiet* hours are eroding fastest — evening and night are converging down onto
the midday floor. Worth a section in the article: the story is not "rush hour
got worse", it is "midtown is becoming rush hour all day".

---

## 5. Composition drift: measured, and it is not a problem

The risk was that a shifting mix of origin–destination pairs moves average speed
with no change in congestion. Holding the trip mix fixed at January 2019 and
letting only pair-level speeds vary:

| Year | Observed | Fixed 2019 mix | Composition effect |
|---|---|---|---|
| 2019 | 5.97 | 5.97 | +0.01 |
| 2020 | 9.19 | 9.20 | −0.01 |
| 2021 | 8.17 | 8.12 | +0.06 |
| 2022 | 6.54 | 6.50 | +0.04 |
| 2023 | 5.59 | 5.57 | +0.03 |
| 2024 | 5.24 | 5.22 | +0.02 |
| 2025 | 5.19 | 5.17 | +0.02 |
| 2026 | 4.96 | 4.95 | +0.01 |

Mean absolute composition effect: **0.034 mph** (extended: 0.024), against an
observed swing of more than 1 mph. Pair coverage is 100% throughout.

**Verdict: cleared.** Composition explains about 3% of the movement. Phase 3 can
use observed speeds directly and report this as a checked-and-dismissed concern
rather than a live caveat.

## 6. Which aggregate: distance-weighted, and it matters

Four ways of summarising the same midtown core trips:

| Aggregate | mph |
|---|---|
| Distance-weighted (miles ÷ hours) | **5.80** |
| Trip-weighted mean of trip speeds | 6.84 |
| Trip-weighted mean of hourly medians | 6.56 |
| Equal weight across hour-rows | 8.29 |

The spread is large enough that the choice changes the story. The last row is an
artefact — it gives a 4am hour with 7 trips the same weight as a 2pm hour with
225 — and should never be quoted.

**Lead with distance-weighted.** It answers "how fast does a mile of midtown
travel go", which is the question, and it is the most conservative of the
defensible options. Hour-by-hour, it runs ~0.85 mph below the plain mean
consistently, so the shape is unaffected by the choice.

The daily curve on the full data: **11.59 mph at 5am, 4.90 mph at 5pm.**

## 7. Weather and event joins both behave

Weather (core, all hours): dry 5.86, wet 5.56, heavy precip 5.49. Business hours
only: dry 5.34 vs wet 5.07. Rain costs roughly **5%**, consistently.

One counter-intuitive result: *snowing* hours show 6.03 mph, faster than dry.
This is composition, not physics — snow suppresses discretionary midtown trips
and concentrates what remains in off-peak hours. Snow needs an hour-and-season
control before it means anything, and is not usable as a raw feature.

Permitted road-closure events, weekdays only:

| Road-affecting permits that day | Days | mph |
|---|---|---|
| 0 | 321 | 7.67 |
| 1–2 | 820 | 6.76 |
| 3–5 | 644 | 5.76 |
| 6+ | 194 | 5.31 |

Cleanly monotonic — the join works. It is *not* yet a causal estimate: permit
counts correlate with season and day type, both of which independently move
speed. Treat as a validated control, not a finding.

---

## Preview: UNGA (RQ2)

Midtown core, weekdays, outage days excluded, ranked slowest-first within each
year:

| Year | UNGA week | mph | Rank | Year median | Gap |
|---|---|---|---|---|---|
| 2019 | 39 | 4.63 | **1 / 52** | 5.77 | −1.14 |
| 2020 | 39 | 9.51 | 28 / 53 | 9.50 | **+0.01** |
| 2021 | 38 | 6.40 | 5 / 52 | 8.08 | −1.68 |
| 2022 | 38 | 4.77 | 5 / 52 | 6.43 | −1.65 |
| 2023 | 38 | 4.15 | **1 / 52** * | 5.40 | −1.24 |
| 2024 | 39 | 4.22 | 5 / 52 | 5.11 | −0.89 |
| 2025 | 39 | 3.78 | 3 / 52 | 5.01 | −1.22 |

\* 2023 measured on the three clean weekdays (Mon–Wed) and ranked against
Mon–Wed blocks; the rest are full weekdays ranked against full weeks.

In 2019 and 2023 UNGA week was the slowest of the year outright. In every ordinary
year it lands in the slowest five, 0.9–1.7 mph below the year's median.

**In 2020 — when the General Debate ran on pre-recorded video with almost no
delegations present — the effect vanishes entirely: rank 28 of 53, +0.01 mph
from the median.** UNGA was on the calendar; the motorcades were not. That is
about as close to a controlled experiment as this data will ever offer, and it
is the strongest evidence in the project so far.

## Preview: congestion pricing (RQ3)

Jan–Jul comparisons, CBD treated against uptown control:

| Transition | CBD | Uptown | Difference-in-differences |
|---|---|---|---|
| 2024 → 2025 | +0.23 | +0.07 | **+0.16 mph** |
| 2025 → 2026 | −0.59 | −0.39 | **−0.20 mph** |

The expected first-year effect is there but small — about +0.16 mph, roughly 2%,
against a CBD baseline of 8.18. By 2026 it has more than reversed.

Midtown core specifically did *worse* than its own control: −0.06 mph across the
2025 introduction, then −0.52 into 2026.

Two cautions before anyone quotes this. It is a raw two-period difference with
no controls for weather, trip mix, or the volume rebound, and 2026 covers only
seven months. It is a reason to do RQ3 carefully, not a result.

---

## What Phase 3 may assume

1. Outage days are excluded — always call `panel.drop_outages()`.
2. Composition drift is negligible; use observed speeds.
3. Distance-weighted mph is the headline aggregate.
4. Weekly resolution on midtown core is sound; hour × day is not.
5. Rain costs ~5% and is a usable control. Snow is not, without more work.
6. 2020–21 are not merely usable, they are the identification strategy for RQ2.
