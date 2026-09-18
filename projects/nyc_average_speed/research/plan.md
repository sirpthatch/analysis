# Working Plan — Midtown Average Speed

Status: **Phases 0-3 done** (2026-09-18). Findings in
`research/profile_findings.md`, `rq1_seasonality.md`, `rq2_unga.md`,
`rq3_congestion_pricing.md`. Phase 4 limitations folded in as we went; Phase 5
(the writeup) is next.

---

## Phase 0 — Scaffolding (done)

- Project skeleton per `projects/specs/project_structure.md`, venv on Python
  3.14 with the standard analysis stack plus `duckdb` and `pyarrow`.
- `src/tlc.py` downloads monthly TLC parquet and the taxi zone reference files.
- `src/speed.py` turns raw trips into a per-trip speed table.
- `src/constants.py` holds zone sets, the event calendar, and filter bounds.
- `src/panel.py` collapses trips to a date x hour x area panel (added after
  review - 250M trips will not fit in memory, and every RQ is answered at that
  grain).
- `src/weather.py`, `src/events.py` fetch the two confounder feeds.

Validated on June 2025 yellow taxi: 4,322,960 raw trips → 3,726,837 after
filtering. Midtown-core-to-midtown-core trips run **5.24 mph** distance-weighted
(6.07 mph as a plain mean of trip speeds), from 11.6 mph at 4am to 4.8 mph at
noon. The shape is right, which says the pipeline and the zone definitions are
working.

Note the gap between the two aggregates — the plain mean sits ~0.8 mph above
the distance-weighted figure, because short hops are both numerous and
relatively quick. Phase 2 item 6 settles which one we lead with.

---

## The measurement

There is no direct "surface street speed" feed for midtown. We derive it:

    mph = trip_distance (meter miles) / (dropoff_time − pickup_time)

restricted to trips whose pickup **and** dropoff zone are both in midtown, so
the trip stayed on the grid rather than escaping to the FDR or an airport.

Two geographies are defined in `constants.py`:

- **Midtown core** (6 zones): Midtown Center/East/North/South, Times Sq,
  Garment District. ~122k yellow trips/month — about 4,000/day.
- **Midtown extended** (12 zones): adds Clinton E/W, Murray Hill, Penn Station,
  Sutton Place/Turtle Bay N, UN/Turtle Bay S. ~369k trips/month.

**Decided:** core is the headline; extended runs alongside it and gets reported
wherever the two disagree. Extended is preferred where sample size forces it,
but never at the cost of drifting off the midtown story.

Every figure in the article therefore carries a core number. Extended appears
as a robustness line, and any place the two diverge materially is itself a
finding worth a sentence.

`src/speed.py` tags five areas in all. Beyond the two midtown ones, the RQ3
groups are `cbd` (38 zones south of 60th St), `uptown` (17 zones above it), and
`cbd_boundary` (6 zones straddling the line, held out of both).

**CBD** throughout means *Central Business District* — MTA's name for the
tolled area in its Central Business District Tolling Program, i.e. Manhattan
south of 60th Street. Midtown sits inside it, which is why `cbd` is the
treated group and `uptown` the control.

---

## Phase 1 — Collect (running)

1. Yellow taxi, **2019-01 through 2026-07** — 91 files, ~6GB. In progress.
2. FHVHV — **skipped**, per decision 3. RQ3's volume question gets answered by
   de-trending yellow instead; see RQ3 below.
3. Taxi zone lookup + shapefile. **Done.**
4. Hourly weather, 2019-01-01 → 2026-07-31. **Done** — 66,456 hours from the
   Open-Meteo ERA5 archive at Bryant Park, 15.6% of hours wet. One point stands
   in for all of midtown; at this scale that is fine.
5. Permitted events, Manhattan 2019+. **Done** — 169,355 permits, 44,610 in
   midtown precincts, 8,083 of them taking road capacity.

Still on the shelf, to pull only if a question demands it:

- NYC DOT real-time traffic speeds (`i4gi-tdb4`) — sensor-based, but coverage
  skews to highways. A cross-check, not a source.
- MTA CBD tolling entry counts — an external check on RQ3's volume story.

### On the closure data specifically

There is **no public NYPD feed** for security closures — UNGA perimeters,
motorcade routes, and dignitary movements are announced by press release, not
published as data. I checked the NYC Open Data catalog; the closest usable
sources are:

- **NYC Permitted Event Information — Historical** (`bkfu-528j`, 2.9M rows,
  2007→present). The Citywide Event Coordination permit feed: parades, street
  festivals, film shoots, demonstrations. Carries `street_closure_type` and
  `police_precinct`, which is what makes it joinable. **This is what we pulled.**
- **Street Closures due to Construction** (`i6b5-j7bu`). DOT construction work,
  geocoded to block segments. Not pulled yet — a candidate if construction turns
  out to matter.

The permit feed passes a sanity check: midtown road-affecting events run ~2-7/day
normally but hit 10, 7, 8, 12 and 6 across 24-28 September 2024, i.e. UNGA week.
"UNGA 79" itself appears as a permitted event.

**Limitation this creates:** the feed captures *permitted* closures, not
*security* closures. For RQ2 specifically, the thing we most want to measure is
the thing least likely to be in the data. It supports the analysis; it cannot
carry it.

Weather came from **Open-Meteo's ERA5 archive** rather than NOAA — hourly rather
than daily (traffic speed is an hourly phenomenon), no API token, and it is the
same source `projects/marathon_results` already standardised on.

---

## Phase 2 — Profile (notebook: `profile_tlc.ipynb`) — DONE

Understand the data before asking it questions. Each item below has to end in a
number we can quote, not just a chart we glanced at.

1. **Volume by month, 2019→2026.** Quantify the yellow decline and the COVID
   hole, and decide whether 2020–21 is usable. Early read from 2019 vs. 2025:
   citywide trips fell ~7.0M/month → ~4.3M, and midtown core trips fell
   312k → 122k. The core fell *harder* than the city, which matters.
2. **Distributions** of distance, duration, speed. Locate the meter errors.
3. **Filter sensitivity.** Re-run the headline under each bound in
   `constants.py` moved up and down. Anything that moves the number more than a
   few percent goes in limitations with its magnitude.
4. **Sample size** per day and per hour, both geographies — sets the finest
   time slice the rest of the analysis is allowed to cut.
5. **Composition drift.** The one that can invalidate everything: if the mix of
   midtown origin–destination pairs shifts, average speed moves with no change
   in congestion. `zone_pairs.parquet` exists for this. Deliverable is a
   decomposition separating "speed changed" from "the trip mix changed",
   probably by reweighting later years to a fixed 2019 pair distribution.
6. **Which aggregate.** Mean vs. median vs. distance-weighted. The panel carries
   `miles` and `minutes` sums precisely so distance-weighted speed can be
   recomputed at any grouping; confirm it is the one we lead with.
7. **Weather and event joins** sanity-checked: wet hours should be visibly
   slower than dry, and permitted-closure days slower than clean days. If they
   are not, the join is wrong.

**Output:** `research/profile_findings.md`. Summary of what came back:

- **A data-quality problem nobody was looking for.** Five days where the TLC
  source file is missing 96–98% of its trips — and four of them sit inside UNGA
  weeks. Confirmed as upload failures rather than taxis avoiding a locked-down
  midtown: JFK pickups fell to 21/day, volume dropped vertically at a single
  hour and held flat for days, and the vendor mix flipped. Excluded via
  `constants.TLC_OUTAGE_DATES`. The 2023 outage starts mid-afternoon on the
  21st, so 18–20 Sept are clean and RQ2 keeps the year via a Mon–Wed cut.
- **Composition drift: cleared.** 0.034 mph mean effect against a >1 mph swing,
  about 3%. The biggest threat to the analysis is not a threat.
- **Filters: safe.** Every bound moves the headline under 0.03 mph except the
  0.2-mile minimum, worth +0.12 mph if raised — and conservative in our favour.
- **Aggregate: distance-weighted**, 5.80 mph overall, ~0.85 below the plain mean.
- **Resolution limit:** weekly on the core is sound; hour × day is not (median
  4am hour has 7 trips).
- **Rain costs ~5%** and is a usable control. Snow is not, without more work.

### What Phase 3 may assume

1. Always call `panel.drop_outages()` first.
2. Use observed speeds — no composition reweighting needed.
3. Distance-weighted mph is the headline.
4. Weekly resolution on core; extended or pooling for finer cuts. For
   time-of-day work use the five bands in `constants.TIME_BANDS` via
   `panel.by_band()` — morning/midday/evening/night carry a daily series on the
   core, overnight needs weekly or midtown extended.
5. 2020–21 are the identification strategy for RQ2, not a problem to exclude.

---

## Phase 3 — Research questions — DONE

**Answers, in brief:**

| RQ | Answer |
|---|---|
| **1. Seasonal shape** | Speed falls **36%** across the year, 6.39 mph (Jan) → 4.10 (Dec). Rush hour is the *most* resilient part of the day over seven years; the quiet hours are eroding fastest. |
| **2. UNGA** | Slows midtown **8–18%** (dry hours), up to **33%** beside the UN. **Not** the slowest week of the year — December is — but the slowest non-December week, rank 1 of 47 in four of six years. The 2020 video-only session shows **no effect at all**. |
| **3. Congestion pricing** | **+2.0%** speed in year one against control, at the right hours; **gone by 2026**. Volume is **not answerable** with yellow taxi data — the de-trending fails its own placebo test, and raw CBD taxi counts went *up*. |

Detail below is the original plan, kept for the record.


### RQ1: Seasonal shape (`seasonality.ipynb`)

- Weekly mean midtown speed across a standard year, averaged over clean years
  (2022–2024 if COVID rules out earlier).
- Decompose: **time band** × day-of-week × week-of-year (hour-of-day is too thin
  for the core — see Phase 2 item 4b). Control for weekday mix so holiday weeks
  are not an artifact of which days fell where.
- **Already visible and worth its own section:** the 2019→2026 decline is far
  from uniform. Night −18.3%, evening −17.4%, midday −16.2%, but morning only
  −9.8%. Midtown's quiet hours are eroding fastest, so evening and night are
  converging onto the midday floor. The story is not "rush hour got worse", it
  is "midtown is becoming rush hour all day".
- Expected findings to test: August and late December fast; September through
  early December slow; holiday-season midtown gridlock is real.
- Deliverable: one chart of "speed over the year" that carries the article.

### RQ2: UNGA (`unga.ipynb`)

**Phase 2 preview — the effect is large and appears identified.** Midtown core,
weekdays, outages excluded, ranked slowest-first within year:

| Year | mph | Rank | Median | Gap |
|---|---|---|---|---|
| 2019 | 4.63 | **1 / 52** | 5.77 | −1.14 |
| 2020 | 9.51 | 28 / 53 | 9.50 | **+0.01** |
| 2021 | 6.40 | 5 / 52 | 8.08 | −1.68 |
| 2022 | 4.77 | 5 / 52 | 6.43 | −1.65 |
| 2023 | 4.15 | **1 / 52** * | 5.40 | −1.24 |
| 2024 | 4.22 | 5 / 52 | 5.11 | −0.89 |
| 2025 | 3.78 | 3 / 52 | 5.01 | −1.22 |

\* 2023 on its three clean weekdays, ranked against Mon–Wed blocks.

2020 — video debate, no delegations — sits exactly at the median. Remaining work
is to nail down *which* days and zones, control for weather and permits, and
identify what beats UNGA in the years it ranks 3rd–5th.

- Rank every week of the year by mean midtown speed; where does UNGA week land?
- Two resolutions: midtown-wide, and the UN-adjacent zones (233, 229) where
  closures actually are. The effect may be sharp and local, and washed out
  midtown-wide — that is itself the finding.
- Compare against the surrounding weeks in the same year, not a cross-year
  average, so the seasonal trend does not contaminate it.
- Repeat across 2019/2022/2023/2024/2025 — one year is an anecdote.
- **Dates verified 2026-09-18** against un.org and the Dag Hammarskjöld Library
  research guide. Two entries in the original table were wrong; `constants.py`
  now carries the corrected windows for the 74th–81st sessions, with a
  `unga_week()` helper that returns the Monday–Sunday week containing the
  opening Tuesday. The debate runs Tue–Sat plus a trailing Monday or Tuesday,
  so the calendar week is the cleaner unit for a traffic comparison.
- **2020 and 2021 are a natural control.** The 75th session ran on pre-recorded
  video with almost no delegations present, and the 76th was hybrid with
  reduced attendance — UNGA on the calendar, no motorcades. If midtown slows in
  2019/2022–2025 but *not* in 2020–21, that is close to causal identification,
  and far better evidence than any before/after within a single year. This is
  now a reason to keep 2020–21 rather than drop them.
- Candidate rivals for slowest week: the week before Christmas, the week of
  Thanksgiving, marathon Sunday, any major storm week.

### RQ3: Congestion pricing (`congestion_pricing.ipynb`)

**Phase 2 preview.** Jan–Jul, CBD treated vs uptown control: DiD of **+0.16 mph**
across the 2025 introduction (~2% on an 8.18 baseline), reversing to **−0.20 mph**
into 2026. Midtown core did *worse* than its control in 2025 (−0.06). Raw, no
controls, and 2026 is 7 months — a reason to do this carefully, not a result.

Note the volume trend is **not monotonic** — midtown recovered to 42% of 2019 by
2025 then fell to 34% in 2026 — so a linear de-trend will not work.

CBD tolling began **2025-01-05**. The 2025 parquet carries a
`cbd_congestion_fee` column, confirmed present, which lets us see the toll at
trip level rather than inferring it from dates.

- Trip volume and mean speed, midtown, before vs. after. Note the naive
  before/after is confounded by season (January vs. December) — hence:
- Difference-in-differences: CBD zones (treated) vs. Manhattan above 60th and
  outer-borough zones (control), using the same months in prior years as the
  seasonal baseline.
- Test the stated expectation directly: *faster trips, fewer rides*.
- **Volume, de-trended** (decision 3/4). Rather than pull FHVHV, fit the secular
  decline in yellow taxi volume on pre-2025 data and ask whether post-2025 CBD
  volume falls *below its own trend* — and whether the uptown control does the
  same. The residual is the congestion-pricing effect. Concretely: a log-volume
  model with a smooth time trend plus month-of-year and day-of-week terms, fit
  through 2024, then extrapolated. Speed stays the focus; volume is explored,
  with the approximation stated plainly.
- Fair warning on what this can and cannot show: a de-trended yellow-taxi volume
  series is a proxy for *taxi* demand, not for *vehicle* counts. If the residual
  is small or ambiguous, the honest answer is "we cannot separate this from the
  secular decline," and MTA's entry counts become the citation instead.
- Check for a fade: is any speed gain still there 6, 12, 18 months on? We now
  have through mid-2026, which is long enough to say something real.
- **Boundary verified 2026-09-18, and it is cleaner than feared.** Splitting
  every Manhattan zone polygon on a W60th→E60th line gives an unusually sharp
  separation: 41 zones sit 100% south of it, 3 straddle it, and every other
  Manhattan zone is 0% south. So the treated group is well defined, not
  approximate. The check also caught two zones wrongly included by hand
  (Central Harlem, Central Park) and three island zones with no road connection
  (Governor's/Ellis/Liberty), all now excluded. Treated group: 38 zones;
  control: 17 zones above 60th, parks and islands removed.
- **New angle the boundary work opened up.** Six zones hug or straddle 60th St
  (Lenox Hill E/W, Lincoln Square E/W, Roosevelt Island, UES South). They are
  held out of *both* DiD groups so neither is contaminated — and they are worth
  looking at on their own: if drivers started ending trips just north of the
  line to dodge the toll, that behaviour shows up here first. Tagged as the
  `cbd_boundary` area in `src/speed.py`.
- **Independent confirmation of the zone assignment.** In June 2025, 97.0% of
  midtown-core trips carry a non-zero `cbd_congestion_fee` against 0.2% of
  uptown-control trips. The toll field and the geometry agree, which is about
  as good a check on the treated/control split as we could ask for.

---

## Phase 4 — Limitations (`research/limitations.md`, written as we go)

Running list, already visible:

1. Taxis are not traffic. They stop for passengers, cruise for fares, and
   concentrate on avenues. Speed here means "taxi speed," not "car speed."
2. `trip_distance` is meter-reported route miles, with its own error.
3. Duration includes pickup and dropoff dwell time.
4. Yellow taxi volume has fallen sharply since 2015; the fleet's composition
   and behavior are not constant across the study period.
5. Filter bounds are judgement calls — Phase 2 quantifies how much they matter.
6. Zone-pair composition drift (see Phase 2).
7. Weather is now controlled (hourly, one midtown point) — but one point, and
   ERA5 reanalysis rather than a station gauge.
8. 2020–2021 are anomalous and probably excluded from the seasonal baseline.
9. Closure data is *permitted* events only. Security closures — exactly what
   UNGA week is made of — are not published anywhere. RQ2's main mechanism is
   therefore unobserved.
10. RQ3's volume answer rests on extrapolating a fitted trend, which is an
    assumption about the counterfactual, not a measurement.

---

## Phase 5 — Writeup (`article/`)

Substack piece for an NYC urban development audience. Likely spine: open with
the noon-vs-4am number (4.8 vs 11.6 mph — midtown at midday is slower than a
bicycle), use the seasonal curve as the backbone, treat UNGA as the "everyone
assumes this" myth-check, and close on congestion pricing as the live policy
question. Charts built with the `dataviz` conventions; assets to `article/lib/`.

---

## Decisions (resolved 2026-09-18)

1. **Headline geography** — midtown core, or extended? (My lean: core for the
   headline, extended for daily cuts.).  Answer: see note above, let's do both but focus on core.
2. **Time range** — start at 2019 (pre-COVID baseline, costs us the COVID mess)
   or 2022 (clean, but only one pre-congestion-pricing year of seasonality)? Answer: let's start in 2019 and see if we need to exclude 2020 and 2021 because of covid.  It will be useful to have 2019, 2023, and 2024 for non-congestion price years.
3. **FHVHV** — pull the 35GB now, or stay yellow-only until RQ3 forces it? Answer: skip the FHVHV for now.  For RQ3 I am interested to see if we can residualize out the secular decline of TLC rides to see the additional affect of congestion pricing.
4. **Scope of RQ3** — is the volume half of "fewer rides" in scope, or do we
   keep this piece about speed and leave volume to MTA's own numbers? Answer: I would like to keep the focus on speed, but explore the volume effect, even if we have to approximate it using de-trended TLC data.
