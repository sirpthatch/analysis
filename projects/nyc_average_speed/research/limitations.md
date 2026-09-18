# Limitations

Running list. Entries struck through were checked in Phase 2 and resolved; see
`research/profile_findings.md` for the numbers.

## Data quality

0. **Five days of TLC source outage** — 2022-09-18 and 2023-09-21 through
   2023-09-24 retain 2–4% of normal volume across every taxi zone at once, on
   dry non-holiday days. The loss is in TLC's published files, not our pipeline.
   Four of the five fall inside UNGA weeks. They are excluded via
   `constants.TLC_OUTAGE_DATES`; the cost is that **UNGA 2023 loses 2 of its 5
   weekdays** and drops out of the week-level comparison.

## Measurement

1. **Taxis are not traffic.** Taxi trips include passenger dwell time, stops,
   and a route preference for avenues. "Midtown speed" here is taxi speed.
2. **`trip_distance` is meter-reported** route mileage with its own error, not
   a GPS track.
3. **Duration spans meter-on to meter-off**, which includes loading time at
   both ends. Short trips are hit hardest, and midtown trips are short.
4. ~~**Filter bounds are judgement calls.**~~ *Quantified:* every bound moves
   the headline by under 0.03 mph except the 0.2-mile minimum trip distance,
   which is worth +0.12 mph if raised to 0.5 and drops 13% of trips. That one
   is conservative — a stricter floor makes midtown look faster, not slower.

## Sample

5. **Yellow taxi volume has fallen sharply** since 2015. The fleet and its
   behavior are not constant across the study period.
6. ~~**Zone-pair composition drift.**~~ *Measured and cleared:* holding the
   trip mix at January 2019 changes the yearly figure by a mean of 0.034 mph
   against an observed swing above 1 mph — about 3%. Not a live concern.
7. **Sample thins fast by hour.** Median 3,550 core trips/day is fine for
   daily and weekly work, but the median 4am hour has **7 trips**. Hour × day
   resolution requires midtown extended or pooling across weeks.

## Confounders

8. **Weather is hourly ERA5 reanalysis at a single midtown point**
   (Bryant Park), not a station gauge and not spatially resolved. Fine for
   "was it raining in midtown"; not for localised squalls. Rain costs a
   consistent ~5%. **Snow is not usable raw** — snowing hours show 6.03 mph
   against 5.86 dry, because snow suppresses discretionary trips and shifts
   what remains off-peak. It needs an hour-and-season control first.
9. **2020–2021 are anomalous** and likely excluded from any seasonal baseline.
10. **Congestion pricing coincides with January**, so naive before/after
    comparisons are confounded by season.
11. ~~**The CBD boundary (60th St) does not align with taxi zones.**~~
    (CBD = Central Business District, the area tolled by MTA's Central
    Business District Tolling Program: Manhattan south of 60th Street.)
    *Checked 2026-09-18 and largely resolved:* 41 Manhattan zones are wholly
    south of 60th, 3 straddle it, and the rest are wholly north. The straddling
    zones are excluded from both DiD groups. What remains is the ordinary
    caveat that the real toll zone excludes the FDR, Route 9A and the Hugh
    Carey/Battery Park approaches, which taxi zones cannot express.
12. **Closure data covers permitted events only.** NYPD security closures —
    UNGA perimeters, motorcade routes — are announced by press release and
    published nowhere. For RQ2 this means the primary causal mechanism is
    unobserved; we can measure the slowdown but not attribute it to specific
    street closures from data.
12b. **Parallel trends is violated in RQ3's pre-period.** The CBD−uptown speed
    gap was already widening 0.3–0.5 mph/year before the toll. That makes the
    +2.0% estimate conservative but imprecise — quote it as "about 2%", never as
    a three-decimal figure.
12c. **The volume de-trending failed its placebo test and was abandoned.**
    Running the method on 2024 (no toll, truth = 0) returns −14.0% to +2.6%
    depending on specification, and the 2025 estimate's sign flips between
    linear and quadratic trends. Volume claims must come from MTA entry counts
    or FHVHV, not from this dataset. See `research/rq3_congestion_pricing.md`.
13. **RQ3's volume finding depends on an extrapolated trend.** De-trending
    yellow taxi volume estimates a counterfactual rather than measuring one,
    and yellow taxis are a shrinking and unrepresentative slice of traffic.
    Worse, the trend is **not monotonic**: midtown volume recovered to 42% of
    2019 by 2025 and fell back to 34% in 2026. A linear trend fit will not do.
14. **Permit counts are a validated control, not a causal estimate.** Weekday
    speed falls monotonically with road-affecting permits (7.67 mph at zero to
    5.31 at six or more), but permits correlate with season and day type, which
    independently move speed.
