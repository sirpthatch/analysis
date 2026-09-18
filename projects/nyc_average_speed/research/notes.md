Thesis: Midtown Manhattan traffic speed over the year, and what moves it
Theme: Seasonality, UNGA week, and congestion pricing on midtown surface streets

Phase 3 answers: see research/rq1_seasonality.md, rq2_unga.md,
rq3_congestion_pricing.md.

Headline facts:
* Midtown speed falls 36% across the year: 6.39 mph in January, 4.10 in
  December. A December evening averages 3.19 mph - slower than a brisk walk.
* Whether UNGA is "the slowest week of the year" depends on geography:
  - Beside the UN (zones 229, 233): rank 1 of 52 in EVERY year 2019-2025.
  - Midtown core: December wins in 2022, 2024, 2025; UNGA in 2019, 2023.
  This reconciles our finding with NYC DOT, which designates every UNGA weekday
  a Gridlock Alert Day and says midtown speeds that week are "historically their
  slowest of the year" - a claim almost certainly measured near First Avenue.
  DOT: https://www.nyc.gov/html/dot/html/pr2026/nyc-dot-announce-gridlock-alert.shtml
* The UNGA effect scales with distance from the UN: -32% in the UN-adjacent
  zones, -18% midtown, -8% across the CBD, ~0 above 60th St.
* In 2020 the General Debate ran on video with no delegations. The traffic
  effect vanished entirely (+3.9% vs neighbouring weeks, rank 28/53).
* Restricting to dry hours, the UNGA effect is -8% to -18% - smaller than the
  raw figure but still large. Use the dry numbers.
* Congestion pricing bought about +2.0% speed in year one, concentrated in
  morning/midday/evening and absent overnight. By 2026 the net effect is -0.4%,
  i.e. gone.
* CBD yellow taxi volume ROSE across the toll introduction (0.420 -> 0.485 of
  2019). The "fewer rides" expectation is wrong for taxis; whether fewer
  vehicles enter the CBD needs MTA entry counts.

Earlier facts (full 2019-2026 data; see research/profile_findings.md):
* Midtown core runs 5.80 mph distance-weighted across the whole period -
  11.59 mph at 5am, 4.90 mph at 5pm.
* Midtown is getting steadily slower: 6.18 mph (2019) -> 4.94 (2026), a 20%
  decline. Uptown fell only 8% over the same span, so this is a midtown story.
* UNGA week lands in the slowest five weeks of the year in every ordinary year,
  0.9-1.7 mph below the year median, and was the slowest week outright in 2019.
  In 2020, when the debate ran on video with no delegations present, the effect
  disappears: rank 28/53, +0.01 mph from the median.
* Congestion pricing shows a small first-year gain - DiD of +0.16 mph for the
  CBD against uptown, about 2% - which more than reverses by 2026 (-0.20).
* Yellow taxi never recovered from COVID. Midtown core sits at 34% of its 2019
  volume in 2026, having peaked at 42% in 2025. The decline is ongoing.

Earlier single-month facts:
* Yellow taxi trips that both start and end in the midtown core average
  6.07 mph (median 5.64) in June 2025.
* The daily curve is dramatic: 11.6 mph at 4am, 4.8 mph at noon. Midday midtown
  is slower than a casual cyclist.
* Evening rush (5pm, 5.0 mph) is barely slower than midday (12pm, 4.8 mph) —
  midtown does not really have a "rush hour," it has a rush *day*.
* June 2025 yellow taxi: 4,322,960 trips citywide; 121,845 both-ends in the
  midtown core, 369,387 in midtown extended.
* About 14% of raw trips fail the plausibility filters (distance, duration, or
  implied speed out of range).

Data notes:
* TLC trip records: monthly parquet at
  https://d37ci6vzurychx.cloudfront.net/trip-data/<service>_tripdata_YYYY-MM.parquet
  Confirmed available through 2026-07 as of 2026-09-18 (roughly a 2-month lag).
* Zone lookup and shapefile under /misc/ at the same host.
* 2025+ yellow files carry `cbd_congestion_fee`, so the congestion toll is
  visible per trip, not just inferred from the date.
* LocationID-based zones replaced lat/lon in TLC data in mid-2016, so anything
  earlier needs different handling.

To verify:
* UNGA General Debate dates per year (constants.py table is from memory).
* CBD tolling zone boundary vs. taxi zone boundaries — 60th St cuts zones.
