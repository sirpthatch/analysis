# Limitations and open questions

Running list of what the data cannot support. Update as things are resolved or found.

## Fatal to the obvious approach

**There is no rider identifier anywhere in these datasets.** Ridership is aggregated by
station, hour and fare class. So you **cannot** compute how many riders would reach a
46-ride monthly cap, how many currently hit the weekly cap, or any per-rider trip
distribution. Any analysis framed as "N riders would benefit" is unsound with these
sources. The revealed-preference workaround — 30-day-pass ridership by station, since
buying the pass *was* the declaration of high frequency — is the only defensible route,
and it measures where affected riders board, not how many there are.

## Numerator / category problems

**The "Other" buckets are unexplained.** `Metrocard - Other` was 1,917,214 rides in the
2024 window; `OMNY - Other` is 2,343,404 in the 2026 window. Nobody has read the MTA
data dictionary to establish what is in them. If pass products are pooled into "Other,"
the headline 7.42% monthly-pass share is either a floor or contaminated. **This is the
most likely way the central number is wrong.** Resolve before publishing.

**The student series moves too much to ignore.** OMNY Students 2,343,858 → 1,570,400
across the two windows, a 33% drop, against 9.8% total ridership growth. Almost
certainly a category or school-calendar artifact rather than behavior. Until explained
it undercuts any claim about *total* composition, though it does not touch the
monthly-pass figure directly.

**Category absence vs zero.** `Metrocard - Unlimited 7-Day` is absent from the 2026
schema entirely, not reported as zero; `Metrocard - Students` is absent from 2026 and
present as 2 rides in 2024. Code that groups across both windows must preserve the
distinction. `fareclass.comparison` keeps NaN rather than filling zero for this reason.

## Reconciliation and seams

**The 2024 window total is unreconciled.** The fare-class split sums to 53,599,970 but
no independent `count(*),sum(ridership)` was ever run for that window. The equivalent
check on 2026 is what caught a silent window-length error, so this gap is real.
`python src/collect.py windows --refresh` closes it.

**The two windows come from two different datasets** (`wujg-7c2s` and `5wq4-mkjj`) with
a seam at the start of 2025. Any schema change, estimation-method change or
reprocessing at that boundary will present as a two-year trend. No MTA methodology-
continuity note has been located.

**These are estimates.** The MTA describes both datasets as providing ridership
*estimates*, not counts. Treat precision accordingly; do not report figures to more
significant digits than an estimate supports.

**Ridership vs transfers.** `ridership` and `transfers` are separate columns. Shares
differ depending on the denominator chosen. Pick one and state it in the piece.

**Station complexes get renamed and merged.** A 2024→2026 join on `station_complex`
silently drops rows. `stations.join_across_windows` joins on `station_complex_id` and
reports the unmatched counts on both sides; check them before trusting any change
figure.

## Unverified facts

**The retired 30-Day Unlimited price.** Widely repeated as $132, arithmetically
consistent with a $2.90 base fare (45.5-ride breakeven), but not found in any MTA fare
schedule or board document during scoping. Held in
`constants.METROCARD_30DAY_FINAL_PRICE_UNVERIFIED` and deliberately unused by
`cap_arithmetic`. **No before/after dollar figure may be published until this is
sourced from a primary document.**

**`wujg-7c2s` coverage end** is assumed from the dataset title (2024-12-31), not
verified by query.

**`5zyy-y8am`-style metadata drift.** For `5wq4-mkjj`, the catalog index and the
metadata endpoint have disagreed on update dates elsewhere in this repo's projects.
The `rowsUpdatedAt` of 2026-09-23 came from the metadata endpoint and is the one to
trust, but re-check.

## Scope limitations

**Fair Fares gives no geography.** `3tw8-6si8` is two columns of citywide monthly
totals. It cannot support any neighborhood or income-level claim. It is the only NYC
Open Data source in the project, which is worth knowing if the piece is framed as a
city-data story.

**Bus is out of scope for now.** The fare cap covers local bus, and `gxb3-akrn` /
`kv7t-n8in` share the subway schema, but their columns are unverified and the bus
analysis is only worth doing if the subway geography holds up.

**The pipeline has never touched the live APIs.** Written and tested against committed
fixtures only. Treat the first live run as a verification step, not a formality.

## Updates, 2026-09-28 first networked session

**Resolved.** 2024 window reconciliation (exact). Retired pass price ($132, MTA doc
118601). Student drop (school calendar: 5 vs 8 school days in the window). "Other"
buckets read from the MTA data dictionary — see research-log.

**Newly found — the 30-Day category is not one product.** It includes the $66
reduced-fare 30-day pass, agency and ADA passes; Fair Fare 30-Day passes sit in
"Fair Fare"; EasyPay Unlimited and annual cards sit in "Other". The 7.42% is both
contaminated and a floor. Never call it "the $132 pass share".

**Newly found — the Sept windows hide a three-year trend.** 30-Day share fell from
10.4% (Jan 2023) to 3.7% (Sep 2025) before retirement. Two-point comparisons across
the MetroCard → OMNY migration overstate what the retirement itself did.

**Station geography is confounded by OMNY adoption.** Share-of-all-rides tracks a
station's MetroCard share (r = 0.54). Report share within MetroCard rides alongside.
Neither measure says anything about rider income; stations are not people.

**Unexplained: "OMNY – Other" step on 2026-07-31** (~94k → 220k/day, back to ~120k
from Sept 8). Do not interpret 2026 "Other" until identified.

**Revenue model (Q3) is calibrated in aggregate.** It reproduces audited farebox
within ~2–4% (except December accruals), which shows its assumptions are jointly
plausible, not individually right. Rides per pass and the share of OMNY taps that
were free under the cap are unobservable here.

**Published ridership excludes evaders.** MTA official subway ridership ≈ tap
entries; true rider counts are ~11–16% higher on subway and ~80–100% higher on bus
at the MTA's own evasion rates. Evasion dollar figures are fare-equivalent value,
not recoverable revenue; CBC's 2024 estimates are ~15–20% lower on value per ride.

**Fare-scheme sketches (Q5) have no rider dimension.** Elasticity −0.3 is a
literature rule of thumb. Distance is straight-line. Staten Island is absent from
the O-D data. The monthly-cap population is a floor (2024 pass holders only).

## Proposal modelling (spec_fare_proposals.md), 2026-09-29

**No rider-level data, so proposals 2–4 run on a synthetic population.** It is
calibrated to observed weekly full-fare paid trips (24.1M, Mar–Jun 2026) and the
Sept 2024 unlimited-pass share of full-fare-plus-pass trips (18.1%, used as the
share of trips in 12+-trip weeks). Mean trips per rider cannot be calibrated and
is swept (3, 4.5, 6/week → 8.0M, 5.4M, 4.0M riders). Its one out-of-sample check:
it implies 3.0–4.2% of taps are free under the weekly cap, matching the 4%
assumption that let the revenue model reproduce audited farebox. Consistent, not
validated.

**Daily-cap results depend on an unobserved quantity** — how many rider-days
have 3+ trips. The model sweeps same-day chaining and reports results against the
share of 3+-trip days it produces. Without chaining a daily cap costs ~nothing
(heavy riders already hit the weekly cap). No open NYC source for this share was
found.

**Simplifications**: full-fare riders only; calendar weeks (MTA's cap is rolling);
30-day windows; stable individual rates all year (overstates annual-cap reach); no
induced trips; visitors are not modelled separately (they are the natural market
for a daily pass).

**Inverse distance uses neighbourhood, not rider, income**, from 2016–2020 ACS
(pre-pandemic), within 0.5 mi of the origin station.

**E-ZPass replenishment point is unpublished** and swept; breakage figures are
illustrative placeholders.

**Subway fare evasion before 2021 is not comparable (added 2026-09-29).** The MTA's
methodology document for `6kj3-ijvb` says the current method "was first used in Q1
2021 … This method is not apples-to-apples comparable to the previous rates." Bus
(`uv5h-dfhp`) switched from surveys to automated counters in 2020-Q4. Fig 4 now
starts at 2021-Q1; any evasion comparison should too. Details and quotes:
`article/lib/fig4_sources.md`.
