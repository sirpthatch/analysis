# Research log

## 2026-09-28 — scoping session

Origin: this came out of a daily news-scan brief. It was initially **cut** from that
brief on the grounds that the data lives on `data.ny.gov` rather than NYC Open Data,
then reopened. Two corrections came out of reopening it, both worth recording because
they are the kind of mistake that recurs:

1. The domain objection was wrong in substance. `data.ny.gov` is the same Socrata API
   with the same verifiability. The real constraint is that the *city* data available
   (Fair Fares, `3tw8-6si8`) is thin — two columns, no geography.
2. The story was initially filed as "fare restructuring," which is too vague to
   analyse. Reading the 2026-09-23 Streetsblog piece showed it is specifically about
   **fare capping** — no distance-based, zone or free-bus proposals are in play. Fare
   capping is arithmetic, which is what made the project tractable.

### The premise, established

Verified from the MTA's own January 2026 board adoption: the 7-Day, 30-Day and Express
Bus Plus unlimited MetroCards were retired and "replaced with the automatic fare cap."
The cap is weekly only. So the 7-day product has a successor and the 30-day product
does not. Everything else in the project follows from that asymmetry.

### Composition finding

Two sixteen-day windows, same calendar position, two years apart. Full figures in
`constants.RECORDED_FARE_CLASS_RIDERSHIP`, fixtures in `tests/fixtures/`.

- Sept 1–16 2024: 53,599,970 rides. MetroCard 38.19%. Unlimited passes 15.22% of all
  rides — 30-Day 3,979,572 (7.42%), 7-Day 4,178,161 (7.80%).
- Sept 1–16 2026: 58,876,993 rides (+9.8%). MetroCard **0.451%**. 30-Day Unlimited: 31
  rides. 7-Day Unlimited: category absent from the schema.

So roughly one subway ride in thirteen used to be taken on a monthly pass, and the
product no longer exists at any price.

### Process error worth keeping

The 2026 fare-class query was first run as `$where=transit_timestamp>'2026-09-01'` with
no upper bound. I read the result as covering two days and got ~58.9M rides, which is
absurd — about 29M/day against an actual system figure near 3.7M. Running the
independent `count(*),sum(ridership),min,max` aggregate revealed the window was
actually 2026-09-01 .. 2026-09-16, sixteen days, at 3.68M/day, which is correct.

Two consequences, both now baked into the code: every window in `farecap.collect` is
explicit and half-open, and `fareclass.composition` reconciles the fare-class split
against an independent total and refuses to report an unreconciled split as reconciled.
The 2024 side of that reconciliation was never run — open item 2.

### Cap arithmetic

$35/week × 52 ÷ 12 = $151.67/month for a rider who caps every week. ETA's proposed
46 rides × $3.00 = $138.00. Gap $13.67/month, $164/year.

Deliberately **not** computed: the change against the retired pass's price. The $132
figure in circulation is arithmetically plausible (132 ÷ 2.90 = 45.5 rides) but was not
found in any MTA fare schedule or board document. Open item 3. `cap_arithmetic()`
carries the caveat in its return value so it cannot be quoted without it.

### Method decision: revealed preference

The direct question — how many riders would hit a 46-ride monthly cap — is
**unanswerable** with these datasets. They are aggregated by station, hour and fare
class; there is no rider identifier and no trip chain. Recorded in limitations.md as
fatal to that framing.

The substitute: riders who bought a 30-Day Unlimited had already declared themselves
high-frequency, so the 2024 data grouped by station gives the share of each station's
ridership taken on a monthly pass — a map of where the affected riders board, with no
per-rider model. Implemented in `farecap.stations`, **never run**.

The kill condition is explicit in `stations.dispersion_verdict`: if the interquartile
range of station-level monthly-pass share is under 2 percentage points, the
distribution is flat, there is no geography, and the equity framing collapses. That
verdict should be published either way.

### Fair Fares aside

`3tw8-6si8`: 92 monthly rows, Jan 2019 – Aug 2026. Enrollment 381,558 (Apr) → 383,377
(May) → 384,914 (Jun) → 384,372 (Jul) → 382,820 (Aug). Flat, slightly down from June,
eight months after the fare increase. Mildly interesting; no geography, so analytically
thin. Not load-bearing.

### Environment note

The scoping environment had no direct HTTP egress from Python — the proxy refused
`requests` calls to `data.ny.gov` with a 403 tunnel failure. All figures above were
verified through a separate fetch path and committed as fixtures. The pipeline runs
end-to-end offline against those fixtures (`python src/seed_cache.py && python
src/analyze.py --composition --arithmetic` reproduces every number in this log) but has
**never contacted the live APIs**. First live run is a verification step.

One genuine bug was caught by the test suite during scoping: `cap_arithmetic` rounds
`rides_to_hit_weekly_cap` to 11.67 for display, and the test compared it against
35/3.00 at 1e-6 relative tolerance. Fixed in the test, not the rounding.

## 2026-09-28 — first networked session

Machine had direct egress; the pipeline touched the live APIs for the first time.
Project set up as a standard analysis directory (venv/, article/lib/, handoff zip
moved to research/, `.gitignore` restored — it was dropped when the zip was unpacked).

### The recorded figures were for a window one hour short

`python src/collect.py windows --refresh` returned 53,643,755 (2024) and 58,904,420
(2026), not the recorded 53,599,970 / 58,876,993. Cause: every scoping query used
`transit_timestamp > 'YYYY-09-01'`, which excludes the 00:00 hour of Sept 1, while
`constants.WINDOWS` and the pipeline use `>=`. **Not a restatement** — re-running the
strict `>` query live returns the recorded numbers exactly. `constants.py` re-recorded
for the half-open window the code actually queries. Headline shares unchanged to the
reported precision: 30-Day 3,981,746 rides (7.42%), MetroCard 38.18%, growth +9.8%;
2026 30-Day rides 34 (was 31).

**Open item 2 closed:** both fare-class splits now reconcile exactly to independent
window totals, and the station-level pulls sum exactly to the same totals.

### Station pull: had to be chunked

The bounded sixteen-day station x fare-class `$group` on `wujg-7c2s` exceeded the
120s read timeout. One day takes <1s. `collect.grouped_by_day` now issues one query
per day. `soql` also now converts `requests` exceptions to `CollectionError` (the
timeout previously escaped as a raw traceback).

### Station verdict: DISPERSED, narrowly

IQR 2.48pp vs the 2.0pp threshold; median 7.65%, p25 6.57%, p75 9.05%, range
2.71%–18.1% across 426 complexes. Top: southern Brooklyn N/D (8 Av N 18.1%,
Avenue U N, 18 Av N, Bay Pkwy N/D), Elmhurst Av (M,R). Bottom: Manhattan destination
stations, Bedford Av (L), Mets-Willets Point. Borough: Queens 9.20%, Brooklyn 8.11%,
Bronx 7.65%, Manhattan 6.64%, SI 6.28%. **Unchecked confounder:** stations with high
2024 OMNY adoption mechanically show low shares of any MetroCard product. Test
30-day share *within MetroCard rides* before mapping.

### Data dictionary (open item 4) — the "Other" buckets and the 30-Day category

MTA_SubwayHourlyRidership_Overview.pdf (saved in research/docs/) lists the fare codes
in each category:

- **MetroCard – Unlimited 30-Day** = 30-Day Unlimited, **30-Day Reduced Fare Media
  Unlimited**, 30-Day Agency, 30-Day ADA. So it is not purely $132 full-fare passes.
- **MetroCard – Fair Fare** includes **Fair Fare 30-Day** — monthly passes outside the
  30-Day category.
- **MetroCard – Other** includes **Mail and Ride EasyPay Unlimited, TransitCheck
  Annual, CB Annual**, CUNY ASAP 120-day, employee passes. Unlimited products.
- **OMNY – Other** = Test-Free, Limited Use, **CUNY Student**.

Net: 7.42% is both contaminated (includes reduced-fare 30-day) and a floor (excludes
Fair Fare 30-Day and annual/EasyPay unlimited). Say "30-Day Unlimited category".

Release notes: OMNY–Students added as a category 2024-10-15; **2025-10-07 bug fix
removed MetroCard trade-in transactions from ride counts** (wujg-7c2s rowsUpdatedAt
2025-10-15, so apparently reprocessed). No other seam note. Both overview PDFs are
byte-identical.

### Retired pass price — open item 3 CLOSED (primary source)

MTA "New Fare Information Effective August 20, 2023" (mta.info/document/118601, saved
in research/docs/): **30-Day $132.00 / $66.00 reduced; 7-Day $34.00 / $17.00**; OMNY
cap $34 rolling 7-day, $17 reduced. Base fare unchanged at $2.90 until 2026-01-04, so
$132 was the final price. Corroborated by the MTA 2026 fare change board materials
(mta.info/document/186881, Title VI table, "30-Day Unlimited MetroCard $132.00").

Before/after: a daily rider's monthly ceiling $132.00 → $151.67 (+$19.67, **+14.9%**)
against a base-fare increase of 3.4%.

### 2026 fare change board materials (doc 186881, Board approval 2025-09-30)

- Effective "on or about January 4, 2026". "No longer sell MetroCard fare media",
  including pay-per-trip, 7-Day, 30-Day, Express Bus Plus.
- The OMNY rolling 7-day cap was a **pilot** until this action made it permanent.
- **Title VI analysis found "no disparate impact or disproportionate burden" from
  discontinuing the 30-Day Unlimited.** Method weights each station equally, excludes
  hub stations (Penn, GCT, PABT, Howard Beach, Jamaica). Average fare paid: minority
  $2.26 vs non-minority $2.56; low-income $2.30 vs high-income $2.42. The piece must
  engage with this finding; the station table can test it.

### Q1 — year-round composition (daily series, 2023-01-01 .. 2026-09-16)

`farecap.timeseries` pulls one query per day (a month grouped by date_trunc took ~5
min; a day takes ~1s). 1,355 days, subway, bus and express bus.

**The 30-Day share declined for three years before retirement**: 10.4% (Jan 2023) →
9.4% (Sep 2023) → 7.4% (Sep 2024) → 3.7% (Sep 2025) → 1.25% (Dec 2025) → 0.45% (Jan
2026). The 7-Day tracked it (10.1% → 7.6% → 3.0% → 0.8%). The Sept-2024 vs Sept-2026
comparison compresses a gradual migration to OMNY into what reads as a cliff.

### Q2 — students: school-calendar artifact, confirmed

Daily student ridership ramps on the first school day: Thu 2024-09-05, Thu 2025-09-04,
**Thu 2026-09-10**. The Sept 1–16 window holds ~9 school days in 2024 and 5 in 2026.
Per school day 2026 is equal or higher (264–296k vs 244–298k). Also visible: student
rides on weekends from Sept 2024 (zero in 2023) and through summer 2025 (July share
4.7% vs 0.7% in July 2024) — consistent with a change in student OMNY card validity;
**policy source not yet found**.

### OMNY – Other: unexplained step on 2026-07-31

~94k/day on 2026-07-30 → 220k on 2026-07-31, ~200k through Sept 7, ~120k from Sept 8.
Also rose from ~37k (Feb 2026) to ~63–80k (Mar–Jun 2026). A one-day step is a program
launch or reclassification. **Unidentified.** It inflates the 2026 window's "Other".

## 2026-09-29 — spec_fare_proposals.md

Modelled the four proposals; findings in `research/fare-proposals.md`.

- Searched for MTA figures on unique riders, trips per rider, and riders reaching
  the weekly cap: none published. Proposals 2–4 therefore run on a synthetic rider
  population (`farecap.proposals`), calibrated to observed full-fare trips (24.1M/wk,
  Mar–Jun 2026) and the Sept 2024 unlimited share of full-fare-plus-pass trips
  (18.13%, verified from the daily series). It implies 3.0–4.2% of taps are free
  under the weekly cap — consistent with the revenue model's 4%.
- Census ACS API now requires a key (302 → missing_key.html). Used HUD LMISD via
  NYC Open Data `qmcw-ur37` + Census 2023 tract Gazetteer instead; 2,327/2,327 tracts
  matched.
- E-ZPass terms: NYSTA form TA-W68167A (commercial prepaid program) is encrypted;
  needed `cryptography` for pypdf. "No interest will be paid on balances."
- FRED DGS3MO 4.24% on 2026-09-25 (fetched CSV directly).

**Two simulator artifacts caught before reporting — keep them fixed:**

1. First version used 28-day "months". Four calendar weeks at the $35 cap is $140, so a
   $138 monthly cap saved $2 and looked nearly free (−$1–2M/yr). Fixed to per-day
   charges, weekly cap applied cumulatively within each Mon–Sun week, then 30-day
   windows. Corrected cost −$12–20M/yr, in line with the independent pass-holder
   estimate ($18–27M).
2. First version dropped round trips on random days WITH replacement, so a five-day
   commuter often got two round trips on one day; 17–22% of rider-days had 3+ trips
   and a $6 daily cap "cost" $480–630M. Fixed: round trips go to distinct days first
   (weighted by observed day-of-week volumes); same-day chaining is an explicit swept
   parameter. With no chaining a daily cap costs $0 — the only 3+-trip days belong to
   riders already at the weekly cap.
