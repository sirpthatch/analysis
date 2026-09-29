# OMNY Fare Cap: the monthly pass the MTA retired and never replaced

Project brief for continuing this analysis. Read this file first, then
`research/research-log.md` for sourced detail and `research/queries.md` for exact API
calls. `README.md` describes the project layout and how to run the pipeline.

The project was scoped on 2026-09-28 from a news-scan session. The same evening it was
run against the live APIs for the first time (research-log, "first networked session"):
all 11 tests pass, the 2024 window reconciles, the station pull ran, and the follow-up
questions in `additional_questions.md` are answered in
`research/additional-questions.md` with figures in `article/lib/`. See
`research/limitations.md` for what the data cannot support and `research/plan.md` for
the phased plan. Environment: `source venv/bin/activate`.

## The story

In January 2026 the MTA raised the base fare from $2.90 to $3.00 and, in the same
board action, retired the 7-Day, 30-Day and Express Bus Plus unlimited MetroCards —
in its own words, they "retire and be replaced with the automatic fare cap for all
riders." But the replacement cap is **weekly only**: $35 a week, $17.50 reduced. The
7-day pass got a successor. The 30-day pass did not.

The angle: quantify the population that absorbed that change. In a sixteen-day window
in September 2024, 7.4% of all subway rides were taken on a 30-Day Unlimited — roughly
one ride in thirteen. Two years later the same window shows 31 rides on that product
and the 7-Day Unlimited category has vanished from the schema entirely. Advocates
(Effective Transit Alliance, PCAC) have been making the argument since February 2026
that this was a stealth fare hike on the city's most frequent riders; ETA proposes a
46-ride/30-day cap to "restore the status quo." Nobody has published the denominator.

On 2026-09-23 MTA chair Janno Lieber said he is open to caps beyond the weekly one and
wants an "equity oriented fare structure," ahead of a planned 2027 fare increase. That
is the hook: there is now a decision-maker to hold a number up against.

## The facts, precisely

- **Fare schedule (effective January 2026):** base $2.90 → $3.00; reduced $1.45 →
  $1.50; express bus $7.00 → $7.25. Weekly cap $35, reduced weekly cap $17.50.
- **Retired:** MetroCard 7-Day, 30-Day and Express Bus Plus unlimited passes.
- **Cap arithmetic:** $35/week × 52 ÷ 12 = **$151.67/month** for a rider who caps every
  week. ETA's proposal is 46 × $3.00 = **$138.00/month**. Gap: **$13.67/month**,
  $164/year. That gap is the publishable number.
- **Composition, 16-day windows (Sept 1–16, 2024 vs Sept 1–16, 2026):**

  | | 2024 | 2026 |
  |---|---|---|
  | Total rides | 53,643,755 | 58,904,420 (+9.8%) |
  | MetroCard share | 38.18% | **0.451%** |
  | Unlimited passes (7- + 30-day) | 15.22% | ~0% |
  | 30-Day Unlimited | 3,981,746 rides (7.42%) | 34 rides |
  | 7-Day Unlimited | 4,181,883 rides (7.80%) | *category absent* |
  | OMNY Fair Fare | 262 | 2,426,999 (4.12%) |

  Both columns reconcile exactly to independent `sum(ridership)` window totals, and
  the station-level pulls sum to the same totals. (Figures re-recorded 2026-09-28:
  the scoping numbers used a `>` filter that dropped the 00:00 hour of Sept 1.)

- **Retired pass price, sourced:** 30-Day $132.00 ($66 reduced), 7-Day $34 ($17),
  MTA "New Fare Information Effective August 20, 2023" (mta.info/document/118601,
  in `research/docs/`), corroborated by the 2026 fare-change board materials
  (document 186881). Daily rider's monthly ceiling $132.00 → $151.67: **+$19.67,
  +14.9%**, against a 3.4% base-fare increase. Effective date **January 4, 2026**.

## Key finding already established (verified live 2026-09-28)

The monthly pass was 7.42% of subway ridership in September 2024 and is functionally
extinct today, while the only replacement product is weekly. The effective monthly
ceiling for a daily rider rose from $132 to $151.67 with no monthly product at any
price.

**Two qualifications that must travel with it** (research/additional-questions.md):

1. **The decline was gradual, not a cliff.** 30-Day share: 10.4% (Jan 2023) → 7.4%
   (Sep 2024) → 3.7% (Sep 2025) → 1.25% (Dec 2025). Half the pass population had
   moved to OMNY before retirement. Lead with the three-year line.
2. **"30-Day Unlimited" is a category, not a product.** Per the MTA data dictionary it
   includes the $66 reduced-fare 30-day pass and agency/ADA passes, while Fair Fare
   30-Day passes sit in "Fair Fare" and EasyPay/annual unlimited cards in "Other".
   The 7.42% is both contaminated and a floor. Say "the 30-Day Unlimited category".

Every figure in that sentence is in `constants.RECORDED_FARE_CLASS_RIDERSHIP` and is
re-checked by `pytest tests/`. A test failure means the MTA restated the data, which
is itself worth a line in the research log — do not loosen the assertion.

## What still needs to be done

Status as of 2026-09-28 evening. Items 1–6 are resolved or reframed; details and
numbers in `research/research-log.md` and `research/additional-questions.md`.

- **1 — DONE, verdict DISPERSED (narrowly: IQR 2.48pp vs 2.0).** Confounded by OMNY
  adoption (r = 0.54 with station MetroCard share). Within MetroCard rides it stays
  dispersed (IQR 6.45pp) and the robust clusters are the Queens Roosevelt Av corridor
  and southern Brooklyn N/D/Q. **The Bronx and Staten Island had the LOWEST
  monthly-pass reliance among MetroCard riders** — the simple equity framing does not
  hold, consistent with the MTA's Title VI finding of no disparate impact (doc 186881).
  Report both measures; do not map the all-rides share alone.
- **2 — DONE.** Both windows reconcile exactly.
- **3 — DONE.** $132 sourced (see facts above).
- **4 — DONE (answer: contaminated both ways).** See qualification 2 above.
- **5 — DONE.** School-calendar artifact: school opened Thu Sep 10 2026 vs Thu Sep 5
  2024, so the window holds 5 school days vs 8. Per school day, flat to up.
- **6 — OPEN, recommendation:** use `ridership` (all entries) for composition shares;
  use `ridership − transfers` (paid entries) for anything priced. That is what the
  code now does.
- **7 — PARTLY.** Release notes: OMNY–Students category added 2024-10-15; MetroCard
  trade-in transactions removed from ride counts 2025-10-07 (wujg-7c2s updated
  2025-10-15). Monthly composition shows no obvious jump across Dec 2024 → Jan 2025
  (30-Day 6.45% → 6.88%, 7-Day 7.50% → 6.27%, Other 3.84% → 3.72%), but that is an
  eyeball check, not a test, and no MTA continuity note has been found.
- **8, 9 — unchanged.** Bus hourly data is now pulled daily by fare class (used for
  revenue), but no bus geography work has been done.
- **NEW — unexplained "OMNY – Other" step on 2026-07-31** (~94k → 220k/day). Identify
  before interpreting 2026 "Other".
- **NEW — student 24/7 and summer validity** is visible in the data from Sept 2024 /
  summer 2025; needs a policy source.

### Fare proposals (spec_fare_proposals.md) — modelled 2026-09-29

Full write-up: `research/fare-proposals.md`; code `farecap/proposals.py`;
`python src/analyze.py --proposals`. Headlines:

- **Inverse distance** (revenue-neutral $3.75 / $3.15 / $2.50 / $1.90 by distance
  band): lower-income neighbourhoods do originate longer trips (Spearman 0.50), and
  the three most low-income quintiles of origins pay ~6% less. But 48% of trips from
  the lowest-income quintile still pay more, and if short trips are more
  fare-sensitive (walk/bike), revenue falls 2–5%.
- **E-ZPass-style reserve**: MTA would hold ~$234M of rider money and earn ~$10M/yr
  interest ($5–18M range); median rider fronts ~$49. Breakage is a placeholder.
- **Monthly cap $138 (ETA)**: −$12–20M/yr, matching the independent pass-holder
  estimate ($18–27M). Annual cap at 12 × $138 does almost nothing ($6–15M).
- **Daily cap**: $0 to −$570M depending entirely on the unobserved share of rider-days
  with 3+ trips. Ask the MTA for OMNY taps-per-card-day.
- Proposals 2–4 run on a SYNTHETIC rider population (no rider IDs in open data);
  calibrated to observed trips and the 2024 unlimited share; implies 3.0–4.2% of taps
  free under the weekly cap, matching the revenue model. Quote ranges, never points.
- The spec's item 4 is truncated ("pay once during the …"); modelled as a daily cap.

Original item text, for reference:

1. **Run the station-level pull.** `farecap.stations.station_table(2024)` is written but
   has never executed. This is the load-bearing question and it is one query away:
   does 30-day-pass share vary by station, or is it flat? `dispersion_verdict()`
   decides it explicitly (IQR < 2pp ⇒ FLAT). **A FLAT verdict kills the geography and
   the equity framing, and that is a publishable finding — report it, don't go hunting
   for a subgroup that survives.** Highest-value work left.
2. **Reconcile the 2024 window total.** The fare-class split sums to 53,599,970 but no
   independent `count(*),sum(ridership)` call was ever made against `wujg-7c2s` for
   that window. `python src/collect.py windows --refresh` closes this. Until it runs,
   every 2024 share in this brief is unreconciled. The analogous check on 2026 is what
   caught a silent sixteen-day/two-day window error during scoping, so this is not
   ceremony.
3. **Source the retired 30-Day price.** Widely repeated as $132 and arithmetically
   consistent (132 ÷ 2.90 = 45.5 rides), but I could not find it in an MTA fare
   schedule or board document. `constants.METROCARD_30DAY_FINAL_PRICE_UNVERIFIED`
   holds it, unused. **Do not publish a before/after dollar figure until this is
   sourced from a primary document.** MTA board minutes or an archived fare schedule.
4. **Read the MTA data dictionary for both ridership datasets.** "Metrocard - Other"
   was 1.9M rides in 2024 and "OMNY - Other" is 2.3M now, and nobody has established
   what is in either bucket. If pass products are mixed into "Other," the 7.42% figure
   is a floor or is contaminated. This is the most likely way the headline number is
   wrong.
5. **Explain the student drop.** OMNY Students fell 2,343,858 → 1,570,400 across the two
   windows. That is large enough to be a category or school-calendar artifact rather
   than behavior, and it undercuts any claim about total composition until explained.
6. **Decide the ridership-vs-transfers denominator.** `ridership` and `transfers` are
   separate columns; shares change depending on which you use. Pick one, state it.
7. **Check the 2024/2026 dataset seam.** The windows come from two different datasets
   (`wujg-7c2s`, `5wq4-mkjj`) with a boundary at the start of 2025. Any schema or
   estimation-method change at that seam appears as a spurious trend. Look for an MTA
   note on methodology continuity.
8. **Decide whether Fair Fares earns a place.** `3tw8-6si8` is the only NYC Open Data
   source here and it is two columns, citywide monthly, no geography. Enrollment is
   flat-to-slightly-down since June 2026 (384,914 → 382,820 by August), eight months
   after the fare increase. Mildly interesting, analytically thin. Currently in scope
   only as an aside.
9. **Bus side, if the subway result holds.** `gxb3-akrn` and `kv7t-n8in` have the same
   shape and the fare cap covers local bus. Unverified columns. Only worth it if the
   subway geography is real.

## Constraints found the hard way during scoping

- The **bounded** sixteen-day station x fare-class `$group` on `wujg-7c2s` also times
  out (>120s). One day takes <1s. Use `collect.grouped_by_day`. A month grouped by
  `date_trunc_ymd(transit_timestamp)` took ~5 min; pull one query per day instead
  (`farecap.timeseries`, ~3.5 min for 1,355 days at 6 workers).
- Any `$group` over the O-D datasets on text columns times out; the station
  coordinates come from one hour of the hourly dataset instead (`od.stations`).
- mta.info press releases and cbcny.org return 403 to automated fetches; MTA
  `/document/<id>` PDFs download fine.
- An unbounded aggregate over `5wq4-mkjj` (45.2M rows) **times out**. Always bound
  `transit_timestamp` in `$where` before any `$group`. `farecap.collect` enforces this
  by construction — there is no code path that groups without a window.
- `transit_timestamp > '2026-09-01'` with no upper bound silently returned sixteen days
  and an implausible ~58M rides, which read as two days' data at first. The
  reconciliation step exists because of that error. Keep it.
- `max(transit_timestamp)` with `$order ... DESC $limit 1` returned 2026-09-02 while the
  same aggregate inside a bounded window returned 2026-09-16. Trust the bounded
  aggregate; the coverage end is 2026-09-16.
- Long query URLs get rejected outright by some HTTP intermediaries. Keep `$where`
  clauses terse.
- Socrata rate-limits unauthenticated requests with 429. `collect.soql` retries once
  then fails loudly rather than returning partial data. Set `SOCRATA_APP_TOKEN`.

## Style / sourcing standard to maintain

Every dataset ID, column name, and statistic in this project must be independently
re-verified by querying the live endpoint or reading the dataset's metadata — don't
trust anything in `data/` as still-current without a fresh check, since these portals
update continuously. Flag anything you cannot verify rather than dropping it silently.
SoQL text comparisons should be wrapped in `upper()` — text fields in these datasets
are inconsistently cased. Note the domain split: ridership is **state** open data
(`data.ny.gov`), Fair Fares is **city** (`data.cityofnewyork.us`).
