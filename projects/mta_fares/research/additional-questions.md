# Additional questions — findings

Answers to `additional_questions.md`, first networked session, 2026-09-28. Every
figure below was produced by code in `src/farecap/` from live pulls cached in
`data/raw/`; exact commands at the bottom. Figures in `article/lib/`.

---

## Q1. Does the composition of ride types stay similar across the year, 2024 vs 2026?

**No — and the reason matters more than the answer.** Composition shifts in the same
direction in every sampled week, because the MetroCard → OMNY migration ran through
all of 2025. The two September windows the brief rests on bracket that migration,
so they turn a three-year decline into what looks like a single cliff.

Second full Monday–Sunday week of each sampled month (`output/q1_sampled_weeks.csv`),
share of subway entries:

| Week of | 30-Day | 7-Day | Sen & Dis | Fair Fare | Students | MetroCard (all) |
|---|---|---|---|---|---|---|
| Feb 2023 | 10.3% | 10.0% | 3.6% | 3.2% | 5.7% | 58.3% |
| Feb 2024 | 8.9% | 8.6% | 3.7% | 3.4% | 5.1% | 49.1% |
| Feb 2025 | 6.7% | 6.1% | 4.1% | 3.7% | 7.6% | 31.5% |
| Feb 2026 | 0.3% | 0.1% | 5.5% | 4.1% | 7.4% | 2.2% |
| Jun 2024 | 8.2% | 8.0% | 4.0% | 3.5% | 5.0% | 45.8% |
| Jun 2025 | 5.4% | 4.9% | 4.9% | 3.5% | 6.5% | 22.9% |
| Jun 2026 | 0.1% | 0.0% | 5.6% | 3.9% | 6.2% | 0.8% |
| Sep 2024 | 7.4% | 7.6% | 3.7% | 3.6% | 6.1% | 37.6% |
| Sep 2025 | 3.7% | 3.2% | 5.1% | 3.7% | 6.1% | 14.1% |

Monthly 30-Day share: **10.4% (Jan 2023) → 7.4% (Sep 2024) → 3.7% (Sep 2025) →
1.25% (Dec 2025) → 0.45% (Jan 2026)**. Annual 30-Day rides: 111.7M (2023), 95.6M
(2024), 60.6M (2025). (`fig1_fare_class_share.png`)

**Implication for the story.** "7.4% of rides were on a monthly pass in Sept 2024"
is true, but by Sept 2025 — four months before retirement — it was already 3.7%.
Half the pass population had moved to OMNY, where the only unlimited product was
the $34 weekly cap pilot. The retirement didn't create the problem; it closed the
last door. Lead with the three-year line, not the two-window table.

**Also found:** "OMNY – Other" steps from ~94k to ~220k rides/day on **2026-07-31**
and falls to ~120k from Sept 8. A one-day step is a program launch or a
reclassification; it is **unidentified** and inflates the 2026 window's "Other"
(4.0% vs 0.5%). Do not interpret "Other" in 2026 until it is explained.

---

## Q2. What accounts for the difference in student and senior tickets?

### Students — a school-calendar artifact (confirmed)

The Sept 1–16 window catches a different number of school days each year. Daily
student entries ramp on the first day of school:

| Year | First school day (from the data) | School days in Sept 1–16 |
|---|---|---|
| 2024 | Thu Sep 5 (after Labor Day Sep 2) | 8 |
| 2025 | Thu Sep 4 | 9 |
| 2026 | **Thu Sep 10** | **5** |

On school days student ridership is flat to up: 264–296k/day in Sept 2026 vs
244–298k in Sept 2024. The 2,343,858 → 1,571,206 "drop" is calendar, not behavior.
Close CLAUDE.md open item 5.

Two real changes are visible, both of which **need a policy source before
publishing**: student rides on weekends appear from Sept 2024 (zero in 2023), and
students ride through summer from 2025 (July share 4.7% vs 0.7% in July 2024).
Annual student entries 48M → 58M → 81M (2023–2025). Consistent with student OMNY
cards gaining 24/7 and year-round validity; not yet sourced.

### Seniors & Disability — mostly reclassification, not new riders

Mean daily S&D entries: 104k (Jan 2023) → 124k (Jan 2025) → 192k (Dec 2025) → ~210k
(2026). The growth is concentrated in 2025, and the S&D share moves almost exactly
opposite the unlimited-pass share (r = −0.92, monthly, Jun 2024–Dec 2025).

Mechanism, from the MTA data dictionary (`research/docs/`): the **7-Day and 30-Day
Reduced-Fare Unlimited MetroCards were counted in the "Unlimited" categories**, not
in "Seniors & Disability". When those riders moved to OMNY reduced-fare cards they
reappear as "OMNY – Seniors & Disability". So a meaningful part of the S&D rise is
riders changing category, which also means **the 2024 "30-Day Unlimited" 7.42%
includes reduced-fare ($66) monthly passes** — the population is not all $132
full-fare riders.

Caveats: the correlation shows consistency, not proof; OMNY reduced-fare
enrollment growth and paratransit growth (Access-A-Ride trips 25k/day in 2023 →
41k/day in 2026, `sayj-mze2`) may also contribute. The MTA reduced-fare history
dataset (`v8fq-z483`) ends in 2023 and cannot separate them.

---

## Q3. Weekly MTA fare revenue, verified against an outside source

**Yes.** Pricing every paid entry — ridership minus free transfers, which the
data dictionary says are a subset of ridership — at its fare class's yield under
the schedule in force that day reproduces the MTA's audited farebox revenue
closely. (`fig2_revenue_model_vs_actual.png`, `fig3_weekly_revenue.png`)

- **Outside source:** MTA Statement of Operations (`yg77-3tkj`), "Farebox Revenue",
  Actual scenario, NYCT + MTA Bus + SIR, monthly through Aug 2026.
- **Model coverage:** subway + SIR (hourly subway data), local and express bus
  (hourly bus data, express routes BM/BXM/QM/SIM/X priced at the express fare),
  Access-A-Ride trips at the base fare.
- **Fit:** central estimate / actual = 0.962 (2023), 0.968 (2024), 0.988 (2025),
  0.983 (2026 YTD). Monthly correlation 0.91. Actual falls inside the low–high band
  in 40 of 41 non-December months; **December** 2023 and 2024 run ~$48M above the
  model (Dec 2025 does not) — a year-end accrual true-up, not ridership.
- **Weekly revenue, central:** ~$65M/week average in 2023, $68M in 2024, $72M in
  2025, **$74M in 2026** (mid-2026 weeks $71–77M; subway ~$58–63M of that).

Assumptions (all in `revenue.py`, set **before** comparing to actuals; low / central /
high): rides per 30-Day pass 70 / 60 / 50; rides per 7-Day 18 / 15 / 13; share of
OMNY full-fare taps that were free under the weekly cap 8% / 4% / 0%; "Other"
yields 0 / 50% / 100% of base; students $0. Fare schedules sourced from MTA
documents 118601 (Aug 2023) and 186881 (Jan 2026).

What it can't do: the model is calibrated in aggregate, so its fit says the
assumptions are jointly plausible, not that each is right. The 4% capped-free
share in particular is unobservable here.

---

## Q4. Fare evasion, and how it compares with revenue

**Data:** subway (`6kj3-ijvb`, quarterly traffic-checker survey, with margin of
error from 2020) and bus (`uv5h-dfhp`, quarterly Automated Passenger Counters from
2020, split local / SBS / express). (`fig4_fare_evasion.png`)

- **Subway evasion stepped down in 2024-Q4**, from 13–14% (2023 through 2024-Q3)
  to ~10% (10.4% → 9.8% → … → 10.2% in 2026-Q2), and has held there. The drop is
  larger than the margin of error (±0.8–1.1pp).
- **Bus evasion** rose from ~23% (2020-Q4) to a peak of 50.6% (2024-Q2), dipped to
  ~44% in 2025, and is back at 48.5% (2026-Q2).

**The MTA's published ridership excludes evaders.** Official daily subway ridership
(`sayj-mze2`) is 0.98–0.99× tap entries; if it included evaders it would be
1.11–1.16×. Bus official is ~1.07× taps (probably coin fares), not the 1.8–2.0× that
evaders would imply. So published ridership counts payers, not riders — riders
are ~11–16% higher on subway and ~80–100% higher on bus.

**Fare-equivalent value of evaded rides** (evaded rides = entries × r/(1−r), valued
at that quarter's modelled revenue per entry):

| Year | Subway | Bus | Subway as % of subway revenue | Bus as % of bus revenue |
|---|---|---|---|---|
| 2023 | $383M | $571M | 15% | 77% |
| 2024 | $412M | $706M | 15% | 95% |
| 2025 | $341M | $642M | 11% | 84% |

**Cross-check against the Citizens Budget Commission** (Sept 2025, via NY1): 2024
losses of $350M subway, $568M bus; 330 subway and 710 bus evaded fares per minute.
Those rates annualise to ~173M subway and ~373M bus evaded rides; this model
implies ~177M and ~364M — **within 2–3% on ride counts**. The whole dollar gap is
the value per ride: CBC implies ~$2.02 (subway) and ~$1.52 (bus), this model
uses observed revenue per entry (~$2.33, ~$1.94). CBC's method is not public in
the coverage; the report PDF blocks automated download. Quote the range
($350–410M subway, $570–710M bus for 2024) and say what drives it.

**How it coincides with revenue:** the 2024-Q4 subway evasion drop coincides with
subway tap entries up 6.7% year on year (Q4 2024 vs Q4 2023) and modelled subway
revenue per entry roughly flat ($2.31 → $2.29), so revenue grew with paid ridership. With only ~13
quarters and several simultaneous changes (congestion pricing Jan 2025, OMNY
migration, enforcement), **no causal claim is supportable** from these series.
The fare-equivalent figures are not recoverable revenue — some evaders would not
ride at a price, and some would qualify for reduced fares.

---

## Q5. Alternative fare schemes: composition and revenue

All three are sketches on aggregate data (`src/farecap/schemes.py`). Each is priced
statically and with a constant fare elasticity of −0.3 (a TCRP rule of thumb, not
an MTA figure).

### A. Monthly cap — ETA's 46 rides / 30 days ($138), on top of the $35 weekly cap

| Riding pattern | Pays today (weekly cap) | Pays with monthly cap | Paid on 30-Day in 2025 |
|---|---|---|---|
| 50 rides/month | $150.00 | $138.00 | $132.00 |
| 60+ rides/month | $151.67 | $138.00 | $132.00 |

Eligible population sized from 2024 30-Day pass holders (30-Day rides ÷ rides per
pass): ~110k–190k. **Static annual cost: roughly $18–27M**, about 0.6–0.9% of
subway fare revenue (~$3.0B modelled for 2025). This is a floor on eligibility (it misses high-frequency
riders who were already on OMNY) and ignores induced trips (ETA cites ~4% ridership
gains from monthly caps elsewhere). `output/q5_monthly_cap.csv`.

**Before/after for the retired pass (now sourced):** $132.00 → $151.67 a month for a
daily rider, **+$19.67 (+14.9%)**, against a 3.4% base-fare increase.

### B. Distance-based fare — revenue-neutral bands on the 2026 O-D matrix

Straight-line distance bands, scaled so the trip-weighted average price is $3.00
(typical week of April 2026, 26.2M trips, 175,910 station pairs, `28vm-gjqr`):

| Band | Price | Share of trips |
|---|---|---|
| < 2 mi | $2.15 | 23.8% |
| 2–5 mi | $2.85 | 39.7% |
| 5–9 mi | $3.55 | 26.9% |
| > 9 mi | $4.25 | 9.7% |

Revenue 1.002× static, 0.997× with elasticity; ridership +0.8%. **63% of trips get
cheaper, but the cost moves outward:** average fare by origin borough Manhattan
−3.4%, Brooklyn +2.5%, Queens +5.6%, **Bronx +11.5%**; Rockaway stations +30%.
(`fig5_distance_fare_by_borough.png`, `output/q5_distance_fare_by_origin.csv`)
Straight-line distance understates indirect trips uniformly; Staten Island is not
in the O-D data.

### C. Off-peak discount funded by a peak fare

46.3% of paid subway entries (April 2026) fall in weekday 6–10am and 4–8pm. A 20%
off-peak discount ($2.40) with a static-revenue-neutral peak fare of **$3.70**:
revenue −0.5% and ridership +0.9% with elasticity; peak entries −6%, off-peak +7%.
10%: $2.70 / $3.35. 30%: $2.10 / $4.04 (revenue −1.1%).

### What none of these can show

Per-rider effects. Without a rider identifier there is no way to say how many
people gain or lose under any scheme — only how trips, stations and revenue move.

---

## Station geography — the confounder check the verdict needed

`dispersion_verdict` said DISPERSED at an IQR of 2.48pp (threshold 2.0). But
30-Day share of *all* rides correlates 0.54 with a station's MetroCard share —
stations where riders had already switched to OMNY mechanically show less of any
MetroCard product. Measured **within MetroCard rides**, dispersion survives (IQR
6.45pp around a 19.3% median) but the ranking changes a lot (Spearman 0.42 between
the two measures). (`fig6_station_30day_share_2024.png`,
`output/station_30day_within_metrocard_2024.csv`)

Robust to both measures: the **Queens Roosevelt Av corridor** (Elmhurst Av,
Grand Av-Newtown, 65 St, 46 St, Broadway N/W) and **southern Brooklyn N/D/Q**
(8 Av, 18 Av, Bay Pkwy, Avenue U, Kings Hwy, Neck Rd).

**Cuts against the equity framing:** among MetroCard riders, the **Bronx (14.9%)
and Staten Island (11.4%) had the lowest monthly-pass reliance**; Brooklyn 20.6%,
Queens 20.2%, Manhattan 19.5%. That is consistent with the MTA's own Title VI
finding of no disparate impact from retiring the 30-day pass (doc 186881) and with
the familiar pattern that lower-income riders can't front $132. The piece should
say this plainly.

---

## Reproduce

```bash
source venv/bin/activate
python src/collect.py all --refresh     # windows, fare classes, stations, fair fares
python -c "import sys; sys.path.insert(0,'src'); from farecap import timeseries as T; \
  [T.pull(end='2026-09-17', series=s) for s in ('subway','bus','bus_express')]"
python -c "import sys; sys.path.insert(0,'src'); from farecap import od, schemes; \
  od.stations(2026); schemes.hourly_profile(); od.pair_week(2026, 4)"
python src/analyze.py --all
python src/analyze.py --questions        # Q1–Q5 tables
python src/figures.py                   # article/lib/*.png
```
