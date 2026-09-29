# Fare proposals — modelled impact

Models for `spec_fare_proposals.md`, 2026-09-29. Code: `src/farecap/proposals.py`
(and `schemes.distance_fare` for proposal 1). Tables in `output/p1_*`, `p2_*`,
`p34_*`; figures `article/lib/fig7`, `fig8`. The fourth proposal in the spec is cut
off ("Daily subway passes (pay once during the )"); it is modelled as a **daily
cap** — pay up to $X in a day, then ride free — which is the automatic form of a
day pass and the better deal for riders of the two.

## How much to trust each result

| Proposal | Rests on | Confidence |
|---|---|---|
| 1. Inverse distance | Observed trip lengths (MTA O-D) + tract income (HUD) | **Good** for direction and size; neighbourhood, not rider, income |
| 2. Reserve / float | Arithmetic; spend per rider is synthetic; E-ZPass rules sourced | **Moderate** for interest; breakage is a placeholder |
| 3. Monthly / annual cap | Synthetic rider population, calibrated | **Moderate**; cross-checks against an independent estimate |
| 4. Daily cap | Same population + one unobserved quantity | **Low** — the answer swings from $0 to ~$570M on it |

**Why a synthetic population.** No MTA open dataset has a rider identifier, and the
MTA has not published unique riders, trips per rider, or the share of riders
reaching the weekly cap (searched; not found). So rider frequency is generated:
each full-fare rider has a stable weekly rate from a gamma distribution, weeks vary
around it, and round trips are spread over distinct days. It is calibrated to
**observed** weekly full-fare paid trips (24.1M, Mar–Jun 2026) and to the Sept
2024 unlimited-pass share of full-fare-plus-pass trips (**18.1%**), taken as the
share of trips in 12+-trip weeks. Mean trips per rider cannot be calibrated and is
swept: 3, 4.5, 6 a week (8.0M, 5.4M, 4.0M riders).
Out-of-sample check: the model implies **3.0–4.2% of taps are free under today's
weekly cap**, which matches the 4% assumption that let the Q3 revenue model
reproduce the MTA's audited farebox. Consistent, not proof.

## 1. Inverse distance — shorter trips pay more, longer trips subsidised

**Data-driven.** Priced on the MTA's O-D estimate (typical week of April 2026,
26.2M trips, `28vm-gjqr`), revenue-neutral before behaviour change, same four
straight-line bands as the distance scheme with the relative prices reversed.

| Band | Distance fare | **Inverse distance** | Share of trips |
|---|---|---|---|
| < 2 mi | $2.15 | **$3.75** | 23.8% |
| 2–5 mi | $2.85 | **$3.15** | 39.7% |
| 5–9 mi | $3.55 | **$2.50** | 26.9% |
| > 9 mi | $4.25 | **$1.90** | 9.7% |

Revenue 0.999× static; 0.995× at a uniform −0.3 elasticity. **63% of trips pay
more; 37% pay less.**

### Does the premise hold? Mostly, yes — at the neighbourhood level.

The spec's reasoning is that long-distance riders are more likely to need a lower
fare. Tested by attaching to every station the HUD low/moderate-income share
(households below 80% of area median income) of census tracts within 0.5 mile
(NYC Open Data `qmcw-ur37`, ACS 2016–2020; tract centroids from the 2023 Census
Gazetteer; all 2,327 tracts matched, every station has ≥1 tract):

- Stations in lower-income neighbourhoods originate longer trips: Spearman 0.50
  between the low/mod share and trip-weighted mean distance.
- Least low-income quintile of origins: 3.9 mi average. Three most low-income
  quintiles: 5.4–5.5 mi.

| Origin neighbourhood, low/mod-income quintile | Low/mod share | Mean trip | Distance fare | **Inverse distance** |
|---|---|---|---|---|
| Q1 (least low-income) | 26% | 3.9 mi | −4.1% | **+3.7%** |
| Q2 | 43% | 4.1 mi | −2.1% | **+2.0%** |
| Q3 | 61% | 5.4 mi | +6.4% | **−5.6%** |
| Q4 | 70% | 5.5 mi | +7.3% | **−6.4%** |
| Q5 (most low-income) | 79% | 5.5 mi | +6.8% | **−6.0%** |

By origin borough: Bronx −10.2%, Queens −4.9%, Brooklyn −2.0%, Manhattan +3.1%.
Rockaway stations about −27%; the dearest origins are short-hop Manhattan and LIC
stations (+9–11%). (`fig7_distance_schemes_by_income.png`,
`output/p1_inverse_distance_by_origin.csv`)

### But it is blunt targeting

- **Half the trips from the lowest-income neighbourhoods still pay more.** In the
  most low-income quintile of origins, 48% of trips are local enough to land in a
  dearer band. In the least low-income quintile, 72% do. The average helps; the
  local trip — to school, a clinic, the shops — gets the price increase.
- **Short trips are the most fare-sensitive**, because walking and cycling
  substitute for them. With a uniform −0.3 elasticity revenue holds (−0.5%). If
  sub-2-mile trips respond at −0.6, revenue falls 2.3% and short-trip ridership
  13%; at −1.0, revenue −4.5% and short trips −20%. The scheme is only
  revenue-neutral if short-trip riders keep paying $3.75.
- Neighbourhood income is not rider income. The measure describes who lives near
  the station a trip starts from, which is the closest thing the data allows.
- A more targeted instrument for the same equity goal already exists: Fair Fares
  (half fare, means-tested). Inverse distance reaches roughly the same
  neighbourhoods by a much noisier route.

## 2. Dollar-based account with a reserve (E-ZPass model)

**Mechanics copied from E-ZPass NY** (NYS Thruway T&C, form TA-W68167A, saved in
`research/docs/`): the prepaid amount is about 30 days of charges ($25 minimum on
the consumer plan), replenished by that amount when the balance falls to a
replenishment point, and "No interest will be paid on balances in your Account."
So the MTA would hold the float and keep the interest.

Rider spending comes from the synthetic population (see §3); interest at the
3-month T-bill rate, **4.24%** (FRED DGS3MO, 2026-09-25). The replenishment point
is not published in those terms, so it is swept (10%, 25%, 50% of the prepaid
amount).

| | Central (4.5 trips/wk, 25% point, 4.24%) | Range across all assumptions |
|---|---|---|
| Riders with accounts | 5.4M | 4.0–8.0M |
| Median prepaid amount a rider must front | **$49** | $26–73 |
| Riders at the $25 minimum | 22% | 3–50% |
| Total balance the MTA holds | **$234M** | $182–358M |
| **Interest earned, per year** | **$9.9M** | $5.5–17.9M |
| Breakage, per year (illustrative only) | $23M | $18–36M |

- **The float is small money** — roughly 0.25% of fare revenue. But it is about the
  same size as the cost of ETA's monthly cap in §3 ($12–20M a year), so the two
  could be paired: a reserve account that funds a monthly cap.
- **Breakage is the bigger lever and the least knowable.** The line above assumes
  20% of accounts go dormant each year and half their balance is never
  reclaimed; both are placeholders, not estimates. For scale, Gothamist (2014,
  citing the New York Times) reported the MTA made ~$200M from unused MetroCard
  balances in the decade to 2010, about $20M a year. That was at a time when
  vending-machine amounts left odd remainders on cards (I Quant NY, 2014).
- **Who pays for it.** The float is interest riders forgo, and the reserve is a
  cash-flow requirement: a daily rider would have to front ~$150 at a time. It is
  also a step backwards from today's tap-to-pay, which needs no account and no
  balance. Unbanked and cash-paying riders would need a retail top-up route, as
  E-ZPass offers at its service centres.
- Today's weekly cap is post-paid: nobody has to hold money with the MTA to get
  it. Moving to a reserve model would make the MTA's cash position better and
  every rider's slightly worse, for ~$10M a year.

## 3. Monthly or annual cap (on top of the $35 weekly cap)

Static annual change in full-fare revenue (status quo ≈ $3.6B), range across 3–6
trips per rider per week (`output/p34_cap_grid.csv`, `fig8_cap_scenarios.png`):

| Cap | Revenue change / yr | Riders who benefit | Avg saving per beneficiary / yr |
|---|---|---|---|
| **Monthly $138** (ETA: 46 rides / 30 days) | **−$12M to −$20M** (−0.3 to −0.5%) | 5–11% | $27–49 |
| Monthly $132 (the retired 30-Day price) | −$24M to −$34M | 6–15% | $39–67 |
| Monthly $120 (40 rides) | −$67M to −$78M | 9–25% | $67–106 |
| Annual $1,500 (500 rides) | −$31M to −$50M | 4–6% | $126–172 |
| Annual $1,656 (12 × $138) | −$6M to −$15M | 2–3% | $68–97 |
| Monthly $138 + annual $1,500 | −$33M to −$50M | 5–11% | $74–124 |

- **ETA's monthly cap is cheap.** $12–20M a year, under half a percent of full-fare
  revenue. An independent method — 2024 30-Day pass holders × the $13.67 gap
  (research/additional-questions.md, Q5) — gave $18–27M. Two methods, overlapping
  ranges.
- **Restoring the old $132 price as a cap** roughly doubles the cost ($24–34M) and
  reaches more riders.
- **An annual cap priced at twelve monthly caps does almost nothing** ($6–15M):
  only riders who ride heavily every month get there, and those riders already
  gain most from the monthly cap. And this model *overstates* annual-cap reach,
  because it holds each rider's rate constant all year (no job changes, moves,
  summers away). An annual cap only bites if priced well below 12 months, e.g.
  $1,500 ($31–50M) — at which point it is a loyalty discount, not a cap.
- Average savings per beneficiary look small ($27–49 a year for the $138 cap)
  because many beneficiaries only hit the cap in some months; a rider who caps
  every week saves the full $13.67 a month, $164 a year.
- Static: no induced trips. ETA cites ~4% ridership gains from monthly caps
  elsewhere (Streetsblog, 2026-02-18); some of the cost would come back as fares
  from new trips, and some would not.

## 4. Daily pass, modelled as a daily cap (with the $35 weekly cap kept)

| Daily cap | No same-day chaining (1–2% of rider-days 3+ trips) | 10% chaining (~7% of days 3+) | 25% chaining (~14–16% of days 3+) |
|---|---|---|---|
| $6 (3rd ride free) | **$0** | −$175M to −$221M | −$456M to −$571M |
| $7.50 | **$0** | −$127M to −$163M | −$347M to −$445M |
| $9 (4th ride free) | **$0** | −$80M to −$107M | −$240M to −$320M |

- **The whole answer is one unobserved number**: how often riders take three or
  more trips in a day while riding fewer than 12 in the week. Nothing in MTA open
  data measures it. The model's riders make round trips on distinct days, and
  "chaining" puts an extra round trip onto one day — the errand after work.
- **Without chaining, a daily cap costs nothing**, because the only 3+-trip days
  belong to riders over 12 trips a week who already hit the weekly cap. A daily
  cap is a benefit for *occasional heavy days* — visitors, weekend errands,
  multi-stop days — not for commuters.
- **With even modest chaining it is expensive**, $80–220M a year, because it
  rewards the most common rider (a few trips a week) rather than the most
  frequent one. That is the opposite targeting from the monthly cap.
- **Visitors are the natural market** and are not in this model (it covers
  resident full-fare riders). An opt-in *paid* day pass (buy $X, ride all day)
  rather than an automatic cap would cost less and could earn breakage from
  buyers who under-ride — but it would recreate the MetroCard problem of riders
  having to guess in advance.
- Before this proposal goes further, the number to get from the MTA is the share
  of OMNY card-days with three or more taps. OMNY has it; the open data does not.

## What would sharpen all of this

One MTA table — the distribution of weekly and daily taps per OMNY card, even
binned — would replace the synthetic population with the real one and collapse
every range in §2–4. It is worth a public-records request.

## 2b. Card-processing fees, and what a reserve would save (added 2026-09-29)

**What the MTA spends is not public.** The MTA cites non-disclosure agreements and
does not publish its card-processing costs (Husock, *New York Post*, 2025-04-02,
reprinted by AEI). The only figure on record is CFO Jai Patel's disclosure at the
February 2025 board meeting: **$1M in credit- and debit-card fees within the $11M
first-month operating cost of congestion pricing.** The op-ed's system-wide
"$400 million a year" applies congestion pricing's ratio of fees to operating costs
to the whole MTA. It is not used here: toll charges and subway fares differ in size
and payment mix.

**How OMNY settles is also not public**: per tap, or batched per day or week. The
OMNY FAQ line "each tap will be charged as a full fare" is about several riders
sharing one card, not settlement.

**Sizing** (`proposals.card_fee_scenarios`, `output/p2_card_fee_scenarios.csv`).
Full-fare riders, ~$3.6B/yr, *all assumed to pay by bank card*. Scale down by the
real bank-card share, which is not public. Fee schedules are illustrative; the
MTA's negotiated rates are unknown.

| Settlement | Card charges / yr | Avg charge | Regulated debit (21¢ + 0.05% + 1¢) | Credit, small-ticket style (1.65% + 4¢) | Credit, standard style (2.0% + 10¢) |
|---|---|---|---|---|---|
| Per tap | 1,204M | $2.99 | $267M (7.4%) | $107M (3.0%) | $192M (5.3%) |
| Daily batch | 663M | $5.43 | $148M | $86M | $138M |
| Weekly batch | 253M | $14.22 | $57M | $69M | $97M |
| **Reserve (E-ZPass style)** | **59M** | **$60.74** | **$15M** | **$62M** | **$78M** |

- **A reserve cuts transactions by ~95% versus per-tap**, so it removes almost all
  *fixed* per-transaction fees. It does nothing to *percentage* fees: the same
  dollars still flow through cards.
- **So the saving depends on the card mix and on how OMNY settles today.** From
  per-tap settlement: $46M (credit-heavy, small-ticket rates) to $252M (debit-heavy).
  From weekly batching, a reserve saves only $8–43M.
- **Weekly batching gets most of the way without asking riders to prepay.** Versus
  per-tap, batching into weekly charges saves $38–209M, 82–83% of what a reserve
  saves on credit schedules and 83% on debit. It needs no account, no float and
  no up-front money from riders. That makes a reserve a weak way to cut card fees,
  unless OMNY already batches and the remaining fees are mostly fixed.
- Rider counts barely matter here: transactions track taps (range across 3–6
  trips/rider/week: 1,203–1,215M per-tap charges).
- Not included: reduced-fare and bus-only riders' cards; retail top-ups by cash
  (retailer commissions instead of card fees); chargebacks; the accounts,
  customer service and dispute costs a reserve system adds.
