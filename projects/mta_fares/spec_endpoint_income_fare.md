# Spec: the neighbourhood fare — priced by the lower-income end of the trip

A proposed fare change to model and write up alongside `spec_fare_proposals.md` and
`spec_golden_station.md`. Drafted 2026-09-29. The numbers below are static
back-of-envelope sizing from data already in this project. They set the design;
they are not the model.

## The proposal

Each subway station is assigned an income tier from the neighbourhood around it. A
trip is charged at the tier of **whichever end — entry or exit — is in the
lower-income neighbourhood**. Trips that start *or* end in a low-income area pay
less. Trips between two higher-income areas pay more, so the scheme is
revenue-neutral.

The pitch: it reaches residents of low-income neighbourhoods on both legs of the day,
going out and coming home, not only when they board at home.

## The first-order problem: NYC has no exit data

**Subway and bus riders tap in; nobody taps out.** The MTA does not observe where a
trip ends. Its O-D dataset (`28vm-gjqr`) *infers* the exit from each rider's next
tap-in, "scaled up" to total ridership (dataset description). So "the lower of entry
and exit" needs one of:

| Mechanism | How | Problems |
|---|---|---|
| **A. Tap-out** | Exit readers at every station, as on distance-fare systems (e.g. DC Metro, BART) | Capital cost at all 428 station complexes (the ridership data's count); fare-control redesign; crowding at exits. Out of scope to cost here, but it is the honest price of the pure version. |
| **B. Charge on entry, refund on the next tap** | Charge the entry-tier fare; when the rider's next tap reveals where they went, refund the difference if the inferred exit is in a cheaper tier | Works only for trips followed by another tap on the same card. Last trip of the day, one-way trips, and cash/single-ride users get no refund. Refunds depend on an inference the MTA itself labels an estimate. Disputes. |
| **C. Entry-only (fallback)** | Price by the entry station's tier alone | Implementable on today's system. Loses the "coming home" leg, the part that makes this proposal different from a plain station discount. |

The model should price **A** (the design as proposed), **B** (with a
refund-capture rate) and **C** (the practical baseline) side by side. Whether OMNY
can price by entry station at all is a question for the MTA; the golden-station spec
depends on it too.

## What the preliminary sizing already says

Inputs: typical week of April 2026 trips by station pair (`od.pair_week`, 26.2M trips,
175,910 pairs, no pairs dropped); station neighbourhood low/moderate-income (LMI)
share (`proposals.station_income`: HUD, ACS 2016–20, tracts within 0.5 mi);
eligibility at HUD's 51% line (289 of 428 stations).

1. **"Lower of the two ends" makes the discount the majority fare.**

   | | Share of trips |
   |---|---|
   | Origin low-income | 39.6% |
   | Destination low-income | 38.6% |
   | Both ends low-income | 17.5% |
   | **Either end low-income (discounted)** | **60.7%** |
   | Neither (full fare) | 39.3% |

2. **So the remaining 39% carry a large increase** (static, revenue-neutral):

   | Discount fare | Full fare needed | Full-fare increase |
   |---|---|---|
   | $2.75 | $3.39 | +13% |
   | **$2.50** | **$3.77** | **+26%** |
   | $2.25 | $4.16 | +39% |

   For comparison, **entry-only (mechanism C)** discounts 39.6% of trips. A $2.50
   entry discount needs a full fare of about **$3.33 (+11%)**: a smaller increase
   because fewer trips qualify.

3. **The benefit is well aimed on average — and leaks.** At $2.50 / $3.77, by
   *origin* neighbourhood:

   | Origin LMI quintile | Trips discounted | Average fare change |
   |---|---|---|
   | Q1 (least low-income) | 35% | **+11.0%** |
   | Q2 | 46% | +6.0% |
   | Q3–Q5 | 100% | **−16.7%** |

   By origin borough: Bronx −16.7%, Queens −10.2%, Brooklyn −7.0%, Manhattan +7.4%.
   But **35% of discounted trips start in a higher-income area**, discounted only
   because they *end* in a low-income one. That includes commuters to jobs in
   neighbourhoods like Harlem, the South Bronx or Sunset Park. The gap between the
   station's neighbourhood and the rider is widest exactly where the "exit" leg
   applies.

4. **A three-tier version is smoother.** Price by the cheaper end's station-income
   tercile (34% / 26% / 40% of trips): **$2.35 / $2.95 / $3.55** balances revenue
   (0.996× after rounding to nickels). The top tier still rises 18%.

5. **Gaming exposure is moderate.** 21% of full-fare trips have an end within half
   a mile of a discount station, a walk or one stop that would drop the fare by
   ~$1.27 in the binary design. Within a quarter mile, 2%. Tier boundaries through
   dense areas will draw riders to the cheaper side.

## Design parameters to decide (defaults in bold)

| Parameter | Options | Why it matters |
|---|---|---|
| Mechanism | **A modelled as designed; B and C as feasibility variants** | See above: the proposal is not implementable today without A or B |
| Tiers | Binary (≥51% LMI) · **three tiers by station-income tercile** · continuous | Binary makes a ~$1.27 cliff at the boundary; three tiers halves it |
| "Lower of" vs "average of" the two ends | **Lower of** (as proposed) · average of both ends' prices | Average cuts discounted trips and the needed full-fare rise; weaker "coming home" benefit |
| Discount size | $2.75 · **$2.50** · $2.35 (tiered) | Drives the full-fare increase (table in sizing point 2) |
| Neighbourhood measure | **HUD LMI share within 0.5 mi** · ACS median income · NYCHA proximity | LMI is published for all tracts and used by HUD; 2016–20 vintage |
| Station exclusions | **Airports, stadiums, regional hubs** (Penn, Grand Central, Port Authority, Jamaica, Howard Beach) | Riders there are not neighbourhood residents; the MTA's Title VI analysis excludes similar hubs (doc 186881) |
| Weekly cap | **Stays in dollars ($35)** · stays in rides (12) | In dollars, discounted riders need more rides to reach it; full-fare riders fewer. Price both. |
| Reduced fares, Fair Fares | **Half of the tier price** · unchanged | Stacking with Fair Fares doubles the targeting |
| Buses | **Subway only**; bus-to-subway transfer inherits the subway trip's tier | Bus stop-level O-D is not published |

## What the model must add beyond the static sizing

1. **Behaviour.** Elasticity on both sides: discounted riders ride more, and full-fare
   riders at +13–39% ride less. The full-fare side is where revenue-neutrality can
   fail. At −0.3, a +26% rise loses ~7% of those trips.
2. **Station switching.** Riders near a tier boundary moving their entry or exit to
   the cheaper side. Use station coordinates, a walk-distance threshold and a
   switching rate range. It raises cost and crowds boundary stations.
3. **Mechanism B capture rate.** The share of trips whose exit can be inferred
   from a later tap on the same card. Not public; sweep it. It sets how much of the
   "coming home" benefit reaches riders.
4. **Time of day.** Exits inferred for morning trips to work are inferred at a
   different rate than evening trips home. The O-D data has `hour_of_day`; use it.
5. **Weekly-cap interaction** via the synthetic rider population
   (`farecap.proposals`).
6. **Distribution, by origin AND by destination.** Net $/rider-trip by
   neighbourhood income, with the leakage in sizing point 3 as its own metric.
7. **Revenue-neutrality check** against the Q3 revenue model rather than a flat
   $3.00, because reduced fares, passes and capped taps change the base.

## Data

| Need | Source | Status |
|---|---|---|
| Trips by station pair and hour | O-D `28vm-gjqr` (2026), `jsu2-fbtj` (2024) | Have April 2026 week; pull more months for seasonality |
| Station neighbourhood income | `proposals.station_income` (HUD `qmcw-ur37` + Census Gazetteer) | Have |
| Station coordinates | `od.stations` | Have |
| Fare class mix by station | Hourly ridership, `collect.grouped_by_day` | Have Sept windows |
| Share of trips with an inferable exit | MTA | **Not public — ask** |
| Tap-out capital cost | MTA capital program / other systems | Not gathered |

## Deliverables

- `src/farecap/endpoint_fare.py`: tier assignment, the three mechanisms, behaviour
  and switching, distribution tables.
- `python src/analyze.py --endpoint` writing `output/e_*.csv`.
- `research/endpoint-fare.md`, confidence table first (format of
  `research/fare-proposals.md`).
- Figures: station tier map; average fare change by origin quintile for A vs C; a
  "leakage" chart of discounted trips by origin income.
- An "Option 6" section for `article/alternative_fare.md` if it holds up.

## Comparisons the write-up should make

- **Inverse distance** — the other place-based scheme: ~6% off for low-income
  origins with a modest increase elsewhere, vs ~17% off here with a 26% increase
  elsewhere.
- **Golden station** — same targeting idea, much smaller transfer.
- **Fair Fares** — targets the rider, not the neighbourhood; no leakage by design.

## Open questions for the author

1. Is entry-only (C) acceptable as the practical version, or is the "coming home"
   leg the point of the proposal?
2. Binary tiers or three?
3. How much full-fare increase is politically tolerable? That caps the discount.
4. Should higher-income-origin trips *to* low-income areas get the discount, the 35%
   leakage? If not, the rule becomes "entry-only", i.e. mechanism C.

## Caveats to carry into the write-up

- Neighbourhood income is not rider income, and the exit end is the weaker proxy:
  many people exiting in a neighbourhood work there rather than live there.
- The O-D destinations are the MTA's inferences, "scaled up", not observed exits.
- HUD LMI is from 2016–2020, pre-pandemic.
- Staten Island is not in the O-D data.
- Sizing prices every trip at $3.00. Reduced fares, passes and capped taps shift
  the base; the model uses the revenue model instead.
