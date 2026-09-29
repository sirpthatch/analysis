# Spec: the golden station

A proposed fare change to model and write up alongside `spec_fare_proposals.md`.
Drafted 2026-09-29. The preliminary numbers below are static back-of-envelope
sizing from data already cached in this project. They set the design; they are
not the model.

## The proposal

Each day, a small set of subway stations is designated **golden**. Every ride that
**starts** at a golden station that day is free (or discounted). Selection is
weighted toward lower-income neighbourhoods. The revenue given up is recovered with
a small, permanent base-fare increase, so the package is **revenue-neutral**.

The pitch: a visible, place-based benefit that reaches low-income neighbourhoods
without means-testing, paperwork or pre-registration. Every rider at the station
gets it automatically by tapping in.

## What the preliminary sizing already says

Inputs: station entries for Sept 1–16 2026 (`data/raw/station_fare_class_2026.json`,
3.68M entries/day across 426 subway complexes), subway revenue per entry $2.40
(2026-Q2, `farecap.evasion.quarterly`), station neighbourhood low/moderate-income
(LMI) share from `farecap.proposals.station_income` (HUD, ACS 2016–20).

1. **One golden station a day is too small to fund with a fare rise.** A randomly
   drawn LMI-eligible station averages ~5,100 entries a day. Making them free costs
   **~$12k a day, $4.4M a year, 0.14% of subway revenue**, which is a 0.4¢
   break-even rise. Fares move in 5¢ steps. **The design must start from the fare
   increment and buy as many golden stations as it pays for**, not the other way
   round.

2. **A 5¢ rise (1.67%) funds ~12 golden stations a day** among LMI-eligible stations
   (static; ~24 for 10¢). Each eligible station would be golden about **15 days a
   year**.

3. **The per-rider transfer is small.** HUD-eligible stations carry **40% of
   subway entries**, so their riders pay 40% of the rise themselves. Only the trip
   *starting* at the station is free, so a commuter's return trip doesn't count.

   | Rider (5¢ rise, ~12 stations/day) | Pays more per year | Saves per year | Net |
   |---|---|---|---|
   | 5-day commuter, home station eligible | $26 | $33 | **+$7** |
   | 5-day commuter, home station not eligible | $26 | $0 | **−$26** |
   | 3 trips/wk, home station eligible | $8 | $13 | **+$5** |

   This is the central weakness to test. The benefit arrives as a lottery: about 15
   free days a year, unpredictable. The cost is certain and paid by everyone,
   including low-income riders whose station never comes up.

4. **Station size is the tail risk.** Eligible stations range from a median ~3,400
   entries a day to Flushing-Main St (~43k) and Jackson Hts-Roosevelt Av (~42k).
   One draw of a large hub costs as much as ten typical stations.

5. **How selection is weighted matters less than expected for cost** (one station a
   day, static):

   | Selection rule | Eligible | Cost / yr | Avg LMI share of selected |
   |---|---|---|---|
   | Uniform, all stations | 426 | $7.6M | 56% |
   | Uniform, HUD-eligible (≥51% LMI) | 287 | $4.4M | 68% |
   | Probability ∝ LMI share | 426 | $6.1M | 63% |
   | Probability ∝ LMI share³ | 426 | $4.8M | 70% |
   | Probability ∝ LMI share ÷ station entries (equal dollars per draw) | 426 | $2.6M | 66% |

   Low-income-area stations are smaller on average, so biasing toward them makes
   each draw *cheaper*, not dearer.

## Design parameters to decide (defaults in bold)

| Parameter | Options | Why it matters |
|---|---|---|
| Benefit | **Free** · 50% off · first ride only | 50% off doubles the stations for the same money; a free day is simpler to communicate |
| Stations per day | **Set by the fare increment** (~12 per 5¢) · fixed N | See sizing point 1 |
| Eligibility | **HUD LMI ≥ 51%** (the CDBG "CD-eligible" line) · top income quintile · weighted over all stations | 51% is an existing, defensible federal threshold, available for all 2,327 NYC tracts |
| Weighting among eligible | **Uniform** · ∝ LMI share · equal dollars per draw | Equal-dollars gives small stations more draws; uniform is easier to explain |
| Exclusions | **Airports (Sutphin–JFK, Howard Beach), stadium/event stations (Mets-Willets Point, 161 St-Yankee Stadium) on event days, regional hubs** | Their riders are mostly not local residents; the MTA's own Title VI analysis excludes similar hubs (doc 186881) |
| Rotation rules | **No repeat within 14 days; at least one station per borough per day** | Spreads benefit; stops a lucky station winning twice in a week |
| Announcement | **Day before, via app, signage and press** · morning-of · published monthly calendar | Earlier notice → more planned trips and more diversion (people travelling to the station) |
| Hours | **All day** · off-peak only | Off-peak only cuts cost and crowding risk, and targets non-commute trips |
| Modes | **Subway only** · include local buses serving the area | Bus inclusion reaches more low-income riders (bus evasion ~48% complicates the accounting) |
| Fare classes | **All** (full, reduced, Fair Fares, students) | Reduced-fare riders are cheaper to include and more likely low-income |
| Weekly cap | **Golden rides don't count toward the cap**; cap rises in step with the fare | Otherwise a golden day pushes heavy riders past the cap sooner, costing extra |
| Fare rise | **+5¢ base (and cap to $35.60 or $35.50)** · +10¢ | Must be a 5¢ step; cap adjustment rule needs a decision |

## What the model must add beyond the static sizing

1. **Diversion.** Riders who would have boarded at a nearby paid station walk or
   bus to the golden one. This raises cost above the static number and may crowd
   the station. Estimate the neighbouring-station pool from station coordinates
   (within ~0.75 mi) and put a range on the switching share.
2. **Induced trips.** Free rides create some new trips: no revenue lost, but
   crowding. Use an elasticity range; a free fare is outside the range where
   constant elasticity holds, so treat as a scenario.
3. **Fare-rise response.** A 5¢ rise at −0.3 elasticity loses ~0.5% of trips.
   Include it so the neutrality holds after behaviour, not only before it.
4. **Weekly-cap interaction.** Use the synthetic rider population
   (`farecap.proposals`) to price the cap rule. Heavy riders already at the cap
   gain nothing from a golden day.
5. **Day-of-week and seasonality.** Draw days from the real calendar, using daily
   station entries by weekday. Weekends are cheaper; school days change the mix.
6. **Variance, not just the mean.** Simulate a year of draws: the distribution of
   daily cost (hub draws), and the distribution of free days per station and per
   borough. Neutrality over a year with a bad-luck budget month is still a
   finance problem.
7. **Distribution.** Net gain/loss per rider by home-station income quintile and
   borough, extending the table in sizing point 3. This is the headline metric: does
   the package move money toward low-income riders, and by how much?

## Data

| Need | Source | Status |
|---|---|---|
| Station daily entries by fare class | Hourly ridership `5wq4-mkjj`, one query per day (`collect.grouped_by_day`) | Have Sept 2026 window; pull a full year |
| Neighbourhood income per station | `proposals.station_income` (HUD LMI `qmcw-ur37` + Census tract centroids) | Have |
| Station coordinates | `od.stations` | Have |
| Where golden-station riders go (trip length, destinations) | O-D matrix `28vm-gjqr` | Have April 2026 |
| Revenue per entry | `farecap.revenue` / `evasion.quarterly` | Have |
| Event calendars (Citi Field, Yankee Stadium) | Team schedules | Needed for exclusions |
| Rider-level frequency | Not public; synthetic population | Same limit as `research/fare-proposals.md` |
| Whether OMNY can price by entry station and date | MTA | **Unknown — ask**; the concept depends on it |

## Deliverables

- `src/farecap/golden.py`: eligibility, weighted draws, a year-long simulation with
  diversion, induced trips and fare-rise response, returning cost, fare-rise
  neutrality and distribution tables.
- `python src/analyze.py --golden` writing `output/g_*.csv`.
- `research/golden-station.md`: findings, in the format of
  `research/fare-proposals.md` (confidence table first).
- Figures: map of eligible stations sized by expected golden days per year; net
  $/rider by home-station income quintile; a year of simulated daily cost.
- A section for `article/alternative_fare.md` ("Option 5") if the results
  warrant it.

## Comparisons the write-up should make

- **Against Fair Fares**, which targets need directly: what does the same ~$54M a
  year (5¢ × subway entries) buy if added to Fair Fares instead?
- **Against the monthly cap** ($12–20M/yr): cheaper, and targets frequency rather
  than place.
- **Against inverse distance**: both are place-based proxies for income.

## Open questions for the author

1. Free or 50% off? (Changes stations/day by 2×.)
2. Subway only, or include the local buses serving golden neighbourhoods?
3. Is 51% LMI the right line, or should it be the lowest-income fifth of stations?
4. Should golden days be announced ahead (a calendar people can plan around) or be
   a surprise (less diversion, less planning value)?
5. How to frame the weakness in sizing point 3: as a finding against the idea, or
   as a design problem that off-peak only or bus inclusion might fix?

## Caveats to carry into the write-up

- Neighbourhood income is not rider income; people boarding at a station are
  mostly, not only, local residents. The pattern flips by time of day: morning
  entries are residents, evening entries are often visitors heading home.
- The HUD data is from 2016–2020 (pre-pandemic).
- Sizing uses a sixteen-day September window that includes Labor Day and the first
  school days; the model should use a full year.
- The ~$54M a year for a 5¢ rise (1.67% of ~$3.23B subway revenue) is the subway-
  only figure; if buses get the rise too, the pot is larger.
