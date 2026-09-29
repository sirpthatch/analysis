# Four Ways to Price a Ride

MTA fares, fair fares in transit, a challenge to the public, and a request back to the MTA

On September 23rd, MTA chair Janno Lieber [said](https://nyc.streetsblog.org/2026/09/23/mta-chair-open-to-new-fare-structure-ahead-of-2027-fare-hike) that he is open to changes in the MTA fare structure ahead of a planned 2027 fare increase, particularly on different ways to organize caps and with a focus on equity for riders. That sounds like a call to the public, so let's explore some options for how the MTA could reorganize its fare structure!

I thought of four ways to approach subway/bus fares in NYC:

* Resurrect the monthly cap
* Introduce an *inverse* distance charge
* Make riders prepay, E-ZPass style
* Create roving "golden" stations with free rides

## Where we are today

On January 4th, 2026, the base fare for subway rides increased from $2.90 to $3.00, and the MTA retired the existing 7-Day, 30-Day, and Express Bus Plus MetroCards and replaced them with the automatic weekly fare cap. Whereas before you had to plan your usage and pre-pay, now the MTA tracks it automatically and you get the benefit if you take more than 12 rides in a week.

This all rolls up to the city booking about $3.85B in fare revenue, or about $320 million a month. The MTA has the data to do final accounting on that, but we can recreate most of it from the hourly ridership data on Open Data ([link](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj), [link](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s), [link](https://data.ny.gov/Transportation/MTA-Bus-Hourly-Ridership-Beginning-2025/gxb3-akrn), [link](https://data.ny.gov/Transportation/MTA-Bus-Hourly-Ridership-2020-2024/kv7t-n8in)... they could have made that easier...).

![fig2_revenue_model_vs_actual](/Users/thatcher/dev/analysis/projects/mta_fares/article/lib/fig2_revenue_model_vs_actual.png)

Another factor in modeling fares is to understand that not everyone pays - fare evasion is a reality, and a rampant problem, particularly on buses, where almost half of the riders do not pay.

![fig4_fare_evasion](/Users/thatcher/dev/analysis/projects/mta_fares/article/lib/fig4_fare_evasion.png)

That is a big loss to the business - in aggregate in 2025, using average ride costs, it was about $341M for subways and $642M for buses, or just shy of **$1B overall**.

## Proposal 1: Resurrect the Monthly Cap

You used to be able to buy a monthly MetroCard that would allow for unlimited rides during the month for $132. This was great for regular commuters, and when we lost it monthly pass riders were hit hard, with a net increase of up to 14.9% in their fare. Effective Transit Alliance has a proposal for a new monthly cap that would make rides free after 46 rides ($138) in 30 days. For someone commuting twice a day, 5 days a week, or an average of 20.8 working days a month (factoring in holidays), you would only get the benefit if you used the subway or bus 4-5 additional times during the month.

The total cost of this proposal is about **$12-20M** per year (using a simulated rider mix) - i.e. the monthly cap subsidy would bring down the total MTA revenue by this much. To make it revenue neutral, normal ticket prices would have to rise by 0.9 to 1.6 cents. More realistically, the cost of each ride would have to increase 5 cents (the smallest realistic step), which after modeling for the price elasticity of ridership would probably rake in an additional $45M per year, or net a **positive $25-33M** for the MTA.

Historically here is how the monthly pass was used around the city:

![fig6_station_30day_share_2024](/Users/thatcher/dev/analysis/projects/mta_fares/article/lib/fig6_station_30day_share_2024.png)

### Proposal Score Card

**Change**: *Monthly cap of $138*, and *increase fares to $3.05*

**Revenue impact**: *Increase* MTA revenue by *$25-33M*

**Verdict**: Mild but workable, revenue positive and gives NY'ers something we used to have back

## Proposal 2: Charge More for Short Trips

Many other cities structure their public transit fares based on distance - longer distance rides cost more. This makes intuitive sense; longer rides utilize the infrastructure more and should pay more to support it. This runs into issues with equity though - generally speaking, lower income neighborhoods commute farther than higher income neighborhoods.

But what if we reversed it? Charged more for short distances, and subsidized longer distances? One revenue neutral model of fares based on trip distances is **$3.75** under 2 miles, **$3.15** for 2–5 miles, **$2.50** for 5–9 miles, **$1.90** over 9 miles. This would result in the lowest income neighborhoods paying about 6% less to commute.

![fig7_distance_schemes_by_income](/Users/thatcher/dev/analysis/projects/mta_fares/article/lib/fig7_distance_schemes_by_income.png)

While that is good, the downside of this proposal is that it may still end up penalizing the lower income neighborhoods, as they would pay the highest prices for local trips within their area. The other downside is that you would have to now also track exits from the bus or subway.

### Proposal Score Card

**Change**: Fares based on inverse distance, **$3.75** under 2 miles, **$3.15** for 2–5 miles, **$2.50** for 5–9 miles, **$1.90** over 9 miles

**Revenue impact**: Neutral to revenue

**Verdict**: Bad, penalizes local trips regardless of neighborhood income and increased complexity from needing to track transit exits.

## Proposal 3: Prepayment

We could take inspiration from systems like E-ZPass and ask riders to pre-fund their accounts with a certain reserve amount that gets drawn down as they take rides. This could be done in conjunction with other approaches (like caps); the only difference is that instead of each transaction being charged individually, there would be less frequent and bigger "funding" transactions that would fill up the reserve, and each trip would debit from that.

There are two benefits from this approach. First, the MTA would be able to administer the account and earn a float on it. At the current 4.24% rate on T-bills and a model of the usage based on the data we have, it looks like the MTA could earn an estimated risk free **$10M a year**. Second, by requiring prefunding (and potentially letting that expire after a certain period) the city could go back to disproportionately reaping tolls from tourists. In the decade before 2010, when we had physical metrocards that carried value, the "breakage" or unused leftover amount on cards came to around **$20M** a year. So it is possible that this could net out an **additional $30M for the MTA** with Metrocard-era breakage.

There is a potential third benefit - saving on credit card fees. It is unknown exactly how much the MTA pays in credit card fees, but if they are not batching them today, getting good commercial terms, and most people are using credit cards it could be upwards of another **$100M saved**. This is unknowable though, as the MTA does not disclose its fees with the credit processing companies.

The big downside of this approach is that it is not particularly equitable. Not everyone can afford to let $50 sit in reserve with the MTA, and the current system lets people pay exactly when they are using the service. But perhaps that could be remediated with a waiver, similar to the current [Fair Fares policy](https://www.nyc.gov/site/fairfares/index.page).

### Proposal Score Card

**Change**: Require riders to maintain around $50 prefunded in their account that riders are debited from

**Revenue impact**: *Increase* MTA revenue by *$30*M (excluding any credit card processing benefit)

**Verdict**: Good, but would require some additional policy to ensure people who cannot afford the prepayment are not squeezed out here.

## Proposal 4: Golden Stations

This is the most fun one. Every day the MTA could designate certain stations as "Golden Stations", and either subsidize or make completely free rides that originate from those stations on those days.

To prioritize equity, the selection of "Golden Stations" could be biased towards stations that are in or near communities serving the lowest income riders. And it is likely that a station being selected would draw additional traffic to it during the day (e.g. people from nearby stations walking an extra stop to get the free ride), hypothetically creating some spillover effect to local businesses (e.g. maybe some market discovery from churn on people's daily patterns).

Assuming some price sensitivity (-0.3 elasticity), each 5 cent increase in subway and bus fare yields **$45M** a year for the MTA. Creating 5 golden stations a day that are randomly selected from lower income neighborhoods would cost an estimated **$22.2M** per year. So net, an increase with the "Golden Station" lottery would net the MTA **$22.8M** a year.

![fig9_golden_eligible_stations](/Users/thatcher/dev/analysis/projects/mta_fares/article/lib/fig9_golden_eligible_stations.png)

The real benefit of this proposal is to take advantage of [prospect theory's](https://en.wikipedia.org/wiki/Prospect_theory) **possibility effect**. That effect suggests that you would be able to increment subway fares more if people internalized the idea that there was a chance (even if it was remote) that their rides were going to be free, even if they are worse off because of the policy and rationally should not accept the price. Applied here, it suggests that people might prefer a higher rise - say an additional 10 cents instead of 5 - if that came with the "Golden Station" lottery. Worth testing that to be sure.

### Proposal Score Card

**Change**: Increase fares and each day designate 5 random stations selected from lower income neighborhoods to be free for that day.

**Revenue impact**: *Increase* MTA revenue by *$22.8M* per 5 cent increase

**Verdict**: Fun. It would need some good audience testing to ensure the possibility effect applied here.

## Parting Thoughts

|                                        | What changes                                                 | Revenue effect                                               | With a 5¢ fare rise                 | Who comes out ahead                                          |
| -------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------- | ------------------------------------------------------------ |
| **1. Monthly cap**                     | Rides free after $138 (46 rides) in 30 days, on top of today's $35 weekly cap | **−$12M to −$20M**                                           | **+$25M to +$33M**                  | 5–11% of full-fare riders, the most frequent; up to $13.67/month for someone who caps every week |
| **2. Inverse distance**                | $3.75 under 2 mi · $3.15 for 2–5 · $2.50 for 5–9 · $1.90 over 9 | **~$0 by design**; −$15M after rider response; **−$73M to −$145M** if short-trip riders switch to walking or biking | Not needed (designed to be neutral) | Commutes from lower-income neighborhoods ~6% cheaper on average |
| **3. Prepaid reserve** (E-ZPass style) | Riders keep ~a month of fares on account (median ~$49; daily riders ~$150) | **+$10M** interest ($5–18M); unused-balance "breakage" **+$20M**; unknown additional card-fee savings | Not modeled                         | The MTA; riders lose the interest and must front the money   |
| **4. Golden stations**                 | 5 stations a day, drawn at random from 287 lower-income stations, ride free that day | **−$22M** (excludes riders traveling to the golden station)  | **+$22.8M**                         | Whoever boards at a golden station that day (each eligible station ~6 days a year); a 5-day commuter from an eligible station saves ~$14/yr but pays ~$26 more, **net ≈ −$12** |

It is fun to think about how to reorient NYC metro fares. Of the proposals discussed here, my recommendation would be to do #1 (Resurrect the Monthly Cap) with some version of #3 (Prepayment). These could be stacked together to allow for the re-introduction of the monthly cap while still being revenue neutral to positive for the MTA.

That said, if the MTA is serious about wanting public creativity engaged on how to price fares to be more equitable they should release a dataset that gives more than just hourly or daily riders per station - in particular, it would be useful to know how many taps per individual anonymized OMNY card there were per day, week, year, etc. at each station. As is, the model underlying this analysis is reconstructing a simulated population of traveling NY'ers - it would be nice to have something closer to the real thing!



## Further Reading

### The news and the policy

- [MTA Chair Open To New Fare Structure Ahead Of 2027 Fare Hike](https://nyc.streetsblog.org/2026/09/23/mta-chair-open-to-new-fare-structure-ahead-of-2027-fare-hike) — Streetsblog NYC, Sept 23, 2026. The hook for this piece.
- [Advocates to MTA: More Fare Caps Will Be Fairer For All](https://nyc.streetsblog.org/2026/02/18/advocates-to-mta-more-fare-caps-will-be-fairer-for-all) — Streetsblog NYC, Feb 18, 2026. Effective Transit Alliance's 46-ride monthly cap proposal.
- [Monthly Fare-Capping is the Ticket to More Equitable Fare Policy in NYC](https://transitcenter.org/monthly-fare-capping-is-the-ticket-to-fare-equity/) — TransitCenter's case for a monthly cap.
- [2026 Fare Change Materials](https://www.mta.info/document/186881) — MTA Board documents for the January 2026 increase, including the Title VI equity analysis of retiring the 30-Day MetroCard.
- [New Fare Information, Effective August 20, 2023](https://www.mta.info/document/118601) — the MTA fare sheet with the last 30-Day Unlimited price ($132).
- [Changes to MTA fares and tolls](https://www.mta.info/fares-tolls/2025-changes) — the MTA's summary of the fare cap and MetroCard retirement.
- [OMNY weekly fare cap](https://omny.info/fares) — how today's cap works.
- [Fair Fares NYC](https://www.nyc.gov/site/fairfares/index.page) — the city's half-price fare program for low-income New Yorkers.
- [Fare capping](https://en.wikipedia.org/wiki/Fare_capping) — how other cities structure daily, weekly and monthly caps.

### Fare evasion and where the money goes

- [Fare evasion cost MTA $1 billion in 2024, report says](https://ny1.com/nyc/all-boroughs/news/2025/09/11/fare-evasion-cost-mta--1-billion-in-2024-report-says) — NY1, Sept 11, 2025, on the Citizens Budget Commission's report ([PDF](https://cbcny.org/sites/default/files/media/files/CBCREPORT_Fare-Evasion_09112025.pdf)).
- [Inside the secret fees grabbing millions a month from our MTA fares](https://www.aei.org/commentary/inside-the-secret-fees-grabbing-millions-a-month-from-our-mta-fares/) — Howard Husock, originally in the New York Post, Apr 2, 2025. On the card fees the MTA doesn't disclose.
- [The MTA Is Making Millions Off Your Old MetroCards](https://gothamist.com/news/the-mta-is-making-millions-off-your-old-metrocards) — Gothamist, Jan 17, 2014. Unused MetroCard balances.
- [Why You Should Put $19.05 on Your MetroCard to Outsmart the MTA](https://gizmodo.com/why-you-should-put-19-05-on-your-metrocard-to-outsmart-1632531219) — Ben Wellington (I Quant NY), Sept 10, 2014. How vending amounts left money on cards.
- [Terms and Conditions of the Pre-Paid Commercial Charge Account Program](https://thruway.ny.gov/sites/default/files/2025-07/ta-w68167a.pdf) — NYS Thruway Authority (E-ZPass). How a prepaid reserve works in practice: about a month of charges held on account, no interest paid to the account holder.

### The data behind this piece

All MTA datasets are published on New York State's open data portal; the income data is on NYC Open Data.

| Dataset | What it was used for |
|---|---|
| [MTA Subway Hourly Ridership: Beginning 2025](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj) | Rides by station, hour and fare type; revenue model; 2026 station traffic |
| [MTA Subway Hourly Ridership: 2020–2024](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s) | Monthly-pass use by station in 2024; the three-year decline of the 30-Day |
| [MTA Bus Hourly Ridership: Beginning 2025](https://data.ny.gov/Transportation/MTA-Bus-Hourly-Ridership-Beginning-2025/gxb3-akrn) | Bus revenue model |
| [MTA Bus Hourly Ridership: 2020–2024](https://data.ny.gov/Transportation/MTA-Bus-Hourly-Ridership-2020-2024/kv7t-n8in) | Bus revenue model |
| [MTA Subway Origin-Destination Ridership Estimate: Beginning 2026](https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-B/28vm-gjqr) | Trip distances for the inverse-distance fare |
| [MTA Subway Origin-Destination Ridership Estimate: 2024](https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-2/jsu2-fbtj) | The 2024 edition of the same estimates (not used in the figures here) |
| [MTA NYCT Subway Fare Evasion: Beginning 2018](https://data.ny.gov/Transportation/MTA-NYCT-Subway-Fare-Evasion-Beginning-2018/6kj3-ijvb) | Subway evasion rates |
| [MTA Bus Fare Evasion: Beginning 2019](https://data.ny.gov/Transportation/MTA-Bus-Fare-Evasion-Beginning-2019/uv5h-dfhp) | Bus evasion rates |
| [MTA Statement of Operations: Beginning 2019](https://data.ny.gov/Transportation/MTA-Statement-of-Operations-Beginning-2019/yg77-3tkj) | The MTA's actual farebox revenue, to check the model |
| [MTA Daily Ridership and Traffic: Beginning 2020](https://data.ny.gov/Transportation/MTA-Daily-Ridership-and-Traffic-Beginning-2020/sayj-mze2) | Access-A-Ride trips; official ridership totals |
| [Community Development Block Grant (CDBG) Eligibility by Census Tract](https://data.cityofnewyork.us/City-Government/Community-Development-Block-Grant-CDBG-Eligibility/qmcw-ur37) | Neighborhood income around each station (HUD low/moderate-income share) |
| [Census 2023 Gazetteer, census tracts (New York)](https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_gaz_tracts_36.txt) | Tract locations, to match income to stations |
| [Fair Fares Enrollees](https://data.cityofnewyork.us/Social-Services/Fair-Fares-Enrollees/3tw8-6si8) | Monthly Fair Fares enrollment |
| [3-Month Treasury Bill Rate (DGS3MO)](https://fred.stlouisfed.org/series/DGS3MO) | Interest rate for the prepaid float (FRED, St. Louis Fed) |

### Background

- Daniel Kahneman and Amos Tversky, [Prospect Theory: An Analysis of Decision under Risk](https://doi.org/10.2307/1914185), *Econometrica*, 1979 — the source of the possibility effect behind the golden-station idea.
