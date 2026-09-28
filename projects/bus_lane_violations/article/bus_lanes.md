# Buses are Moving Traffic Cameras

Concentration in ticketing, ticketing decay curve, and behavior of repeat offenders

The [Broadway and Isham St](https://maps.app.goo.gl/bJvgbfLD6K9HCzVw5) stop on the BX12+ must be something special - it has been the site of **32,771 tickets** issued by [ACE](https://www.mta.info/agency/new-york-city-transit/automated-camera-enforcement) equipped buses, more than most entire routes generate in a year.

In 2023 NYS expanded the ACE program, which attaches cameras to public buses which will automatically issue tickets to people double parking, driving in dedicated bus lanes, or parking in bus stops. The MTA's buses have logged 6.53 million events since October 2019, and issued 3.54 million tickets. Boston is about to introduce its [own variant](https://www.mbta.com/projects/automated-camera-enforcement-program) of the program on October 1st, so this is a great time to take a step back and see any interesting trends in the data.

Three things pop out from exploring the [violations data](https://data.ny.gov/Transportation/MTA-Bus-Automated-Camera-Enforcement-Violations-Be/kh8p-hcbm/about_data): there is a surprising amount of **concentration** in the tickets, the cameras become **more of a toll than a deterrent** over time, and **repeat offenders** offer up a great example of NYC's IDGAF energy.

## Some routes are definitely worse than others...

Take a look at this [interactive map](https://storage.googleapis.com/urbancalc_images/ace_violations_map.html) to see where tickets have been issued along bus routes:

![bus_lane_map_cap](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/bus_lane_map_cap.png)

There are 66 routes that issue tickets, but the top routes account for a disproportionate share of the tickets issued. The **top 3 (M15+, BX19, M101) are responsible for 26% of tickets**, the top 10 expands to 58%, and by the time we get to the top 20 it covers 82% of the ticket volume.

And as mentioned before the Broadway and Isham St stop on the BX12+ generated 32,771 tickets, more than 41 out of the 66 *entire routes*.

![top20](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/top20.png)

This seemed wild. A double check on it though is to look at the [DOF Parking Violations](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2026/9mwx-gamw/about_data) data, which similarly has its top 12 locations issuing 49.8% of all of its tickets. Similar pattern; some locations are truly special.

## How much do the violations change behavior?

It is reasonable to expect that for any given route, violations would start high and decay over time as people learn that they should not be messing around in the bus lane anymore. But how much? Are the violations a perfect deterrent, such that ticket volume drops close to 0 as people learn? Or do people blow it off and volume stays high?

Turns out, on average, ticket volume decays 30% by 6 months, and then plateaus at around half strength by 1 year in.

32 out of the 66 routes have sufficient history to look at their ticket decays, from which we can take a median and reconstruct a "typical" route decay curve:

![typical_decay](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/typical_decay.png) 

"All types" refers to the full range of ticket types (double parking, stopped at bus stop, traveling in bus lane), whereas "lane only" refers to only traveling in bus lane tickets.

Here are the decay curves for all of the 32 routes with enough history:

![decay_curves_full](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/decay_curves_full.png)   

Note, the greyed out regions are where there appear to be anomalies in the data (likely technical issues). 

The M15+ route again is interesting - it appears to have no measurable decay since turning on its cameras.

## Let's see those repeat offenders!

57% of ticketed plates are ticketed once, but the **25% of the plates that get 3 or more tickets are responsible for 62% of all tickets**!

The data actually allows us to compute a probability curve for the chance that someone will get another ticket given they have a certain number of tickets so far. It is pleasingly smooth.

![pnplus1ticket](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/pnplus1ticket.png)

Some other interesting statistics on the heaviest repeat offenders (11+ tickets, 35,581 plates):

* They drive on a **consistent route**. Median **64%** of a heavy repeater's tickets land on one route, vs **17%** if their tickets were scattered like everyone else's.
* They drive on a **regular schedule**. **67%** of consecutive same-route tickets land within ±3 hours of the same time of day on nearby days (would happen 25% by chance alone), with 36% of plates getting at least half their top-route tickets inside the same 2-hour window (would happen 4% by chance alone).

Here is a plot of frequency of tickets for the repeat offenders:

![frequent_plot](/Users/thatcher/dev/analysis/projects/bus_lane_violations/article/lib/frequent_plot.png)

In short, this looks like regular commuters eating the tickets, mixed in with some fleet vehicles managing it as the cost of doing business. 

This also partially explains the decay behavior in ticket volume - the repeat offender share holds between **31% and 39%** of all volume from week 6 onward, even as total volume drops 50%. So a third of tickets, shortly after launch and a year later, go to plates with 3+ prior tickets, creating a floor in how low the ticket volumes will likely go.

## Parting Thoughts

There is a blip in the data in March-April 2026 where it looks like fewer tickets were issued than expected - a disproportionate share of the ticket volume was tagged as `TECHNICAL ISSUE/OTHER`, suggesting tickets were never issued. We can estimate the lost ticket volume (~108,500 tickets) and using that estimate the lost revenue from the fines - **$14.2M** (face value, before any dismissals or non-payments).

The ACE program is a money machine for the city, and repeat offenders pay a disproportionate share of that. A third of the tickets go to people who have decided it's a toll worth paying. I love NY'ers.



## Caveats

* **These are tickets issued, not tickets upheld or paid.** The dataset records whether a camera event became a ticket, not what happened to it afterwards. Dismissals, successful disputes and unpaid fines aren't in it, so every revenue figure here is face value.
* **Not every camera event is a ticket.** Of 6.53 million events, 46% were rejected, for exemptions such as emergency vehicles, buses and commercial vehicles stopped under 20 minutes, or for technical issues or missing vehicle information. Everything in this piece counts only the 3.54 million that became tickets.
* **Some "tickets" are warnings.** Each new route gets 60 days of warning notices before fines start, and the data appears to record those as issued tickets. On the M2 and M4, for example, fines began July 18, 2025, but "tickets" start May 18–19, when the warnings did. If that holds for every route, up to about 13% of the 3.54 million are warnings. That includes the first weeks of every decay curve, so the "launch level" the curves are measured against is really the warning-period level.
* **The data stops in June 2026, and two 2026 windows are excluded.** Tickets only appear in the data once the notice has gone out, so issued tickets thin out after mid-June 2026 even though camera events continue to August. Everything here ends at June 7, 2026. Two windows are shaded in the charts and left out of the typical curve:
  * **26 Jan – 15 Feb 2026:** bus-lane tickets roughly tripled on every major route at once, which looks like a snowstorm, though I haven't checked weather records.
  * **23 Mar – 26 Apr 2026:** the technical outage described above.
* **Seasonality isn't removed.** Each decay curve runs on calendar time as well as time since launch, so winter dips show up inside every curve.

*Data: [MTA Bus Automated Camera Enforcement Violations](https://data.ny.gov/Transportation/MTA-Bus-Automated-Camera-Enforcement-Violations-Be/kh8p-hcbm/about_data) (data.ny.gov `kh8p-hcbm`), pulled September 25, 2026: 6.53 million camera events from October 2019 to August 2026. Route shapes are from MTA's static GTFS feeds. The fine schedule is from [MTA](https://www.mta.info/agency/new-york-city-transit/automated-camera-enforcement) and [NYC Department of Finance](https://www.nyc.gov/site/finance/vehicles/mta-bus-camera-violations.page).*
