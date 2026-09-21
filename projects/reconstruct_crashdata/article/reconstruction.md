# The City's Crash Data Was Never Missing. It Just Stopped Being Published.

For some reason NYC is about 3 months late publishing its data about [Vehicle Collisions](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data). This has been reported on in [Streetsblog](https://nyc.streetsblog.org/2026/09/09/empty-quarter-citys-crash-database-hasnt-been-updated-in-three-months) and [Hoodline](https://hoodline.com/2026/09/nyc-s-crash-data-has-been-frozen-since-june-hiding-thousands-of-wrecks/), and it affects useful services like [CrashMapper](https://crashmapper.org/#/). The last time the [dataset](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data) was updated on OpenData was June 11th, 2026, and there is a delightfully unhelpful message at the top stating:

> This dataset is temporarily not updating while its automated update process is being fixed. This fix is expected to be completed during the month of August.

Seeing as we are nearing the end of September and this is still not working, let's see if we can help the city out here a bit!

If you want to skip to the end, here is a **[patch data file](https://github.com/sirpthatch/analysis/blob/main/projects/reconstruct_crashdata/reconstructed_crashes_gap.csv)** that brings some of the columns from the [Vehicle Collisions](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data) data up to date as of 9/13/2026. This includes **20,625 vehicle collision records**, including 9,749 crashes with injuries and 47 crashes with fatalities.

## The Source

The Vehicle Collision data cites the NYPD as the source of its crash data, and if you poke around a bit you can see that NYPD publishes data about vehicle collisions on [https://trafficstat.nypdonline.org](https://trafficstat.nypdonline.org), and it appears to have records that are more up to date. This data is publicly available and requires no key or authentication.

So it seems like the breakdown is somewhere in the translation between whatever system publishes to TrafficStat and what is publishing to OpenData. Let's see if we can do that translation ourselves!

## The Data Wrangle

The key is that TrafficStat reports incidents as markers on the map, and we need to translate that into vehicle collision records with as much of the associated metadata as possible.

You can query the marker values (which translate to lat/long coordinates) and their tooltips, and from there drill into individual accidents. Then for each accident (which ends up being a lat/long/timestamp) you can query TrafficStat for other metrics about the incident that help fill out the full collision record.

The vehicle collision data has helpful fields for Street Name and ZIP code which are not present in the raw TrafficStat data, but those can be derived by doing a nearest neighbor join from the lat/long coordinates onto street segment centerlines. You can similarly derive many of the other missing fields from the TrafficStat data.

There are some fields that do not seem as easily derivable — notably, the vehicle types, contributing factors, and the time of the crash. Most of that metadata is likely buried in the backend data pipeline and not accessible on a public-facing API. There is one interesting case — time of day — that looks like it degrades slowly in TrafficStat. That is populated 95% in April, 12.5% in May, and 0% from June onward. Maybe that is related to the source of whatever is breaking the pipeline.

## The Final Result

In the end I was able to generate a [patch data file](https://github.com/sirpthatch/analysis/blob/main/projects/reconstruct_crashdata/reconstructed_crashes_gap.csv) that brings some of the columns from the [Vehicle Collisions](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data) data up to date as of 9/13/2026.

It was not able to generate all of the same columns; here is a mapping of what was/was not possible:

![column_status](/Users/thatcher/dev/analysis/projects/reconstruct_crashdata/article/lib/column_status.png)

To validate the data, I ran the data generation process on TrafficStat back to January 1 so that it would overlap with the Socrata data, and each month is within 1.1% agreement of its collision counts. Hoodline reported NYPD had logged 54,832 crashes through August 30. My reconstruction, built independently from TrafficStat, gives 54,730 — a 0.2% difference. Here is the TrafficStat data plotted against the available Socrata data:

![output](/Users/thatcher/dev/analysis/projects/reconstruct_crashdata/article/lib/output.png)

And here is a visualization of where the recent crashes happened, in the Jun-Sept period that is currently missing from the OpenData dataset.

![crash_map](/Users/thatcher/dev/analysis/projects/reconstruct_crashdata/article/lib/crash_map.png)

## Some Caveats

- Coordinates are an independent geocode, so a series spliced onto Socrata history has a discontinuity at June 11
- Injury fields are booleans, not counts — "was anyone hurt," not "how many"
- Highway crashes snap to the nearest mile marker

## Please Update the Data

Everything here is an approximation and a band-aid for the data being properly available in the [Vehicle Collisions](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data) dataset. Having this data publicly available supports a rich ecosystem of journalists and data scientists hoping to do their small part in making the city safer. So to whoever is in charge of this in City Hall or the NYPD, let's get the data moving again!
