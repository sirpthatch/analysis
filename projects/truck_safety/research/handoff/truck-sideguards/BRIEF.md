# The city waived the truck safety rule on half its garbage fleet, and the crash curve never moved

## Working headline and argument

New York required side guards on heavy trade waste vehicles in 2015 with a 2023
compliance deadline, then exempted a large share of the fleet by waiver. Refuse
trucks appear in NYPD crash records at almost exactly the same annual rate after
the deadline as before it — which is what you would expect if the rule is being
waived into irrelevance, and is the empirical question the current news cycle is
actually asking.

## The news hook

Daria Serkov, 15, was killed by a truck while riding a Citi Bike at 14th Street and
First Avenue on Friday, September 19, 2026.

On Monday, September 21, Streetsblog reported that the Badger hydro-excavation
truck involved had received a Business Integrity Commission side-guard exemption in
2023; that roughly half the city's trade waste trucks are exempted; and that BIC
issued 312 waivers this year against 572 the year before. Those waiver counts are
Streetsblog's reporting, not open data — treat them as sourced claims, not as
independently verified figures.

The rule itself: side guards were mandated in 2015, three years after cyclist
Mathieu Lefevre's 2012 death, with companies given until 2023 to comply. BIC grants
exemptions where "the design or operation of the vehicle" makes installation
impractical — per BIC, things like incorporated toolboxes, ladders and fuel tanks on
vehicle sidewalls.

Why it's a this-week story: the waiver is the news, it surfaced Monday, and the
Council and the Mamdani administration are already mid-argument about large
vehicles. An amNY editorial the same day went after the mayor on street safety
progress. The window is open now.

## Datasets

**Motor Vehicle Collisions – Vehicles** — `bm4k-52h4`
https://data.cityofnewyork.us/resource/bm4k-52h4.json

Columns to use (all confirmed present): `unique_id`, `collision_id`, `crash_date`,
`vehicle_type`, `vehicle_make`, `vehicle_year`, `pre_crash`, `point_of_impact`,
`vehicle_damage`, `contributing_factor_1`.

**Motor Vehicle Collisions – Crashes** — `h9gi-nx95`
https://data.cityofnewyork.us/resource/h9gi-nx95.json

Columns to use (all confirmed present): `collision_id`, `crash_date`, `borough`,
`zip_code`, `latitude`, `longitude`, `on_street_name`, `number_of_cyclist_injured`,
`number_of_cyclist_killed`, `number_of_pedestrians_injured`,
`number_of_pedestrians_killed`.

Join on `collision_id`.

`point_of_impact` and `vehicle_damage` are the fields that isolate side-impact
events — the collision geometry side guards actually address. That narrowing is
what makes this an analysis rather than a chart.

## Sample query (verified, returns the yearly series)

    https://data.cityofnewyork.us/resource/bm4k-52h4.json?
      $select=date_trunc_y(crash_date) as yr,count(*) as n
      &$where=upper(vehicle_type) in ('GARBAGE OR REFUSE')
      &$group=yr&$order=yr

Output is in `data/refuse_vehicle_records_by_year.csv`.

## What the analysis would likely show

Refuse-truck involvement is flat across the 2023 compliance deadline, and
side-impact cyclist and pedestrian injuries in those crashes are flat too.

## What would kill the idea

Kill it — don't fudge it — if either of these holds:

1. Side-impact injury crashes involving refuse trucks *do* fall measurably after
   2023. That's a rule working, and the post becomes a different, smaller piece.
2. The flat total conceals a real decline once normalized by citywide crash volume,
   which itself dropped sharply after 2019. **Run the citywide denominator before
   running anything else.** If refuse trucks held flat while all crashes fell, the
   story arguably gets stronger; if they fell in step, there is no story.

A third, softer kill: if the `GARBAGE OR REFUSE` code turns out to be dominated by
DSNY municipal trucks rather than private carters, the waiver framing doesn't
attach to the data at all.

## Limitations to state in the post, not bury

- `GARBAGE OR REFUSE` is a self-reported NYPD field and will not cleanly separate
  DSNY municipal trucks from private carters — which is exactly the distinction the
  rule turns on, since the waiver applies to licensed trade waste vehicles.
  `vehicle_make` gets you partway.
- The crash file runs roughly three months behind. The most recent crash as of
  2026-09-22 is dated 2026-06-11, so Friday's fatality is not in the data and won't
  be for months. Say this explicitly.
- The jump from 1 record (2015) to 552 (2016) is a reporting-schema change, not a
  real event. Start the series in 2016.
- `vehicle_type` is filthy: junk codes (`PK`, `DELV`, `UN/C`), duplicate concepts
  (`BIKE` vs `BICY`), and 29,436 blank records since 2025 alone. Build an explicit
  heavy-vehicle code list, apply `upper()` on both sides of every comparison, and
  publish the list in the post.
- There is no open dataset of which specific trucks hold waivers. That's a FOIL to
  BIC — file it today even if the post runs first.
- Selection effect: crash records exist when NYPD files a report. Minor
  truck-vs-cyclist contact that produces no report never appears.

## Rough effort

A day. The vehicle-type cleanup and the DSNY-versus-carter separation are most of
it. The yearly series itself is twenty minutes.
