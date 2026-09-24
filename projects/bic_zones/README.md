# Commercial Waste Zones and Truck Crashes

Research bundle for the story idea: *"The commercial waste zones were sold partly as a
safety measure. Queens Central has been live 20 months. Garbage truck crashes haven't budged."*

Assembled 2026-09-23 / 2026-09-24. Every dataset id, column name and figure in this bundle
was fetched live from NYC Open Data during assembly. Anything I could not verify is listed
in `NOTES.md` rather than quietly dropped.

## The argument

Commercial waste zoning was supposed to cut the miles private carting trucks drive, and fewer
truck miles was supposed to mean fewer crashes. The rollout is staggered by zone with published
implementation dates, which makes it one of the cleanest natural experiments in city policy
right now. Citywide, crashes involving refuse vehicles have been flat at roughly 415-450 a year
since 2022 — no visible response to Queens Central going fully live on 2025-01-03.

The second thread, and the one that gives the piece its edge: BIC has written **47 side-guard
violations ever** against a mandate in force since 2015, while granting roughly 884 waivers in
the last two years alone. The enforcement side of the truck safety regime is close to dormant.

## The news hook

A 15-year-old, Daria Serkov, was killed on East 14th Street on 2026-09-18 by a hydro-excavation
truck that BIC had exempted from the side guard requirement in 2023. BIC confirmed ~312 waivers
this year and 572 the year before, and that about half of all trade waste trucks in the city are
exempted. See `SOURCES.md`.

## What's in here

```
README.md                       this file — pitch, method, kill conditions
SOURCES.md                      every news and policy link, dated, with what each establishes
NOTES.md                        limitations, open questions, FOIL list, unverified claims
data/datasets.md                dataset reference: ids, columns, freshness, gotchas
data/queries.md                 verified SoQL queries with the results they returned
data/verified_figures.csv       every number I checked, with its source query
data/cwz_zones.csv              20 commercial waste zones with their community districts
data/cwz_rollout.csv            phase, zones, implementation date, treatment status
data/refuse_vehicle_variants.csv  the 10 casing variants of vehicle_type and their counts
data/refuse_crashes_by_year.csv   annual refuse-vehicle crash counts, 2014-2026
data/bic_sideguard_rules.csv      the BIC rule texts mentioning side guards, with counts
scripts/fetch_data.py           pulls the four datasets into ./raw as CSV
scripts/build_panel.py          normalizes casing, joins crashes to zones, builds the panel
requirements.txt
```

## Method, in order

1. `python scripts/fetch_data.py` — pulls crashes, crash-vehicles, BIC violations and the zone
   polygons. Unauthenticated Socrata throttles; the script pages and backs off. Get an app token
   from https://data.cityofnewyork.us/profile/edit/developer_settings and set `SOCRATA_APP_TOKEN`
   if you hit 429s.
2. `python scripts/build_panel.py` — normalizes `vehicle_type` casing, joins Vehicles to Crashes
   on `collision_id`, assigns each crash to a waste zone by point-in-polygon, and attaches each
   zone's implementation date from `data/cwz_rollout.csv`.
3. The analysis itself: crashes per zone per month, indexed to each zone's implementation date.
   Treated group is Queens Central (2025-01-03), Bronx East and West (2025-12-01), Brooklyn South
   and Queens Northeast (2026-03-01). Everything implemented after the crash data ends is control.

**Do the DSNY-versus-private separation before anything else.** It is the crux. `vehicle_type`
does not distinguish a municipal Sanitation truck from a private carter, and only the private
ones are zoned. If you cannot separate them, the treatment effect is diluted by roughly the
municipal share of refuse-vehicle crashes and the whole comparison weakens.

**Checked 2026-09-24: there is no row-level fix.** Neither `vehicle_make`/`state_registration`
in the crash data nor BIC's fleet roster (`n84m-kx4j`, 7,106 active licensed vehicles) can be
joined to crash records — the crash Vehicles table carries no plate or VIN field at all, and
`n84m-kx4j`'s only identifier (`bic_plate_number`) has no counterpart there. What `n84m-kx4j`
gives instead is the licensed private fleet's own make distribution as an aggregate cross-check:
Kenworth (21.0%) edges Mack (19.2%) among licensed vehicles, while Mack is 61.2% of refuse-vehicle
crashes — suggestive that much of the Mack-heavy crash count is DSNY's own fleet, not private
carters, but that's composition evidence, not an identified split. See `data/datasets.md` #6 and
`NOTES.md` #2. Absent a real split, report citywide flatness and the enforcement finding as
primary and the zone comparison as a caveated secondary exhibit.

## What the analysis would likely show

Flat-to-nothing in the treated zones. That is the finding — a policy whose safety rationale
hasn't appeared in the crash data twenty months into its first zone, next to an enforcement
agency that has written 47 side-guard tickets in four years.

## What would kill it

- **A clear decline in Queens Central and the Bronx zones relative to untreated zones.** Then the
  post flips: the waste zones are quietly working and the waiver story is the real scandal. Still
  a post, and a better one than most would expect.
- **Numbers too small to say anything.** This is the live risk, not the directional one. A ~400
  crash annual citywide base split across 20 zones will not support confident per-zone claims.
  Pool the early zones, report counts and rates rather than significance tests, and say plainly
  where the noise floor is.
- **The decline is an artifact of reporting.** If refuse-vehicle crash counts fall everywhere
  including untreated zones, you are looking at NYPD crash-reporting practice, not carting.
