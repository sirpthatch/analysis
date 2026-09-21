# Recovering street name and ZIP from coordinates

Tested 2026-09-21. **Verdict: yes for ZIP, yes with a stated error rate for
street name.** Implemented in `src/reverse_geocode.py`.

## Method

DCP's Citywide Street Centerline (**CSCL, `inkn-q76z`**, 122,313 segments) carries
`full_street_name` and per-side ZIP (`l_zip` / `r_zip`) on every segment, so one
nearest-segment join yields both fields at once. No API, no key, no rate limit —
it's a local spatial join, which matters at 20,625 points.

Distances are computed in **EPSG:2263** (NY State Plane, feet). A nearest-neighbour
search in raw lat/lon degrees is wrong at this latitude and would silently bias
toward north-south streets.

Non-roadway segment types (paths, step streets, ferry routes, non-physical
segments) are dropped so they can't win a match; 115,335 segments remain. Points
further than `max_feet=300` from any segment return null rather than a guess.

## Validation

Socrata rows carry coordinates *and* officer-written street names and ZIPs, so
geocoding their own coordinates and comparing measures the method directly.
Sample of 6,000 crashes:

| | Rate |
|---|---|
| Geocoded within 300 ft | **99.8%** |
| Street name matches a name on the report | **88.2%** |
| ZIP matches Socrata's `zip_code` | **95.0%** |
| ZIP ambiguous (sides of street differ) | 1.9% |

Median distance from a crash point to the nearest centerline is **2 feet**.
Accuracy is stable across distance bands up to 100 ft (89%, 80%, 90%, 89%) and
falls off only beyond that (51%, n=136).

"Matches a name on the report" is the fair test: at an intersection the nearest
segment can legitimately be any of the streets named, so the geocoded value is
checked against `on_street_name`, `cross_street_name` and `off_street_name`.
Exact agreement with `on_street_name` alone is 51% — that number understates the
method rather than measuring it.

## Two traps that silently deflate the match rate

Both were hit while building this, and both produced plausible-looking wrong
numbers rather than errors:

1. **Socrata pads a house number ahead of the street name** —
   `"215       VAN PELT AVE"`, `"82-66     BROADWAY"`. It has to be stripped. But
   NYC streets are themselves numbered (`64 RD`, `2 AVE`), so a naive
   leading-digit rule eats the street name and turns `64 RD` into `RD`. The
   padding is the discriminator: a house number is followed by **two or more**
   spaces, a numbered street by one.
2. **Suffix substitution needs word boundaries.** `ROAD -> RD` applied without
   `\b` turns `BROADWAY` into `BRDWAY`, quietly failing every Broadway crash in
   the city.

Together these cost 5.8 points of apparent accuracy (82.4% -> 88.2%).

A third trap affects ZIP: **CSCL serves `l_zip` as a number and `r_zip` as a
string**, so comparing them raw is always unequal and an ambiguity flag built on
it reads 99.6% when the true rate is 1.9%. Both are coerced to digit strings in
`load_centerline()`.

## Applied to the TrafficStat gap

20,625 collisions, 2026-06-12 → 2026-09-13:

| | Rate |
|---|---|
| Street name assigned | 99.7% |
| ZIP assigned | 99.5% |

**TrafficStat's coordinates behave identically to Socrata's** against the street
grid — median 2 ft, p75 37, p90 54, p99 155, against Socrata's 2 / 37 / 54 / 154.
This is evidence against the earlier worry that highway mile-marker snapping
would make TrafficStat positions systematically coarser: at the level the street
grid can detect, it doesn't.

Nearest-segment roadway type: 88.2% Street, 8.7% Highway, 1.7% Bridge, 1.1% Ramp.

Top reconstructed streets in the gap period: Belt Pkwy (272), 3 Ave (232),
Broadway (207), Long Island Expy (196), Grand Central Pkwy (193).

Output: `data/processed/trafficstat_gap_geocoded.csv`.

## What this does and doesn't change

It closes two of the three schema gaps in
`research/socrata_gap_reconstruction.md`: street name and ZIP move from "absent"
to "reconstructed, 88% / 95% agreement." **Contributing factors, injury counts
and `collision_id` remain unrecoverable** — no geocoder can supply those.

One caveat to carry into any writeup: a reconstructed street name is *nearest
centerline*, not *what the officer wrote*. For counting collisions per street
that distinction is immaterial at 88% agreement. For quoting a specific
collision's location it is not — check the individual record.

## Also available from CSCL

The centerline carries more than street and ZIP: `rw_type` (Street / Highway /
Bridge / Tunnel / Ramp / …), `posted_speed`, `number_travel_lanes`,
`truck_route_type` and `snow_priority`, all per segment. Any of these can be
attached to a reconstructed crash by the same join, which recovers roadway
context Socrata never carried in the first place.

`roadway_type` is already attached in the output. On the gap period it splits
88.2% Street, 8.7% Highway, 1.7% Bridge, 1.1% Ramp.
