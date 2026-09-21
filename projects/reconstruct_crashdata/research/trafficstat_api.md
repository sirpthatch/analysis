# NYPD TrafficStat — undocumented JSON API

`https://trafficstat.nypdonline.org` is an Angular front end over a public JSON
API. No key, no auth, no rate limiting observed. Recovered from the site's own
bundle (`main-*.js`) on 2026-09-21 and wrapped in `src/trafficstat.py`.

**This is an internal API with no published contract.** It can change or vanish
without notice — and this project depends on it entirely, so that is a standing
risk, not a footnote. Re-verify before depending on a fresh pull; everything
already pulled is archived under `data/raw/`.

## Endpoints

```
POST /api/reports/{report_id}/data    body: [{"key": ..., "values": [...]}]
POST /api/filters/{filter_id}/data    body: {}
GET  /api/visitors/count
GET  /api/reports/mapsMeta/{id}
```

Note the report body is a JSON **array** of filter objects. Posting `{}` returns
a 400 naming the expected type (`List<ParameterFilter>`); posting `[]` returns
204 for most reports.

## Reports

| ID | Type | What it is |
|---|---|---|
| `7d9951ab-45d8-41af-bcc7-10e2f1874de0` | Map | **Record-level geocoded incidents** |
| `b805fa11-d5d2-43f7-8c23-1649f5d387f1` | Table | TrafficStat Book (the aggregate) |
| `3ed925ee-caa8-4e9c-8d87-9e56fc8269f8` | Html | Methodology notes |

Charts (28-day by precinct, by hour, by collision type, by day of week, 52-week
and year-over-year timelines) exist too — see the catalog in `src/trafficstat.py`.

## Filters

| ID | What |
|---|---|
| `ec81a9e0-...` | Precinct keys — 79, zero-padded (`001`, `073`) |
| `154dcf7a-...` | Patrol boroughs (`PBBN`, `PBBS`, …) |
| `33e40f74-...` | 38 metrics across Collisions / Fatalities / Collisions+ / Fatalities+ |
| `50d3635e-...` | The as-of date |

Metrics include per-mode breakouts the Socrata file doesn't carry as cleanly:
Car, SUV, Pedestrian, Traditional Bicycle, **E-bike**, Stand-up Scooter, Moped,
Motorcycle, Off Road (Dirt Bike/ATV), each for both collisions and fatalities.

## Why this is worth having

**TrafficStat was current to 2026-09-13 when checked. The Socrata crash file
(`h9gi-nx95`) has been frozen since 2026-06-11.** This is the only public route
found to recent citywide collision geography — it closes limitation #6, though
only partly (see below).

## Caveats — these are substantial

1. **Markers are not records.** Where several collisions share a coordinate the
   API returns one marker whose `metric` is the count and whose tooltip lists
   each occurrence. In precinct 073 YTD, 149 of 563 markers were collapses —
   563 markers expand to 882 collisions. Treating markers as records undercounts
   by ~36%. `trafficstat.incidents()` expands them.
2. **Expansion recovers ~99.3%, not all.** Six markers in that sample yielded
   fewer pairs than claimed (882 parsed vs 888 claimed). The gap is reported as
   `df.attrs["unparsed"]` — check it, don't assume zero.
3. **Everything useful is inside tooltip HTML.** Only the coordinate pair is a
   real field. Category and date are recovered by stripping tags, and long
   category labels arrive truncated in the markup (`SIDE SWIPE (SAME..`).
4. **The 5,000-record cap is real and binding.** Citywide 28D and YTD both hit
   it exactly. Pull per precinct — `trafficstat.all_incidents()` does.
5. **Three time windows only: WTD, 28D, YTD.** No arbitrary date ranges, no
   prior years. For history you still need Socrata. This complements the frozen
   file for *recent* data; it does not replace it.
6. **No time of day per record.** Every timestamp renders `12AM` — the date is
   real, the hour is not. Hour-of-day exists only in aggregate (the 28-day hour
   chart).
7. **Thin schema.** Coordinates, collision type, date. None of Socrata's
   contributing factors, vehicle types per record, injury/fatality counts per
   record, or street names. Mode is selectable as a *metric* rather than being a
   per-record field, so a mode breakdown means one pull per mode.
8. **Collision type is heavily `UNKNOWN`** — 453 of 882 in the 073 sample (51%).
   That is NYPD's own data, not a parsing artifact, and it caps what any
   type-based analysis can claim.
9. **Preliminary by NYPD's own statement**: subject to reclassification,
   especially fatalities. Highway collisions geocode to the nearest mile marker,
   not the actual location — **directly relevant to any parkway analysis here.**
   Collisions in parks, beaches and open areas often can't be mapped at all.

## Role in this project

TrafficStat is the sole source for the reconstruction. Everything in
`data/processed/reconstructed_crashes_gap.csv` originates here, with street name
and ZIP added from CSCL afterwards.

Caveat 9 is the one that most constrains the result: highway collisions geocode
to the nearest mile marker rather than the actual location. Tested indirectly —
TrafficStat's distance-to-centerline distribution is indistinguishable from
Socrata's — so it is not detectable at street-grid resolution, but it has not
been ruled out for fine-grained highway work.

## Usage

```python
import trafficstat as ts

ts.as_of()                                    # '09/13/26'
ts.precincts()                                # 79 keys
ts.metrics()                                  # 38 (group, key) pairs

df = ts.incidents("073", "YTD", "Collisions") # one precinct
df.attrs["markers"], df.attrs["unparsed"]     # expansion diagnostics

everything = ts.all_incidents("YTD")          # all 79 precincts, clears the cap
```
