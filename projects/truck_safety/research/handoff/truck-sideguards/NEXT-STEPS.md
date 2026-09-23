# Next steps, in order

## 0. File the FOIL today

There is no open dataset of which trucks hold side-guard waivers. Request from the
Business Integrity Commission: all side-guard exemption applications and
determinations issued under the heavy-duty trade waste vehicle rule, by year, with
licensee name, vehicle make/model/VIN or plate, stated basis for exemption, and
determination date. This is the spine of the follow-up post even if the first post
runs without it. Everything below can proceed in parallel.

## 1. Run the citywide denominator — BLOCKING

queries.md §4. Total crashes per year, 2016–2025. Until this exists, the flat
refuse series says nothing. Three outcomes:

- Citywide crashes fell substantially, refuse held flat → story is stronger than
  the pitch. Refuse trucks became *relatively* more dangerous.
- Both fell in step → no story. Stop here and say so.
- Citywide rose, refuse flat → story weakens; refuse trucks improved relatively.
  Probably not publishable as framed.

## 2. Separate DSNY from private carters

This is the hardest problem and it decides whether the waiver framing attaches to
the data at all. The rule covers licensed *trade waste* vehicles — private carters.
`GARBAGE OR REFUSE` in the NYPD data covers both.

Approaches, roughly in order of promise:
- `vehicle_make` — DSNY runs a distinctive fleet; carters are more varied.
- `state_registration` — DSNY vehicles are NY-registered municipal.
- Geography and time of day — DSNY collects residential on set routes; carters run
  commercial corridors overnight. Join to DSNY Commercial Waste Zones (`8ev8-jjxq`)
  and look at whether crash locations cluster in carter zones.
- If none of these separate cleanly, say so in the post and present the combined
  figure with the caveat. Do not silently assume.

## 3. Enumerate the impact vocabulary, then narrow

queries.md §6. Pull the actual distinct values of `point_of_impact`, `pre_crash`
and `vehicle_damage` before writing any filter. Do not guess value strings.

## 4. Build the heavy-vehicle code list

Start from `scripts/heavy_vehicle_codes.py` and `data/vehicle_type_counts_2025plus.csv`.
Re-run queries.md §3 with `$limit=200` over the full period. Decide explicitly what
counts, publish the list in the post, and check whether the code vocabulary changed
across the series — a category that appears or vanishes mid-series will look like a
trend.

## 5. Join and compute

`scripts/analysis.py`. Refuse-truck crashes per year, side-impact subset, cyclist
and pedestrian injuries and deaths in each, indexed against the citywide
denominator.

## 6. Check it against the Substack archive

https://urbancalc.substack.com/archive returned 429 during preparation and was
never read. Confirm this doesn't duplicate or contradict a recent post.

## Open questions worth answering if time allows

- Does the 2016 series break (1 → 552 records) reflect a schema change only, or
  also a change in what got reported? Check NYPD's data dictionary revisions.
- Are waiver-holding carters concentrated in particular commercial waste zones? If
  the FOIL lands, this is the map that makes the piece.
- `f55k-p6yu` (Person table) has per-person detail — worth checking whether it
  distinguishes injury severity better than the crash-level counts.
