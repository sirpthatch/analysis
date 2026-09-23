# Provenance ledger

Every figure below was returned by a successful query against NYC Open Data on
**2026-09-22**. Anything not in this file should be treated as unconfirmed.

## Confirmed dataset identities

| Dataset | 4x4 | Agency | Catalog updatedAt |
|---|---|---|---|
| Motor Vehicle Collisions - Crashes | `h9gi-nx95` | NYPD | 2026-07-31T20:59:38Z |
| Motor Vehicle Collisions - Vehicles | `bm4k-52h4` | NYPD | 2026-07-31T20:53:35Z |
| Motor Vehicle Collisions - Person | `f55k-p6yu` | NYPD | 2026-07-31T20:54:34Z |
| DSNY Commercial Waste Zones | `8ev8-jjxq` | DSNY | 2026-09-22T10:19:09Z |
| Parking Violations Issued - FY2026 | `9mwx-gamw` | DOF | created 2026-08-18 |
| Parking Violations Issued - FY2027 | `pvqr-7yc4` | DOF | 2026-09-17T19:37:13Z |

`f55k-p6yu`, `8ev8-jjxq`, `9mwx-gamw` and `pvqr-7yc4` are listed because they are
plausible extensions — see NEXT-STEPS.md. Only the first two are used in the brief.

## Confirmed columns

**`h9gi-nx95` (Crashes)** — full column list confirmed via
`https://data.cityofnewyork.us/api/views/h9gi-nx95.json`:

    crash_date, crash_time, borough, zip_code, latitude, longitude, location,
    on_street_name, off_street_name, cross_street_name,
    number_of_persons_injured, number_of_persons_killed,
    number_of_pedestrians_injured, number_of_pedestrians_killed,
    number_of_cyclist_injured, number_of_cyclist_killed,
    number_of_motorist_injured, number_of_motorist_killed,
    contributing_factor_vehicle_1 .. _5, collision_id,
    vehicle_type_code1, vehicle_type_code2, vehicle_type_code_3,
    vehicle_type_code_4, vehicle_type_code_5

Note the inconsistent naming: `vehicle_type_code1` and `vehicle_type_code2` have no
underscore before the digit; `_3`, `_4`, `_5` do. Also note `off_street_name` and
`cross_street_name` have their display labels swapped in the metadata
(`off_street_name` → "CROSS STREET NAME"). Don't assume.

**`bm4k-52h4` (Vehicles)** — full column list confirmed via
`https://data.cityofnewyork.us/api/views/bm4k-52h4.json`:

    unique_id, collision_id, crash_date, crash_time, vehicle_id,
    state_registration, vehicle_type, vehicle_make, vehicle_model, vehicle_year,
    travel_direction, vehicle_occupants, driver_sex, driver_license_status,
    driver_license_jurisdiction, pre_crash, point_of_impact, vehicle_damage,
    vehicle_damage_1, vehicle_damage_2, vehicle_damage_3,
    public_property_damage, public_property_damage_type,
    contributing_factor_1, contributing_factor_2

## Confirmed figures

**Crash table extent** — query:
`h9gi-nx95.json?$select=max(crash_date),min(crash_date),count(*)`

    count      2,269,187
    min        2012-07-01
    max        2026-06-11

The max date is the single most important number in this bundle. It establishes a
~3.5 month reporting lag and means the September 19, 2026 fatality that prompted
the story is not in the data.

**Refuse vehicle records by year** — query in queries.md §2, output in
`data/refuse_vehicle_records_by_year.csv`:

    2014      1      2020    385      2026    191 (partial)
    2015      1      2021    401
    2016    552      2022    429
    2017    743      2023    398
    2018    785      2024    406
    2019    729      2025    402

**Vehicle type frequency since 2025-01-01** — query in queries.md §3, output in
`data/vehicle_type_counts_2025plus.csv`. Top of the list: SEDAN 85,852; STATION
WAGON/SPORT UTILITY VEHICLE 68,826; *blank* 29,436; BIKE 6,797; 4 DR SEDAN 6,107;
PICK-UP TRUCK 5,760. Heavy vehicles: BOX TRUCK 4,383; TRACTOR TRUCK DIESEL 1,671;
DUMP 845; GARBAGE OR REFUSE 593; FLAT BED 454; TOW TRUCK / WRECKER 317; TRACTOR
TRUCK GASOLINE 286.

## NOT verified — do not repeat these as fact without checking

- **312 BIC waivers this year, down from 572.** Streetsblog reporting. No open
  dataset backs it. FOIL BIC.
- **Roughly half of all trade waste trucks are exempted.** Streetsblog.
- **Side guards reduce fatalities by 40%.** Cited by Streetsblog; trace to the
  underlying study (likely Volpe/DOT literature) before using.
- **2021 study: conventional truck cabs have a frontal obstructed view of 20+
  feet.** Cited by Streetsblog; find the primary source.
- **NYPD failure-to-yield summonses: 81,710 in 2019 vs 42,686 last year (−48%).**
  Streetsblog. Not pulled from open data in this run.
- **The Badger truck received its exemption in 2023.** Streetsblog.
- **Nine hit-and-run deaths in August 2026.** Attributed to NYT via Streetsblog.
  Hit-and-run is not a field in the crash file, so this cannot be checked there.
- **Bike-lane parking violation description string.** Two attempts to query
  `9mwx-gamw` for `violation_description LIKE '%BIKE%'` timed out. The exact text
  is unconfirmed — establish it before building anything on it.

## Failed / unavailable in this run

- `therealdeal.com` returned 403 (DOB no-show inspection fee story).
- `urbancalc.substack.com/archive` and `/feed` both returned 429, so the idea was
  never checked against Thatcher's own recent Substack posts. The older
  urbancalc.com blog was readable and contains two adjacent 2016 posts — "NYC Bike
  Safety" (07 Oct 2016) and "NYC General Bike Stats" (06 Nov 2016). Different
  question, ten years old, but check the Substack before publishing.
