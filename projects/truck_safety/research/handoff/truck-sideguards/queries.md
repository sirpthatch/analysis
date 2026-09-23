# SoQL queries

Base endpoints:

    https://data.cityofnewyork.us/resource/h9gi-nx95.json    (crashes)
    https://data.cityofnewyork.us/resource/bm4k-52h4.json    (vehicles)

Metadata + full column list for any dataset:

    https://data.cityofnewyork.us/api/views/<4x4>.json

## The case-sensitivity trap

SoQL text matching is case-sensitive and these columns are inconsistently cased.
Always wrap **both sides** of a text comparison in `upper()`:

    $where=upper(vehicle_type) like '%GARBAGE%'        -- correct
    $where=vehicle_type like '%Garbage%'               -- silently wrong

Same for grouping. If you group on a raw text column you will get split categories
that look like real distinctions. A zero-row result is far more often a casing
artifact than a genuine absence — re-run with `upper()` before concluding anything
is missing.

## §1 — Establish the data extent (run this first, every time)

    h9gi-nx95.json?$select=max(crash_date),min(crash_date),count(*)

Confirmed 2026-09-22: 2,269,187 rows, 2012-07-01 to 2026-06-11.

## §2 — Refuse vehicle records by year (VERIFIED)

    bm4k-52h4.json?$select=date_trunc_y(crash_date) as yr,count(*) as n
      &$where=upper(vehicle_type) in ('GARBAGE OR REFUSE')
      &$group=yr&$order=yr

## §3 — Vehicle type frequency, to build the code list (VERIFIED)

    bm4k-52h4.json?$select=upper(vehicle_type) as vt,count(*) as n
      &$where=crash_date>'2025-01-01'
      &$group=vt&$order=n desc&$limit=30

Raise `$limit` to 200 and widen the date range when curating the full code list.

## §4 — Citywide denominator by year (NOT YET RUN — do this before anything else)

    h9gi-nx95.json?$select=date_trunc_y(crash_date) as yr,count(*) as n
      &$group=yr&$order=yr

The refuse series is meaningless without this. If all crashes fell ~45% after 2019
and refuse-truck crashes held flat, that is the story. If both fell in step, there
is no story.

## §5 — Heavy vehicles by year, all codes (NOT YET RUN)

    bm4k-52h4.json?$select=upper(vehicle_type) as vt,date_trunc_y(crash_date) as yr,
      count(*) as n
      &$where=upper(vehicle_type) in ('GARBAGE OR REFUSE','BOX TRUCK',
        'TRACTOR TRUCK DIESEL','TRACTOR TRUCK GASOLINE','DUMP','FLAT BED',
        'TOW TRUCK / WRECKER')
      &$group=vt,yr&$order=vt,yr

## §6 — Side-impact narrowing (NOT YET RUN — check the value vocabulary first)

Before filtering, enumerate what `point_of_impact` actually contains:

    bm4k-52h4.json?$select=upper(point_of_impact) as poi,count(*) as n
      &$where=upper(vehicle_type)='GARBAGE OR REFUSE'
      &$group=poi&$order=n desc

Do the same for `pre_crash` and `vehicle_damage`. Only then write the side-impact
filter. Do not guess the value strings.

## §7 — The join (NOT YET RUN)

Socrata has no cross-dataset join. Pull both tables filtered and join locally on
`collision_id` — that's what `scripts/analysis.py` does. To keep the crash pull
small, collect `collision_id` values from the vehicle query first and page them
through an `in(...)` clause in batches of ~500, or just pull all crashes since 2016
and join in pandas.

## Pagination

Default `$limit` is 1000. Page with `$limit=50000&$offset=N`, and always add a
stable `$order` (e.g. `$order=unique_id`) or paging will silently repeat rows.

## Rate limiting

Unauthenticated requests get 429'd under load. Wait and retry once. If it still
fails, say so rather than guessing at what the data holds. An app token
(`$$app_token=`) raises the limit and is free.
