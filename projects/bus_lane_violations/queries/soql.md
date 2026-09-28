# SoQL queries — all verified against the live API on 2026-09-25

Base: `https://data.cityofnewyork.us/resource/9mwx-gamw.json`
Dataset: Parking Violations Issued – Fiscal Year 2026 (DOF), created 2026-08-18,
covering issue dates July 1 2025 – June 30 2026 (plus a June 2025 spill, see README).

Landing page: https://data.cityofnewyork.us/d/9mwx-gamw
Metadata + full column list: https://data.cityofnewyork.us/api/views/9mwx-gamw.json

Sibling years: FY2025 = `m5vz-tzqv`, FY2027 (in progress) = `pvqr-7yc4`.
Same schema, so every query below works by swapping the four-by-four.

---

## 0. Find every bus-lane category (ALWAYS start here)

    ?$select=violation_description,count(*)
    &$where=upper(violation_description) like '%BUS LANE%'
    &$group=violation_description

→ data/violation_categories_fy2026.csv

There are FOUR categories, not one. Filtering on a single literal string
silently discards most of the enforcement.

## 1. Monthly volume by category

    ?$select=date_trunc_ym(issue_date) as month,violation_description,count(*)
    &$where=upper(violation_description) like '%BUS LANE%'
    &$group=month,violation_description
    &$order=month
    &$limit=80

→ data/monthly_by_category_fy2026.csv

## 2. Top camera locations, fixed roadside ACE

    ?$select=street_name,count(*)
    &$where=violation_description='BUS LANE VIOLATION'
    &$group=street_name&$order=count desc&$limit=60

→ data/top_locations_fixed_fy2026.csv

## 3. Top camera locations, bus-mounted mobile ACE

    ?$select=street_name,count(*)
    &$where=violation_description='MOBILE BUS LANE VIOLATION'
    &$group=street_name&$order=count desc&$limit=60

→ data/top_locations_mobile_fy2026.csv

## 4. Borough split by category

    ?$select=violation_county,violation_description,count(*)
    &$where=upper(violation_description) like '%BUS LANE%'
    &$group=violation_county,violation_description
    &$order=count desc&$limit=40

→ data/by_county_by_category_fy2026.csv

## 5. Monthly series for ONE location (the decay curve)

    ?$select=date_trunc_ym(issue_date) as month,count(*)
    &$where=street_name='EB W 14TH STREET @ 5'
    &$group=month&$order=month&$limit=15

→ data/monthly_w14th_at_5th_fy2026.csv
(Shape example only — 14th St cameras are long-established, so this is
NOT an activation curve. See README, "the actual experiment".)

## 6. Row-level pull for a corridor (for your own aggregation)

    ?$where=violation_description='BUS LANE VIOLATION'
      AND upper(street_name) like '%JAMAICA AVE%'
    &$limit=50000

## 7. Repeat-offender / fleet cut

    ?$select=plate_id,registration_state,count(*)
    &$where=violation_description='BUS LANE VIOLATION'
    &$group=plate_id,registration_state
    &$having=count(*) > 50
    &$order=count desc&$limit=500

Untested — included as a starting point, verify before quoting.

---

## Gotchas that cost me time

- `street_name` is truncated to 20 characters. "NB WOODHAVEN BLVD @" has lost
  its cross street entirely. Dedupe/normalise before publishing a leaderboard.
- Fixed-camera rows are ALL CAPS; mobile-camera rows are Mixed Case. Grouping
  across both categories without `upper()` splits every shared location in two.
- `violation_county` uses at least four coding schemes at once — see README.
- URL-encode `%` as `%25` when passing these through a browser or a fetcher
  that does not encode for you.
- Unauthenticated SoQL is rate-limited. An app token (free) removes the limit.
