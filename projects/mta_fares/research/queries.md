# Exact API calls

Every query below was run on 2026-09-28 and returned the recorded result. Copy-pasteable.
Note the domain split: ridership is `data.ny.gov`, Fair Fares is `data.cityofnewyork.us`.

## Verified

### Fare-class composition, 2026 window (`5wq4-mkjj`)

```
https://data.ny.gov/resource/5wq4-mkjj.json?$select=fare_class_category,sum(ridership)&$where=transit_timestamp>'2026-09-01'&$group=fare_class_category
```

Returned 10 categories. See `tests/fixtures/fare_class_2026.json`.

**Caution:** this is the exact query that caused the scoping error. With no upper
bound it covers 2026-09-01 .. 2026-09-16 (the dataset's coverage end), not two days.
The pipeline uses an explicit half-open window instead.

### Fare-class composition, 2024 window (`wujg-7c2s`)

```
https://data.ny.gov/resource/wujg-7c2s.json?$select=fare_class_category,sum(ridership)&$where=transit_timestamp>'2024-09-01' AND transit_timestamp<'2024-09-17'&$group=fare_class_category
```

Returned 12 categories, including both `Metrocard - Unlimited 30-Day` (3,979,572) and
`Metrocard - Unlimited 7-Day` (4,178,161). See `tests/fixtures/fare_class_2024.json`.

### Window reconciliation, 2026

```
https://data.ny.gov/resource/5wq4-mkjj.json?$select=count(*),sum(ridership),min(transit_timestamp),max(transit_timestamp)&$where=transit_timestamp>'2026-09-01'
```

```json
[{"count":"807594","sum_ridership":"58876993.0",
  "min_transit_timestamp":"2026-09-01T01:00:00.000",
  "max_transit_timestamp":"2026-09-16T23:00:00.000"}]
```

807,594 rows / 16 days ≈ 3.68M rides per day, which is the sanity check that the
window is right.

### Dataset metadata, `5wq4-mkjj`

```
https://data.ny.gov/api/views/5wq4-mkjj.json
```

45,243,727 rows; `rowsUpdatedAt` 1790174781 = 2026-09-23 14:46:21 UTC.

### Column confirmation

```
https://data.ny.gov/resource/5wq4-mkjj.json?$limit=2
```

Keys: `transit_timestamp`, `transit_mode`, `station_complex_id`, `station_complex`,
`borough`, `payment_method`, `fare_class_category`, `ridership`, `transfers`,
`latitude`, `longitude`, `georeference`.

### Fair Fares enrollment (`3tw8-6si8`, city domain)

```
https://data.cityofnewyork.us/resource/3tw8-6si8.json?$limit=5&$order=month DESC
https://data.cityofnewyork.us/resource/3tw8-6si8.json?$select=count(*),min(month),max(month)
```

92 monthly rows, 2019-01-01 .. 2026-08-01. Columns: `month`,
`total_fair_fares_enrollees`. August 2026: 382,820.

## Written but never run

### Station x fare class — the load-bearing pull

```
https://data.ny.gov/resource/wujg-7c2s.json?$select=station_complex_id,station_complex,borough,fare_class_category,sum(ridership)&$where=transit_timestamp>'2024-09-01' AND transit_timestamp<'2024-09-17'&$group=station_complex_id,station_complex,borough,fare_class_category&$limit=50000
```

Implemented as `farecap.collect.station_fare_class(2024)`. Run this first.

### 2024 window reconciliation — still open

```
https://data.ny.gov/resource/wujg-7c2s.json?$select=count(*),sum(ridership),min(transit_timestamp),max(transit_timestamp)&$where=transit_timestamp>'2024-09-01' AND transit_timestamp<'2024-09-17'
```

Expected to return 53,599,970 if the fare-class split is complete. Never run.

## Queries that failed, and why

- Unbounded `$select=fare_class_category,payment_method,sum(ridership)&$group=...` over
  `5wq4-mkjj`: **read timeout.** 45.2M rows. Always bound the window.
- `$select=transit_timestamp&$order=transit_timestamp DESC&$limit=1` returned
  `2026-09-02T23:00:00.000`, contradicting the bounded aggregate's 2026-09-16. Trust
  the bounded aggregate.
- A long query URL with a fully-qualified two-sided timestamp filter plus `$group` and
  `$order` was rejected by an HTTP intermediary for length before reaching Socrata.
  Keep clauses terse.

## Added 2026-09-28 (first networked session) — all run live

### The recorded-window discrepancy, reproduced

```
https://data.ny.gov/resource/wujg-7c2s.json?$select=sum(ridership),min(transit_timestamp)&$where=transit_timestamp>'2024-09-01' AND transit_timestamp<'2024-09-17'
-> 53599970.0, min 2024-09-01T01:00   (strict >: drops the 00:00 hour)
https://data.ny.gov/resource/wujg-7c2s.json?$select=count(*),sum(ridership),min(transit_timestamp),max(transit_timestamp)&$where=transit_timestamp>='2024-09-01' AND transit_timestamp<'2024-09-17'
-> 1222525 rows, 53643755.0, 2024-09-01T00:00 .. 2024-09-16T23:00
```
2026 equivalents: 58,876,993 (strict) vs 58,904,420 (half-open, 809,194 rows).

### Daily fare-class series (`farecap.timeseries`), one query per day

```
$select=fare_class_category,sum(ridership),sum(transfers)
$where=transit_timestamp >= 'D' AND transit_timestamp < 'D+1'
$group=fare_class_category
```
Subway `wujg-7c2s` before 2025, `5wq4-mkjj` after; bus `kv7t-n8in` / `gxb3-akrn`;
express bus adds `AND (upper(bus_route) like 'BM%' OR ... 'BXM%' ... 'QM%' ... 'SIM%'
... 'X%')`. Do NOT use `date_trunc_ymd` over a month: ~5 minutes server-side.

### Station x fare class, day-chunked (`collect.grouped_by_day`)

Same as the station query above with a one-day window; ~0.6s/day. The single
sixteen-day version timed out at 120s.

### Farebox actuals (`yg77-3tkj`)

```
$select=month,agency,sum(amount)
$where=scenario='Actual' AND upper(general_ledger)='FAREBOX REVENUE' AND month>='2023-01-01'
$group=month,agency
```

### Fare evasion

```
https://data.ny.gov/resource/6kj3-ijvb.json?$limit=1000     (34 quarters, 2018-Q1..2026-Q2; 2020-Q2 blank)
https://data.ny.gov/resource/uv5h-dfhp.json?$limit=1000     (by trip_type: Express, Local, SBS, Total)
```

### Official daily ridership (`sayj-mze2`)

```
$select=date,count&$where=mode='Subway' AND date>='2023-01-01'&$limit=5000
```
Modes: Subway, Bus, AAR, SIR, LIRR, MNR, BT, CBD Entries, CRZ Entries.

### O-D matrix (`28vm-gjqr`), one typical week of April 2026

```
$select=origin_station_complex_id AS o,destination_station_complex_id AS d,sum(estimated_average_ridership) AS r
$where=month=4 AND day_of_week='Wednesday'
$group=o,d&$order=o,d&$limit=50000&$offset=N
```
~22s per page; 7 days -> 175,910 pairs, 26,194,743 weekly trips. Grouping the O-D
data by station name/lat/long timed out; coordinates come from one hour of the
hourly dataset instead (`od.stations`).
