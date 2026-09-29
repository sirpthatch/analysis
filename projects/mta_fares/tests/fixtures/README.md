# Fixtures — live pulls from the first networked run, 2026-09-28

Verbatim Socrata responses for the half-open windows in `constants.WINDOWS`
(`[YYYY-09-01, YYYY-09-17)`). They exist so the analysis can run and be tested
without network access, and as a committed record of what the API said on that
date (`data/raw/` is gitignored).

Seed them into the cache with:

    python src/seed_cache.py

Then `python src/analyze.py --composition --arithmetic --stations` runs offline.

| Fixture | Query | Check |
|---|---|---|
| `fare_class_2024.json` | fare class x ridership, 2024 window, `wujg-7c2s` | sums to window total |
| `fare_class_2026.json` | fare class x ridership, 2026 window, `5wq4-mkjj` | sums to window total |
| `window_total_2024.json` | count/sum/min/max, 2024 window | 53,643,755 |
| `window_total_2026.json` | count/sum/min/max, 2026 window | 58,904,420 |
| `station_fare_class_2024.json` | station x fare class, 2024 window, day-chunked | sums to window total |
| `station_fare_class_2026.json` | station x fare class, 2026 window, day-chunked | sums to window total |
| `fair_fares.json` | all 92 months of `3tw8-6si8` | — |

The daily fare-class series, bus series and O-D matrix (`data/raw/daily*/`,
`data/raw/od/`) are too large to commit; rebuild them per
`research/additional-questions.md` § Reproduce.

## The scoping-session fixtures

The pulls recorded during scoping (no network egress, fetched by a separate path)
are preserved unchanged in `research/handoff/scoping_2026-09-28/`. They used
`transit_timestamp > 'YYYY-09-01'`, which drops the 00:00 hour of Sept 1, so they
do **not** match `constants.WINDOWS`. See `research/research-log.md`, 2026-09-28
first networked session.
