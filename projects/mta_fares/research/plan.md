# Phased plan

## Phase 0 — verify the scaffold (do this first, ~20 min)

1. `python src/collect.py all --refresh` — first live contact. Expect the 2024 and 2026
   fare-class pulls to match `tests/fixtures/`.
2. `pytest tests/` — 11 tests. A failure means the MTA restated; log it, update
   `constants.py`, do not loosen the assertion.
3. `python src/collect.py windows --refresh` — closes the 2024 reconciliation gap
   (open item 2 in CLAUDE.md).

## Phase 1 — the decision query (~30 min)

4. `python src/analyze.py --stations`. Read the verdict.
   - `DISPERSED` → the geography is real. Go to Phase 2.
   - `FLAT` → **stop and rescope.** The piece becomes a short citywide arithmetic note:
     the monthly pass was 7.4% of ridership, it's gone, the effective monthly ceiling is
     $151.67, ETA wants $138. That is publishable at ~800 words. Do not go hunting for
     a subgroup that survives the flatness result.

## Phase 2 — establish the composition claim properly (~2 hours)

5. Read the MTA data dictionary for both ridership datasets; settle the "Other" buckets
   (limitations.md, most likely failure mode).
6. Explain or bound the student-series drop.
7. Decide ridership-vs-transfers denominator; re-run shares.
8. Source the retired 30-Day price from a primary document, or commit to publishing
   without a before/after dollar figure.

## Phase 3 — the map, only if Phase 1 said DISPERSED (~half a day)

9. `stations.station_table(2024)` → choropleth on the lat/long already in the data.
   No external geometry needed for a point map; community-district or NTA aggregation
   would need a boundary file.
10. `stations.join_across_windows()` — check the unmatched-id counts before reporting
    any station-level change.
11. Cross-reference the top-share stations against line and trip-length characteristics
    to see whether the pattern is "long commutes" or something else.

## Phase 4 — write (~half a day)

12. `article/outline.md`. Lead with the retirement, not the arithmetic.
13. Put the flatness verdict in the piece either way — if the geography is flat, saying
    so is more useful than omitting it.

## Explicitly out of scope unless Phase 1–3 land

- Bus (`gxb3-akrn`, `kv7t-n8in`).
- Fare evasion (`6kj3-ijvb`, `uv5h-dfhp`) — different story, adjacent data.
- Any per-rider modelling. See limitations.md; the data cannot support it.
