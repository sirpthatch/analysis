# Handoff

Ad-hoc pulls preserved from earlier sessions once the pipeline superseded them.

## `scoping_2026-09-28/`

The four fixtures recorded during the scoping session, byte-identical to the copies
in `omny_fare_cap.zip`. They were fetched with `transit_timestamp > 'YYYY-09-01'`
(strict inequality), which excludes the 00:00 hour of Sept 1. Re-running that exact
filter live on 2026-09-28 still returned them exactly, so they are correct for the
window they describe; they are simply not the half-open window the code queries.
The live, correct-window pulls are in `tests/fixtures/`.
