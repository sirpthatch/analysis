# Initial setup spec

What was built on 2026-09-28, and the decisions behind it. Reference for whoever picks
this up; `CLAUDE.md` is the brief, this is the rationale.

## Scope

Quantify the rider population affected by the January 2026 retirement of the 30-Day
Unlimited MetroCard, given that its replacement — the automatic fare cap — is weekly
only. Hook: the MTA chair said on 2026-09-23 he is open to additional cap tiers.

Out of scope at setup: bus, fare evasion, per-rider modelling. See `research/plan.md`.

## Design decisions

**Server-side aggregation, never bulk download.** `5wq4-mkjj` is 45.2M rows. Every pull
in `farecap.collect` is a `$select` with `$group` bounded by a `$where` on
`transit_timestamp`, returning tens of rows. There is deliberately no code path that
issues a `$group` without a window, because the unbounded version times out.

**Explicit half-open windows.** `constants.WINDOWS` holds `[start, end)` pairs. This
exists because a one-sided `>` filter silently returned sixteen days when two were
intended during scoping. See `research/research-log.md`.

**Reconciliation is a first-class step, not a sanity check.** `fareclass.composition`
queries an independent window total and sets `attrs["reconciles"]`. When the total is
unavailable it sets `None`, never `True` — an unreconciled split must never print as
reconciled. That distinction caught the window error.

**Recorded figures live in `constants.py` and are tested against the live API.** The
project's sourcing standard is that nothing is trusted as current without a fresh
check. `tests/test_verified_figures.py` implements that standard: it re-queries the
recorded numbers. A failure is a restatement to be logged, not an assertion to loosen.

**Verified and unverified are marked separately.** `DATASETS` entries carry
`verified: True/False` depending on whether columns were confirmed by pulling records.
The one unverified *figure* — the retired pass price — is named
`METROCARD_30DAY_FINAL_PRICE_UNVERIFIED` and is unused by any calculation, so it cannot
leak into output by accident.

**The kill condition is code, not judgement.** `stations.dispersion_verdict` returns
`FLAT` or `DISPERSED` against an IQR threshold. Deciding in advance what result would
sink the geography angle prevents talking oneself into a pattern.

## Fixtures instead of live data

The scoping environment had no direct HTTP egress from Python (proxy returned a 403
tunnel failure for `data.ny.gov`). Rather than ship unexercised code, the real pulls —
verified through a separate fetch path — are committed to `tests/fixtures/` and seeded
into the gitignored cache by `src/seed_cache.py`. The analysis then runs fully offline
and reproduces every figure in the research log.

Two fixtures are **deliberately absent** rather than fabricated: the 2024 window
reconciliation total, and the station-level pull. Both are first-run tasks.

## Verification performed at setup

- All modules byte-compile; the notebook re-parses as valid JSON.
- `python src/seed_cache.py && python src/analyze.py --composition --arithmetic` runs
  clean offline and reproduces: MetroCard share 38.189% → 0.451%, unlimited share
  15.22%, monthly-pass share 7.42% (3,979,572 rides), ridership growth +9.8%, 2026
  reconciliation exact, monthly equivalent $151.67 vs ETA $138.00.
- `pytest` collects 11 tests; the one offline test passes (after fixing a rounding
  mismatch it caught in its own assertion).
- The 2024 reconciliation correctly reports `NOT RECONCILED` rather than failing or
  silently passing.

## Known gaps at handoff

Open items 1–9 in `CLAUDE.md`. The two that block publication: the station-level run
(item 1) and the "Other" fare-class buckets (item 4).
