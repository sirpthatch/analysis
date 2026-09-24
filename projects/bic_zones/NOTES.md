# Notes, limitations, and open questions

## Things I could not verify

- **BIC's waiver numbers are not in Open Data**, but there is now a verified proxy for the "about
  half exempted" claim. The ~312-this-year and 572-last-year figures still come only from BIC's
  statement to Streetsblog on 2026-09-21 — no waiver-grant dataset exists. But
  `n84m-kx4j` (BIC's active fleet roster, checked 2026-09-24) reports **53.0% of the 7,106
  active licensed vehicles (3,766) as `vehicle_has_side_guard = No`** — self-reported via the
  Vehicle Management Portal, not BIC-audited, and silent on *why* (waiver vs. plain
  non-compliance), but it's an Open Data figure that lands almost exactly on BIC's ~50% claim.
  Worth citing alongside the agency statement, not instead of it. **FOIL the waiver list** —
  request grant date, licensee, BIC number, vehicle identifier and stated grounds, 2022 to
  present — is still the single highest-value follow-up, because it's the only way to confirm
  the "No" vehicles are waivered rather than simply out of compliance, and to date the waivers
  precisely enough to line up against the 47-violation count.
- **`867j-5pgi` structure.** 694,957 rows for a few hundred licensees plus an `export_date`
  column implies daily snapshots, but the `count(distinct bic_number)` query timed out and I did
  not retry it. Confirm before using it as a roster.
- **CWZ's original safety promise.** I have the *rationale* ("improve safety and reduce vehicle
  miles traveled," per advocates in the Waste Dive piece) but not a quantified projection from
  DSNY. The implementation plan appendix and the Safety Task Force minutes in `SOURCES.md` are
  unread and are the likely home of a number you can hold the city to. Without one, the post can
  only say crashes are flat, not that the city missed a target.
- **Prior art.** The Center for New York City Affairs piece and the Waste Dive first-year
  retrospective are unread — titles and dates only. The Council oversight hearing from
  2025-04-23 is unread and probably the richest unexploited source here.

## Data limitations to disclose in anything published

1. **The crash feed is stale and the news crash is not in it.** `h9gi-nx95` last refreshed
   2026-07-31, maximum `crash_date` 2026-06-11. The 2026-09-18 death that prompted this week's
   coverage does not appear. Refuse-vehicle rows in `bm4k-52h4` thin out even earlier, around
   2026-05-04. Say this in the first few paragraphs, not a footnote.
2. **Municipal versus private is the crux, and it cannot be solved with a join — checked
   2026-09-24.** `vehicle_type` lumps DSNY trucks with private carters; only carters are zoned.
   Tried `vehicle_make`/`state_registration` in the crash Vehicles table (`bm4k-52h4`) as a
   heuristic — dead end, both fleets run heavily on the same chassis makes and
   `state_registration` is only the issuing state, never a plate. Also checked BIC's
   **Licensees and Registrants Fleet Information** dataset (`n84m-kx4j`, 7,106 active
   BIC-licensed vehicles) as a possible join target: it has no VIN and its only identifier,
   `bic_plate_number`, has no counterpart anywhere in the crash data, which carries no plate or
   VIN field at all. **No row-level join is possible between any crash dataset and any BIC fleet
   dataset.** See `data/datasets.md` #6 for the full writeup.
   What `n84m-kx4j` *does* give: the licensed private fleet's own make distribution as an
   aggregate cross-check, not a per-crash identifier — Kenworth (21.0%) edges out Mack (19.2%) in
   the licensed fleet, while Mack is 61.2% of refuse-vehicle crashes. That mismatch is suggestive
   that much of the Mack-heavy crash count is DSNY's own fleet, not private carters, but it's
   composition evidence, not proof. The honest move is still to report citywide flatness and the
   enforcement finding as primary, and leave the zone comparison as a caveated secondary exhibit.
3. **Free-text vehicle typing.** `vehicle_type` is officer-entered. Undercounting is certain and
   its rate may itself drift over time, which would masquerade as a trend. Check whether the share
   of blank/other vehicle types changed across the study window before attributing anything to
   policy.
4. **Small numbers.** ~400 refuse-vehicle crashes a year citywide, split 20 ways. Pool phases 1-3.
5. **Only three zones have a usable post-period.** Queens Central (17 months), Bronx East and West
   (6 months), Brooklyn South and Queens Northeast (3 months). Lower Manhattan went live 10 days
   before the data ends. Everything else is control by default, which is convenient but means the
   comparison rests heavily on one zone.
6. **Zone boundaries follow community districts**, so crashes on boundary streets are ambiguous.
   The `multipolygon` join handles this mechanically but does not make the assignment meaningful
   for a crash at a zone edge.
7. **Exposure is unmeasured.** Crash counts without truck-miles are counts, not rates. If DSNY
   publishes tonnage or route data per zone post-implementation, that is the denominator the
   analysis actually wants. The Council hearing on implementation data is the place to find out
   whether it exists.
8. **The 1,662 trap.** Do not report `%SIDE GUARD%` matches as side-guard enforcement. See
   `data/datasets.md` section 4 and `data/bic_sideguard_rules.csv`. The mandate-specific count is
   47. My own arithmetic across the 15 rule texts reconciles to 1,662 exactly, which is a good
   sign the classification in that CSV is right, but read the rule texts yourself before quoting.

## Reporting to do

- BIC: how many trade waste vehicles are in the licensed fleet, how many have guards, how many
  waivers are active right now (as opposed to ever granted), and what review a waiver gets after
  it is issued. The 2023 waiver on the truck that killed Daria Serkov was three years old.
- DSNY: what safety outcomes it committed to measure per zone, and whether it has measured them.
- The Council sanitation committee: Abreu was already pressing DSNY on implementation data in
  April 2025. Ask what he got.
- Side-guard manufacturers or fleet operators on whether "toolboxes and fuel tanks on the
  sidewalls" is a genuine engineering obstacle or a paperwork one. BIC's justification is
  checkable against industry practice.

## A smaller story sitting inside this one

The crash datasets have gone quiet — last refresh 2026-07-31, data stopping 2026-06-11. Vision
Zero reporting depends on that feed. A three-month gap in the city's crash data is arguably its
own short post, and it is the kind of thing that is easier to publish while you have the freshness
figures already in hand (`data/verified_figures.csv`).
