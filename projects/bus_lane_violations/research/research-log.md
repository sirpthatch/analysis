# Research log

## 2026-09-25 — the "mobile-camera cliff" is a fiscal-year filing artifact, not a pause

**Status: resolved.** The README flagged this as blocking. It is not real.

The FY2026 file (`9mwx-gamw`) already carries a known spill at its front edge:
21,023 fixed-camera records with `issue_date` in June 2025 (prior fiscal year)
filed under `fiscal_year='2026'`. It turns out the same thing happens at the
**tail** edge. Checked whether April-June 2026 tickets still being processed
when the FY2026 extract was cut show up in the FY2027 file (`pvqr-7yc4`)
instead, even though their `issue_date` falls inside the nominal FY2026
window (Jul 2025 - Jun 2026):

    FY2025 file (m5vz-tzqv): issue_date spans 2024-06 .. 2025-06 (13 months)
    FY2026 file (9mwx-gamw): issue_date spans 2025-06 .. 2026-06 (13 months)
    FY2027 file (pvqr-7yc4): issue_date spans 2026-04 .. 2026-08 (in progress)

Each fiscal-year file runs 13 months of issue dates, one month of overlap-by-design
at the front (the spill) and, it turns out, up to three months of overlap at
the back with the *next* file. Verified with `summons_number`: zero overlap
between the FY2026 and FY2027 files for June 2026 fixed-camera rows (38,342
vs 24,005 rows, 0 shared IDs) — so this is a clean split to merge, not a
dedupe problem. `scripts/reconcile.py` reproduces the check and the merge.

**Corrected Apr-Jun 2026, fixed vs. mobile** (was: mobile falls to 6,175 in
April then **zero** in May and June):

| month | fixed (corrected) | mobile (corrected) | mobile, as originally reported |
|---|---:|---:|---:|
| 2026-04 | 53,489 | 9,164 | 6,175 |
| 2026-05 | 60,374 | 18,848 | 0 |
| 2026-06 | 62,347 | 18,296 | 0 |

Mobile enforcement did not stop. It ran at a normal ~18-19k/month through
June, and 34,697 of those records were sitting in the FY2027 file the whole
time, filed by processing date rather than issue date.

This also revises the "fixed-camera trough" finding, though it doesn't kill
it: June's true total (62,347) is *higher* than May's (60,374), continuing
the recovery rather than plateauing under it as the uncorrected series
suggested. The Sept-to-Feb decline (77,036 -> 31,350) is untouched by this —
neither FY2025 nor FY2027 have any records in that range — so that part of
the trough is still real and still unexplained.

**Corrected FY2026 grand total: 978,781** (was 941,312 — mobile alone is
undercounted by 34,697 in the shipped `data/` CSVs). See
`data/monthly_by_category_fy2026_corrected.csv` and
`scripts/reconcile.py`.

**New trap for the README:** the "June 2025 spill" is not a one-off edge
case, it's how these files work. Every DOF fiscal-year extract likely spills
into the *next* year's file at its tail by the same mechanism (processing
lag), for as long as the next file is still "in progress." Any monthly series
that touches the last ~3 months of a fiscal year needs the following year's
file merged in, filtered back down to the issue-date window, before you trust
the shape.

**Not yet checked:** whether FY2025 has the same forward spill into FY2026
for Apr-Jun 2025 (i.e., whether the *already-published* FY2026 file's own
Jul-Sep 2025 numbers are themselves still missing a late-filed tail from
FY2025). Do this before quoting the Jul-Sep 2025 figures as final.

## 2026-09-25 — MTA ACE violations (`kh8p-hcbm`): profile, repeaters, map, decay

Worked through `research/directions.md`. Everything is in
`notebooks/ace_violations.ipynb`; the map is `out/ace_violations_map.html`. Data
pulled with `scripts/fetch_ace.py` (6,532,000 rows, 2019-10-07 to 2026-08-23)
and route geometry with `scripts/build_routes.py` (MTA static GTFS, 24 Aug 2026).

**Unit.** ACE cameras ride on buses, so there is no fixed camera ID. The route
is the "camera"; its activation date is its first issued ticket.

**Traps found in this file**
- *Pending tail.* Issued tickets stop around 17 Jun 2026 while rejected events
  continue to 23 Aug. The tail is pending notices, not a pause. Series cut at
  2026-06-08.
- *Technical outage, 23 Mar – 26 Apr 2026.* `TECHNICAL ISSUE/OTHER` rejections
  rose from ~5k to ~50k a week; issued tickets fell to 290 in the week of 13 Apr.
  This probably explains part of the April collapse in the DOF mobile-camera
  series too.
- *Bus-lane surge, 26 Jan – 15 Feb 2026.* 15 bus-lane tickets on 25 Jan, then
  roughly triple the normal daily count on every major route for three weeks.
  Looks like snow — **unverified**. It likely explains the DOF February 2026
  mobile-over-fixed inversion.
- *Route ID changes.* BX38's tickets move to `BX28-BX38` from Jan 2026. `Q44?+`
  (3 rows) is a typo for Q44+. Q6 has no shape in the current GTFS.

**Findings**
- 3.54M of 6.53M events became tickets (54%). Bus-stop and double-parking
  enforcement (from Jun 2024) now dominate: bus-lane tickets were only 15% of
  2025's 1.57M.
- Top 3 routes (M15+, BX19, M101) hold 26% of tickets, top 10 hold 58%. 100 of
  4,325 stops take 30%. The BX12+ stops at Broadway/Isham St and W 207 St/10 Av
  lead with ~32k tickets each.
- 57% of plates were ticketed once. Plates with 3+ tickets are 25% of plates
  and 62% of tickets. The chance of another ticket rises steeply only up to the
  third ticket (43% → 58% → 65%, then flattens), so **repeat offender = more
  than 2 prior tickets**.
- Heavy repeaters (11+ tickets, 35.6k plates) are corridor-loyal: a median 64%
  of their tickets fall on one route, against 17% for random tickets. They also
  keep a clock: 67% of same-route repeat gaps fall within ±3h of a whole day
  (25% by chance), with echoes at 7 and 14 days. 36% put half their top-route
  tickets in one 2-hour window, against 4% when hours are shuffled within the
  route. The top 15 plates are ~100% weekday daytime, which suggests work
  vehicles.
- Decay, all-types routes (n=19): the median route is at 70% of its launch level
  by week 24 and 50% by week 52, then flat to about week 80. That is a floor, not
  zero. Repeat share is ~35% from week 6 onward and doesn't fall. Lane-only
  routes' rising repeat share (9% → 25%) is mostly citywide priors accumulating.
- Step drops, not decay: S46 (Feb 2025), M60+ (early 2025), Q54 (Dec 2024).
  Cause **unverified**.

## 2026-09-25 — outage cost estimate: ~108,500 tickets, ~$14.2M face value

Follow-up on the 23 Mar - 26 Apr 2026 technical-rejection outage (above).
`scripts/estimate_outage_revenue.py` reproduces this.

**Fine schedule** (MTA/NYC DOF, per plate within a trailing 12 months):
$50 / $100 / $150 / $200 / $250 for the 1st through 5th+ ACE violation, same
schedule across all three violation types. Sourced from mta.info and nyc.gov
- see script docstring for links.

**Shortfall:** baseline daily rate (the 5 weeks before + 6 weeks after the
outage, which agree within 2%, so a flat baseline is reasonable) implies
157,678 tickets expected over the 35-day outage; 49,135 were actually issued.
**Shortfall: ~108,500 tickets** - 50,194 double-parking, 40,668 bus-stop,
17,680 bus-lane (baseline type mix applied).

**Revenue:** applying each plate's own trailing-12-month offense count (same
basis as the fine schedule) to the baseline weeks gives an average fee of
$130.38/ticket. **Estimated lost face value: ~$14.2M** (range ~$12.7M-$15.6M
for a +-10% error band on the shortfall itself). Floor if every lost ticket
were a first offense: $5.4M.

**This is face value of tickets never issued, not lost collected revenue.**
Not adjusted for dismissal rate, non-payment, or adjudication - the same
ceiling the README names for the DOF data. Also assumes the outage suppressed
tickets uniformly rather than some events being un-rejectable and simply
delayed past `END`; not checked.

## Still open (unchanged from README)

- The Sept 2025 -> Feb 2026 fixed-camera trough itself: real, cause unknown.
- The activation decay-curve experiment (Jan 2026 corridors from the NY1
  piece): not started, needs camera locations matched to those routes.
- `queries/soql.md` §7 (repeat-offender / fleet cut): untested.
