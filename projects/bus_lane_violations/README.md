# NYC bus lane camera enforcement, FY2026

Working bundle for the third idea in the 25 Sep 2026 research brief:
*"Half of all bus lane camera tickets come from about a dozen cameras."*

Everything in `data/` was pulled from the live NYC Open Data API on
**2026-09-25** and reconciles against the dataset's own totals. Nothing here
is estimated or carried over from anywhere else. `scripts/explore.py` re-derives
every figure below from the CSVs, offline, in about a second — run it first.

---

## The dataset

**Parking Violations Issued – Fiscal Year 2026**, four-by-four `9mwx-gamw`,
NYC Department of Finance. Created 2026-08-18, which is what makes this idea
possible now and not last month: it is the first full fiscal year covering the
big ACE expansion.

- Landing page — https://data.cityofnewyork.us/d/9mwx-gamw
- Metadata and full column list — https://data.cityofnewyork.us/api/views/9mwx-gamw.json
- Rows — https://data.cityofnewyork.us/resource/9mwx-gamw.json

Columns used: `issue_date`, `violation_code`, `violation_description`,
`street_name`, `violation_county`, `issuing_agency`, `plate_id`,
`registration_state`, `vehicle_body_type`, `fiscal_year`.

Sibling years, same schema, confirmed present in the catalog:
FY2025 `m5vz-tzqv`, FY2027 (in progress, last updated 2026-09-17) `pvqr-7yc4`.

## The news hooks

- DOT is extending and rebuilding the **Livingston Street busway** in Downtown
  Brooklyn this fall, explicitly to deal with illegal parking in the bus lanes
  — Streetsblog, 22 Sep 2026.
  https://nyc.streetsblog.org/2026/09/22/a-better-busway-is-coming-to-livingston-street-in-downtown-brooklyn
- The **MBTA turns on camera enforcement 1 Oct 2026**, which is the comparison
  that earns this piece a readership beyond New York — Streetsblog Mass,
  24 Sep 2026.
  https://mass.streetsblog.org/2026/09/24/mbta-board-meeting-recap-bus-lane-enforcement-cameras-go-live-next-week
- Four more routes got ACE cameras in **January 2026** — NY1, 9 Jan 2026.
  https://ny1.com/nyc/brooklyn/news/2026/01/09/mta-continues-crackdown-on-bus-lane-parking-with-more-cameras
- Prior art to stay off: Gothamist already did enforcement-versus-bus-speeds.
  https://gothamist.com/news/the-mta-bus-routes-that-slowed-down-after-cameras-began-ticketing-double-parked-drivers

---

## What the data says

**941,312** bus-lane-related violations in FY2026, across four categories that
are easy to mistake for one:

| category | count | what it is |
|---|---:|---|
| `BUS LANE VIOLATION` | 715,363 | fixed roadside ACE camera |
| `MOBILE BUS LANE VIOLATION` | 198,405 | bus-mounted ACE camera |
| `18-No Stand (bus lane)` | 27,507 | officer-issued |
| `No Standing Bus Lane` | 37 | officer-issued, variant label |

This table is the shipped, as-pulled data and is now known to undercount —
see below. The corrected total is **978,781** (mobile alone is short 34,697
records, filed under the FY2027 extract instead). The concentration and
location figures below are unaffected — they're fixed-camera-only and the
correction is almost entirely on the mobile side.

**The concentration is the story.** Of the 715,363 fixed-camera tickets, the
top 3 locations account for 23.0 percent, the top 12 for 49.8 percent, and the
top 60 for 87.2 percent. Four Jamaica Avenue locations alone produce 163,409
tickets — 22.8 percent of citywide fixed-camera enforcement. This is not a
citywide enforcement regime; it is a dozen cameras.

**One anomaly resolved, one still open — see `research/research-log.md`.**

*The mobile-camera cliff — RESOLVED, it's not real.* Mobile records climb
through the winter, invert the usual ratio in February 2026 (39,239 mobile
against 31,350 fixed), then the shipped CSVs show a collapse to 6,175 in
April and **zero** for May and June. That collapse is a filing artifact, not
an enforcement pause: DOF's fiscal-year files split by processing date, not
`issue_date`, and April-June 2026 tickets still being processed when the
FY2026 extract (`9mwx-gamw`) was cut got filed under FY2027 (`pvqr-7yc4`)
instead — same mechanism as the known June-2025 spill, just running forward
off the other end. Corrected: April 9,164, May 18,848, June 18,296 — mobile
enforcement never stopped. `scripts/reconcile.py` verifies zero
`summons_number` overlap between the two files and rebuilds the merged
series at `data/monthly_by_category_fy2026_corrected.csv`. **Corrected
FY2026 grand total: 978,781**, not 941,312.

*The fixed-camera trough — still open.* Fixed-camera tickets fall from 77,036
in September 2025 to 31,350 in February 2026. That part is untouched by the
correction above (neither adjacent file has records in that range) and is
still unexplained. The "recovery" side revises upward, though: June's
corrected total (62,347) is *higher* than May's (60,374), so it reads as
continued recovery, not a plateau.

## The actual experiment

The decay curve is the second half of the piece, and the shipped example is
**not** it. `data/monthly_w14th_at_5th_fy2026.csv` is a shape demonstration
only — 14th Street has had cameras for years, so its curve is not an activation
response. For a clean before-and-after inside a single file, use the four
routes MTA activated in January 2026 (see the NY1 link) and pull their camera
locations with `scripts/fetch.py --rows`.

The question: after cameras switch on, does ticket volume decay toward zero
(deterrence) or fall and plateau well above it (a toll drivers have decided to
pay)? The 14th Street shape suggests the latter, which is the more interesting
answer and the one Boston would want to hear on 1 October.

---

## Traps

These cost me time; they will cost you the same time if the README does not
say so.

**`street_name` is truncated to 20 characters.** `NB WOODHAVEN BLVD @` has lost
its cross street entirely. Distinct cameras at one intersection can collapse
into a single string and one camera can appear under variants. Normalise before
publishing any leaderboard — the ranks in `data/top_locations_*.csv` are raw and
un-deduped on purpose, so you can see the problem rather than inherit it.

**The two camera types use different case conventions in the same column.**
Fixed rows are `WB JAMAICA AVE @ 149`; mobile rows are `SB University Ave @`.
Group across both categories without `upper()` and every shared location splits
in two. Same applies to `violation_description` — always match with `upper()`.

**`violation_county` runs four coding schemes at once**, varying by category:
`QN`/`MN`/`BK`/`BX`/`ST` for fixed cameras, `NY`/`Q`/`BX`/`K`/`R` for
officer-issued, and spelled-out `Bronx`/`Kings`/`Qns` for the legacy label.
Worse, **51,077 mobile-camera rows carry no borough at all** — 26 percent of
all mobile records. Do not drop them silently.

**The file contains a June 2025 spill**: 21,023 fixed-camera records dated
before FY2026 began. Filter on `issue_date`, never trust `fiscal_year`. This
is not a one-off: the same mechanism spills forward too — see
`research/research-log.md` (2026-09-25). Any monthly series touching the
last ~3 months of a fiscal year needs the *next* year's file merged in and
re-filtered to the issue-date window before you trust its shape.

**These are tickets issued, not upheld.** Dismissal rates differ by corridor,
and adjudication status lives in a separate DOF dataset. And a ticket count is
jointly determined by how many drivers violate and whether the camera was
working — which this data cannot separate. That is the honest ceiling on the
argument.

---

## Layout

    README.md
    queries/soql.md              every query, annotated, copy-pasteable
    data/
      violation_categories_fy2026.csv
      monthly_by_category_fy2026.csv
      monthly_by_category_fy2026_corrected.csv   Apr-Jun spillover merged in, see log
      top_locations_fixed_fy2026.csv     60 rows, raw, un-deduped
      top_locations_mobile_fy2026.csv    60 rows, raw, un-deduped
      by_county_by_category_fy2026.csv
      monthly_w14th_at_5th_fy2026.csv    shape example, not an activation curve
    scripts/
      explore.py                 offline; re-derives every figure above
      fetch.py                   re-pulls from the API; --fy 2025/2027; --rows
      reconcile.py                verifies + rebuilds the corrected monthly series
      fetch_ace.py                MTA ACE violations (data.ny.gov kh8p-hcbm) -> data/raw/ parquet
      build_routes.py             MTA GTFS -> data/raw/route_shapes.json
    research/
      research-log.md             dated findings, in order, with what's still open
      directions.md               the analysis brief for the MTA ACE work
      handoff/                    original research-idea bundle, unmodified
    notebooks/
      ace_violations.ipynb        MTA ACE: profile, repeat offenders, map, decay curves
    data/ace_*.csv                small derived tables from the notebook
    data/raw/                     row-level pulls, gitignored, re-created by the scripts
    out/                          ace_violations_map.html, decay-curve PNGs

`explore.py` and `reconcile.py` need pandas; `fetch.py` and `reconcile.py`
need requests, and an optional free app token in `SODA_APP_TOKEN` to avoid
the anonymous rate limit. `python3 -m venv venv && source venv/bin/activate
&& pip install -r requirements.txt` sets up everything this project needs.

## Still unverified

The repeat-offender query in `queries/soql.md` (§7) is a starting point I did
not run. The January 2026 activation corridors are named in the NY1 piece but I
have not matched them to camera locations in the data. The Sept-Feb
fixed-camera trough is still unexplained (see `research/research-log.md`).
And whether FY2025 spills forward into FY2026 the same way FY2026 spills into
FY2027 has not been checked — if it does, the already-published Jul-Sep 2025
figures above may themselves be a slight undercount.
