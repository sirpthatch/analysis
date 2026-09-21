# Reconstructing NYC's Missing Crash Data

Project brief. Read this first, then `research/socrata_gap_reconstruction.md`
for the viability assessment that defines the project's scope, and
`research/limitations.md` for what the result cannot support. `README.md`
describes the layout and how to run the pipeline.

Repurposed on 2026-09-21 from an earlier bridge-strike investigation; that
material was moved to `../truck_routes_archive/` and is not part of this project.
The directory was renamed from `truck_routes` to `reconstruct_crashdata` the
same day.

## The story

New York City's public crash file (`h9gi-nx95`) — the dataset behind most
street-safety reporting in the city, and the basis for Vision Zero analysis —
**stopped updating on 2026-06-11**. NYPD did not stop recording crashes. The
public simply stopped being able to see them.

Meanwhile NYPD's own TrafficStat dashboard has been publishing current data the
entire time, through an undocumented JSON API with no key and no authentication.
As of 2026-09-21 it was current through 09/13/26.

The project reconstructs the missing three months from that source and validates
it against the period both cover. **The headline is not just the reconstruction —
it is that the data existed and was publicly reachable the whole time.**

## What the validation established

Over Jan 1 – Jun 11, where both sources are live:

| | Collisions |
|---|---|
| Socrata | 36,424 |
| TrafficStat | 36,839 |
| **Ratio** | **1.011** |

Monthly ratios: 0.995, 1.001, 0.996, 0.995, 0.989 — within ±1.1% throughout.

Independent corroboration: Hoodline reported (Sept 10) that NYPD had logged
**54,832** crashes through Aug 30 against 36,195 visible publicly. TrafficStat
through Aug 30 gives **54,730** — 0.2% off a figure derived by a completely
separate route. Two independent paths agreeing this closely is the strongest
evidence available that TrafficStat measures the same universe of events.

**The gap: 20,625 collisions, 2026-06-12 to 2026-09-13.**

## What can and cannot be reconstructed

| Field | Status |
|---|---|
| date, lat/lon, precinct | Full fidelity |
| collision type | Available, `UNKNOWN` on ~51% (NYPD's own gap) |
| street name | Reconstructed from CSCL — **88.2%** agreement |
| ZIP | Reconstructed from CSCL — **95.0%** agreement |
| injury, fatality, per-mode injury | **Boolean flags**, never counts. Validated to within 0.1pp of Socrata on matched months |
| time of day | **Lost for the gap period.** Present for ~95% of Jan–Apr, 12.5% of May, **0% from June on** |
| injury/fatality **counts** | **Lost.** TrafficStat returns one row per collision (ratio 1.00 vs Socrata's true 1.35) |
| contributing factors | **Lost.** Not exposed anywhere |
| `collision_id` | **Lost.** No stable record identifier |

## Standing cautions

- **Coordinates are a different geocode, not the same numbers.** On the overlap,
  only 4.3% agree with Socrata at 6 decimals, 35.8% at 5, 66.3% at 4 (~11 m). A
  reconstructed series spliced onto Socrata history carries a geocoding
  discontinuity at June 11. Say so rather than presenting one seamless series.
- **Frame it as a parallel NYPD-sourced series that agrees to 1.1%**, published
  alongside its validation — not as "the recovered Socrata rows."
- **TrafficStat is an undocumented internal API.** It can change or vanish. Everything
  pulled is archived under `data/raw/`; re-verify before relying on a fresh pull.
- **No stable record ID** means that when Socrata unfreezes, reconciling this
  against the official rows is fuzzy spatial-temporal matching, not a key join.

## What still needs to be done

1. ~~**Establish why the file froze.**~~ **Largely answered** — see
   `research/the_freeze.md`. OTI (not NYPD) runs the portal; the stated cause is
   an in-progress rebuild of the automated upload process, with on-record quotes
   from OTI and DOT. The August fix deadline passed and the note is unchanged as
   of 2026-09-21. Still open: why a migration took three months, why the dataset
   wasn't flagged until ~July 31, and whether anyone will say more.
2. **Check whether the freeze has ended** — `python collect.py freshness` says so
   immediately, and it changes the framing if it has.
3. **Compare the gap period against the pre-freeze period** (research question 5):
   is there a trend, spike or geographic shift the public could not see?
4. **Hand-verify a sample of reconstructed records** against any independent
   account — news reports of specific fatal crashes are the obvious check.
5. **Quantify the geocoding discontinuity** well enough to state it precisely in
   a writeup, rather than as "coordinates differ."
6. **Cross-check against Vision Zero View**, which reporting says runs on raw
   NYPD data and stayed current through July 31 while Open Data was frozen. A
   third independent source agreeing with the reconstruction would make it very
   hard to dispute. Highest-value verification left.
7. **Chase the May time-of-day degradation** (`limitations.md` #7) — 95%
   populated through April, 12.5% in May, 0% from June. It precedes the
   publishing freeze by a month and nobody has reported it. This is the project's
   one genuinely novel finding; establish whether the two share a cause.
8. **Fatality reconciliation.** NYPD publishes fatality counts separately; the
   150 YTD fatalities TrafficStat returns should be checkable against them.
9. **Pull the four remaining injury modes** (Other MV, Stand-up Scooter, Off
   Road, Other Device) to take mode coverage of injury collisions from 94% to
   ~100%. Roughly 25 minutes of requests; `reconstruct.py metrics` handles it
   once they're added to `ATTRIBUTE_METRICS`.

## Style / sourcing standard to maintain

Every dataset ID, column name, and statistic must be independently re-verified
against the live endpoint or the dataset's metadata — don't trust anything in
`data/` as still-current without a fresh check. Flag anything you cannot verify
rather than dropping it silently. SoQL text comparisons wrap in `upper()`.

Two lessons from building this, both of which produced plausible wrong numbers
rather than errors, and both worth remembering before trusting any rate:

- A categorical flag can be accurate and still not mean what its name suggests.
  Check what the "no" rows actually are.
- A string-normalization bug fails silently and biases a match rate downward.
  `ROAD -> RD` without a word boundary turns BROADWAY into BRDWAY; a leading-digit
  strip turns `64 RD` into `RD`. Spot-check the normalizer on real values before
  believing the aggregate.
