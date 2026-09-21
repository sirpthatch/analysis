# Can TrafficStat replace the Socrata crash data for the June–September 2026 gap?

Tested 2026-09-21. **Verdict: viable as a geographic and temporal substitute,
not as a schema-level replacement.** It reconstructs where and when, plus coarse
what. It cannot reconstruct why, how badly, or on what street.

## The gap

Socrata `h9gi-nx95` stops at **2026-06-11**. TrafficStat's YTD window runs
**2026-01-01 → 2026-09-13**, so it spans the gap *and* overlaps five months where
Socrata is still live. That overlap is what makes validation possible rather than
a leap of faith.

TrafficStat would fill **20,625 collisions** for 2026-06-12 → 2026-09-13.

## Validation on the overlap — this is the strong part

Citywide, Jan 1 – Jun 11, both sources live:

| | Collisions |
|---|---|
| Socrata | 36,424 |
| TrafficStat | 36,839 |
| **Ratio** | **1.011** |

Month by month: 0.995, 1.001, 0.996, 0.995, 0.989. Within ±1.1% throughout.

Independent corroboration: Hoodline (Sept 10) reported NYPD had logged **54,832**
crashes through Aug 30 against 36,195 visible in the public file. TrafficStat
through Aug 30 gives **54,730** — 0.2% off a figure derived from an entirely
separate route. Two independent sources agreeing this closely is strong evidence
TrafficStat measures the same universe of events Socrata does.

**On counts, TrafficStat is a valid substitute.** Anything that is fundamentally
a count — collisions per week, per precinct, per borough, trend lines, before/after
comparisons — can be carried across the gap with confidence.

## Field-by-field: what survives reconstruction

Socrata carries 29 columns. Pulling one TrafficStat metric at a time and joining
on (lat, lon, timestamp) recovers a subset. The join itself works well: injury
and pedestrian subsets matched **100%** onto the base collision set, and the key
is **99.8% unique** (4 collisions of 1,748 shared a key).

| Socrata field | Recoverable? | Notes |
|---|---|---|
| `crash_date` | **Yes** | Full fidelity |
| `latitude` / `longitude` | **Yes** | But see geocoding below |
| `borough` | **Yes** | Derivable from precinct |
| `crash_time` | **No, for the gap** | ~95% present Jan–Apr, 12.5% in May, 0% from June on |
| `number_of_persons_killed` | **Partial** | Fatality flag joins; not a count |
| `number_of_*_injured` | **Flag only** | "Total Injuries" returns one row per *collision*, ratio 1.00 vs Socrata's true 1.35. Counts are unrecoverable. |
| pedestrian / cyclist / occupant **injury by mode** | **Flag, validated** | Mode metrics are injury subsets; rates match Socrata to 0.1pp on matched months (see below) |
| `vehicle_type_code_1..5` | **Coarse** | Collisions+ metrics give Car/SUV/E-bike/Moped/Scooter/Motorcycle/Off-Road involvement flags |
| `contributing_factor_vehicle_1..5` | **No** | Not exposed anywhere |
| `on_street_name` / `cross_street_name` / `off_street_name` | **Reconstructable** | Nearest CSCL centerline segment — 88.2% agreement with the recorded name. See `research/reverse_geocoding.md`. |
| `zip_code` | **Reconstructable** | From CSCL per-side ZIP — 95.0% agreement. See `research/reverse_geocoding.md`. |
| `collision_id` | **No** | No stable record identifier — reconstructed rows cannot be linked back to Socrata rows, or deduplicated against them once Socrata unfreezes |

Collision type *is* available (`REAR END`, `SIDE SWIPE`, `HEAD ON`, …) — a field
Socrata doesn't carry — but it is **`UNKNOWN` on 51%** of records.

## The attribute flags validate almost exactly

The mode metrics are **subsets of injury collisions, not of all collisions** —
each is 100% contained in "Injury Collisions" by set-containment test. `Col
Pedestrian` means a pedestrian was *hurt*, not that one was involved. Naming them
`pedestrian_involved` would have overstated them by a factor of six. Together the
seven modes pulled cover 94% of injury collisions; the rest are modes not pulled
(Other MV, Stand-up Scooter, Off Road, Other Device).

That makes them the direct analogue of Socrata's
`number_of_pedestrians_injured` / `number_of_cyclist_injured` — same structure,
as booleans rather than counts.

Measured over the **same months** (Jan 1 – Jun 11), which separates method bias
from seasonality:

| Rate | TrafficStat | Socrata |
|---|---|---|
| Injury collisions | 41.9% | 41.8% |
| Pedestrian injured | 10.2% | 10.2% |
| Cyclist injured | 4.2% | 5.2% |
| Cyclist **+ e-bike** | **5.3%** | **5.2%** |

The apparent cyclist shortfall is a classification difference, not an error:
**Socrata folds e-bike riders into `number_of_cyclist_injured`; TrafficStat
reports them separately.** Adding the two reconciles to within 0.1 points. This
is a place where the reconstruction is *more* granular than the dataset it
replaces.

Within TrafficStat, pre-freeze against gap: injury 41.9% -> 47.2%, cyclist
4.2% -> 7.8%, pedestrian 10.2% -> 9.3%. Those are summer seasonality, measured
against a like-for-like baseline rather than assumed.

## Four things that constrain the result

1. **Time of day is gone for the gap period entirely.** Measured by month, the
   share of TrafficStat records carrying a real hour is ~95% for January–April,
   12.5% for May, and **0% from June onward**. The citywide average of ~76%
   present is misleading: all of it sits before the freeze. Hour-of-day analysis
   of the reconstructed period is impossible.

   The field degraded about a month before the public file stopped updating.
   That ordering is the one quantitative lead the data offers on the freeze.
2. **Coordinates are a different geocode, not the same numbers.** Matching
   TrafficStat to Socrata on the overlap: 4.3% agree at 6 decimals, 35.8% at 5,
   66.3% at 4 (~11 m). The two systems geocode independently. Consequence:
   TrafficStat is internally consistent and fine for density, hotspots and
   aggregation, but a reconstructed series spliced onto Socrata history will
   contain a **geocoding discontinuity at June 11**. Do not present the joined
   series as a single homogeneous dataset without saying so.
3. **TrafficStat reports zero null coordinates; Socrata reports 3.1%
   ungeocodable.** TrafficStat is placing crashes Socrata would honestly decline
   to place. Its own documentation says unmappable collisions may be mapped to
   the precinct house. Checked for this: the top 100 points hold 3.9% of
   TrafficStat collisions against 3.2% for Socrata — mildly elevated, consistent
   with genuine bad intersections rather than stationhouse dumping. Not a
   disqualifier, but the ~3% has to go *somewhere* and it is not flagged.
4. **No stable record ID.** When Socrata unfreezes, reconciling or de-duplicating
   a reconstructed period against the official rows will be fuzzy spatial-temporal
   matching, not a key join.

## Recommendation

**Build it for counts and geography; label it clearly; don't overstate it.**

Defensible uses:
- Collision and fatality counts by precinct, borough, week, mode, across the gap
- Density, hotspot and cluster geography at neighborhood scale
- Trend continuation and before/after comparisons on counts

- Street- and ZIP-level aggregation, via reverse geocoding (added 2026-09-21;
  88.2% / 95.0% agreement — see `research/reverse_geocoding.md`)

Not defensible:
- Injury severity (counts unrecoverable)
- Contributing-factor analysis (field absent everywhere)
- Hour-of-day (absent entirely for the gap period)
- Anything requiring metre-accurate position, or a clean splice onto Socrata
  coordinates without disclosure

Treat it as **a parallel NYPD-sourced series that happens to agree with Socrata
to 1.1% on the overlap**, published alongside its validation, rather than as
"the missing Socrata rows." That framing is both more honest and more
interesting — the fact that the city's own dashboard has been showing current
data this whole time, while the public file sat frozen, is itself reportable.

## Reproducing

```bash
cd src
python collect.py all          # Socrata + TrafficStat + CSCL
python reconstruct.py metrics  # attribute pulls, cached per metric
python reconstruct.py build    # -> data/processed/reconstructed_crashes_gap.csv
python reconstruct.py validate # the overlap comparison above
```

Saved: `data/raw/trafficstat_ytd_collisions.csv` (57,465 rows, pulled 2026-09-21),
`data/raw/socrata_crashes_2026.csv` (36,424 rows).
