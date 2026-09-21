# Running list of what the reconstruction cannot support

Kept current as work proceeds. Anything that can't be verified gets flagged here
rather than dropped silently.

## Fields that are permanently lost

1. **Injury and fatality counts.** TrafficStat's map report returns one row per
   *collision*, not per injured person — measured ratio 1.00 against Socrata's
   true 1.35 injuries per injury crash. The reconstruction carries boolean
   `injury` / `fatality` flags and per-mode injury flags. Any claim about injury
   *severity*, or about totals of injured people, is unsupported. Counts of
   injury *collisions* are fine.

   The mode flags are subsets of injury collisions, not general involvement:
   `injured_pedestrian` means a pedestrian was hurt. They validate to within
   0.1pp of Socrata on matched months, and cover 94% of injury collisions — the
   remaining 6% are modes not pulled (Other MV, Stand-up Scooter, Off Road,
   Other Device), which would close the gap if added.
2. **Contributing factors.** `contributing_factor_vehicle_1..5` is not exposed by
   TrafficStat in any form. Emitted as null.
3. **`collision_id`.** No stable record identifier exists. Consequence: when
   Socrata unfreezes, reconciling or de-duplicating the reconstruction against
   the official rows will be fuzzy spatial-temporal matching, not a key join.
4. **Cross and off street names.** Only the nearest single centerline segment is
   recoverable, so the reconstruction has one street per crash, not the two or
   three an intersection record carries.

## Fields recovered with a measured error rate

5. **Street name: 88.2% agreement.** Reconstructed as *nearest CSCL centerline
   segment*, which is not the same thing as *what the officer wrote*. Fine for
   counting collisions per street; not fine for quoting an individual crash's
   location without checking the record. See `research/reverse_geocoding.md`.
6. **ZIP: 95.0% agreement.** CSCL stores ZIP per side of the street; the sides
   differ on 1.9% of segments, where the left value is taken and `zip_ambiguous`
   is set.
7. **Time of day is absent for the entire gap period — this is total, not
   partial.** TrafficStat carries a real hour for ~95% of Jan–Apr records, 12.5%
   of May, and **0% from June 2026 onward**. Every reconstructed crash is
   midnight-coded, and `time_is_known` is False throughout. Hour-of-day analysis
   of the gap is impossible, not merely weak.

   The degradation began roughly a month *before* the public file froze. Whether
   the two share a cause is unestablished and worth chasing — it is the only
   quantitative lead on the freeze itself that the data offers.
8. **Collision type is `UNKNOWN` on ~51%.** NYPD's own data quality, not a
   parsing artifact. Caps what any type-based analysis can claim.

## Structural cautions

9. **Coordinates are an independent geocode, not Socrata's numbers.** Matching
   the two on the overlap: 4.3% agree at 6 decimals, 35.8% at 5, 66.3% at 4
   (~11 m). The reconstruction is internally consistent and fine for density,
   hotspots and aggregation, but a series spliced onto Socrata history contains a
   **geocoding discontinuity at 2026-06-11**. Do not present the joined series as
   one homogeneous dataset without saying so.
10. **TrafficStat reports zero null coordinates; Socrata reports 3.1%
    ungeocodable.** TrafficStat places crashes Socrata declines to place, and its
    own documentation says unmappable collisions may be mapped to the precinct
    house. Checked: the top 100 points hold 3.9% of TrafficStat collisions against
    3.2% for Socrata — mildly elevated, consistent with genuine bad intersections
    rather than stationhouse dumping. Not a disqualifier, but that ~3% goes
    somewhere and is not flagged.
11. **Highway collisions geocode to the nearest mile marker** by NYPD's own
    statement. Tested indirectly: TrafficStat's distance-to-centerline
    distribution is indistinguishable from Socrata's (median 2 ft, p99 155 vs
    154), so the effect is not detectable at street-grid resolution. It may still
    matter for fine-grained highway work.
12. **TrafficStat data is preliminary** by NYPD's own statement, subject to
    reclassification — especially fatalities, where a death determined later to be
    unrelated, or a death occurring after the crash date, changes the count.
13. **Marker expansion recovers ~99.3%, not all.** Collisions sharing a coordinate
    collapse into one marker; expansion of a sample recovered 882 of 888 claimed
    occurrences. Reported as `df.attrs["unparsed"]` — check it, don't assume zero.
14. **Only WTD / 28D / YTD windows exist, and no prior years.** The reconstruction
    is possible only because YTD happens to span both the gap and an overlap
    period. It would not work for a gap in a previous year, and it will stop
    working on 2027-01-01 when YTD resets.

## Open questions, not limitations

15. **Why the file froze is unestablished.** Nothing in the data explains it.
    Hoodline reported the gap on 2026-09-10; the city's own account is missing.
    This is the piece's centre and it is not a data question.
16. **Whether the freeze has since ended.** `python src/collect.py freshness`
    answers it in one call and should be run at the start of any session.
