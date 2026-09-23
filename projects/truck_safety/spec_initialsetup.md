**Objective**

Profile the safety of trucks in New York City, particularly in relation to the
side-guard rule aimed at preventing pedestrians and cyclists from being swept
under a truck's wheels, which took effect with a 2023 compliance deadline.
A Business Integrity Commission waiver process exempted a large share of the
trade-waste fleet. The question to explore is whether that waiver process
effectively neutralized the rule — i.e. whether it means we see no change in
crashes, injuries or fatalities across the deadline.

The analysis uses the NYPD motor vehicle collision data (crashes + vehicles),
and folds in the patched crash file produced by the companion
`../reconstruct_crashdata` project, which covers the period after the public
crash file froze on 2026-06-11.

**Research Questions**

1. How many vehicle collisions involving trucks happen in NYC? How are they
   distributed around the city, and how often do they happen on versus off
   established truck routes?
2. What is the trend in crashes involving bikes and pedestrians, and fatalities
   in particular? Is there any interruption in the rate of incidents after the
   side-guard rule took effect?
3. Will the redrawn truck routes make things safer — i.e. do they avoid areas
   with a history of crashes or heavy pedestrian traffic?

**Research Method**

1. Collect data
2. Profile and analyze it
3. Explore questions
4. Document limitations
5. Repeat 3 & 4 until satisfactory findings are made (with human input)
6. Publish results

**Progress**

Question 2 (the side-guard trend) has its blocking denominator answered; see
`research/research-log.md`. Question 1 is answered in
`notebooks/truck_crash_profile.ipynb`. Question 3 is not started — it needs the
redrawn route network, which has not been sourced yet.

**Inherited starting point**

`research/handoff/truck-sideguards/` is a research-idea package prepared
2026-09-22 (unpacked from `research/nyc-truck-sideguards-handoff.zip`). It
carries a pitch (`BRIEF.md`), a provenance ledger of what was actually queried
(`VERIFIED-DATA.md`), an ordered work plan (`NEXT-STEPS.md`), working SoQL
(`queries.md`) and sources (`SOURCES.md`). Its ground rule carries forward:
nothing counts as a fact until a live query returns it, and anything that
cannot be verified is flagged rather than dropped.
