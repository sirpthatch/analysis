# NYC truck safety and the side-guard waiver

Project brief. Read this first, then `research/limitations.md` for what the
numbers cannot support, and `research/research-log.md` for the provenance of
every figure. `README.md` describes the layout and how to run the pipeline;
`spec_initialsetup.md` holds the objective and research questions.

Set up 2026-09-22 from a research-idea handoff bundle, preserved unmodified at
`research/handoff/truck-sideguards/` (zip in `research/`). Its `NEXT-STEPS.md`
is still the work plan.

## The story

Local Law 56 of 2015 mandated side guards on the **city fleet and commercial
waste trucks**, three years after cyclist Mathieu Lefevre's death, with a January
1, 2023 compliance deadline; Local Law 108 of 2021 extended it to city
contractors. DCAS administers the city-fleet and contractor side and reported its
installs complete in January 2023. **BIC administers the private trade waste
side**, and told Streetsblog roughly half that fleet is exempted by waiver, where
"the design or operation of the vehicle" makes installation impractical. Waivers
run up to two years and are renewable. The question is whether the waivers
neutralized the rule.

The news hook: Daria Serkov, 15, was killed by a truck while riding a Citi Bike
at 14th Street and First Avenue on 2026-09-19. Streetsblog reported on 09-21
that the truck involved held a 2023 BIC side-guard exemption.

## What the blocking question established

The pitch rested on a flat refuse-truck crash count. The denominator moved:

| | 2016 | 2020 | 2023 | 2025 |
|---|---|---|---|---|
| Citywide crashes | 229,833 | 112,918 | 96,607 | 85,546 |
| Refuse vehicle rows | 552 | 385 | 398 | 402 |
| **Refuse per 10k crashes** | **24.0** | **34.1** | **41.2** | **47.0** |

Citywide crashes fell 63%; refuse fell 27%. The relative rate roughly doubled
and runs straight through the 2023 deadline with no break. Every other heavy
class (box truck, tractor trailer, dump, flat bed) is flat or falling on the
same denominator — so this is specific to refuse trucks, not a coding drift.

**This is the outcome that strengthens the pitch, not the one that kills it.**

## Standing cautions

- **The harm counts are too small to test the rule.** 0–5 cyclists injured and
  0–1 killed per year in side-impact refuse crashes. A 40% fatality reduction
  against that base is undetectable — its absence is not evidence of failure.
  Rest the argument on the crash-rate series. This is `limitations.md` #1 and
  it is the one that most constrains the post.
- **`GARBAGE OR REFUSE` does not map onto the regulated fleet.** DSNY trucks are
  *not* exempt — city fleet and carters were both bound by the same 2023
  deadline under §16-526, just administered separately (DCAS vs BIC). But only
  carters are exposed to the **BIC waiver** the story is about, and the
  BIC-covered fleet is broader than garbage trucks (the Badger hydro-excavation
  truck is the example). Separating DSNY from carters would turn this into a
  treated-versus-untreated comparison. Unresolved. See `limitations.md` #2.
- **A rising ratio is not a rising count.** Present the heavy-vehicle control
  group alongside the refuse series, always.
- **The public crash file froze at 2026-06-11.** Confirmed live. The September
  fatality is not in the data. The companion reconstruction covers the gap but
  carries **no vehicle type** — it extends the denominator, not the truck series.
- **2016 is the first usable year.** 1 record in each of 2014 and 2015 is a
  schema artifact. Whether the 2016→2019 rise is code adoption is not settled.

## What still needs to be done

1. **File the FOIL to BIC** for side-guard exemption applications and
   determinations by year, with licensee, vehicle identifiers, stated basis and
   determination date. No open dataset carries this. It is the spine of the
   follow-up even if the first post runs without it.
2. **Separate DSNY from private carters** — `vehicle_make`, `state_registration`,
   then a spatial join to DSNY Commercial Waste Zones (`8ev8-jjxq`). Hardest
   problem in the project, and the highest-value one: a complying DSNY fleet
   against a half-waived carter fleet on the same deadline is a natural
   experiment, not just a cleaner series.
3. ~~**Research question 1: geography.**~~ **Done** — see
   `notebooks/truck_crash_profile.ipynb` and the research log. Trucks are in
   7.2–8.3% of crashes with no trend across the decade; 62.9% of truck crashes
   are on the designated network against 47.0% of non-truck crashes; 37% are
   off it. Off-route is *not* illegal — see `limitations.md` #9.
4. **Research question 3: the redrawn truck routes.** Do they avoid crash
   history and heavy pedestrian traffic? Not started. The off-route crash
   geography from question 1 is the input; the redrawn network still needs to
   be sourced (the current network is `jjja-shxy`).
5. **Extend the citywide series through 2026-09-13** using the companion
   reconstruction, for the denominator and the citywide cyclist/pedestrian
   series only.
6. **Trace the 40% side-guard figure** to the Volpe LPD literature before using
   it.
7. **Check the Substack archive** (`urbancalc.substack.com/archive`) — it 429'd
   during the handoff preparation and has never been read.

## Style / sourcing standard to maintain

Every dataset ID, column name and statistic is re-verified against the live
endpoint before use — the handoff bundle's figures were all re-queried rather
than trusted, and so should these be. Flag anything unverifiable rather than
dropping it silently.

- **SoQL text comparisons wrap both sides in `upper()`.** Casing is inconsistent
  and a mismatch returns zero rows rather than an error. A zero-row result is
  more often a casing artifact than a genuine absence.
- **Enumerate a column's value vocabulary before filtering on it.** Guessing
  value strings is how you get a confident zero. `point_of_impact` has 19
  distinct values on refuse rows; they are in `src/vehicle_codes.py` because
  they were pulled, not assumed.
- **Paging needs a stable `$order`** or rows silently repeat across pages.
- **De-duplicate on `collision_id` before summing harm.** Injury counts live on
  the crash row; summing them over vehicle rows double-counts any crash with two
  trucks in it.
- **`vehicle_type` is truncated to five characters on some records** (`TRACT`,
  `BOX T`, `GARBA`). Any code list must fold the truncated variants in or it
  undercounts silently.
- **Never describe a map from the picture.** Compute the values and quote them.
  A claim that "Midtown sits below the citywide truck share" survived into a
  draft this way; Midtown is roughly double it.
- **Charts follow `src/viz.py`** — one validated palette, sequential blue for
  magnitude, diverging blue/red for over/under a reference, categorical blue and
  orange. Never a second y-axis; two scales get two panels.
