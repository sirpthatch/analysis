# What crediting existing affordable stock would do to the list

During rulemaking, commenters asked DCP to let a district's existing
affordable housing count toward the measure. The Commission made no change.
This models the counterfactual so the refusal can be quantified rather than
only described.

Run `python src/rank.py` and see `stock_comparison_map.png`, or call
`fasttrack.rank_with_existing_stock()`.

## The alternative formula

    (existing affordable stock + new affordable production) / total housing units

against the adopted rule's

    new affordable production / total housing units

## Result: five of the twelve are replaced

**Drops off** — listed only because existing stock is ignored:

| District | Adopted rate | Rank | With stock | Rank | Existing stock |
|---|---|---|---|---|---|
| MN-07 Upper West Side | 0.054% | 7 | 7.873% | 29 | 9,905 (7,197 NYCHA) |
| BK-18 Canarsie | 0.068% | 8 | 6.301% | 23 | 4,513 (4,393 NYCHA) |
| MN-05 Midtown | 0.000% | 1 | 3.668% | 20 | 1,571 |
| SI-02 Mid-Island | 0.044% | 6 | 3.556% | 19 | 1,819 (1,430 NYCHA) |
| MN-08 Upper East Side | 0.108% | 9 | 1.930% | 14 | 2,540 (1,323 NYCHA) |

**Joins** — would be listed if existing stock counted: QN-09 Richmond Hill &
Woodhaven, BK-11 Bensonhurst & Bath Beach, MN-02 Greenwich Village & SoHo,
QN-03 Jackson Heights, QN-04 Elmhurst & Corona.

**Stays either way** (seven): BK-10 Bay Ridge, QN-10 Howard Beach, SI-03 South
Shore, QN-13 Queens Village, QN-11 Bayside, QN-05 Ridgewood, BK-12 Borough
Park.

## The finding

**All three Manhattan districts fall off.** The Upper West Side is the extreme
case: it ranks 7th-lowest in the city on the adopted rule and 29th — the
middle of the pack — once its roughly 9,900 existing affordable units are
counted, over 7,000 of them NYCHA. The rule's stock-blindness is the entire
reason Manhattan appears on this list.

What survives the change is the outer-borough, low-density core of the list —
Bay Ridge, Howard Beach, South Shore, Queens Village, Bayside, Ridgewood,
Borough Park. Those districts are low-production *and* low-stock, and no
reading of the metric rescues them.

So the choice of formula is not a technicality. It decides whether the fast
track targets places that have never absorbed affordable housing, or places
that are not building it right now regardless of what they already host.

## How "existing stock" was measured, and what is wrong with it

There is no authoritative count of existing affordable housing by community
district. This uses the two components that do publish clean per-district
numbers:

- **NYCHA public housing** (`evjd-dqpz`, NYCHA Development Data Book, as of
  2025-01-01): 183,141 apartments citywide, of which 180,776 map to a
  community district. Ten developments carry no district and are dropped; two
  straddle a borough line and are assigned to the first borough named.
  Distribution sanity-checks against reality — Brooklyn 60,566, Manhattan
  55,519, Bronx 43,438, Queens 16,743, Staten Island 4,510, with East Harlem
  (15,355) and the Lower East Side (14,326) the largest districts.
- **HPD-counted units created or preserved before the cycle** (`hg8x-zxpr`,
  start date before 2021-07-01, both construction types). Preservation is
  included deliberately: the rule excludes it from *production*, and whether
  that exclusion is fair is exactly what this counterfactual tests.

**What it misses, all in the same direction:** Mitchell-Lama, HDFC co-ops,
older LIHTC properties, project-based Section 8 outside NYCHA, and the
roughly one million rent-stabilized private units. None publish a
per-district count. Every district's stock is therefore understated, and
probably the outer boroughs' most of all, since HPD's file only reaches back
to 2014 and NYCHA is concentrated in a handful of districts.

That matters for how far the result can be pushed. The direction is solid —
districts with large NYCHA holdings move down sharply and cannot move up. The
precise membership of the alternative twelve is not: a fuller stock measure
would likely move more districts, and the five that join here are the least
robust part of the finding. Report it as "counting existing stock would
replace at least five of the twelve, including all three Manhattan
districts," not as a competing list of record.
