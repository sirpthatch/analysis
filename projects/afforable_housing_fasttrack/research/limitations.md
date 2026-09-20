# Limitations

The running list of what this data cannot support. Each entry says what the
gap is, which direction it biases, and what would close it.

## Numerator

**The HPD file stops a quarter short of the cycle.** `hg8x-zxpr` was last
refreshed 2026-05-19 and its latest `project_start_date` is 2026-03-31. The
cycle ran through 2026-06-30, so the final quarter is simply absent from the
public file. This biases every district's numerator *down*, but not evenly —
a district with one large project starting in May 2026 is affected far more
than a district with none. Closed only by DCP refreshing the file, or by DCP
publishing the official list. Varied in `rank.py --sensitivity` as
`cycle_through_hpd_data`, which at least makes the shortened cycle explicit
rather than implicit.

**"New Construction" is a proxy for the rule's exclusion, and a loose one.**
The rule excludes affordable units in *existing residential buildings* under a
preservation program receiving financial assistance. HPD's
`reporting_construction_type` is a clean binary — New Construction /
Preservation — but the two definitions are not the same one. A gut rehab of a
vacant non-residential building is new production under the rule and could be
reported either way here. Varied as `include_preservation`, which brackets the
effect rather than resolving it.

**The DOB permit condition depends on a BBL/BIN join that does not fully
land.** The rule requires a DOB construction permit, so an unmatched building
drops out of the numerator entirely — which pushes a district's rate *down*
and makes it *more* likely to be listed. This is the most dangerous gap in the
analysis, because it biases toward exactly the finding the project is looking
for. Quantified in Phase 1; `no_permit_condition` brackets it.

**1,518 HPD rows since 2016 carry no BBL.** They can still match on BIN, but
rows missing both are unmatchable. Recoverable by geocoding the address.

**"Permit for construction work" is an interpretation.** DOB NOW names 21 work
types; the pipeline counts general construction, structural, foundation, earth
work and support of excavation, and ignores plumbing, sprinklers, sheds,
scaffolds and signs. Legacy BIS is read as job types NB, A1 and A2. A reading
that counted any permit at all would move dates earlier and pull more
buildings in-cycle.

### How much of this could actually change the answer

Run `python src/rank.py --understatement`. Both suppressors — unmatched units
and the missing quarter — can only raise a rate if corrected, so both can only
push a district *off* the list, never onto it. Crediting them back to every
district at once and re-ranking:

- **Eleven of the twelve never leave**, under any scenario tested, including
  the deliberately pessimistic one where each district is credited its single
  best April–June quarter of the last five years on top of every unmatched
  unit.
- **QN-11 (Bayside) is the exception.** It needs 26 units to clear the
  13th-place rate and has only 6 unmatched, so the permit gap alone cannot
  save it — but it produced 31 units in its best second quarter, and on that
  scenario it falls to 13th and **BK-11 (Bensonhurst)** takes the twelfth
  place. Bayside is the one district on this list that should be reported as
  genuinely uncertain.
- **The denominator choice changes nothing.** Re-ranking on CDTA geography
  instead of community district geography returns the identical twelve in
  nearly the identical order.

The margin table is the useful artifact: BK-12 (29 units) and QN-11 (26 units)
are the only districts within 60 units of escaping. Every other district on
the list would need between 77 and 177 additional affordable units — several
times anything the data gaps could plausibly be hiding.

## Denominator

**Net new units are credited by calendar year, not to the cycle start.**
`dbdt-5s7j` reports completions annually, and the cycle starts July 1 2021, so
crediting all of 2021 overshoots by roughly half that year's completions. The
effect is small — a few hundred units against denominators in the tens of
thousands — but the districts around the twelfth position are separated by
hundredths of a percentage point, so it is not nothing.

**Two pairs of districts report identical 2020 stock, and it is probably
coincidence.** `dbdt-5s7j` gives MN-11 and BX-10 both 54,738, and BK-12 and
SI-03 both 62,782. The handoff flagged this as a likely data artifact. Checked
against `48dt-mn3z` (the same DCP pipeline on CDTA geography): MN-11 matches
exactly, SI-03 is within 0.2%, and BK-12 and BX-10 differ by 2.4% and 1.1% —
differences smaller than the CD/CDTA gap for eleven districts that have no
duplicate at all (BK-13 differs by 4.6%, MN-05 by 6.5%). So the collisions do
not stand out against normal geography-approximation noise. Downgraded from
"probable artifact" to "unresolved but unremarkable." Settling it properly
needs block-level 2020 Decennial data, which now requires a Census API key.

**No independent verification of the Census counts yet.** Both DCP files come
out of the same Housing Database pipeline, so agreement between them is not
independent confirmation. The Census API returns "Missing Key" for
unauthenticated calls, so a free API key is needed before `cenunits20` can be
checked against the source.

## The rule itself

**No tie-break procedure appears in the rule.** If two districts finish level
at the twelfth position, nothing found so far says what happens. The pipeline
breaks ties by the smaller numerator for reproducibility, which is a choice
the rule does not authorize.

**Joint interest areas are excluded on judgment, not on rule text.** Central
Park, the airports, Rikers and the large cemeteries appear in the DCP file
with near-zero housing stock; left in, they would take the top twelve places
outright. No exclusion for them was found in the rule. The pipeline
whitelists the 59 real community districts, which is certainly what DCP
intends and is not, so far, what the rule says.

**The stated rationale for ignoring existing affordable stock is still
unknown.** Commenters asked for it during rulemaking and the Commission made
no change. Whether that rejection rests on the charter's own language or on a
policy judgment is unresolved, and it is the difference between a voter choice
and an agency choice. Phase 3.
