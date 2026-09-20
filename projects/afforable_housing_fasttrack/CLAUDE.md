# NYC Affordable Housing Fast-Track: Rate vs. Count Investigation

Project brief for continuing this analysis. Read this file first, then
`research/research-log.md` for sourced detail and `research/queries.md` for exact API
calls. `README.md` describes the project layout and how to run the pipeline.

The project was set up as a full analysis project on 2026-09-19: see `README.md`,
`research/plan.md` for the phased plan, `research/profile_findings.md` for what
profiling the sources established, and `research/limitations.md` for the running list
of what the data cannot support. The prior session's ad-hoc pulls are preserved in
`research/handoff/`; `data/raw/` holds the current, re-queried sources and is
gitignored — rebuild it with `python src/collect.py all`.

## The story

In November 2025, NYC voters approved a charter amendment (Question 2) that strips City Council approval out of the land-use process for affordable housing in the twelve community districts with the lowest **rate** of affordable housing development over the preceding five years. The City Planning Commission adopted the methodology by rule in April 2026. The first five-year measurement cycle closed June 30, 2026, and DCP must publish the list of twelve districts **no later than October 1, 2026** — about two weeks out as of this handoff.

The angle: reverse-engineer the list before DCP publishes it, using DCP's own published rule and public NYC Open Data, and show that the rate-based ranking produces meaningfully different results than the intuitive count-based ranking most coverage assumes — pulling in large, wealthy, high-stock Manhattan districts (Upper East Side, Upper West Side) that built more affordable units in absolute terms than most of the "obvious" low-production outer-borough districts, purely because the rule doesn't touch counts, it touches counts-over-stock.

## The rule, precisely

- **Cycle:** five years, July 1–June 30. First cycle just closed (6/30/2026). Repeats every five years.
- **Numerator:** "affordable dwelling units" (income-restricted units in an affordable housing building) that hit *both* an HPD-reported Start Date *and* a DOB construction permit, with at least one of those two milestones falling inside the cycle. Explicitly **excludes** affordable units in existing residential buildings under a preservation program receiving financial assistance — new production only.
- **Denominator:** total housing units = 2020 Decennial Census count + net new housing units through the cycle start. No distinction between affordable and market-rate — it's a raw housing-unit count.
- **Ranking:** straight citywide ranking of numerator/denominator, twelve lowest, all 59 community districts pooled (no borough quotas found in the rule text).
- **No exclusions found** in the rule for small/non-residential "districts" (joint interest areas), no tie-break procedure specified, no exemption for districts that recently rezoned.
- **Denominator is charter-mandated**, not a rulemaking choice — see
  `research/charter_text.md`. This qualifies the item below.
- **Contested and settled:** commenters during rulemaking explicitly asked DCP to account for existing affordable housing stock in the denominator/formula. The Commission's adoption document states no changes were made in response. This is the crux of "why a rate that ignores existing stock" — see research-log.md for the exact quote and what's still unverified about the *rationale* (as opposed to the fact of rejection).

## Key finding already established (needs your verification before publishing)

Using new-construction affordable units with HPD start dates after 6/30/2021 as numerator, and 2020 Census housing units as denominator (a proxy — see caveats), a rate-based ranking swaps in Manhattan's Upper East Side (MN-08) and Upper West Side (MN-07) in place of Queens Community Board 8 (Fresh Meadows) and Queens Community Board 9 (Richmond Hill), compared to a simple count-based ranking of the bottom twelve. MN-08 and MN-07 each produced *more* affordable units in absolute terms (154 and 158) than every count-based-bottom-twelve district except QN-08 and QN-09 — but their housing stock (138,922 and 126,397 units respectively, the two largest of any community district) is roughly 2.5x the size of the districts they displace, so their rate is still worse.

This is the headline. See `research/research-log.md` for the full comparison table and every caveat attached to it.

## What still needs to be done

Items 4-8 below were addressed by the 2026-09-19 setup session; the pipeline in
`src/` now implements them. What they turned up is in `research/profile_findings.md`
and `research/limitations.md`. The documentary questions (1-3) and the comparison
against DCP's official list (9) are still open, and 1-3 are now the highest-value
work left.

1. **Read the full CPC "Statement of Basis and Purpose"** (URL in
   `research/research-log.md`) end to end — the prior session only pulled fragments via
   an automated summarizer, not the full text. Look specifically for any stated
   rationale for rejecting a stock-adjusted metric (equity argument? simplicity?
   statutory constraint from the charter language itself?). This is the single most
   valuable thing to nail down — it upgrades "the rule structurally does X" into "the
   city says it means to do X." **Still open.**
2. ~~**Find the charter amendment's actual text.**~~ **Done, and it changes the
   framing.** Charter § 197-f(a) specifies the formula itself: "the lowest rate of
   affordable housing development, as measured by the total number of new affordable
   dwelling units in a community district as a percentage of the total number of
   housing units." The rate *and* the stock-blind denominator are what voters enacted;
   CPC's discretion runs to the numerator methodology only. So the counterfactual in
   `research/stock_adjusted_counterfactual.md` critiques the charter, not the agency.
   Full quotes and what else the text settles: `research/charter_text.md`.
3. **Search Council Land Use Committee hearings / 2025 Charter Revision Commission
   testimony** for anyone — DCP, HPD, advocates, opponents — explicitly addressing why
   existing affordable stock isn't credited. Bay Ridge council member testimony (Kayla
   Santosuosso) might be a source given her district is expected to land on the list.
   **Still open.**
4. ~~**Correct the denominator.**~~ Done: `fasttrack.build_denominator` adds
   `comp2020` + `comp2021` net completions to `cenunits20`. Still overshoots by roughly
   half of 2021, because the source reports completions by calendar year and the cycle
   starts July 1 — flagged in `research/limitations.md`.
5. ~~**Check for duplicate denominators.**~~ Done, and downgraded. Cross-checked
   against `48dt-mn3z` (same DCP pipeline, CDTA geography): the duplicate pairs differ
   by less than eleven non-duplicated districts do, so the collisions do not stand out
   against normal geography-approximation noise. Not independently verified — both
   files come from one pipeline, and the Census API now requires a key.
   `fasttrack.denominator_discrepancies()` reports it.
6. ~~**Tighten the numerator.**~~ Done: the DOB permit condition is implemented
   against three sources (DOB NOW `rbx6-tga4`, legacy BIS `ipu4-2q9a`, DCP project-level
   `br6q-ssj3`), joined on BBL then BIN. Named projects match at 96.7%. The residual
   problem is not the join: HPD blanks BBL/BIN/address for 498 in-cycle rows it labels
   `CONFIDENTIAL`, which are unmatchable by construction. See
   `research/profile_findings.md`.
7. ~~**Filter denominator file to real community districts only.**~~ Done:
   `constants.COMMUNITY_DISTRICTS` whitelists the 59, and all 59 are present in the
   source. Note this is still a judgment call, not rule text — no joint-interest-area
   exclusion was found in the rule.
8. ~~**Account for the missing quarter.**~~ Done as far as the data allows: the gap is
   confirmed (file stops at 2026-03-31) and `rank.py --sensitivity` ranks the shortened
   cycle explicitly. It produces the identical twelve, but that tests the cycle
   definition, not the missing rows.
9. **Watch for DCP's actual list**, due by 10/1/2026 — compare against this
   reverse-engineered version once it posts, and write up any divergence (that's a
   story in itself if the two don't match). **Still open**, and now about two weeks out.

### Corrections from the 2026-09-19 run

- The mechanism is **ELURP** (Expedited Land Use Review Procedure), created
  citywide by the charter amendment. The **Affordable Housing Fast Track** is
  the provision that expands ELURP eligibility to larger projects *inside* the
  twelve listed districts — it is not a separate process. Rules effective
  2026-06-17. See `research/process_timeline.md`.

### New open items from the 2026-09-19 run

10. **Geocode `250 WEST 49TH STREET` (MN-05).** Midtown's entire in-cycle production is
    this one 42-unit project, and it has no BBL in the HPD file, so it fails the permit
    test and Midtown ranks 1st with a numerator of zero. With it, Midtown is around 7th.
    One geocode against PLUTO or Geosupport decides several places on the list.
11. **Get a Census API key** to verify `cenunits20` against block-level 2020 Decennial
    data. Unauthenticated calls now return "Missing Key".
12. **Decide how to report the redacted `CONFIDENTIAL` rows.** BK-10 (Bay Ridge) is the
    extreme case: all six of its in-cycle affordable units are redacted single-unit
    rows, so the rule as applied to public data gives it a numerator of zero. It is on
    the list either way, but the published rate for it would be wrong.

## Style / sourcing standard to maintain

Every dataset ID, column name, and statistic in this project must be independently re-verified by querying the live endpoint or reading the dataset's metadata — don't trust anything in `data/` as still-current without a fresh check, since NYC Open Data updates continuously. Flag anything you cannot verify rather than dropping it silently. SoQL text comparisons should be wrapped in `upper()` — text fields in these datasets are inconsistently cased.
