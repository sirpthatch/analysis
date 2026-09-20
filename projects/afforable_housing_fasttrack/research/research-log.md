# Research log — NYC fast-track rate vs. count

Everything below was fetched and verified in a prior Claude session on 2026-09-19. Treat data pulls as a starting point, not final numbers — re-verify anything before publication, since these are live public datasets.

## The charter mechanism

**Ballot measure:** New York, New York, Question 2, "Expedited Public Process for Affordable Housing," approved by voters November 4, 2025.
Source: https://ballotpedia.org/New_York,_New_York,_Question_2,_Expedited_Public_Process_for_Affordable_Housing_Charter_Amendment_(November_2025)

Two pathways created:
1. Expedited rezoning review for Mandatory Inclusionary Housing applications in designated low-production districts — concurrent Community Board/Borough President review (60 days total), CPC decision within 30–45 days, **removing City Council approval** from the standard ~7-month ULURP process.
2. Board of Standards and Appeals may grant zoning modifications (use, bulk, parking) for 100%-affordable projects by Housing Development Fund Companies.

Every five years starting 2026, DCP identifies "12 community districts with the lowest rates of affordable housing development." First identification: 2026.

**Not yet verified:** whether the ballot language itself specifies "rate" (as opposed to count) or whether that was DCP's choice in rulemaking. The Ballotpedia summary (a secondary source, AI-summarized in the prior session) says "rate," consistent with the adopted rule, but the primary charter text hasn't been pulled directly. This is worth doing — it's the difference between "voters chose a rate" and "DCP chose a rate."

## The adopted rule

**Source (primary, PDF):** NYC Rules — City Planning Commission, "Affordable Housing Fast Track Methodology Rules," Notice of Adoption / Statement of Basis and Purpose, April 2026.
https://rules.cityofnewyork.us/wp-content/uploads/2026/04/City-Planning-Commission-Affordable-Housing-Fast-Track-Methodology-Rules-Notice-of-Adoption-Statement-of-Basis-and-Purpose-Final-Rule-Text.pdf

This was only fetched via an automated summarizer tool in the prior session (WebFetch), not read in full by a human or as complete raw text. **Priority action: pull and read the full PDF directly.** What the summarizer extracted and attributed as direct quotes:

> "the director of city planning shall determine...a list of the twelve community districts that, during the preceding five years, had the lowest rate of affordable housing development"

> An "Affordable Dwelling Unit" is "an income-restricted dwelling unit in an affordable housing building."

Two milestones required (both, by cycle end, at least one within the cycle):
- "a Start Date as publicly reported by the department of housing preservation and development"
- "a permit for construction work issued by the department of buildings"

Exclusion: "Affordable Dwelling Units in an existing residential building subject to a preservation program where the owner...receives financial assistance."

Denominator: "The total number of housing units...shall consist only of the number of housing units identified in the prior Decennial Census plus the number of Net New Housing Units."

Cycles: five years, July 1–June 30, first cycle ending June 30, 2026. List due "no later than October 1, 2026."

On the stock-adjustment question — the one this project is chasing:
> "Many other comments called for the methodology to account for factors that...cannot be considered in determining the rate of affordable housing development, such as the stock of existing affordable housing."
> Commission's response, per the summarizer: "no changes to the rule were made in response to the comments received."

**This is a fact of rejection, not a stated rationale for it.** The summarizer did not surface (or the document may not contain, unclear which) an affirmative explanation of *why* existing stock isn't credited. Reading the full PDF is the top priority action — look for a "Response to Comments" section that may have more detail than what was extracted, and check whether the rejection is grounded in the charter's own language (i.e., DCP may be saying "the charter told us to measure rate of production, not adjust for stock, so we have no discretion here") versus a policy judgment DCP made on its own.

No borough quotas, tie-break rules, size-threshold exclusions, or rezoning-recency exclusions were found in the rule as summarized — but again, confirm against the full text.

## The news hook / present-tense context

- City Limits, Sept 18, 2026: Council Member Kayla Santosuosso (Bay Ridge) is advancing her own rezoning of the "Bay Ridge Triangle" (86th–95th Streets, 4th–5th Aves) partly *because* her district is expected to land on the fast-track list next year and she'd rather shape it than have it imposed. Bay Ridge (BK-10) ranked dead last on both count and rate in the reverse-engineered analysis below.
  https://citylimits.org/a-rezoning-grows-in-brooklyn-and-what-else-happened-this-week-in-housing/
- HPD/DCP, Aug 19, 2026: released draft "Fair Housing Growth Strategy" (first-ever, required by Local Law 167 of 2023) — citywide housing-need estimate (~700,000 homes over a decade) and per-community-district production targets, coordinated with the "Block by Block" plan (200,000 affordable homes over 10 years). Public comment closed Sept 19, 2026. Worth checking whether this document's district-level targets reference the fast-track rate methodology or use a different metric — if HPD/DCP use *different* denominators for different purposes, that's itself a finding.
  https://www.nyc.gov/site/hpd/news/048-26/hpd-dcp-release-city-s-first-ever-fair-housing-growth-strategy
- Prior journalism on this topic (already done, don't repeat): City Limits, "The 12 Communities Where Mayor Adams' Charter Commission Could 'Fast Track' Affordable Housing" — published when the charter commission floated an *illustrative* list, before the ballot passed and before CPC adopted the binding methodology (April 2026). Different data, different rule version. What's still open is running the *adopted* rule against the *completed* first cycle.
  https://citylimits.org/the-12-communities-where-mayor-adams-charter-commission-could-fast-track-affordable-housing/

## Datasets used

### Affordable Housing Production by Building
- ID: `hg8x-zxpr`
- URL: https://data.cityofnewyork.us/resource/hg8x-zxpr.json
- Agency: HPD
- Verified via direct query: 9,250 total rows; max `project_start_date` = 2026-03-31 (i.e., the file is missing April–June 2026, the cycle's last quarter, as of last update 2026-05-19)
- Columns confirmed present by querying: `project_id`, `project_name`, `project_start_date`, `project_completion_date`, `building_id`, `house_number`, `street_name`, `borough`, `postcode`, `bbl`, `bin`, `community_board` (format "BK-05"), `council_district`, `census_tract`, `neighborhood_tabulation_area`, `latitude`, `longitude`, `reporting_construction_type` ("New Construction" / "Preservation" — confirmed clean binary, no third value, 4,674 / 4,576 split citywide), `extended_affordability_status`, `prevailing_wage_status`, `extremely_low_income_units`, `very_low_income_units`, `low_income_units`, `other_income_units`, `studio_units`, `_1_br_units`, `_2_br_units`, `_3_br_units`, `counted_rental_units`, `all_counted_units`, `total_units`

### Housing Database by Community District
- ID: `dbdt-5s7j`
- URL: https://data.cityofnewyork.us/resource/dbdt-5s7j.json
- Agency: DCP
- Columns confirmed: `the_geom`, `commntydst` (3-digit code, e.g. "301"), `comp2010ap`, `comp2010`...`comp2024` (net unit completions by calendar year), `cenunits20` (2020 Census housing units), `filed`, `approved`, `permitted`, `withdrawn`, `inactive`, `shape_area`, `shape_leng`
- Full `commntydst` + `cenunits20` listing pulled — see `data/community_district_housing_units_2020.json`. Includes non-CD codes (joint interest areas) that must be filtered out before ranking: `164` (Central Park, 5 units), `226`/`227`/`228`, `355`/`356`, `480`–`484`, `595`. Real community district codes run in the pattern `[borough 1-5][district 01-18]`.

## Reverse-engineered numbers (proxy methodology — re-verify before publishing)

Numerator query used: new-construction affordable units with `project_start_date > '2021-06-30'` (proxy for "cycle start" — actual cycle start rule may differ slightly), grouped by `community_board`, summed on `all_counted_units`.

Denominator used: `cenunits20` alone (proxy — rule wants + net new units through cycle start; not yet added).

Full result set (30 districts, ordered by count ascending) is in `data/affordable_units_new_construction_by_cb_since_2021Q3.json`.

Rate comparison table (count rank vs. rate rank) for the bottom ~14:

| District | Units (numerator) | 2020 housing units (denominator, proxy) | Rate | Rank by count | Rank by rate |
|---|---|---|---|---|---|
| BK-10 Bay Ridge | 6 | 55,768 | 0.011% | 1 | 1 |
| QN-13 Queens Village | 19 | 66,259 | 0.029% | 3 | 2 |
| QN-10 Howard Beach | 15 | 43,252 | 0.035% | 2 | 3 |
| SI-03 South Shore | 26 | 62,782 | 0.041% | 4 | 4 |
| SI-02 Mid-Island | 37 | 51,599 | 0.072% | 5 | 5 |
| QN-11 Bayside | 41 | 47,357 | 0.087% | 6 | 6 |
| MN-05 Midtown | 42 | 42,323 | 0.099% | 7 | 7 |
| BK-18 Canarsie | 73 | 72,361 | 0.101% | 8 | 8 |
| MN-08 Upper East Side | 154 | 138,922 | 0.111% | 14 | 9 |
| QN-05 Ridgewood | 81 | 69,416 | 0.117% | 9 | 10 |
| MN-07 Upper West Side | 158 | 126,397 | 0.125% | 15 | 11 |
| BK-12 Borough Park | 115 | 62,782 | 0.183% | 12 | 12 |
| QN-08 Fresh Meadows | 111 | 59,385 | 0.187% | 11 | 13 (drops off top 12) |
| QN-09 Richmond Hill | 103 | 50,877 | 0.202% | 10 | 14 (drops off top 12) |

Note: BK-12 and SI-03 show identical denominators (62,782) — flagged as a possible data artifact, see open item #5 in CLAUDE.md.

Headline finding: MN-08 (Upper East Side) and MN-07 (Upper West Side) — the two largest community districts by housing stock in NYC — displace QN-08 (Fresh Meadows) and QN-09 (Richmond Hill) when moving from a count ranking to the rate ranking the rule actually specifies, despite MN-07/MN-08 having built more affordable units in absolute terms than either Queens district.

## Caveats attached to every number above

1. Missing Q2 2026 data (see dataset note above) — last quarter of the cycle isn't in the public file yet.
2. Numerator proxy uses `project_start_date` alone, not the rule's actual "Start Date AND DOB permit, either in-cycle" test.
3. Denominator proxy uses 2020 Census units alone, not Census + net new units through cycle start.
4. Non-community-district codes not yet filtered out of any citywide ranking — must exclude `164`, `226`–`228`, `355`–`356`, `480`–`484`, `595` before ranking all districts, or tiny/zero-unit "joint interest areas" will falsely rank at the top.
5. Possible duplicate/coincidental denominator values (BK-12/SI-03 both 62,782; BX-10/MN-11 both 54,738 per earlier spot-check) not yet cross-checked against Census.
6. `reporting_construction_type` = "Preservation" may not map exactly onto the rule's narrower exclusion language.

---

# Session 2026-09-19 — project set up, rule implemented, list projected

The analysis was moved out of ad-hoc queries into a project (`README.md`), the
rule was codified in `src/fasttrack.py`, and all five sources were re-pulled
live. Everything above this line is the prior session's work and its numbers
are superseded where they conflict.

## What changed against the prior session's proxy

Three of the prior caveats were closed, and they move the numbers:

1. **The DOB permit condition is now applied.** Three permit sources — DOB NOW
   (`rbx6-tga4`), legacy BIS (`ipu4-2q9a`) and DCP's project-level housing
   database (`br6q-ssj3`) — joined to HPD on BBL, falling back to BIN.
2. **The denominator now adds net new units** (`comp2020` + `comp2021`) to
   `cenunits20`, as the rule requires.
3. **Joint interest areas are excluded** by whitelisting the 59 real districts.

## Projected list — base run

Cycle 2021-07-01 to 2026-06-30. `data/processed/fast_track_ranking.csv`.

| # | District | Units | Housing units | Rate |
|---|---|---|---|---|
| 1 | MN-05 Midtown | 0 | 42,832 | 0.000% |
| 2 | BK-10 Bay Ridge | 0 | 55,824 | 0.000% |
| 3 | SI-03 South Shore | 4 | 63,218 | 0.006% |
| 4 | QN-13 Queens Village | 9 | 66,373 | 0.014% |
| 5 | QN-10 Howard Beach | 7 | 43,293 | 0.016% |
| 6 | SI-02 Mid-Island | 23 | 51,805 | 0.044% |
| 7 | MN-07 Upper West Side | 68 | 126,681 | 0.054% |
| 8 | BK-18 Canarsie | 49 | 72,398 | 0.068% |
| 9 | MN-08 Upper East Side | 150 | 139,409 | 0.108% |
| 10 | QN-05 Ridgewood | 76 | 69,676 | 0.109% |
| 11 | QN-11 Bayside | 66 | 47,481 | 0.139% |
| 12 | BK-12 Borough Park | 94 | 63,546 | 0.148% |

The zeroes for MN-05 and BK-10 are artifacts of HPD redaction, not of those
districts building nothing — see `research/profile_findings.md`. Both are on
the list under every reading, so the artifact does not change membership, but
the rates as printed are wrong for them.

## The headline survives, and gets sharper

MN-08 (Upper East Side) is on the rule's list at rank 9 and off the
count-based bottom twelve at count rank 16 — it built 150 affordable units,
more than any other district on the list, and lands there purely because it
has the largest housing stock in the city. It displaces QN-09 (Richmond Hill),
which built 106.

MN-07 (Upper West Side) is on the list too, at rank 7, but for a different
reason than the prior session found, and the difference matters. Its two real
projects are 266 West 96th Street (68 units, permit 2022-03-29) and the
84-unit WSFSSH project at 105 West 108th Street, which started 2026-01-30 and
whose DOB permit issued **2026-07-15 — fifteen days after the cycle closed**.
The rule requires the permit by cycle end, so those 84 units do not count.
With them MN-07 is at 152 units and out of the bottom twelve; without them it
is at 68 and ranked 7th. Two weeks of permit processing decides whether the
Upper West Side loses City Council review over its land use.

The prior session's count-based comparison had QN-08 (Fresh Meadows) and QN-09
both displaced. On these numbers QN-08 is off the count-based bottom twelve
too (count rank 13), so only QN-09 is a clean swap.

## Stability

`python src/rank.py --sensitivity`. Base, `no_permit_condition` and
`cycle_through_hpd_data` select the **same twelve districts** — the list is
robust to whether the permit condition is applied at all, which is the
reassurance the 77% overall match rate needed.

`include_preservation` selects a very different twelve, but that variant
describes a different rule, not a different reading of this one. It is a
bracket on the risk that HPD's `reporting_construction_type` does not map onto
the rule's narrower exclusion, and on that reading QN-13 and QN-10 top the
list while MN-05, MN-07 and SI-03 fall to 22nd, 24th and 19th.

Around the cutoff, BK-12 (12th) and BK-11 (13th) are separated by 0.046
percentage points. QN-08 and QN-09 sit at 14th and 15th, within 0.06 points.

## Sources as pulled

| Dataset | Rows pulled | Last refreshed |
|---|---|---|
| `hg8x-zxpr` HPD production (2011+) | 9,250 | 2026-05-19 |
| `rbx6-tga4` DOB NOW approved permits | 64,833 | 2026-09-18 |
| `ipu4-2q9a` DOB BIS permit issuance | 88,028 | 2026-09-18 |
| `br6q-ssj3` DCP housing DB, project level | 6,083 | 2026-03-16 |
| `dbdt-5s7j` DCP housing DB by CD | 71 | 2026-03-16 |
| `48dt-mn3z` DCP housing DB by CDTA | 71 | 2026-03-16 |

The DOB and DCP-project pulls are filtered to the BBLs and BINs of HPD
buildings with a start date from 2016 on, not citywide.
