# What "affordable" means in this analysis

Three definitions stack here, and they are not the same thing. The rule points
at HPD's data; HPD's data counts units against a mayoral housing plan; and
that plan's idea of affordable runs up to 165% of Area Median Income. Anything
published off this analysis should say which of the three it means.

## 1. The rule's definition

The CPC methodology rule defines an **Affordable Dwelling Unit** as:

> "an income-restricted dwelling unit in an affordable housing building"

with two milestones required — an HPD-reported Start Date and a DOB
construction permit — and one exclusion: units in an existing residential
building under a preservation program where the owner receives financial
assistance.

The rule does not set its own income thresholds. It defers to what HPD
reports, which makes HPD's counting rules the operative definition.

Source: CPC, "Affordable Housing Fast Track Methodology Rules," Notice of
Adoption / Statement of Basis and Purpose, April 2026 —
https://rules.cityofnewyork.us/wp-content/uploads/2026/04/City-Planning-Commission-Affordable-Housing-Fast-Track-Methodology-Rules-Notice-of-Adoption-Statement-of-Basis-and-Purpose-Final-Rule-Text.pdf

(Still to verify against the full PDF rather than the prior session's
summarizer extract — see open item 1 in `CLAUDE.md`.)

## 2. What HPD actually counts

The numerator uses `all_counted_units` from HPD's Affordable Housing
Production by Building (`hg8x-zxpr`), documented as:

> "the total number of affordable units, counted towards the Housing New York
> plan, that are in the building"

So a unit is "affordable" here if HPD counted it toward the city's housing
plan — Housing New York (1/1/2014–12/31/2021) or Housing Our Neighbors: A
Blueprint for Housing & Homelessness (1/1/2022–present). It is an
administrative category, not a market test. `total_units` is the separate
field that includes market-rate units in the same building.

Two sub-categories, both counted:

- **Counted Rental Units** — "assistance has been provided to landlords in
  exchange for a requirement for affordable units."
- **Counted Homeownership Units** — "assistance has been provided directly to
  homeowners."

Sources: dataset https://data.cityofnewyork.us/d/hg8x-zxpr and its attached
data dictionary,
https://data.cityofnewyork.us/api/views/hg8x-zxpr/files/e75b3c3e-d8bc-4137-a710-f8509eea0b6a?download=true&filename=Affordable_Housing_Production_by_Building_Data_Dictionary.xlsx

## 3. The income bands

HPD's own definitions, quoted from the dataset's column documentation, with
2026 dollar figures for a three-person household. 100% AMI for the New York
City region in 2026 is **$152,700** for a three-person family.

| Band | HPD definition | 3-person income ceiling (2026) |
|---|---|---|
| Extremely Low Income | "rents that are affordable to households earning 0 to 30% of the area median income (AMI)" | $45,810 |
| Very Low Income | "31 to 50% of the area median income" | $76,350 |
| Low Income | "51 to 80% of the area median income" | $122,160 |
| Moderate Income | "81 to 120% of the area median income" | $183,240 |
| Middle Income | "121 to 165% of the area median income" | $251,955 |
| Other | "units reserved for building superintendents" | n/a |

AMI is set annually by HUD for the New York–Newark metro area and published by
HPD: https://www.nyc.gov/site/hpd/services-and-information/area-median-income.page

**The ceiling is the thing to flag.** A unit restricted to a household earning
up to roughly $252,000 counts as affordable production under this rule,
identically to a unit restricted to a household earning $45,810.

## Why that matters to this project

The rule ranks districts on affordable units per housing unit and treats every
band alike. Of the 60,262 units in this cycle's citywide numerator:

| Band | Units | Share |
|---|---|---|
| Extremely low (0–30% AMI) | 11,058 | 18.3% |
| Very low (31–50%) | 7,561 | 12.5% |
| Low (51–80%) | 14,781 | 24.5% |
| Moderate (81–120%) | 2,779 | 4.6% |
| **Middle (121–165%)** | **23,943** | **39.7%** |
| Other (supers' units) | 140 | 0.2% |

Middle-income units are the single largest share of what the city counts as
affordable production, and among the twelve districts projected for the list
they often account for all of it:

| District | Counted units | Middle income | At or below 80% AMI |
|---|---|---|---|
| SI-03 South Shore | 4 | 4 | 0% |
| QN-13 Queens Village | 9 | 3 | 22% |
| QN-10 Howard Beach | 7 | 6 | 0% |
| SI-02 Mid-Island | 23 | 0 | 0% (all moderate) |
| MN-07 Upper West Side | 68 | 0 | 47% |
| BK-18 Canarsie | 49 | 49 | 0% |
| MN-08 Upper East Side | 150 | 94 | 37% |
| QN-05 Ridgewood | 76 | 76 | 0% |
| QN-11 Bayside | 66 | 36 | 45% |
| BK-12 Borough Park | 94 | 89 | 2% |

(MN-05 and BK-10 have no counted units — see `profile_findings.md`.)

This cuts both ways and should be reported as such. A district can keep itself
off the fast-track list by producing units affordable only to households
earning up to $252,000 — Canarsie, Ridgewood and Borough Park are close to
pure cases. But it is also the deeply affordable production on the Upper West
Side and Upper East Side, not middle-income units, that makes up the larger
share of their counted totals.
