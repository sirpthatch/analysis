# The charter text itself — primary source

Retrieved and read 2026-09-19 from the 2025 Charter Revision Commission's
Appendix C, "Proposed Amendments":
https://www.nyc.gov/assets/charter/downloads/pdf/2025/Appendix-C-Amendments.pdf

Extracted text saved at `data/external/charter_appendix_c_proposed_amendments.txt`.
This is the enacted amendment language, not a secondary summary — it
supersedes the Ballotpedia-derived account in `research-log.md`.

## The operative provision, verbatim

New York City Charter **§ 197-f**, "Affordable housing fast track," subdivision a:

> "No later than October 1, 2026, and every five years thereafter, the
> director of city planning shall determine and post on the website of the
> department a list of the twelve community districts which, during the
> preceding five years, had **the lowest rate of affordable housing
> development, as measured by the total number of new affordable dwelling
> units in a community district as a percentage of the total number of
> housing units located in such community district at the start of each
> five-year cycle.** The city planning commission shall, in consultation with
> the commissioner of housing preservation and development, develop a
> methodology to calculate the total number of affordable dwelling units in
> each community district, considering data that includes, but need not be
> limited to, the total number of affordable dwelling units for which the
> department of buildings has issued a permit for construction work and,
> where applicable, the date upon which an affordable housing unit becomes
> subject to a regulatory agreement or other similar instrument that provides
> for the creation of one or more affordable dwelling units. For the purposes
> of this section, the term 'affordable dwelling unit' has the same meaning
> as set forth in subdivision a of section sixteen-a."

## What this settles

**Open item 2 is closed. The rate is in the charter, not in DCP's rulemaking.**
Voters enacted the formula itself: new affordable units over *total housing
units*. This is not an agency choice.

**And so is the stock-blindness.** The denominator is fixed by the charter as
"the total number of housing units located in such community district."
Nothing in it distinguishes affordable from market-rate housing, and the
Commission had no authority to add such a distinction by rule. That is almost
certainly why rulemaking commenters who asked for existing affordable stock to
be credited were refused — not a policy judgment DCP made, but a constraint it
was under. **This materially reframes the project's central question:** the
counterfactual in `stock_adjusted_counterfactual.md` is a critique of what
voters approved, not of what the agency did with it.

(Still worth confirming against the Statement of Basis and Purpose, open item
1 — whether the Commission says this explicitly.)

**CPC's discretion was over the numerator only.** The charter directs the
Commission to "develop a methodology to calculate the total number of
affordable dwelling units," and § 197-f(d) lets it promulgate rules "relating
to the selection of data used to identify the number of affordable housing
units." Numerator, yes. Denominator, no.

**"New" is in the charter.** "Total number of *new* affordable dwelling
units" supports the rule's exclusion of preservation, which had been read as
a rulemaking choice.

**The denominator's timing is confirmed.** "At the start of each five-year
cycle" — which is what `build_denominator` implements (2020 Census plus net
new completions through cycle start).

**The effective date is confirmed.** § 197-f(b)(3): an application qualifies
if "filed between the first of January succeeding the posting of the list
required by subdivision a of this section and the thirty-first of December
five years thereafter." So the list posted by 2026-10-01 governs applications
filed from **January 1, 2027** through December 31, 2031.

## Limits on the fast track the project had not recorded

§ 197-f(c) imposes two findings that are not automatic:

1. DCP "shall not certify an application as complete unless it confirms that a
   primary purpose of such application is to facilitate additional housing and
   affordable housing."
2. The CPC "shall assess and make a finding regarding the consistency of such
   application with the fair housing plan submitted pursuant to subdivision b
   of section sixteen-a and the adequacy of existing transportation, sewer and
   other infrastructure."

The second is a live constraint — an infrastructure-adequacy finding is
exactly the kind of thing that can be litigated or used to slow an
application, and it ties the fast track to the Fair Housing Growth Strategy
noted in `research-log.md`.

Eligibility also requires the property be subject to a program "that mandates
that any new housing on designated lots include minimum percentages of
permanently affordable housing equivalent to or exceeding the requirements
under any mandatory inclusionary housing program" (§ 197-f(b)(2)).

## Still open

- **The definition of "affordable dwelling unit" is by cross-reference** to
  Charter § 16-a(a), which is not reproduced in Appendix C. That section comes
  from the 2024 fair housing amendment. Look it up — it is the authoritative
  answer to "what counts as affordable," and `research/definitions.md`
  currently answers that question from HPD's data conventions instead.
- **Question numbering.** Appendix C heads this "Question 1: Fast Track for
  Affordable Housing." Ballotpedia and the project's notes call it Question 2.
  The Commission numbered its own proposals before ballot positions were
  assigned, and the November 2025 NYC ballot opened with a statewide
  proposal — but confirm the ballot number before printing it.
