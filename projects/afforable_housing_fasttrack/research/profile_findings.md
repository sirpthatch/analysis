# Data profiling — what the sources can and cannot carry

Run 2026-09-19 against live Socrata endpoints. Source freshness at that date:

| Dataset | Last refreshed | Covers the cycle end (2026-06-30)? |
|---|---|---|
| `hg8x-zxpr` HPD Affordable Housing Production | 2026-05-19 | **No** — latest start date is 2026-03-31 |
| `dbdt-5s7j` DCP Housing DB by CD | 2026-03-16 | Denominator is a 2020 base, so not a problem |
| `48dt-mn3z` DCP Housing DB by CDTA | 2026-03-16 | Same |
| `br6q-ssj3` DCP Housing DB project level | 2026-03-16 | Lags; used only as a cross-check |
| `rbx6-tga4` DOB NOW approved permits | 2026-09-18 | **Yes** |
| `ipu4-2q9a` DOB permit issuance (BIS) | 2026-09-18 | **Yes** |

## The permit join works, except where HPD redacts the address

The rule makes a DOB construction permit a *condition*, so a building that
cannot be tied to one drops out of the numerator — which lowers a district's
rate and makes it *more* likely to be listed. That made the join the biggest
threat to the whole analysis. It turns out to be two very different problems:

Of 2,423 new-construction HPD rows with a start date inside the cycle
(63,145 affordable units):

- **1,925 named projects, 62,573 units — 96.7% match** to a DOB construction
  permit on BBL or BIN. The join is sound.
- **498 rows, 572 units — 0% match, by construction.** These are the rows
  where HPD writes `CONFIDENTIAL` in place of the project name and leaves
  BBL, BIN and address blank. 497 of the 498 carry exactly one unit. They are
  unmatchable from public data, not because the join is weak but because the
  identifying fields are deliberately withheld.

572 units is 0.9% of citywide in-cycle production, which sounds ignorable and
is not — the whole question is the bottom of a ranking, and the redacted rows
sit disproportionately in the districts near the cutoff. SI-01 has 52 of them,
BK-18 has 24, SI-03 has 22.

These two failure modes are different and should not be described as one.
Bay Ridge's units are *redacted*; Midtown's project is *named* and merely
missing its identifier. Both end at a numerator of zero.

**The sharpest case is BK-10 (Bay Ridge).** Every one of its six in-cycle
affordable units is a redacted single-unit row. Applying the rule as written
to public data therefore gives Bay Ridge a numerator of zero — not because it
built nothing, but because what it built cannot be shown to hold a permit.
Bay Ridge is on the list either way, so this does not change the answer; it
does mean the reported rate for it is wrong.

**MN-05 (Midtown) is the case where it might change the answer.** Midtown's
entire in-cycle production is a single 42-unit project, 250 West 49th Street,
which is *named* but still has no BBL in the HPD file — one of the 63 named
rows that fail to match. With it, Midtown's rate is 0.098% and it sits around
7th; without it, Midtown ranks 1st with a numerator of zero. One geocode
decides several places on the list. This should be resolved by geocoding the
address against PLUTO or Geosupport before anything is published.

## Why HPD redacts those rows

Nothing in the dataset's documentation explains the `CONFIDENTIAL` label, so
this is read off the rows themselves. 1,868 of them exist across the file,
carrying 2,702 affordable units. What they have in common:

- **`house_number` and `street_name` are the literal string `'----`**, and
  `bbl`, `bin`, `postcode`, `census_tract`, `latitude` and `longitude` are
  empty for every one. Borough, community district and council district
  survive. The suppression is specifically of anything that identifies the
  *building*, while keeping the geography coarse enough to report by district.
- **They are overwhelmingly small owner-occupied homes.** 1,534 of the 1,868
  are single-unit and 1,863 are four units or fewer; 2,288 of the 2,702 units
  are homeownership rather than rental. Every row carries an income band, and
  most are low or very low income.

Put those together and the reason is disclosure risk about a person rather
than a building. A one-unit income-restricted homeownership project is one
household, so publishing the address publishes that an identifiable family
received income-targeted subsidy, and at which income band. Size alone is not
the trigger — 1,725 *named* projects also report four units or fewer, but only
444 of their units are homeownership. It is the combination of small and
owner-occupied that gets suppressed.

A second, much smaller group is redacted rental: 14 rows, including new
construction of 171, 75, 62, 50 and 42 units that are almost entirely
extremely-low-income. That is the signature of supportive and confidential-site
housing, where address confidentiality is standard and sometimes legally
required.

Neither reason is objectionable. But both collide with a rule that makes a
building-level permit match a condition of being counted, and the collision
falls hardest on exactly the districts whose production is small and
scattered — which are the districts the rule is looking for.

## Two named projects are excluded by the permit timing, correctly

The rule requires the permit by cycle end, and two in-cycle projects got
theirs just after:

| District | Project | HPD start | DOB permit | Units |
|---|---|---|---|---|
| MN-07 | WSFSSH, 105 West 108 Street | 2026-01-30 | 2026-07-15 | 84 |
| BK-05 | Sutter Place | 2025-12-24 | 2026-07-13 | 6 |

This is the rule working as written, not a data problem, and it matters a
lot: the 84-unit WSFSSH project is most of the Upper West Side's cycle
production. Counting it would take MN-07 from 68 units to 152 and lift it out
of the bottom twelve. Two weeks of permit processing decides whether the
Upper West Side loses City Council review over its land use.

## The missing quarter

`hg8x-zxpr` stops at 2026-03-31, so April–June 2026 — a fifth of the final
year, and the last quarter of the cycle — is absent. Every numerator here is
therefore a floor. Ranking the shortened cycle explicitly
(`cycle_through_hpd_data`) produces the identical twelve districts, which is
mildly reassuring but not decisive: the variant tests the cycle *definition*,
not the missing data, and it cannot conjure rows the file does not have.

## The denominator's duplicate values are probably not an artifact

The handoff flagged that `dbdt-5s7j` reports identical 2020 stock for MN-11
and BX-10 (54,738) and for BK-12 and SI-03 (62,782). Checked against
`48dt-mn3z`, the same DCP pipeline run on CDTA geography: MN-11 matches
exactly, SI-03 is within 0.2%, BK-12 differs by 2.4% and BX-10 by 1.1%. Eleven
districts with no duplicate at all differ by more than that — BK-13 by 4.6%,
MN-05 by 6.5% — because CDTAs are built from whole census tracts and only
approximate community district lines. The collisions do not stand out against
that noise.

Downgraded to unresolved-but-unremarkable. Both files come from one pipeline,
so their agreement is not independent verification; settling it needs
block-level 2020 Decennial data, and the Census API now rejects
unauthenticated calls with "Missing Key". A free API key would close this.

## Geography

All 59 real community districts are present in `dbdt-5s7j`, alongside 12
joint interest areas — `164` (Central Park), `226`–`228`, `355`–`356`,
`480`–`484`, `595` — with near-zero housing stock. Left in, they take the top
twelve places outright and the analysis returns Central Park. The pipeline
whitelists the 59.
