# Working plan

Phase 0 is done; the project scaffold, the collection pipeline and the ranking
code are in place and the first four of five sources are on disk. What follows
is the order of work from here.

## Phase 0 — Scaffold and collection (done)

- `src/constants.py` — cycle dates, dataset IDs, the 59 real community
  districts, and the two district-code spellings NYC uses.
- `src/socrata.py` — paginated SoQL pulls, cached to `data/raw/`.
- `src/collect.py` — one subcommand per source, plus `freshness`.
- `src/fasttrack.py` — the rule, codified.
- `src/rank.py` — the projected list and the sensitivity run.

## Phase 1 — Profile the sources

Before any number is reported, establish:

1. **Permit match rate.** What share of HPD affordable buildings can be tied
   to a DOB construction permit at all, by BBL or BIN? A district whose
   buildings systematically fail to match will look artificially low-rate.
   This is the single biggest threat to the ranking's validity, because the
   rule makes the permit a *condition*, not a nice-to-have.
2. **Where the unmatched rows are.** If non-matching is uniform across
   districts it mostly cancels out of a ranking; if it clusters, it does not.
3. **The missing quarter.** `hg8x-zxpr` stops at 2026-03-31, so April–June
   2026 is absent. Estimate its size from the same quarter in prior years and
   say what it could move.
4. **The 1,518 HPD rows since 2016 with no BBL.** Can they be recovered by
   geocoding the address, or do they have to be reported as a known loss?

Written up in `research/profile_findings.md`.

## Phase 2 — The projected list

Run `rank.py --sensitivity`. Report three things, not one:

- the twelve districts the base run selects;
- the districts that hold their place under every alternative reading;
- the contested band around the cutoff, where the answer depends on a
  judgment call rather than on the data.

The comparison against the count-based bottom twelve is the story, so it gets
reported at the same level of confidence as the list itself.

## Phase 3 — Close the rule questions

These are documentary, not computational, and they are what turn "the rule
structurally does this" into "the city means it to."

1. Read the CPC Statement of Basis and Purpose in full (URL in
   `research-log.md`). Look for a stated rationale for refusing to credit
   existing affordable stock — equity, simplicity, or a claim that the charter
   left DCP no discretion.

   **Answer**: the 

2. Pull the charter amendment's actual text. Did voters approve the word
   "rate," or did DCP choose it in rulemaking?

   **Anwser**: The voted amendment included the word "rate", so the DCP did not have any choice here.

3. Search Council Land Use hearings and 2025 Charter Revision Commission
   testimony for anyone addressing the stock question directly.

   **Answer**:

## Phase 4 — Compare against the official list

DCP must publish by 2026-10-01. `collect.py` should grow a check for a new
DCP dataset or press release. If the reverse-engineered list and the official
one diverge, the divergence is the story: it means the published data cannot
reproduce a decision that removes the City Council from land-use review in
twelve districts.

## Phase 5 — Write up

`article/`, for an NYC urban-development Substack.
