# Why the crash file stopped updating — what's on the record

Compiled 2026-09-21. This closes the project's largest open question, though not
completely: there is an official explanation, but it is thin and its own deadline
has passed.

## Press coverage

Two stories, one day apart. Streetsblog broke it; Hoodline followed.

- **Streetsblog NYC, 2026-09-09** — ["Empty Quarter: City's Crash Database Hasn't Been Updated In Three Months"](https://nyc.streetsblog.org/2026/09/09/empty-quarter-citys-crash-database-hasnt-been-updated-in-three-months).
  The original reporting, and the only piece carrying on-record agency comment.
- **Hoodline, 2026-09-10** — ["NYC's Crash Data Has Been Frozen Since June, Hiding Thousands of Wrecks"](https://hoodline.com/2026/09/nyc-s-crash-data-has-been-frozen-since-june-hiding-thousands-of-wrecks/).
  Follows Streetsblog; source of the 54,832 figure this project validated against.

No other coverage found as of 2026-09-21, and no city press release.

## The official explanation

**The portal is run by the Office of Technology and Innovation (OTI)**, not NYPD —
though the dataset's own metadata still lists NYPD as the owning agency, update
frequency "Daily", automation "Yes".

Three statements on the record:

1. **The dataset page itself** (still live, verified 2026-09-21):
   > "This dataset is temporarily not updating while its automated update process
   > is being fixed. This fix is expected to be completed during the month of
   > August."

2. **OTI spokesperson Gloria Chin** (Aug. 28), declining specific questions:
   > "The work to improve access to the data is complete and undergoing a final
   > review."

3. **DOT spokesperson Vin Barone**:
   > "Granular data uploads have been temporarily paused as we work with OTI to
   > update the uploading process with improved data presentation."

So the stated cause is a **pipeline migration, not a data problem** — the crashes
exist, the publishing mechanism was being rebuilt. Note the framing in all three:
this is described as an *improvement* in progress, not an outage.

**The August deadline passed without a fix.** As of 2026-09-21 the note is
unchanged and the data is still frozen — three weeks past its own stated
completion, and over three months since the last published row.

## Verified against the live catalog (2026-09-21)

| Field | Value |
|---|---|
| Newest crash row | 2026-06-11 |
| `rowsUpdatedAt` | 2026-06-15 |
| `viewLastModified` | 2026-07-31 |
| Update frequency (metadata) | Daily |
| Automation (metadata) | Yes |

`viewLastModified` of 2026-07-31 is probably when the note was added to the
description — worth noting that the city did not flag the problem until roughly
seven weeks after the data stopped.

## Others affected

The freeze propagates to every downstream tool built on the feed:

- **Crashmapper** (CHEKPEDS) and **CrashCount** (Michael Freedman) both map from
  this dataset and have been silently missing crashes since June 11. Streetsblog's
  concrete example: Court Street in Brooklyn showed six injuries through July 31
  in Vision Zero View but only four in Crashmapper, because two occurred after
  the freeze.
- Quoted in the coverage: **Michael Freedman** (CrashCount), **Noel Hidalgo**
  (BetaNYC executive director).

The consequence Streetsblog draws — that the public cannot check DOT's own safety
claims against the data — is the strongest framing available for this project.

## A third source worth pursuing

**Vision Zero View** (nyc.gov) is described in the coverage as running on *raw
NYPD data* and was current through July 31 while the Open Data file was frozen.
If so it is an **independent cross-check on the reconstruction**, separate from
both Socrata and TrafficStat. The Court Street example implies record-level
detail is reachable.

This is the highest-value next step: a third source agreeing would make the
reconstruction very hard to dispute. See open items in `CLAUDE.md`.

## What is still unexplained

- **Why a routine pipeline migration took over three months** and blew its own
  deadline, with no revised estimate.
- **Why the dataset was not flagged until ~July 31**, seven weeks in.
- **Whether TrafficStat's time-of-day field degrading in May** — 95% populated
  through April, 12.5% in May, 0% from June (see `research/limitations.md` #7) —
  shares a cause with the June publishing freeze. The timing is suggestive and
  entirely uninvestigated. Nobody has reported this; it came out of this
  project's own profiling.

That last point is the one genuinely novel finding here. The press has the
freeze; nobody has the May degradation that preceded it.
