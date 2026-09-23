# Handoff bundle: NYC truck side-guard waivers vs. crash data

Prepared 2026-09-22 for Thatcher Clay (UrbanCalc, https://urbancalc.substack.com).
This is a research-idea package, not a finished analysis. Everything here is a
starting point for reporting/analysis that has not been done yet.

## Drop-in prompt for a fresh Claude session

> I'm developing a data story for my NYC urban-development Substack. Attached is a
> handoff bundle. Read `BRIEF.md` for the argument and the news hook,
> `VERIFIED-DATA.md` for exactly which dataset IDs, columns and figures were already
> confirmed against NYC Open Data (and which were not), and `queries.md` for the
> working SoQL. `scripts/analysis.py` is an unrun starter that pulls the crash and
> vehicle tables and builds the yearly series.
>
> Start with the open question at the top of `NEXT-STEPS.md`: I need the citywide
> crash denominator before the flat refuse-truck series means anything. Do not
> trust any number that isn't in `VERIFIED-DATA.md` — re-query it.

## What's in here

    README.md            this file
    BRIEF.md             the pitch: argument, hook, datasets, kill conditions, limits
    VERIFIED-DATA.md     provenance ledger — what was actually queried on 2026-09-22
    NEXT-STEPS.md        ordered work plan, including the FOIL that should go out now
    queries.md           copy-pasteable SoQL, with the case-sensitivity trap explained
    SOURCES.md           every link cited
    data/refuse_vehicle_records_by_year.csv    verified query output
    data/vehicle_type_counts_2025plus.csv      verified query output
    scripts/analysis.py  runnable starter (NOT YET RUN — no results are baked in)
    scripts/heavy_vehicle_codes.py  the vehicle_type code list to curate

## The one-line version

New York required side guards on heavy trade waste trucks in 2015 with a 2023
compliance deadline, then waived roughly half the fleet. Refuse trucks show up in
NYPD crash records at about the same rate every year since 2020 — 398 in 2023, 406
in 2024, 402 in 2025 — which is what you'd expect if the rule is being waived into
irrelevance. The analysis is to narrow that to side-impact crashes involving
cyclists and pedestrians, normalize against citywide crash volume, and see whether
the 2023 deadline shows up at all.

## Ground rules carried over

No dataset ID, column name or statistic in this bundle was invented. Anything not
confirmed by a successful query on 2026-09-22 is flagged as unverified in
`VERIFIED-DATA.md`. Keep that discipline going.
