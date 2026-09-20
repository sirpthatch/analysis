"""Apply the CPC fast-track methodology and rank the 59 community districts.

The rule, restated (full text and sourcing in `CLAUDE.md` / `research/`):

  numerator    affordable dwelling units that have both an HPD-reported Start
               Date and a DOB construction permit by cycle end, with at least
               one of the two falling inside the cycle, excluding units in
               existing residential buildings under a preservation program
  denominator  2020 Decennial Census housing units + net new housing units
               through the cycle start
  ranking      numerator / denominator, ascending, twelve lowest, all 59
               community districts pooled

`rank_districts` returns every district with both the rule's rate ranking and
the count ranking most coverage assumes, because the gap between them is the
question this project exists to answer.
"""

import numpy as np
import pandas as pd

from constants import (
    COMMUNITY_DISTRICTS,
    CYCLE_END,
    CYCLE_START,
    DENOMINATOR_COMPLETION_YEARS,
    FAST_TRACK_LIST_SIZE,
    PROCESSED_DIR,
    RAW_FILES,
    to_hpd_district,
)

# HPD's `reporting_construction_type` is a clean binary.  "New Construction" is
# the closest available proxy for the rule's exclusion, which is narrower: it
# drops affordable units in *existing residential buildings* under a
# preservation program receiving financial assistance.  A gut rehab of a
# vacant non-residential building is new construction to the rule but may be
# reported either way here.  Tracked as a limitation, tested in `sensitivity`.
NEW_CONSTRUCTION = "NEW CONSTRUCTION"

# The unit measure.  HPD's `all_counted_units` is the income-restricted count;
# `total_units` includes market-rate units in the same building, which the
# rule's "affordable dwelling unit" definition excludes.
#
# Note what this does and does not mean by "affordable": it is every unit HPD
# counted toward the mayoral housing plan, across all income bands, from 0-30%
# of AMI up to 121-165% - roughly $46,000 to $252,000 for a three-person
# household in 2026.  Middle-income units are the largest single share of it.
# The rule weights every band identically.  See `research/definitions.md`.
UNIT_COLUMN = "all_counted_units"

# HPD's income bands, shallowest last.  Kept here so any cut of the numerator
# by depth of affordability uses one ordering.
INCOME_BANDS = (
    "extremely_low_income_units",   # 0-30% AMI
    "very_low_income_units",        # 31-50%
    "low_income_units",             # 51-80%
    "moderate_income_units",        # 81-120%
    "middle_income_units",          # 121-165%
    "other_income_units",           # building superintendents
)

# What counts as "a permit for construction work issued by the department of
# buildings".  DOB NOW's vocabulary is descriptive; the set below is the work
# that actually builds a building, and excludes the permits that merely
# accompany it (plumbing, sprinklers, sidewalk sheds, scaffolds, signs).  A
# new building's first qualifying permit is normally foundation or earthwork,
# which is why those are in rather than general construction alone.
DOB_NOW_WORK_TYPES = frozenset({
    "GENERAL CONSTRUCTION",
    "STRUCTURAL",
    "FOUNDATION",
    "EARTH WORK",
    "SUPPORT OF EXCAVATION",
})

# Legacy BIS codes the same idea as a job type: NB is a new building, A1 and
# A2 are alterations.  A3 (minor work, no change to use, egress or occupancy)
# and DM (demolition) are out.
DOB_BIS_JOB_TYPES = frozenset({"NB", "A1", "A2"})


def load_hpd():
    """HPD production rows, dates parsed and district normalized."""
    frame = pd.read_csv(RAW_FILES["hpd_production"], dtype=str, low_memory=False)

    for column in ("project_start_date", "project_completion_date"):
        frame[column] = pd.to_datetime(frame[column], errors="coerce")

    for column in (UNIT_COLUMN, "total_units"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    frame["district"] = frame["community_board"].str.strip().str.upper()
    frame["construction_type"] = (
        frame["reporting_construction_type"].fillna("").str.strip().str.upper()
    )
    frame["bbl"] = frame["bbl"].str.strip()
    frame["bin"] = frame["bin"].str.strip()
    return frame


def load_permit_dates():
    """Earliest DOB construction-permit date per BBL and per BIN.

    Three sources, unioned: DOB NOW (current through this week, and the only
    one covering the cycle's last quarter), legacy BIS (jobs filed before the
    DOB NOW cutover), and DCP's own housing database at job level (already
    geocoded, but a quarter behind).  Taking the earliest across all three is
    deliberate - the rule asks whether a permit exists by a date, so the first
    qualifying permit is the one that matters.
    """
    by_bbl, by_bin = [], []

    now = _load_optional(RAW_FILES["dob_now_permits"])
    if now is not None:
        issued = pd.to_datetime(now["issued_date"], errors="coerce")
        # Fall back to the approval date where a permit row has no issue date.
        issued = issued.fillna(pd.to_datetime(now["approved_date"], errors="coerce"))
        construction = now["work_type"].fillna("").str.upper().isin(DOB_NOW_WORK_TYPES)
        frame = pd.DataFrame(
            {"bbl": now["bbl"].astype(str).str.strip(),
             "bin": now["bin"].astype(str).str.strip(),
             "permit_date": issued}
        )[construction]
        by_bbl.append(frame[["bbl", "permit_date"]])
        by_bin.append(frame[["bin", "permit_date"]])

    bis = _load_optional(RAW_FILES["dob_bis_permits"])
    if bis is not None:
        issued = pd.to_datetime(bis["issuance_date"], errors="coerce", format="mixed")
        construction = bis["job_type"].fillna("").str.upper().isin(DOB_BIS_JOB_TYPES)
        frame = pd.DataFrame(
            {"bin": bis["bin__"].astype(str).str.strip(), "permit_date": issued}
        )[construction]
        by_bin.append(frame)

    projects = _load_optional(RAW_FILES["dcp_housing_project"])
    if projects is not None:
        frame = pd.DataFrame(
            {"bbl": projects["bbl"].astype(str).str.strip(),
             "bin": projects["bin"].astype(str).str.strip(),
             "permit_date": pd.to_datetime(projects["datepermit"], errors="coerce")}
        )
        by_bbl.append(frame[["bbl", "permit_date"]])
        by_bin.append(frame[["bin", "permit_date"]])

    return _earliest(by_bbl, "bbl"), _earliest(by_bin, "bin")


def _load_optional(path):
    if not path.exists() or path.stat().st_size == 0:
        return None
    return pd.read_csv(path, dtype=str, low_memory=False)


def _earliest(frames, key):
    if not frames:
        return pd.Series(dtype="datetime64[ns]", name="permit_date")
    stacked = pd.concat(frames, ignore_index=True).dropna(subset=["permit_date"])
    stacked = stacked[stacked[key].notna() & (stacked[key] != "") & (stacked[key] != "nan")]
    return stacked.groupby(key)["permit_date"].min()


def attach_permits(hpd, permit_by_bbl, permit_by_bin):
    """Add the earliest DOB construction permit date to each HPD building."""
    hpd = hpd.copy()
    from_bbl = hpd["bbl"].map(permit_by_bbl)
    from_bin = hpd["bin"].map(permit_by_bin)
    hpd["permit_date"] = from_bbl.fillna(from_bin)
    hpd["permit_source"] = np.where(
        from_bbl.notna(), "bbl", np.where(from_bin.notna(), "bin", "unmatched")
    )
    return hpd


def apply_rule(hpd, require_permit=True, exclude_preservation=True,
               cycle_start=CYCLE_START, cycle_end=CYCLE_END):
    """Flag the rows whose units count toward the cycle's numerator."""
    start, end = pd.Timestamp(cycle_start), pd.Timestamp(cycle_end)
    hpd = hpd.copy()

    started_by_end = hpd["project_start_date"].notna() & (hpd["project_start_date"] <= end)
    permitted_by_end = hpd["permit_date"].notna() & (hpd["permit_date"] <= end)

    start_in_cycle = hpd["project_start_date"].between(start, end)
    permit_in_cycle = hpd["permit_date"].between(start, end)

    qualifies = started_by_end & (start_in_cycle | permit_in_cycle)
    if require_permit:
        qualifies &= permitted_by_end
    if exclude_preservation:
        qualifies &= hpd["construction_type"] == NEW_CONSTRUCTION

    hpd["in_cycle"] = qualifies

    # Units that start in-cycle but carry no findable permit are not merely
    # absent from the numerator - they are *removed* from it by a gap in the
    # public data rather than by the rule.  Tracking them separately is what
    # lets the ranking say which rates are depressed by the data and by how
    # much, instead of printing a zero as though it were a measurement.
    blocked = start_in_cycle & hpd["permit_date"].isna()
    if exclude_preservation:
        blocked &= hpd["construction_type"] == NEW_CONSTRUCTION
    hpd["units_missing_permit"] = blocked
    return hpd


def build_numerator(hpd):
    """Affordable units per district among the rows `apply_rule` flagged.

    Also carries `unmatched_units`: in-cycle units that no DOB permit could be
    found for, and which the rule therefore drops.  See `apply_rule`.
    """
    counted = hpd[hpd["in_cycle"]]
    units = counted.groupby("district")[UNIT_COLUMN].sum()
    buildings = counted.groupby("district").size().rename("buildings")

    blocked = hpd[hpd["units_missing_permit"]]
    unmatched = blocked.groupby("district")[UNIT_COLUMN].sum().rename("unmatched_units")

    return pd.concat(
        [units.rename("affordable_units"), buildings, unmatched], axis=1
    )


def build_denominator_cdta(completion_years=DENOMINATOR_COMPLETION_YEARS):
    """The denominator again, on CDTA geography.

    CDTAs are built from whole 2020 census tracts; some are exact community
    district equivalents and some are approximations, so this will never match
    the CD file exactly.  It is here because it is the only independent read
    available, and because it contradicts the CD file on two of the districts
    where that file reports suspiciously identical stock - see
    `denominator_discrepancies`.
    """
    frame = pd.read_csv(RAW_FILES["dcp_housing_cdta"], dtype=str)
    frame["district"] = frame["cdta2020"].str.strip().str.upper().map(
        lambda code: f"{code[:2]}-{code[2:]}" if code[2:].isdigit() else None
    )
    frame = frame[frame["district"].isin(
        [to_hpd_district(code) for code in COMMUNITY_DISTRICTS]
    )].copy()

    census = pd.to_numeric(frame["cenunits20"], errors="coerce").fillna(0)
    net_new = sum(
        pd.to_numeric(frame[year], errors="coerce").fillna(0)
        for year in completion_years
    )
    frame["census_units_2020"] = census.astype(int)
    frame["net_new_units"] = net_new.astype(int)
    frame["housing_units"] = (census + net_new).astype(int)
    return frame.set_index("district")[
        ["census_units_2020", "net_new_units", "housing_units"]
    ].sort_index()


def denominator_discrepancies(threshold=0.01):
    """Districts where the CD and CDTA denominators disagree materially.

    `dbdt-5s7j` reports byte-identical 2020 stock for MN-11/BX-10 and for
    BK-12/SI-03.  Two exact collisions among 59 values spread across tens of
    thousands is not what chance looks like, and the CDTA file gives different
    numbers for one member of each pair - so at least one value in each pair
    is suspect.  Settling it needs block-level Decennial data, which now
    requires a Census API key; until then both readings get ranked.
    """
    cd = build_denominator().rename(columns=lambda name: f"cd_{name}")
    cdta = build_denominator_cdta().rename(columns=lambda name: f"cdta_{name}")
    joined = cd.join(cdta, how="outer")
    joined["pct_difference"] = (
        (joined["cdta_census_units_2020"] - joined["cd_census_units_2020"])
        / joined["cd_census_units_2020"]
    )
    duplicated = joined["cd_census_units_2020"].duplicated(keep=False)
    joined["duplicated_in_cd_file"] = duplicated
    material = joined["pct_difference"].abs() >= threshold
    return joined[material | duplicated].sort_values("pct_difference")


def build_denominator(completion_years=DENOMINATOR_COMPLETION_YEARS):
    """2020 Census housing units plus net completions through the cycle start.

    `dbdt-5s7j` reports completions by calendar year, and the cycle starts on
    July 1 2021, so crediting all of 2021 overshoots by roughly half a year of
    that district's completions.  `sensitivity` reports the ranking with and
    without the adjustment; it moves hundredths of a point, which is enough to
    matter around the twelfth position.
    """
    frame = pd.read_csv(RAW_FILES["dcp_housing_cd"], dtype=str)
    frame = frame[frame["commntydst"].isin(COMMUNITY_DISTRICTS)].copy()

    census = pd.to_numeric(frame["cenunits20"], errors="coerce").fillna(0)
    net_new = sum(
        pd.to_numeric(frame[year], errors="coerce").fillna(0)
        for year in completion_years
    )

    frame["district"] = frame["commntydst"].map(to_hpd_district)
    frame["census_units_2020"] = census.astype(int)
    frame["net_new_units"] = net_new.astype(int)
    frame["housing_units"] = (census + net_new).astype(int)
    return frame.set_index("district")[
        ["census_units_2020", "net_new_units", "housing_units"]
    ].sort_index()


DENOMINATORS = {"cd": build_denominator, "cdta": build_denominator_cdta}


def rank_districts(hpd=None, denominator="cd", completion_years=DENOMINATOR_COMPLETION_YEARS,
                   **rule_kwargs):
    """The full 59-district table, ranked by the rule's rate and by raw count."""
    if hpd is None:
        hpd = attach_permits(load_hpd(), *load_permit_dates())

    numerator = build_numerator(apply_rule(hpd, **rule_kwargs))
    table = DENOMINATORS[denominator](completion_years).join(numerator)
    table = table.fillna({"affordable_units": 0, "buildings": 0, "unmatched_units": 0})

    table["affordable_units"] = table["affordable_units"].astype(int)
    table["buildings"] = table["buildings"].astype(int)
    table["unmatched_units"] = table["unmatched_units"].astype(int)
    table["rate"] = table["affordable_units"] / table["housing_units"]
    table["rate_pct"] = table["rate"] * 100

    # Ties broken by the smaller numerator, then by district, so the ordering
    # is reproducible.  The rule specifies no tie-break - flagged as an open
    # question, and `ties_near_cutoff` reports any that would actually bite.
    table = table.sort_values(
        ["rate", "affordable_units", "housing_units"], ascending=[True, True, True]
    )
    table["rate_rank"] = range(1, len(table) + 1)
    table["count_rank"] = table["affordable_units"].rank(method="first").astype(int)
    table["on_fast_track"] = table["rate_rank"] <= FAST_TRACK_LIST_SIZE
    table["on_count_list"] = table["count_rank"] <= FAST_TRACK_LIST_SIZE
    return table


# --- The counterfactual the rulemaking commenters asked for -----------------
# Commenters asked DCP to let a district's *existing* affordable housing count
# toward the measure; the Commission made no change.  Everything below models
# what that alternative would have produced, so the refusal can be quantified
# instead of only described.
#
# There is no authoritative count of "existing affordable stock" in NYC, so
# this is assembled from the two components that do have clean per-district
# numbers in open data:
#
#   NYCHA public housing apartments (183,141 citywide), plus
#   HPD-counted affordable units created or preserved before the cycle opened.
#
# What it misses is substantial and all in the same direction - it understates
# every district's stock: Mitchell-Lama, HDFC co-ops, older LIHTC properties,
# project-based Section 8 outside NYCHA, and the roughly one million
# rent-stabilized private units, none of which publish a per-district count.
# Treat the result as one defensible reading of the counterfactual, not as the
# stock-adjusted list.

NYCHA_BOROUGH_CODES = {
    "MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
    "QUEENS": "4", "STATEN ISLAND": "5",
    # Two developments straddle a borough line; the data book names both.
    # Assigned to the first, which is where the bulk of each sits.
    "BRONX/QUEENS": "2", "BROOKLYN/QUEENS": "3",
}


def nycha_units_by_district():
    """Public housing apartments per community district."""
    frame = pd.read_csv(RAW_FILES["nycha_developments"], dtype=str)
    borough = frame["borough"].str.strip().str.upper().map(NYCHA_BOROUGH_CODES)
    number = pd.to_numeric(frame["community_distirct"], errors="coerce")

    frame["district"] = [
        to_hpd_district(f"{b}{int(n):02d}") if pd.notna(b) and pd.notna(n) else None
        for b, n in zip(borough, number)
    ]
    frame["apartments"] = pd.to_numeric(
        frame["total_number_of_apartments"].str.replace(",", ""), errors="coerce"
    ).fillna(0)

    return frame.dropna(subset=["district"]).groupby("district")["apartments"].sum()


def prior_hpd_units_by_district(hpd=None, cycle_start=CYCLE_START):
    """HPD-counted affordable units created or preserved before the cycle.

    Both construction types, because preservation is exactly what "existing
    affordable stock" means here - the rule excludes it from production, which
    is the asymmetry this counterfactual is testing.
    """
    if hpd is None:
        hpd = load_hpd()
    prior = hpd[hpd["project_start_date"] < pd.Timestamp(cycle_start)]
    return prior.groupby("district")[UNIT_COLUMN].sum()


def existing_affordable_stock(hpd=None):
    """The stock proxy: NYCHA apartments plus pre-cycle HPD-counted units."""
    nycha = nycha_units_by_district()
    prior = prior_hpd_units_by_district(hpd)
    stock = pd.concat([nycha.rename("nycha"), prior.rename("hpd_prior")], axis=1)
    stock = stock.fillna(0)
    stock["existing_affordable"] = stock["nycha"] + stock["hpd_prior"]
    return stock


def rank_with_existing_stock(hpd=None, table=None):
    """Rank districts on (existing affordable + new production) / total homes.

    Returns the base table with the alternative rate, its rank, and which of
    the two lists each district lands on.
    """
    if hpd is None:
        hpd = attach_permits(load_hpd(), *load_permit_dates())
    if table is None:
        table = rank_districts(hpd)

    stock = existing_affordable_stock(hpd)
    combined = table.join(stock).fillna(
        {"nycha": 0, "hpd_prior": 0, "existing_affordable": 0}
    )

    combined["stock_adjusted_units"] = (
        combined["affordable_units"] + combined["existing_affordable"]
    )
    combined["stock_adjusted_rate"] = (
        combined["stock_adjusted_units"] / combined["housing_units"]
    )
    combined["stock_adjusted_pct"] = combined["stock_adjusted_rate"] * 100

    ordered = combined.sort_values(["stock_adjusted_rate", "stock_adjusted_units"])
    combined["stock_adjusted_rank"] = pd.Series(
        range(1, len(ordered) + 1), index=ordered.index
    )
    combined["on_stock_adjusted_list"] = (
        combined["stock_adjusted_rank"] <= FAST_TRACK_LIST_SIZE
    )

    combined["list_membership"] = np.select(
        [
            combined["on_fast_track"] & combined["on_stock_adjusted_list"],
            combined["on_fast_track"] & ~combined["on_stock_adjusted_list"],
            ~combined["on_fast_track"] & combined["on_stock_adjusted_list"],
        ],
        ["both", "adopted_rule_only", "stock_adjusted_only"],
        default="neither",
    )
    return combined


def rate_is_artifact(table):
    """Districts whose printed rate is an artifact of the permit data, not a
    measurement: every in-cycle unit they produced was dropped for want of a
    findable DOB permit, so the rate reads zero however much they built."""
    return (table["affordable_units"] == 0) & (table["unmatched_units"] > 0)


def units_to_escape(table, cutoff_rank=FAST_TRACK_LIST_SIZE + 1):
    """Extra affordable units each district would need to fall off the list.

    Measured against the rate of the district just below the cutoff, holding
    that bar fixed.  That is deliberately generous: if the districts below
    also gained units the bar would rise, so a district that cannot clear the
    fixed bar certainly cannot clear the moving one.
    """
    bar = table.loc[table["rate_rank"] == cutoff_rank, "rate"].iloc[0]
    needed = bar * table["housing_units"] - table["affordable_units"]
    return needed.clip(lower=0).rename("units_to_escape")


def typical_missing_quarter(hpd, years=range(2021, 2026)):
    """Per-district April-June production, from the quarters that are present.

    The HPD file stops at 2026-03-31, so the cycle's last quarter is missing
    entirely.  Prior years' second quarters are the only guide to how big it
    was.  Returns the mean and the largest single year - a central estimate
    and a deliberately pessimistic one.
    """
    new_build = hpd[hpd["construction_type"] == NEW_CONSTRUCTION]
    second_quarter = new_build[
        (new_build["project_start_date"].dt.quarter == 2)
        & (new_build["project_start_date"].dt.year.isin(list(years)))
    ]
    by_year = (
        second_quarter.groupby(
            ["district", second_quarter["project_start_date"].dt.year]
        )[UNIT_COLUMN]
        .sum()
        .unstack(fill_value=0)
    )
    return by_year.mean(axis=1).rename("typical"), by_year.max(axis=1).rename("largest")


def understatement_scenarios(hpd=None, table=None):
    """Could a district be on the list only because its production is undercounted?

    Two things suppress the numerator: units whose building cannot be matched
    to a DOB permit, and the cycle's missing final quarter.  Both can only push
    a rate *up* if corrected, so both can only push a district *off* the list.
    Each scenario credits them back - to every district at once, since the
    ranking is relative - and re-ranks.

    Returns (ranks, membership) indexed by district, one column per scenario.
    """
    if hpd is None:
        hpd = attach_permits(load_hpd(), *load_permit_dates())
    if table is None:
        table = rank_districts(hpd)

    typical, largest = typical_missing_quarter(hpd)
    unmatched = table["unmatched_units"].astype(float)
    scenarios = {
        "base": pd.Series(0.0, index=table.index),
        "credit_unmatched": unmatched,
        "plus_typical_quarter": unmatched + typical.reindex(table.index).fillna(0),
        "plus_largest_quarter": unmatched + largest.reindex(table.index).fillna(0),
    }

    ranks, membership = {}, {}
    for name, extra in scenarios.items():
        scenario = table.copy()
        scenario["affordable_units"] = scenario["affordable_units"] + extra
        scenario["rate"] = scenario["affordable_units"] / scenario["housing_units"]
        scenario = scenario.sort_values(["rate", "affordable_units"])
        scenario["rate_rank"] = range(1, len(scenario) + 1)
        ranks[name] = scenario["rate_rank"]
        membership[name] = scenario["rate_rank"] <= FAST_TRACK_LIST_SIZE

    return pd.DataFrame(ranks), pd.DataFrame(membership)


def ties_near_cutoff(table, window=3):
    """Districts whose rate is close enough to the cutoff to flip on noise."""
    cutoff = table.loc[table["rate_rank"] == FAST_TRACK_LIST_SIZE, "rate"].iloc[0]
    lower, upper = FAST_TRACK_LIST_SIZE - window, FAST_TRACK_LIST_SIZE + window
    near = table[table["rate_rank"].between(lower, upper)].copy()
    near["pct_points_from_cutoff"] = (near["rate"] - cutoff) * 100
    return near


def save(table, name="fast_track_ranking.csv"):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    path = PROCESSED_DIR / name
    table.to_csv(path)
    return path
