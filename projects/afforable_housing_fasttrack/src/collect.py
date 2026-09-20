"""Pull every source dataset the fast-track ranking needs.

    python src/collect.py all
    python src/collect.py hpd --refresh

Pulls are cached as CSV under `data/raw/` and skipped if already present, so
an interrupted run can be topped up by re-running the same command.  The DOB
and DCP-project pulls are keyed off the BBLs in the HPD numerator set, so run
`hpd` before them (`all` does this in order).
"""

import argparse
import sys

import pandas as pd

import socrata
from constants import (
    SOCRATA_DOMAIN,
    CYCLE_END,
    EXTERNAL_FILES,
    CYCLE_START,
    DCP_HOUSING_CD_ID,
    DCP_HOUSING_CDTA_ID,
    DCP_HOUSING_PROJECT_ID,
    DOB_BIS_PERMITS_ID,
    DOB_NOW_PERMITS_ID,
    HPD_PRODUCTION_ID,
    NYCHA_DEVELOPMENTS_ID,
    RAW_FILES,
)

# The rule counts a unit whose HPD Start Date or DOB permit falls in-cycle,
# with both milestones met by cycle end.  A project that started before the
# cycle can still qualify on an in-cycle permit, so the HPD pull deliberately
# reaches back further than CYCLE_START and the in-cycle test is applied later,
# in `fasttrack.py`, once both dates are joined together.
HPD_LOOKBACK_START = "2011-01-01"

HPD_FIELDS = (
    "project_id,project_name,project_start_date,project_completion_date,"
    "building_id,house_number,street_name,borough,bbl,bin,community_board,"
    "council_district,reporting_construction_type,extended_affordability_status,"
    "extremely_low_income_units,very_low_income_units,low_income_units,"
    "moderate_income_units,middle_income_units,other_income_units,"
    "counted_rental_units,counted_homeownership_units,all_counted_units,total_units"
)

DOB_NOW_FIELDS = (
    "job_filing_number,bbl,bin,c_b_no,work_type,filing_reason,permit_status,"
    "approved_date,issued_date,job_description,house_no,street_name,borough"
)

DOB_BIS_FIELDS = (
    "job__,bin__,bbl,borough,community_board,job_type,permit_type,permit_subtype,"
    "work_type,permit_status,filing_date,issuance_date,job_start_date,"
    "house__,street_name"
)

DCP_PROJECT_FIELDS = (
    "job_number,job_type,job_status,bbl,bin,commntydst,classainit,classaprop,"
    "classanet,units_co,datefiled,datepermit,datecomplt,compltyear,permityear,"
    "job_desc,addressnum,addressst"
)


def fetch_hpd(refresh=False):
    """Every affordable-housing building HPD has reported since 2011."""
    def loader():
        return socrata.fetch_all(
            HPD_PRODUCTION_ID,
            select=HPD_FIELDS,
            where=f"project_start_date >= '{HPD_LOOKBACK_START}'",
        )

    return socrata.cached_pull(RAW_FILES["hpd_production"], loader, refresh)


def fetch_dcp_cd(refresh=False):
    """The denominator table: 2020 Census units + net completions by year."""
    def loader():
        fields = ",".join(
            ["commntydst", "cenunits20", "comp2010ap"]
            + [f"comp{year}" for year in range(2010, 2025)]
            + ["filed", "approved", "permitted", "withdrawn", "inactive"]
        )
        return socrata.fetch_all(DCP_HOUSING_CD_ID, select=fields)

    return socrata.cached_pull(RAW_FILES["dcp_housing_cd"], loader, refresh)


def fetch_dcp_cdta(refresh=False):
    """The same denominator on CDTA geography - the cross-check on `dcp-cd`."""
    def loader():
        fields = ",".join(
            ["cdta2020", "cdtaname20", "cenunits20"]
            + [f"comp{year}" for year in range(2010, 2025)]
        )
        return socrata.fetch_all(DCP_HOUSING_CDTA_ID, select=fields)

    return socrata.cached_pull(RAW_FILES["dcp_housing_cdta"], loader, refresh)


def fetch_nycha(refresh=False):
    """NYCHA public housing apartments per development."""
    def loader():
        return socrata.fetch_all(
            NYCHA_DEVELOPMENTS_ID,
            select=("development,borough,community_distirct,program,"
                    "total_number_of_apartments,number_of_current_apartments,"
                    "completion_date,data_as_of"),
        )

    return socrata.cached_pull(RAW_FILES["nycha_developments"], loader, refresh)


def fetch_boundaries(refresh=False):
    """Community district polygons, for the maps.

    Taken from the denominator dataset rather than DCP's standalone boundary
    file so the geometry and the housing counts are keyed by the same
    `commntydst` values and cannot drift apart.
    """
    path = EXTERNAL_FILES["community_district_geom"]
    if path.exists() and path.stat().st_size > 0 and not refresh:
        print(f"  have {path.name}")
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    response = socrata._get(
        f"https://{SOCRATA_DOMAIN}/resource/{DCP_HOUSING_CD_ID}.geojson",
        {"$select": "commntydst,the_geom", "$limit": 200},
    )
    path.write_bytes(response.content)
    print(f"  got  {path.name}")
    return path


def _numerator_bbls():
    """BBLs of the HPD buildings that could plausibly land in the cycle.

    Restricting the DOB pulls to these keeps them to thousands of rows rather
    than the millions of permits citywide.
    """
    hpd = fetch_hpd()
    recent = hpd[hpd["project_start_date"].fillna("") >= "2016-01-01"]
    return sorted(recent["bbl"].dropna().astype(str).str.strip().unique())


def fetch_dob_now(refresh=False):
    """DOB NOW approved permits for the numerator BBLs.

    DOB NOW is where new-building work has filed since the 2021 cutover, and
    it is refreshed daily - the only permit source current enough to cover
    April-June 2026, the quarter missing from the HPD file.
    """
    def loader():
        return socrata.fetch_in_chunks(
            DOB_NOW_PERMITS_ID, "bbl", _numerator_bbls(), select=DOB_NOW_FIELDS
        )

    return socrata.cached_pull(RAW_FILES["dob_now_permits"], loader, refresh)


def fetch_dob_bis(refresh=False):
    """Legacy BIS permit issuance, joined on BIN.

    `bbl` is sparse in this table, so the BIN is the reliable key.  Jobs filed
    before the DOB NOW cutover still issue permits here, so the cycle's early
    years need it.
    """
    def loader():
        hpd = fetch_hpd()
        recent = hpd[hpd["project_start_date"].fillna("") >= "2016-01-01"]
        bins = sorted(recent["bin"].dropna().astype(str).str.strip().unique())
        return socrata.fetch_in_chunks(
            DOB_BIS_PERMITS_ID, "bin__", bins, select=DOB_BIS_FIELDS
        )

    return socrata.cached_pull(RAW_FILES["dob_bis_permits"], loader, refresh)


def fetch_dcp_projects(refresh=False):
    """DCP's own housing database at job level - `datepermit` per DOB job.

    Same source family as the denominator, already geocoded and deduped, so
    it is the cross-check on the two raw DOB pulls rather than a substitute:
    it lags by a quarter or more.
    """
    def loader():
        return socrata.fetch_in_chunks(
            DCP_HOUSING_PROJECT_ID, "bbl", _numerator_bbls(), select=DCP_PROJECT_FIELDS
        )

    return socrata.cached_pull(RAW_FILES["dcp_housing_project"], loader, refresh)


COMMANDS = {
    "hpd": fetch_hpd,
    "dcp-cd": fetch_dcp_cd,
    "dcp-cdta": fetch_dcp_cdta,
    "boundaries": fetch_boundaries,
    "nycha": fetch_nycha,
    "dcp-projects": fetch_dcp_projects,
    "dob-now": fetch_dob_now,
    "dob-bis": fetch_dob_bis,
}

# HPD first: the other three pulls are keyed off its BBLs and BINs.
ALL_ORDER = (
    "hpd", "dcp-cd", "dcp-cdta", "boundaries", "nycha", "dcp-projects",
    "dob-now", "dob-bis",
)


def report_freshness():
    """Print each source's last-updated stamp against the cycle it must cover."""
    print(f"\nCycle under measurement: {CYCLE_START} .. {CYCLE_END}")
    for dataset_id in (
        HPD_PRODUCTION_ID,
        DCP_HOUSING_CD_ID,
    DCP_HOUSING_CDTA_ID,
        DCP_HOUSING_PROJECT_ID,
        DOB_NOW_PERMITS_ID,
        DOB_BIS_PERMITS_ID,
    ):
        info = socrata.metadata(dataset_id)
        stale = " <-- ends before cycle end" if info["rows_updated_at"] < pd.Timestamp(CYCLE_END) else ""
        print(f"  {dataset_id}  {info['rows_updated_at']:%Y-%m-%d}  {info['name']}{stale}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=list(COMMANDS) + ["all", "freshness"])
    parser.add_argument("--refresh", action="store_true", help="re-fetch even if cached")
    args = parser.parse_args()

    if args.command == "freshness":
        report_freshness()
        return

    names = ALL_ORDER if args.command == "all" else [args.command]
    for name in names:
        print(f"{name}:")
        COMMANDS[name](refresh=args.refresh)


if __name__ == "__main__":
    sys.exit(main())
