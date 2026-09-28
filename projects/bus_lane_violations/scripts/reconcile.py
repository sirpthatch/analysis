#!/usr/bin/env python3
"""
Resolves the "mobile-camera cliff" flagged in the README as blocking.

DOF's fiscal-year files are split by *processing* date, not `issue_date`.
The FY2026 file (9mwx-gamw) already known to carry a June-2025 spill at its
front edge; this script shows it also spills FORWARD at its tail edge --
April-June 2026 tickets that were still being processed when the FY2026
file was cut get filed under FY2027 (pvqr-7yc4) instead, even though their
issue_date falls inside the nominal FY2026 window (Jul 2025 - Jun 2026).

Verifies zero summons_number overlap between the two files for June 2026,
so this is a merge, not a dedupe problem. Writes the corrected monthly
series to ../data/monthly_by_category_fy2026_corrected.csv.

    python3 reconcile.py

Needs requests and pandas.
"""
import os
import sys
import requests
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, os.pardir, "data")

FY2026 = "9mwx-gamw"
FY2027 = "pvqr-7yc4"
BUS_LANE = "upper(violation_description) like '%BUS LANE%'"


def session():
    s = requests.Session()
    tok = os.environ.get("SODA_APP_TOKEN")
    if tok:
        s.headers["X-App-Token"] = tok
    else:
        print("note: no SODA_APP_TOKEN set; you may hit HTTP 429", file=sys.stderr)
    return s


def soql(s, fourby, **params):
    url = f"https://data.cityofnewyork.us/resource/{fourby}.json"
    q = {"$" + k: v for k, v in params.items()}
    r = s.get(url, params=q, timeout=60)
    r.raise_for_status()
    return r.json()


def verify_no_overlap(s):
    """Spot-check: June 2026 fixed-camera summons numbers, FY2026 vs FY2027."""
    where = (
        "violation_description='BUS LANE VIOLATION' AND issue_date "
        "between '2026-06-01T00:00:00' and '2026-06-30T23:59:59'"
    )
    a = soql(s, FY2026, select="summons_number", where=where, limit="50000")
    b = soql(s, FY2027, select="summons_number", where=where, limit="50000")
    sa = {r["summons_number"] for r in a}
    sb = {r["summons_number"] for r in b}
    overlap = sa & sb
    print(f"June 2026 fixed-camera rows: {len(sa)} in FY2026 file, "
          f"{len(sb)} in FY2027 file, {len(overlap)} overlap")
    if overlap:
        raise SystemExit(
            "overlap found -- these files are NOT a clean split, "
            "dedupe before merging"
        )


def main():
    s = session()
    verify_no_overlap(s)

    orig = pd.read_csv(os.path.join(DATA, "monthly_by_category_fy2026.csv"))
    orig["count"] = orig["count"].astype(int)
    # drop the known June-2025 spill (issue_date outside the Jul25-Jun26 window)
    orig_w = orig[orig["month"] != "2025-06"].copy()

    fy27_rows = soql(
        s, FY2027,
        select="date_trunc_ym(issue_date) as month,violation_description,count(*)",
        where=BUS_LANE, group="month,violation_description",
        order="month", limit="200",
    )
    fy27 = pd.DataFrame(fy27_rows)
    fy27["month"] = fy27["month"].str[:7]
    fy27["count"] = fy27["count"].astype(int)
    # only the slice that falls inside the FY2026 issue-date window
    spill = fy27[fy27["month"].between("2026-04", "2026-06")].copy()

    merged = pd.concat([orig_w, spill], ignore_index=True)
    merged = (
        merged.groupby(["month", "violation_description"], as_index=False)["count"]
        .sum()
        .sort_values(["month", "violation_description"])
    )
    out = os.path.join(DATA, "monthly_by_category_fy2026_corrected.csv")
    merged.to_csv(out, index=False)
    print(f"wrote {len(merged)} rows -> {os.path.relpath(out)}")

    print("\ncorrected fixed vs mobile, Apr-Jun 2026:")
    piv = (
        merged.pivot(index="month", columns="violation_description", values="count")
        .fillna(0).astype(int)
    )
    print(piv.loc["2026-04":"2026-06", ["BUS LANE VIOLATION", "MOBILE BUS LANE VIOLATION"]])

    print("\ncorrected category totals (true issue-date FY2026 window):")
    print(merged.groupby("violation_description")["count"].sum())
    print("corrected grand total:", merged["count"].sum())
    print("original (uncorrected) grand total: 941312")


if __name__ == "__main__":
    main()
