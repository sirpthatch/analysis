#!/usr/bin/env python3
"""
Offline sanity pass over the shipped CSVs. No network, no dependencies
beyond pandas. Reproduces every number quoted in the README so you can
confirm nothing drifted before you build on it.

    python3 explore.py
"""
import os
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, os.pardir, "data")


def load(name):
    return pd.read_csv(os.path.join(DATA, name))


def main():
    cats = load("violation_categories_fy2026.csv")
    monthly = load("monthly_by_category_fy2026.csv")
    fixed = load("top_locations_fixed_fy2026.csv")
    mobile = load("top_locations_mobile_fy2026.csv")
    county = load("by_county_by_category_fy2026.csv")

    print("=" * 62)
    print("CATEGORIES")
    print(cats.to_string(index=False))
    total = cats["count"].sum()
    print(f"\nall bus-lane-related violations, FY2026: {total:,}")

    print("\n" + "=" * 62)
    print("RECONCILIATION  (monthly series must equal category totals)")
    roll = monthly.groupby("violation_description")["count"].sum()
    for desc, n in roll.items():
        want = int(cats.loc[cats.violation_description == desc, "count"].iloc[0])
        flag = "OK " if n == want else "MISMATCH"
        print(f"  {flag} {desc:<28} {n:>8,} vs {want:>8,}")

    print("\n" + "=" * 62)
    print("CONCENTRATION  (fixed roadside cameras)")
    fx_total = int(cats.loc[cats.violation_description == "BUS LANE VIOLATION",
                            "count"].iloc[0])
    for n in (3, 6, 12, 60):
        share = fixed.head(n)["count"].sum() / fx_total
        print(f"  top {n:>2} locations: {fixed.head(n)['count'].sum():>7,}"
              f"  = {share:6.1%} of all fixed-camera tickets")

    jam = fixed[fixed.street_name.str.contains("JAMAICA AVE")]
    print(f"\n  Jamaica Ave locations in top 60: {len(jam)}"
          f"  -> {jam['count'].sum():,} = {jam['count'].sum()/fx_total:.1%}")

    print("\n" + "=" * 62)
    print("THE MOBILE-CAMERA CLIFF")
    mob = (monthly[monthly.violation_description == "MOBILE BUS LANE VIOLATION"]
           .set_index("month")["count"])
    fx = (monthly[monthly.violation_description == "BUS LANE VIOLATION"]
          .set_index("month")["count"])
    both = pd.DataFrame({"fixed": fx, "mobile": mob}).fillna(0).astype(int)
    both["mobile_share"] = both.mobile / (both.fixed + both.mobile)
    print(both.to_string(formatters={"mobile_share": "{:.1%}".format}))
    print("\n  Mobile records stop after 2026-04. Enforcement pause, or a")
    print("  reporting gap in the file? Resolve this before publishing.")

    print("\n" + "=" * 62)
    print("COUNTY CODING IS A MESS  (distinct values per category)")
    for desc, grp in county.groupby("violation_description"):
        vals = [("<blank>" if pd.isna(v) else v) for v in grp.violation_county]
        print(f"  {desc:<28} {vals}")
    blank = county[county.violation_county.isna()]["count"].sum()
    print(f"\n  rows with NO borough at all: {blank:,}"
          " (all mobile-camera) — do not drop these silently")

    print("\n" + "=" * 62)
    print("CASING TRAP")
    print(f"  fixed  sample: {fixed.street_name.iloc[0]!r}")
    print(f"  mobile sample: {mobile.street_name.iloc[0]!r}")
    print("  Same dataset, same column, different case convention.")
    print("  Always group on upper(street_name).")


if __name__ == "__main__":
    main()
