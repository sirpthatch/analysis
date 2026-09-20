"""Build and print the projected fast-track list.

    python src/rank.py
    python src/rank.py --sensitivity

Writes `data/processed/fast_track_ranking.csv`.  The sensitivity run varies
each place where the public data does not line up exactly with the rule's
language, and reports which districts change status as a result - that set is
the honest uncertainty around the projection.
"""

import argparse

import pandas as pd

import fasttrack as ft
from constants import BOROUGH_NAMES, CYCLE_END, CYCLE_START, FAST_TRACK_LIST_SIZE

DISPLAY_COLUMNS = [
    "affordable_units",
    "buildings",
    "housing_units",
    "rate_pct",
    "rate_rank",
    "count_rank",
]

# Each variant is one defensible alternative reading, held against the base
# run.  Districts that hold their position across all of them are the ones to
# report with confidence.
VARIANTS = {
    "base": {},
    "no_permit_condition": {"require_permit": False},
    "include_preservation": {"exclude_preservation": False},
    "cycle_through_hpd_data": {"cycle_end": "2026-03-31"},
}


def _label(district):
    return f"{district} ({BOROUGH_NAMES[district[:2]]})"


def print_list(table):
    fast_track = table[table["on_fast_track"]]
    print(f"\nProjected fast-track list - cycle {CYCLE_START} to {CYCLE_END}")
    print("=" * 78)
    for district, row in fast_track.iterrows():
        moved = "" if row["on_count_list"] else "   <- not on the count-based list"
        print(
            f"{int(row['rate_rank']):>3}. {_label(district):<22}"
            f"{int(row['affordable_units']):>6} units /"
            f"{int(row['housing_units']):>8,} homes ="
            f"{row['rate_pct']:>7.3f}%{moved}"
        )

    displaced = table[table["on_count_list"] & ~table["on_fast_track"]]
    if not displaced.empty:
        print("\nOn the count-based bottom twelve, but NOT on the rule's list:")
        for district, row in displaced.iterrows():
            print(
                f"     {_label(district):<22}"
                f"{int(row['affordable_units']):>6} units /"
                f"{int(row['housing_units']):>8,} homes ="
                f"{row['rate_pct']:>7.3f}%  (rate rank {int(row['rate_rank'])})"
            )


def print_cutoff(table):
    print(f"\nAround the cutoff (rank {FAST_TRACK_LIST_SIZE}):")
    near = ft.ties_near_cutoff(table)
    print(
        near[DISPLAY_COLUMNS + ["pct_points_from_cutoff"]]
        .round({"rate_pct": 4, "pct_points_from_cutoff": 4})
        .to_string()
    )


def run_sensitivity():
    """Rank under every variant and report which districts are not stable."""
    results = {}
    hpd = ft.attach_permits(ft.load_hpd(), *ft.load_permit_dates())
    for name, kwargs in VARIANTS.items():
        results[name] = ft.rank_districts(hpd, **kwargs)

    membership = pd.DataFrame(
        {name: table["on_fast_track"] for name, table in results.items()}
    )
    ranks = pd.DataFrame({name: table["rate_rank"] for name, table in results.items()})

    stable_in = membership.all(axis=1)
    stable_out = ~membership.any(axis=1)
    contested = membership[~(stable_in | stable_out)]

    print("\nOn the list under every variant:")
    for district in membership.index[stable_in]:
        print(f"  {_label(district)}")

    print("\nContested - on the list under some readings and not others:")
    if contested.empty:
        print("  (none)")
    for district in contested.index:
        variants = ", ".join(name for name in VARIANTS if membership.loc[district, name])
        print(f"  {_label(district):<22} on under: {variants}")

    print("\nRate rank by variant (districts ranked 1-18 in the base run):")
    base_top = results["base"].sort_values("rate_rank").head(18).index
    print(ranks.loc[base_top].to_string())
    return results


def run_understatement():
    """Report whether undercounting alone could put a district on the list."""
    hpd = ft.attach_permits(ft.load_hpd(), *ft.load_permit_dates())
    table = ft.rank_districts(hpd)
    ranks, membership = ft.understatement_scenarios(hpd, table)

    on_list = table[table["on_fast_track"]].sort_values("rate_rank")
    margin = ft.units_to_escape(table)

    print("\nCould undercounting alone explain a district's place on the list?")
    print("=" * 78)
    print("Units each district would need to clear the 13th-place rate, against")
    print("the in-cycle units it produced that no DOB permit could be found for:\n")
    report = on_list.join(margin)[
        ["rate_rank", "affordable_units", "unmatched_units", "units_to_escape"]
    ]
    report["covers_the_gap"] = report["unmatched_units"] > report["units_to_escape"]
    print(report.round(1).to_string())

    stable = [d for d in membership.index if membership.loc[d].all()]
    flips = [
        d for d in membership.index
        if membership.loc[d].any() and not membership.loc[d].all()
    ]
    print("\nOn the list under every understatement scenario:")
    print("  " + ", ".join(_label(d) for d in sorted(stable)))
    print("\nStatus changes under some scenario:")
    print("  " + (", ".join(_label(d) for d in sorted(flips)) if flips else "(none)"))
    print("\nRate rank by scenario:")
    print(ranks.loc[on_list.index.union(flips, sort=False)].to_string())
    return ranks, membership


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sensitivity", action="store_true",
                        help="also rank under each alternative reading of the rule")
    parser.add_argument("--understatement", action="store_true",
                        help="test whether undercounted production could explain the list")
    args = parser.parse_args()

    pd.set_option("display.width", 200)

    table = ft.rank_districts()
    path = ft.save(table)

    combined = ft.rank_with_existing_stock(table=table)
    ft.save(combined, "stock_adjusted_ranking.csv")

    print_list(table)
    print_cutoff(table)
    print(f"\nWrote {path}")

    if args.sensitivity:
        run_sensitivity()

    if args.understatement:
        run_understatement()


if __name__ == "__main__":
    main()
