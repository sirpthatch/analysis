#!/usr/bin/env python3
"""Produce the analysis tables.

    python src/analyze.py --all
    python src/analyze.py --composition   # fare-class split, both windows
    python src/analyze.py --arithmetic    # weekly cap vs a restored monthly cap
    python src/analyze.py --stations      # per-station monthly-pass share + verdict
    python src/analyze.py --questions     # additional_questions.md Q1-Q5 (needs daily/OD pulls)
    python src/analyze.py --proposals     # spec_fare_proposals.md 1-4 (~2 min; needs data/external)

Writes CSVs to output/ and prints the figures worth reading aloud.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from farecap import (  # noqa: E402
    constants as C, evasion, fareclass, proposals, revenue, schemes, stations, timeseries,
)

OUT = Path(__file__).resolve().parents[1] / "output"


def _write(df: pd.DataFrame, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    df.to_csv(path, index=True)
    print(f"  -> {path.relative_to(OUT.parent)}")


def do_composition(refresh: bool) -> None:
    print("\n=== Fare-class composition, 16-day windows two years apart ===")
    wide = fareclass.comparison(refresh=refresh)
    with pd.option_context("display.width", 140, "display.max_columns", 20):
        print(wide.to_string())
    _write(wide, "fare_class_comparison.csv")

    print("\n--- reconciliation ---")
    for year in sorted(C.WINDOWS):
        df = fareclass.composition(year, refresh=refresh)
        rec = df.attrs["reconciles"]
        if rec is None:
            print(f"  {year}: fare-class sum {df.attrs['fare_class_sum']:,.0f} vs "
                  f"window total UNAVAILABLE [NOT RECONCILED]")
            print(f"        reason: {df.attrs['reconcile_error']}")
        else:
            span = df.attrs["observed_span"]
            print(f"  {year}: fare-class sum {df.attrs['fare_class_sum']:,.0f} vs "
                  f"window total {df.attrs['window_total']:,.0f} "
                  f"[{'OK' if rec else 'MISMATCH'}]")
            print(f"        observed span {span[0]} .. {span[1]}")

    print("\n--- headline shares ---")
    for k, v in fareclass.headline_shares(refresh=refresh).items():
        print(f"  {k:34s} {v:,}")


def do_arithmetic() -> None:
    print("\n=== Cap arithmetic ===")
    for k, v in fareclass.cap_arithmetic().items():
        if k == "caveat":
            print(f"\n  CAVEAT: {v}")
        else:
            print(f"  {k:36s} {v}")


def do_stations(refresh: bool) -> None:
    print("\n=== Station-level monthly-pass concentration (2024 window) ===")
    verdict = stations.dispersion_verdict(2024, refresh=refresh)
    for k, v in verdict.items():
        print(f"  {k:30s} {v}")

    tbl = stations.station_table(2024, refresh=refresh)
    ranked = tbl[~tbl["below_volume_floor"]]
    print("\n--- top 15 by monthly-pass share ---")
    print(ranked.head(15)[["station_complex", "borough", "total_ridership",
                           "monthly_pass_share_pct"]].to_string(index=False))
    print("\n--- bottom 10 by monthly-pass share ---")
    print(ranked.tail(10)[["station_complex", "borough", "total_ridership",
                           "monthly_pass_share_pct"]].to_string(index=False))
    _write(tbl, "station_monthly_pass_share_2024.csv")

    print("\n--- borough rollup ---")
    print(stations.borough_rollup(2024, refresh=refresh).to_string(index=False))


def do_questions() -> None:
    print("\n=== Q1: monthly fare-class share of subway entries, % ===")
    w = timeseries.wide()
    m = w.resample("MS").sum()
    share = m.div(m.sum(axis=1), axis=0) * 100
    cols = ["Metrocard - Unlimited 30-Day", "Metrocard - Unlimited 7-Day"]
    print(share[cols].round(2).iloc[::3].to_string())
    _write(share.round(3), "q1_monthly_share.csv")

    print("\n=== Q3: modelled vs actual farebox (NYCT + MTA Bus + SIR) ===")
    days = {b: revenue.daily_revenue(b) for b in revenue.BAND}
    chk = revenue.monthly_check(days)
    print(chk["ratio_central"].groupby(chk.index.year).mean().round(3).to_string())
    _write(chk, "q3_monthly_check.csv")
    wk = revenue.weekly_revenue(days)
    _write(wk, "q3_weekly_revenue.csv")
    print("mean weekly central $M by year:",
          (wk["total_central"] / 1e6).groupby(wk.index.year).mean().round(1).to_dict())

    print("\n=== Q4: fare evasion, fare-equivalent value of evaded rides ($M) ===")
    q = evasion.quarterly(days)
    ann = q[["subway_evaded_value", "bus_evaded_value"]].groupby(q.index.year).sum() / 1e6
    print(ann.round(0).to_string())
    _write(q, "q4_evasion_quarterly.csv")

    print("\n=== Q5: schemes ===")
    mc = schemes.monthly_cap()
    print(mc[["population_basis", "rides_per_pass", "pass_holders",
              "annual_cost_static_$M"]].to_string(index=False))
    _write(mc, "q5_monthly_cap.csv")
    d = schemes.distance_fare()
    print("distance bands:", d["band_prices"], "revenue ratio static/elastic:",
          round(d["revenue_ratio_static"], 4), round(d["revenue_ratio_elastic"], 4))
    _write(d["by_origin"], "q5_distance_fare_by_origin.csv")
    for disc in (0.1, 0.2, 0.3):
        r = schemes.offpeak_fare(disc)
        print(f"off-peak -{disc:.0%}: off ${r['offpeak_price']:.2f} peak ${r['peak_price']:.2f} "
              f"revenue x{r['revenue_ratio_elastic']:.3f}")


def do_proposals() -> None:
    print("\n=== Proposal 1: inverse distance, by origin neighbourhood income ===")
    si = proposals.station_income()
    r = schemes.distance_fare(relative=schemes.INVERSE_DISTANCE_RELATIVE)
    bo = r["by_origin"].join(si[["lowmod_share"]])
    print("bands:", r["band_prices"], "revenue static/elastic:",
          round(r["revenue_ratio_static"], 4), round(r["revenue_ratio_elastic"], 4))
    q = pd.qcut(bo["lowmod_share"], 5, labels=False)
    print(bo.groupby(q).apply(lambda d: (np.average(d["mean_price"], weights=d["weekly_trips"])
                                         / proposals.BASE - 1) * 100,
                              include_groups=False).round(1).to_string())
    _write(bo, "p1_inverse_distance_by_origin.csv")

    print("\n=== Proposal 2: reserve account float ===")
    f = proposals.float_grid()
    central = f[(f["replenish_point"] == 0.25) & (f["rate"] == 0.0424)]
    print(central.drop(columns=["replenish_point", "rate"]).round(1).to_string(index=False))
    print("(replenishment point 25%, interest 4.24%)")
    _write(f, "p2_float_grid.csv")

    print("\n=== Proposals 3-4: caps (synthetic riders) ===")
    g = proposals.cap_grid()
    print(g.groupby(["scenario", "chain"], sort=False)["revenue_change_$M"]
          .agg(["min", "max"]).round(0).to_string())
    _write(g, "p34_cap_grid.csv")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--all", action="store_true")
    p.add_argument("--composition", action="store_true")
    p.add_argument("--arithmetic", action="store_true")
    p.add_argument("--stations", action="store_true")
    p.add_argument("--questions", action="store_true")
    p.add_argument("--proposals", action="store_true")
    p.add_argument("--refresh", action="store_true", help="ignore cache, re-query")
    args = p.parse_args()

    if not any([args.all, args.composition, args.arithmetic, args.stations, args.questions,
                args.proposals]):
        p.print_help()
        return 2

    if args.all or args.composition:
        do_composition(args.refresh)
    if args.all or args.arithmetic:
        do_arithmetic()
    if args.all or args.stations:
        do_stations(args.refresh)
    if args.questions:
        do_questions()
    if args.proposals:
        do_proposals()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
