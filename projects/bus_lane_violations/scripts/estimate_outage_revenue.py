#!/usr/bin/env python3
"""
Estimates the ticket/revenue cost of the technical-rejection outage found in
notebooks/ace_violations.ipynb Sec 1c (23 Mar - 26 Apr 2026, TECHNICAL
ISSUE/OTHER rejections spike from ~5k/wk to ~50k/wk).

Method:
  1. Shortfall = (baseline daily ticket rate) x (outage days) - (actual issued)
     Baseline = the 5 weeks immediately before and the 6 weeks immediately
     after the outage (chosen to dodge the separate Jan-Feb 2026 surge and
     the pending-data tail after 2026-06-07). Pre and post rates agree within
     2%, so a flat baseline is a reasonable stand-in for "no outage".
  2. Revenue per lost ticket = the average ACE fine (see MTA fine schedule:
     $50/$100/$150/$200/$250 for the 1st/2nd/3rd/4th/5th+ violation by the
     SAME plate in a trailing 12 months), applied at the same offense-number
     mix seen in the baseline weeks.

    python3 scripts/estimate_outage_revenue.py

Needs pandas, numpy.
"""
import os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(HERE, os.pardir, "data", "raw")

OUT_START, OUT_END = pd.Timestamp("2026-03-23"), pd.Timestamp("2026-04-27")
PRE = ("2026-02-16", "2026-03-22")
POST = ("2026-04-27", "2026-06-07")
FEE = {1: 50, 2: 100, 3: 150, 4: 200}  # 5th+ = 250, handled by .fillna below


def offense_number(times):
    """1-indexed count of this ticket within a trailing 365-day window,
    for one plate's tickets sorted by time (matches the MTA fine schedule's
    own 12-month basis)."""
    times = times.values.astype("datetime64[ns]")
    n = len(times)
    out = np.empty(n, dtype=int)
    j = 0
    for i in range(n):
        while times[i] - times[j] > np.timedelta64(365, "D"):
            j += 1
        out[i] = i - j + 1
    return out


def main():
    df = pd.read_parquet(os.path.join(RAW, "ace_violations.parquet"),
                          columns=["vehicle_id", "first_occurrence", "violation_status", "violation_type"])
    iss = df[df.violation_status == "VIOLATION ISSUED"].dropna(subset=["vehicle_id"])

    daily = iss.set_index("first_occurrence").resample("D").size()
    outage = daily.loc[OUT_START:OUT_END - pd.Timedelta(days=1)]
    pre, post = daily.loc[PRE[0]:PRE[1]], daily.loc[POST[0]:POST[1]]
    baseline_rate = pd.concat([pre, post]).mean()
    expected = baseline_rate * len(outage)
    shortfall = expected - outage.sum()

    print(f"pre-outage rate:  {pre.mean():,.0f} tickets/day (n={len(pre)} days)")
    print(f"post-outage rate: {post.mean():,.0f} tickets/day (n={len(post)} days)")
    print(f"outage window: {len(outage)} days, {outage.sum():,.0f} tickets actually issued")
    print(f"expected at baseline rate: {expected:,.0f}")
    print(f"shortfall (missing tickets): {shortfall:,.0f}\n")

    typ_mix = iss[(iss.first_occurrence.between(*PRE)) | (iss.first_occurrence.between(*POST))] \
        .violation_type.value_counts(normalize=True)
    print("shortfall by type (baseline mix applied):")
    for t, share in typ_mix.items():
        print(f"  {t}: {shortfall * share:,.0f}")

    iss = iss.sort_values(["vehicle_id", "first_occurrence"])
    iss["offense_n"] = iss.groupby("vehicle_id", sort=False)["first_occurrence"] \
        .transform(lambda s: offense_number(s))
    iss["fee"] = iss.offense_n.map(FEE).fillna(250)

    base = iss[(iss.first_occurrence.between(*PRE)) | (iss.first_occurrence.between(*POST))]
    avg_fee = base.fee.mean()
    print(f"\naverage fee/ticket in baseline weeks (trailing-12mo offense mix): ${avg_fee:.2f}")
    print(f"estimated face-value revenue lost to the outage: ${shortfall * avg_fee:,.0f}")
    print(f"  lower bound, all lost tickets @ $50 first-offense rate: ${shortfall * 50:,.0f}")
    print("\nThis is FACE VALUE of tickets never issued, not collected revenue -- "
          "no adjustment for dismissal/non-payment. See research-log.md.")


if __name__ == "__main__":
    main()
