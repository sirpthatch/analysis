"""Fare-class composition: what happened to the unlimited pass.

The central claim this module supports: the MTA retired the 30-Day Unlimited
MetroCard and replaced it with a WEEKLY-only fare cap, so the riders who used
the monthly product absorbed an effective increase. The composition tables here
size that population; `cap_arithmetic` prices it.
"""

from __future__ import annotations

import pandas as pd

from . import collect, constants as C


def composition(year: int, *, refresh: bool = False) -> pd.DataFrame:
    """Fare-class table for one window, with shares and a reconciliation flag."""
    totals = collect.fare_class_totals(year, refresh=refresh)

    # The reconciliation total is a separate query and may be unavailable
    # (no network, or never recorded — 2024's was not). Degrade to
    # reconciles=None rather than crashing, so the composition still prints,
    # but never report an unreconciled split as reconciled.
    try:
        check = collect.window_total(year, refresh=refresh)
    except Exception as exc:  # noqa: BLE001 - network or missing cache
        check = None
        reconcile_error = str(exc)[:200]

    df = pd.DataFrame(
        sorted(totals.items(), key=lambda kv: -kv[1]),
        columns=["fare_class_category", "ridership"],
    )
    df["year"] = year
    grand = df["ridership"].sum()
    df["share_pct"] = (df["ridership"] / grand * 100).round(3)
    df["payment_method"] = df["fare_class_category"].str.split(" - ").str[0].str.lower()

    # Reconcile the split against the independently queried window total.
    df.attrs["fare_class_sum"] = grand
    if check is None:
        df.attrs["reconciles"] = None
        df.attrs["window_total"] = None
        df.attrs["observed_span"] = None
        df.attrs["reconcile_error"] = reconcile_error
    else:
        df.attrs["reconciles"] = abs(grand - check["ridership"]) < 1.0
        df.attrs["window_total"] = check["ridership"]
        df.attrs["observed_span"] = (check["first"], check["last"])
    return df


def comparison(*, refresh: bool = False) -> pd.DataFrame:
    """Side-by-side fare-class table for both windows.

    Categories present in one window and absent in the other are kept with NaN
    rather than filled with zero. 'Metrocard - Unlimited 7-Day' is absent from
    the 2026 schema entirely, and that distinction (absent vs zero) is real.
    """
    frames = [composition(y, refresh=refresh) for y in sorted(C.WINDOWS)]
    wide = None
    for df in frames:
        year = df["year"].iloc[0]
        part = df.set_index("fare_class_category")[["ridership", "share_pct"]]
        part.columns = [f"ridership_{year}", f"share_pct_{year}"]
        wide = part if wide is None else wide.join(part, how="outer")
    return wide.sort_values(wide.columns[0], ascending=False)


def headline_shares(*, refresh: bool = False) -> dict[str, float]:
    """The handful of numbers the argument actually rests on."""
    out: dict[str, float] = {}
    for year in sorted(C.WINDOWS):
        df = composition(year, refresh=refresh)
        total = df["ridership"].sum()
        mc = df.loc[df["payment_method"] == "metrocard", "ridership"].sum()
        unl = df.loc[
            df["fare_class_category"].isin(C.UNLIMITED_CATEGORIES), "ridership"
        ].sum()
        monthly = df.loc[
            df["fare_class_category"] == C.MONTHLY_PASS_CATEGORY, "ridership"
        ].sum()
        out[f"total_{year}"] = total
        out[f"metrocard_share_{year}"] = round(mc / total * 100, 3)
        out[f"unlimited_share_{year}"] = round(unl / total * 100, 2)
        out[f"monthly_pass_share_{year}"] = round(monthly / total * 100, 2)
        out[f"monthly_pass_rides_{year}"] = monthly

    years = sorted(C.WINDOWS)
    a, b = years[0], years[-1]
    out["ridership_growth_pct"] = round(
        (out[f"total_{b}"] - out[f"total_{a}"]) / out[f"total_{a}"] * 100, 1
    )
    return out


def cap_arithmetic() -> dict[str, float | str]:
    """Price the gap between the weekly cap and a restored monthly cap.

    A rider who hits the weekly cap every week pays 52/12 weekly caps a month.
    ETA's proposal (46 rides over 30 days at the base fare) is the comparison.
    The pre-retirement monthly pass price is deliberately NOT used here — it is
    unverified. See constants.METROCARD_30DAY_FINAL_PRICE_UNVERIFIED.
    """
    weekly = C.FARE_2026["weekly_cap"]
    monthly_equiv = weekly * 52 / 12
    eta_cap = C.ETA_PROPOSED_CAP_RIDES * C.FARE_2026["base"]
    rides_to_weekly_cap = weekly / C.FARE_2026["base"]

    return {
        "weekly_cap": weekly,
        "rides_to_hit_weekly_cap": round(rides_to_weekly_cap, 2),
        "monthly_equivalent_under_weekly_cap": round(monthly_equiv, 2),
        "eta_proposed_monthly_cap": round(eta_cap, 2),
        "gap_per_month": round(monthly_equiv - eta_cap, 2),
        "gap_per_year": round((monthly_equiv - eta_cap) * 12, 2),
        "caveat": (
            "Compares the weekly cap against ETA's proposal, not against the "
            "retired pass price, which is unverified. Do not publish a "
            "before/after dollar figure without an MTA fare schedule."
        ),
    }
