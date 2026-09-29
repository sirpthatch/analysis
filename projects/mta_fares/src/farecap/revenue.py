"""Weekly fare revenue, modelled from ridership and checked against MTA actuals.

additional_questions.md Q3. The model prices every PAID entry — ridership minus
free transfers (the `transfers` column is a subset of `ridership`, per the MTA
data dictionary) — at a per-class yield from the fare schedule in force that
day. It is then summed to calendar months and compared with the "Farebox
Revenue" line of the MTA Statement of Operations (`yg77-3tkj`, Actual scenario)
for NYCT + MTA Bus + SIR, which is the audited figure.

What the ridership data cannot tell us, and is therefore an explicit assumption
here with a low/high band rather than a hidden constant:

* Rides per unlimited pass. Pass revenue is price / rides taken on it. The MTA
  does not publish rides per pass in these datasets.
* The share of OMNY full-fare taps that were FREE under the weekly cap. Capped
  taps are still counted as ridership but earn nothing.
* What the "Other" buckets earn (employee passes are free; TransitCheck and
  EasyPay pay full fare; CUNY cards are funded separately).
* Student rides are priced at $0. DOE student fares are reimbursed by the city
  and state; whether that reimbursement sits inside the farebox line is not
  established here.

Fare schedules: MTA "New Fare Information Effective August 20, 2023"
(mta.info/document/118601) and the 2026 fare change board materials
(mta.info/document/186881), both saved in research/docs/.
"""

from __future__ import annotations

import pandas as pd

from . import collect, timeseries

# (effective date, base, 30-day, 7-day, express). Reduced fares are half base.
FARE_SCHEDULES = [
    ("2019-04-21", 2.75, 127.00, 33.00, 6.75),
    ("2023-08-20", 2.90, 132.00, 34.00, 7.00),
    ("2026-01-04", 3.00, None, None, 7.25),   # passes retired; weekly cap $35
]

# Assumption bands: (low, central, high).
RIDES_PER_30DAY = (70, 60, 50)          # more rides per pass -> lower yield
RIDES_PER_7DAY = (18, 15, 13)
OMNY_CAPPED_FREE_SHARE = (0.08, 0.04, 0.0)  # share of OMNY full-fare taps earning $0
OTHER_YIELD_SHARE_OF_BASE = (0.0, 0.5, 1.0)
BAND = {"low": 0, "central": 1, "high": 2}

FAREBOX_AGENCIES = ["NYCT", "MTABC", "SIR"]


def schedule_on(dates) -> pd.DataFrame:
    """Fare schedule in force on each date."""
    sched = pd.DataFrame(FARE_SCHEDULES, columns=["from", "base", "p30", "p7", "express"])
    sched["from"] = pd.to_datetime(sched["from"])
    idx = sched["from"].searchsorted(pd.to_datetime(pd.Series(dates)), side="right") - 1
    return sched.iloc[idx].reset_index(drop=True)


def _yield(df: pd.DataFrame, band: str, express: bool) -> pd.Series:
    """Revenue per paid entry for each row of a long daily fare-class table."""
    b = BAND[band]
    s = schedule_on(df["date"])
    base = (s["express"] if express else s["base"]).to_numpy()
    cat = df["fare_class_category"].reset_index(drop=True)
    out = pd.Series(0.0, index=cat.index)
    full = cat.str.endswith("Full Fare").to_numpy()
    out[full] = base[full]
    omny_full = (cat == "OMNY - Full Fare").to_numpy()
    out[omny_full] = base[omny_full] * (1 - OMNY_CAPPED_FREE_SHARE[b])
    reduced = cat.str.contains("Seniors|Fair Fare").to_numpy()
    out[reduced] = base[reduced] / 2
    other = cat.str.endswith("Other").to_numpy()
    out[other] = base[other] * OTHER_YIELD_SHARE_OF_BASE[b]
    if not express:
        # After retirement the pass price is None; residual pass rides priced at base.
        p30 = s["p30"].fillna(s["base"] * RIDES_PER_30DAY[b]).to_numpy()
        p7 = s["p7"].fillna(s["base"] * RIDES_PER_7DAY[b]).to_numpy()
        m30 = (cat == "Metrocard - Unlimited 30-Day").to_numpy()
        m7 = (cat == "Metrocard - Unlimited 7-Day").to_numpy()
        out[m30] = p30[m30] / RIDES_PER_30DAY[b]
        out[m7] = p7[m7] / RIDES_PER_7DAY[b]
    # Students ($0) fall through.
    out.index = df.index
    return out


def _mode_revenue(series: str, band: str, express: bool) -> pd.Series:
    df = timeseries.load(series)
    paid = (df["ridership"] - df["transfers"]).clip(lower=0)
    return (paid * _yield(df, band, express)).groupby(df["date"]).sum()


def daily_revenue(band: str = "central") -> pd.DataFrame:
    """Modelled revenue per day by mode: subway (incl. SIR), local bus, express bus."""
    bus_all_local = _mode_revenue("bus", band, express=False)
    exp_local = _mode_revenue("bus_express", band, express=False)
    exp = _mode_revenue("bus_express", band, express=True)
    out = pd.DataFrame({
        "subway": _mode_revenue("subway", band, express=False),
        # 'bus' includes the express routes; swap their local pricing for express.
        "local_bus": bus_all_local - exp_local,
        "express_bus": exp,
    })
    return out.dropna()


def weekly_revenue(days: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    """Monday-start weekly totals: central by mode, plus low/high totals. Full weeks only."""
    days = days or {b: daily_revenue(b) for b in BAND}
    d = days["central"].copy()
    d["total_central"] = d.sum(axis=1)
    d["total_low"] = days["low"].sum(axis=1)
    d["total_high"] = days["high"].sum(axis=1)
    w = d.resample("W-MON", label="left", closed="left")
    out = w.sum()
    return out[w.count()["total_central"] == 7]


def farebox_actuals(*, refresh: bool = False) -> pd.DataFrame:
    """Monthly farebox revenue, Actual scenario, by agency (dollars)."""
    rows = collect.soql("statement_of_operations", {
        "$select": "month,agency,sum(amount)",
        "$where": "scenario='Actual' AND upper(general_ledger)='FAREBOX REVENUE' "
                  "AND month>='2023-01-01'",
        "$group": "month,agency",
        "$limit": "5000",
    }, cache="farebox_actuals", refresh=refresh)
    df = pd.DataFrame(rows)
    df["month"] = pd.to_datetime(df["month"])
    df["amount"] = df["sum_amount"].astype(float)
    return df.pivot_table(index="month", columns="agency", values="amount", aggfunc="sum")


def paratransit_daily(*, refresh: bool = False) -> pd.Series:
    """Access-A-Ride daily trips (`sayj-mze2`), for the paratransit share of NYCT farebox."""
    rows = collect.soql("daily_ridership", {
        "$select": "date,count", "$where": "mode='AAR' AND date>='2023-01-01'",
        "$limit": "5000",
    }, cache="aar_daily", refresh=refresh)
    s = pd.Series({pd.Timestamp(r["date"][:10]): float(r["count"]) for r in rows})
    return s.sort_index()


def monthly_check(days: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    """Modelled vs actual farebox by month, NYCT + MTA Bus + SIR. Full months only."""
    days = days or {b: daily_revenue(b) for b in BAND}
    act = farebox_actuals()
    out = pd.DataFrame({"actual": act.reindex(columns=FAREBOX_AGENCIES).sum(axis=1, min_count=1)})
    aar = paratransit_daily()
    aar_rev = pd.Series(aar.to_numpy() * schedule_on(aar.index)["base"].to_numpy(), index=aar.index)
    for b, d in days.items():
        tot = d.sum(axis=1).add(aar_rev.reindex(d.index), fill_value=0)
        m = tot.resample("MS").agg(["sum", "count"])
        out[f"model_{b}"] = m["sum"].where(m["count"] == m.index.days_in_month)
    out = out.dropna()
    for b in BAND:
        out[f"ratio_{b}"] = out[f"model_{b}"] / out["actual"]
    return out
