"""Fare evasion against revenue (additional_questions.md Q4).

Two MTA series, both quarterly and both an estimated SHARE OF RIDERS WHO ENTER
WITHOUT PAYING:

* Subway, `6kj3-ijvb`: traffic-checker surveys, stratified sample, with a margin
  of error from 2020 onward.
* Bus, `uv5h-dfhp`: Automated Passenger Counters from 2020 (surveys before),
  split Local / SBS / Express / Total.

Evaders never tap, so they are absent from the hourly ridership data. With a
paid-entry count P and an evasion rate r, total riders are P / (1 - r) and
evaded rides are P * r / (1 - r).

The dollar figure produced here is a FARE-EQUIVALENT value — evaded rides priced
at the average yield of paid rides that quarter. It is not recoverable revenue:
some evaders would not ride at all if made to pay, and some would qualify for
reduced fares. Say so wherever it is quoted.
"""

from __future__ import annotations

import pandas as pd

from . import collect, revenue, timeseries


def _quarter_start(tp: pd.Series) -> pd.Series:
    y = tp.str[:4].astype(int)
    q = tp.str[-1].astype(int)
    return pd.to_datetime(dict(year=y, month=(q - 1) * 3 + 1, day=1))


def rates(*, refresh: bool = False) -> pd.DataFrame:
    """Quarterly evasion rates: subway (+ margin of error), bus local/SBS/express/total."""
    sub = pd.DataFrame(collect.soql("subway_fare_evasion", {"$limit": "1000"},
                                    cache="evasion_subway", refresh=refresh))
    sub["quarter"] = _quarter_start(sub["time_period"])
    sub = sub.set_index("quarter")[["fare_evasion", "margin_of_error"]].astype(float)
    sub.columns = ["subway", "subway_moe"]

    bus = pd.DataFrame(collect.soql("bus_fare_evasion", {"$limit": "1000"},
                                    cache="evasion_bus", refresh=refresh))
    bus["quarter"] = _quarter_start(bus["time_period"])
    bus["fare_evasion"] = bus["fare_evasion"].astype(float)
    bus = bus.pivot_table(index="quarter", columns="trip_type", values="fare_evasion")
    bus.columns = [f"bus_{c.lower()}" for c in bus.columns]
    return sub.join(bus, how="outer").sort_index()


def official_daily(mode: str, *, refresh: bool = False) -> pd.Series:
    """MTA's published daily ridership estimate for one mode (`sayj-mze2`)."""
    rows = collect.soql("daily_ridership", {
        "$select": "date,count", "$where": f"mode='{mode}' AND date>='2023-01-01'",
        "$limit": "5000",
    }, cache=f"official_daily_{mode.lower()}", refresh=refresh)
    return pd.Series({pd.Timestamp(r["date"][:10]): float(r["count"]) for r in rows}).sort_index()


def quarterly(days: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    """Per quarter: paid entries, evasion rates, implied evaded rides and their fare value."""
    days = days or {"central": revenue.daily_revenue("central")}
    rev = days["central"]
    out = {}
    for mode, series, rate_col in (("subway", "subway", "subway"), ("bus", "bus", "bus_total")):
        df = timeseries.load(series)
        paid = (df["ridership"] - df["transfers"]).clip(lower=0).groupby(df["date"]).sum()
        entries = df.groupby("date")["ridership"].sum()
        r = rev["subway"] if mode == "subway" else rev["local_bus"] + rev["express_bus"]
        q = pd.DataFrame({"paid": paid, "entries": entries, "revenue": r}).dropna()
        g = q.resample("QS").agg(["sum", "count"])
        full = g[("paid", "count")] >= 89
        g = g.xs("sum", axis=1, level=1)[full]
        out[mode] = g.add_prefix(f"{mode}_")
    t = pd.concat(out.values(), axis=1)
    t = t.join(rates(), how="inner")
    for mode, rate_col in (("subway", "subway"), ("bus", "bus_total")):
        r = t[rate_col]
        t[f"{mode}_evaded_rides"] = t[f"{mode}_entries"] * r / (1 - r)
        # The rate is a share of riders, i.e. of entries (transfers included), so
        # value evaded rides at revenue per entry, not per paid entry.
        t[f"{mode}_yield"] = t[f"{mode}_revenue"] / t[f"{mode}_entries"]
        t[f"{mode}_evaded_value"] = t[f"{mode}_evaded_rides"] * t[f"{mode}_yield"]
    return t
