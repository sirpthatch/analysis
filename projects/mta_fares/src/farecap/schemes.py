"""Alternative fare schemes and what they do to revenue (additional_questions.md Q5).

These are SKETCHES built on aggregate data, not a ridership forecast. There is
no rider identifier anywhere in the MTA open data, so nothing here can say how
many individual riders gain or lose. Each scheme is priced two ways:

* static — today's trips, new prices;
* with a constant fare elasticity of -0.3 on the price change a trip sees. That
  value is a transit rule of thumb (TCRP Report 95, ch. 12 puts most US
  estimates at -0.2 to -0.5), NOT an MTA figure; the MTA's own attenuation
  rates are unpublished.

Schemes:

1. `monthly_cap` — ETA's 46 rides / 30 days ($138) layered on the $35 weekly
   cap. Sized from the revealed population of 2024 30-Day pass holders.
2. `distance_fare` — revenue-neutral distance bands on the 2026 O-D matrix.
3. `offpeak_fare` — revenue-neutral off-peak discount funded by a peak price,
   on the hourly profile of a sample month.
"""

from __future__ import annotations

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from . import collect, constants as C, od, timeseries

ELASTICITY = -0.3
BASE = C.FARE_2026["base"]
WEEKLY_CAP = C.FARE_2026["weekly_cap"]
WEEKS_PER_MONTH = 52 / 12


def _elastic(trips: np.ndarray, price_ratio: np.ndarray) -> np.ndarray:
    """Trips after a price change, constant-elasticity form."""
    return trips * np.power(price_ratio, ELASTICITY)


# ---------------------------------------------------------------------------
# 1. Monthly cap
# ---------------------------------------------------------------------------

def monthly_spend_weekly_cap(rides_per_month: np.ndarray) -> np.ndarray:
    """What a rider pays in a month under the $35 weekly cap, rides spread evenly."""
    per_week = np.asarray(rides_per_month, float) / WEEKS_PER_MONTH
    return np.minimum(per_week * BASE, WEEKLY_CAP) * WEEKS_PER_MONTH


def monthly_cap(cap_rides: int = C.ETA_PROPOSED_CAP_RIDES,
                rides_per_pass=(50, 60, 70)) -> pd.DataFrame:
    """Annual cost of a monthly cap, bounded from the 2024 30-Day pass population.

    Pass holders per month = 30-Day rides in a month / rides per pass. Each is
    assumed to keep riding `rides_per_pass` times a month and would pay
    min(weekly-capped spend, cap_rides * base). Riders who never bought a pass
    but ride >= cap_rides a month are NOT counted, so this is a floor on the
    eligible population; riders who have since cut back push the other way.
    """
    w = timeseries.wide()
    m30 = w["Metrocard - Unlimited 30-Day"].resample("MS").sum()
    rows = []
    for label, month in (("Jan 2023 (pre-OMNY-shift peak)", "2023-01-01"),
                         ("Sep 2024 (headline window)", "2024-09-01"),
                         ("2024 average", None)):
        rides = m30["2024"].mean() if month is None else m30[month]
        for rpp in rides_per_pass:
            holders = rides / rpp
            weekly = float(monthly_spend_weekly_cap(rpp))
            capped = min(weekly, cap_rides * BASE)
            saving = weekly - capped
            rows.append({
                "population_basis": label, "rides_per_pass": rpp,
                "pass_holders": round(holders),
                "monthly_spend_weekly_cap": round(weekly, 2),
                "monthly_spend_with_cap": round(capped, 2),
                "saving_per_rider_month": round(saving, 2),
                "annual_cost_static_$M": round(holders * saving * 12 / 1e6, 1),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2. Distance-based fare
# ---------------------------------------------------------------------------

DISTANCE_BANDS_MI = [0, 2, 5, 9, np.inf]      # band edges, straight-line miles
DISTANCE_RELATIVE = [0.75, 1.0, 1.25, 1.5]    # price relative to lowest-band x
# spec_fare_proposals.md #1: shorter trips pay more, longer trips are subsidised.
INVERSE_DISTANCE_RELATIVE = DISTANCE_RELATIVE[::-1]


def _band_prices(miles: np.ndarray, trips: np.ndarray, target: float,
                 relative=DISTANCE_RELATIVE) -> tuple[np.ndarray, list]:
    """Scale the relative band prices so trip-weighted mean price == target."""
    band = np.digitize(miles, DISTANCE_BANDS_MI[1:-1])
    rel = np.asarray(relative)[band]
    scale = target * trips.sum() / (rel * trips).sum()
    # Round band prices to the nickel, as fares are.
    prices = [round(r * scale * 20) / 20 for r in relative]
    return np.asarray(prices)[band], prices


def distance_fare(year: int = 2026, month: int = 4, relative=DISTANCE_RELATIVE) -> dict:
    """Revenue-neutral (static) distance bands on a typical week of O-D trips.

    Prices are expressed relative to today's flat fare, so the result applies to
    every fare class proportionally (a reduced fare is half the band price).
    Pass `relative=INVERSE_DISTANCE_RELATIVE` for the inverse-distance scheme.
    """
    pairs = od.with_distance(od.pair_week(year, month), od.stations(year)).dropna()
    st = od.stations(year).set_index("id")
    trips = pairs["weekly_trips"].to_numpy()
    price, band_prices = _band_prices(pairs["miles"].to_numpy(), trips, BASE, relative)
    ratio = price / BASE
    after = _elastic(trips, ratio)
    pairs = pairs.assign(price=price, ratio=ratio, trips_after=after)

    by_origin = pairs.groupby("origin_id").apply(
        lambda g: pd.Series({
            "weekly_trips": g["weekly_trips"].sum(),
            "mean_miles": np.average(g["miles"], weights=g["weekly_trips"]),
            "mean_price": np.average(g["price"], weights=g["weekly_trips"]),
        }), include_groups=False)
    by_origin = by_origin.join(st[["name", "borough", "lat", "lon"]])
    by_origin["change_pct"] = (by_origin["mean_price"] / BASE - 1) * 100

    band = np.digitize(pairs["miles"], DISTANCE_BANDS_MI[1:-1])
    band_tbl = pairs.groupby(band).agg(trips=("weekly_trips", "sum"),
                                       price=("price", "first"))
    band_tbl["share_pct"] = band_tbl["trips"] / band_tbl["trips"].sum() * 100
    band_tbl.index = [f"{lo:g}-{hi:g} mi" for lo, hi in
                      zip(DISTANCE_BANDS_MI[:-1], DISTANCE_BANDS_MI[1:])]
    return {
        "band_prices": band_prices,
        "bands": band_tbl,
        "revenue_ratio_static": float((price * trips).sum() / (BASE * trips.sum())),
        "revenue_ratio_elastic": float((price * after).sum() / (BASE * trips.sum())),
        "ridership_ratio_elastic": float(after.sum() / trips.sum()),
        "share_trips_cheaper": float(trips[ratio < 1].sum() / trips.sum()),
        "share_trips_dearer": float(trips[ratio > 1].sum() / trips.sum()),
        "by_origin": by_origin,
        "pairs": pairs,
    }


# ---------------------------------------------------------------------------
# 3. Off-peak discount
# ---------------------------------------------------------------------------

PEAK_HOURS = set(range(6, 10)) | set(range(16, 20))   # weekday 6-10am, 4-8pm


def hourly_profile(start: str = "2026-04-01", end: str = "2026-05-01", *,
                   refresh: bool = False) -> pd.DataFrame:
    """Subway entries by date x hour for a sample month, one query per day."""
    path = collect.DATA_RAW / f"hourly_profile_{start}_{end}.json"
    if path.exists() and not refresh:
        return pd.read_json(path)
    d0, d1 = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    days = [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days)]

    def one(day):
        nxt = day + dt.timedelta(days=1)
        rows = collect.soql(timeseries.SERIES["subway"][1 if day >= timeseries.SEAM else 0], {
            "$select": "date_extract_hh(transit_timestamp) AS hh,sum(ridership),sum(transfers)",
            "$where": f"transit_timestamp >= '{day}' AND transit_timestamp < '{nxt}'",
            "$group": "hh",
        })
        return [dict(date=str(day), hour=int(r["hh"]),
                     ridership=float(r["sum_ridership"]),
                     transfers=float(r["sum_transfers"])) for r in rows]

    with ThreadPoolExecutor(max_workers=6) as ex:
        rows = [r for part in ex.map(one, days) for r in part]
    df = pd.DataFrame(rows)
    path.write_text(json.dumps(rows))
    return df


def offpeak_fare(discount: float = 0.20, **kw) -> dict:
    """Off-peak discount with the peak price set so static revenue is unchanged."""
    h = hourly_profile(**kw)
    h["date"] = pd.to_datetime(h["date"])
    weekday = h["date"].dt.dayofweek < 5
    h["peak"] = weekday & h["hour"].isin(PEAK_HOURS)
    paid = (h["ridership"] - h["transfers"]).clip(lower=0)
    peak_share = paid[h["peak"]].sum() / paid.sum()
    off = 1 - discount
    peak = (1 - off * (1 - peak_share)) / peak_share      # static revenue neutral
    ratio = np.where(h["peak"], peak, off)
    after = _elastic(paid.to_numpy(), ratio)
    return {
        "peak_share_of_paid_entries": float(peak_share),
        "offpeak_price": round(BASE * off, 2),
        "peak_price": round(BASE * peak, 2),
        "revenue_ratio_elastic": float((ratio * after).sum() / paid.sum()),
        "ridership_ratio_elastic": float(after.sum() / paid.sum()),
        "offpeak_ridership_change_pct": float(
            (after[~h["peak"].to_numpy()].sum() / paid[~h["peak"]].sum() - 1) * 100),
        "peak_ridership_change_pct": float(
            (after[h["peak"].to_numpy()].sum() / paid[h["peak"]].sum() - 1) * 100),
    }
