"""Models for spec_fare_proposals.md.

1. Inverse distance        -> schemes.distance_fare(relative=INVERSE_DISTANCE_RELATIVE)
                              plus `station_income` here, to test its equity premise.
2. E-ZPass-style reserve   -> `reserve_float`
3. Monthly / annual cap    -> `simulate` with `monthly_cap` / `annual_cap`
4. Daily pass (cap)        -> `simulate` with `daily_cap`

THE CENTRAL CAVEAT. Proposals 2-4 depend on how often individual riders ride.
No MTA open dataset has a rider identifier, so that distribution is not
observed. It is SYNTHESISED here and calibrated to two things that are observed:

* total weekly full-fare paid entries (subway + local bus), from the hourly data;
* the share of full-fare-plus-pass trips taken on 7-/30-Day Unlimited passes in
  Sept 2024 (~18%), used as the share of trips by riders making >= 12 trips a
  week — the people for whom an unlimited product paid.

One further parameter, mean weekly trips per rider (which fixes how many riders
there are), cannot be calibrated from open data and is swept. Every output from
2-4 is therefore a RANGE across assumptions, and the write-up must say so.

Simplifications, each stated where it bites:
* Full-fare adult riders only. Reduced-fare, Fair Fares and students are left out
  (their caps are half-price mirrors; including them would scale, not change,
  the conclusions).
* Calendar weeks and 28-day "months", not the MTA's rolling 7-day window.
* A rider's underlying rate is stable across the simulated year (no churn, no
  seasonality). That overstates how many riders would reach an ANNUAL cap.
* Static: nobody rides more because a cap makes extra trips free.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from . import collect, constants as C, od, timeseries

BASE = C.FARE_2026["base"]
WEEKLY_CAP = C.FARE_2026["weekly_cap"]            # $35 = 11.67 rides -> 12th ride caps
HIGH_FREQ_TRIPS_PER_WEEK = 12
HIGH_FREQ_SHARE_TARGET = 0.18                     # see module docstring
# Mon..Sun entries relative to the weekday mean, subway Mar-Jun 2026 (timeseries).
DAY_WEIGHTS = np.array([0.92, 1.04, 1.05, 1.04, 0.96, 0.68, 0.54])

# ---------------------------------------------------------------------------
# Neighbourhood income around each station (tests proposal 1's premise)
# ---------------------------------------------------------------------------

EXTERNAL = collect.DATA_RAW.parent / "external"
BORO_TO_COUNTY = {"1": "061", "2": "005", "3": "047", "4": "081", "5": "085"}


def tracts() -> pd.DataFrame:
    """2020 tracts: HUD low/mod-income share (NYC CDBG file) + Census centroids.

    Source: NYC Open Data `qmcw-ur37` (HUD LMISD, ACS 2016-2020) and the Census
    2023 tract Gazetteer. Low/moderate income = household income below 80% of
    area median income.
    """
    cd = pd.DataFrame(json.loads((EXTERNAL / "cdbg_tracts_qmcw-ur37.json").read_text()))
    cd["geoid"] = "36" + cd["borocode"].map(BORO_TO_COUNTY) + cd["boroct"].str[1:]
    for c in ("totalpop", "lowmod_population"):
        cd[c] = cd[c].astype(float)
    gz = pd.read_csv(EXTERNAL / "2023_gaz_tracts_36.txt", sep="\t", dtype={"GEOID": str})
    gz.columns = gz.columns.str.strip()
    gz = gz.rename(columns={"GEOID": "geoid", "INTPTLAT": "lat", "INTPTLONG": "lon"})
    out = cd.merge(gz[["geoid", "lat", "lon"]], on="geoid", how="left")
    out.attrs["unmatched"] = int(out["lat"].isna().sum())
    return out


def station_income(radius_mi: float = 0.5, year: int = 2026) -> pd.DataFrame:
    """Population-weighted low/mod-income share of tracts within `radius_mi` of each station.

    Uses tract centroids, so it is a neighbourhood measure, not a rider measure:
    it describes who LIVES near a station, which is closest to riders who START
    trips there (the O-D matrix is by origin).
    """
    t = tracts().dropna(subset=["lat"])
    t = t[t["totalpop"] > 0]
    st = od.stations(year)
    lat1 = np.radians(st["lat"].to_numpy())[:, None]
    lon1 = np.radians(st["lon"].to_numpy())[:, None]
    lat2 = np.radians(t["lat"].to_numpy())[None, :]
    lon2 = np.radians(t["lon"].to_numpy())[None, :]
    a = (np.sin((lat2 - lat1) / 2) ** 2
         + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    miles = 3958.8 * 2 * np.arcsin(np.sqrt(a))
    near = miles <= radius_mi
    pop = t["totalpop"].to_numpy()[None, :] * near
    low = t["lowmod_population"].to_numpy()[None, :] * near
    out = st[["id", "name", "borough"]].copy()
    out["tract_pop"] = pop.sum(axis=1)
    out["lowmod_share"] = np.where(out["tract_pop"] > 0, low.sum(axis=1) / out["tract_pop"], np.nan)
    out["n_tracts"] = near.sum(axis=1)
    return out.set_index("id")


# ---------------------------------------------------------------------------
# Synthetic rider population (proposals 2-4)
# ---------------------------------------------------------------------------

def observed_weekly_full_fare_trips(start="2026-03-01", end="2026-06-30") -> float:
    """Mean weekly full-fare PAID entries, subway + local/express bus, over [start, end]."""
    tot = 0.0
    for series in ("subway", "bus"):
        df = timeseries.load(series)
        df = df[df["fare_class_category"].str.endswith("Full Fare")]
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        paid = (df["ridership"] - df["transfers"]).clip(lower=0)
        tot += paid.groupby(df["date"]).sum().mean() * 7
    return tot


def _gamma_rates(n: int, mean: float, shape: float, rng) -> np.ndarray:
    return rng.gamma(shape, mean / shape, size=n)


def high_freq_share(mean: float, shape: float, n: int = 200_000, seed: int = 0) -> float:
    """Share of trips taken in rider-weeks with >= 12 trips."""
    rng = np.random.default_rng(seed)
    lam = _gamma_rates(n, mean, shape, rng)
    wk = rng.poisson(lam[:, None], size=(n, 8))
    return float(wk[wk >= HIGH_FREQ_TRIPS_PER_WEEK].sum() / wk.sum())


def calibrate_shape(mean: float, target: float = HIGH_FREQ_SHARE_TARGET) -> float:
    """Gamma shape giving `target` share of trips in >=12-trip weeks (bisection on log-shape)."""
    lo, hi = np.log(0.05), np.log(20.0)
    for _ in range(30):
        mid = (lo + hi) / 2
        # Higher shape -> less dispersion -> fewer heavy riders (for mean < 12).
        if high_freq_share(mean, np.exp(mid)) > target:
            lo = mid
        else:
            hi = mid
    return float(np.exp((lo + hi) / 2))


def _daily_trips(weekly: np.ndarray, rng, chain: float = 0.0) -> np.ndarray:
    """Spread each rider-week's trips over 7 days as round-trip 'tours'.

    weekly: (riders, weeks) -> (riders, weeks, 7). Trips form ceil(T/2) tours of
    two (the last is one trip when T is odd). Tours go to DISTINCT days first —
    a 10-trip commuter rides five days, one round trip each — with days ordered
    by a weighted draw (DAY_WEIGHTS) without replacement. Only an 8th+ tour
    doubles up a day. `chain` is the probability that a tour after the first is
    instead chained onto the rider's first day of the week (an errand after
    work), making a 3-4 trip day. It is NOT observed in any open dataset; it is
    swept, and the daily-cap result is reported against the share of rider-days
    with 3+ trips it produces.
    """
    r, w = weekly.shape
    tours = (weekly + 1) // 2
    max_t = max(int(tours.max()) if tours.size else 0, 1)
    # Weighted order of days without replacement (Gumbel top-k).
    keys = np.log(DAY_WEIGHTS)[None, None, :] + rng.gumbel(size=(r, w, 7))
    order = np.argsort(-keys, axis=2)
    k = np.arange(max_t)
    day = order[..., k % 7]                                   # (r, w, max_t)
    if chain > 0:
        chained = (rng.random((r, w, max_t)) < chain) & (k[None, None, :] >= 1)
        day = np.where(chained, order[..., :1], day)
    valid = k[None, None, :] < tours[..., None]
    size = np.where((k[None, None, :] == tours[..., None] - 1) & (weekly[..., None] % 2 == 1), 1, 2) * valid
    out = np.zeros((r, w, 7), dtype=np.int32)
    for d in range(7):
        out[..., d] = (size * (day == d)).sum(axis=2)
    return out


def _charges(daily: np.ndarray, dcap: float | None) -> np.ndarray:
    """Per-day amount charged under a daily cap (optional) and the $35 calendar-week cap.

    The weekly cap is applied cumulatively within each Mon-Sun week, so a day's
    charge is what remains under the cap after the earlier days of that week.
    Returns (riders, weeks*7) dollars.
    """
    d = daily * BASE
    if dcap is not None:
        d = np.minimum(d, dcap)
    cum = np.minimum(np.cumsum(d, axis=2), WEEKLY_CAP)
    charged = np.diff(cum, axis=2, prepend=0.0)
    return charged.reshape(charged.shape[0], -1)


def simulate(mean: float = 4.5, shape: float | None = None, *, n: int = 60_000, weeks: int = 52,
             daily_cap: float | None = None, monthly_cap: float | None = None,
             annual_cap: float | None = None, chain: float = 0.0, seed: int = 1,
             chunk: int = 4_000) -> dict:
    """Revenue per rider-year under a set of caps, relative to the $35 weekly cap alone.

    Cap order: daily -> weekly (calendar, cumulative) -> 30-day window -> year.
    The year is 364 days: twelve 30-day windows plus four trailing days that
    only the weekly and annual caps see. The $35 weekly cap is always on (it is
    the status quo). The same seed gives the same riders and trips for every
    cap setting, so scenarios are compared like for like.
    """
    rng = np.random.default_rng(seed)
    shape = shape or calibrate_shape(mean)
    lam = _gamma_rates(n, mean, shape, rng)

    def year_spend(charged, mcap, acap):
        months = charged[:, :360].reshape(charged.shape[0], 12, 30).sum(axis=2)
        if mcap is not None:
            months = np.minimum(months, mcap)
        y = months.sum(axis=1) + charged[:, 360:].sum(axis=1)
        if acap is not None:
            y = np.minimum(y, acap)
        return y

    base, alt, trips = [], [], []
    days3 = days_any = 0
    for i in range(0, n, chunk):
        lam_c = lam[i:i + chunk]
        weekly = rng.poisson(lam_c[:, None], size=(len(lam_c), weeks)).astype(np.int32)
        daily = _daily_trips(weekly, rng, chain)
        base.append(year_spend(_charges(daily, None), None, None))
        alt.append(year_spend(_charges(daily, daily_cap), monthly_cap, annual_cap))
        trips.append(weekly.sum(axis=1))
        days3 += int((daily >= 3).sum())
        days_any += int((daily > 0).sum())
    base, alt, trips = map(np.concatenate, (base, alt, trips))
    gross = trips * BASE
    ben = alt < base - 1e-9
    return {
        "mean_weekly_trips": mean, "gamma_shape": round(shape, 3), "chain": chain,
        "share_taps_free_under_weekly_cap": float(1 - base.sum() / gross.sum()),
        "share_rider_days_3plus_trips": float(days3 / max(days_any, 1)),
        "revenue_ratio": float(alt.sum() / base.sum()),
        "share_riders_benefiting": float(ben.mean()),
        "share_trips_by_beneficiaries": float(trips[ben].sum() / trips.sum()),
        "mean_annual_saving_beneficiary": float((base - alt)[ben].mean()) if ben.any() else 0.0,
        "annual_spend_status_quo_per_rider": float(base.mean()),
    }


def full_fare_revenue_annual(mean_weekly_trips_obs: float | None = None) -> float:
    """Annual status-quo revenue from full-fare riders implied by observed trips, at $3.00
    less the weekly-cap discount (computed per scenario by the caller)."""
    wk = mean_weekly_trips_obs or observed_weekly_full_fare_trips()
    return wk * 52 * BASE


def reserve_float(mean: float = 4.5, shape: float | None = None, *, n: int = 60_000,
                  minimum: float = 25.0, replenish_point: float = 0.25,
                  rate: float = 0.0424, churn: float = 0.20, unclaimed: float = 0.5,
                  seed: int = 1) -> dict:
    """E-ZPass-style fare account: hold ~a month of fares, replenish at a trigger.

    Mirrors the E-ZPass NY rule (NYSTA T&C TA-W68167A): prepaid amount = about 30
    days of charges, minimum $25, replenished by the prepaid amount when the
    balance falls to the plan's replenishment point; NO INTEREST paid to the
    account holder. The replenishment point is not published in those terms;
    `replenish_point` (as a fraction of the prepaid amount) is an assumption.

    A balance that cycles from (1+rp)*P down to rp*P averages (rp + 1/2)*P.
    Interest at `rate` (default: 3-month T-bill 4.24%, FRED DGS3MO 2026-09-25).

    Breakage (the spec's "charged $X, used $Y"): each year a share `churn` of
    accounts goes dormant — visitors, people who leave — holding on average one
    cycle's balance, and a share `unclaimed` of that is never refunded. Both are
    illustrative assumptions, NOT estimates. The only NYC reference point found:
    ~$200M from unused MetroCard balances in the decade to 2010, ~$20M/yr
    (Gothamist 2014-01-17, citing the NYT). See research/sources.md.
    Returns per-rider figures; the caller scales by rider count.
    """
    rng = np.random.default_rng(seed)
    shape = shape or calibrate_shape(mean)
    lam = _gamma_rates(n, mean, shape, rng)
    # Monthly spend under the weekly cap, expected value (4.33 weeks).
    weekly = rng.poisson(lam[:, None], size=(n, 12))
    monthly = (np.minimum(weekly * BASE, WEEKLY_CAP).mean(axis=1)) * 52 / 12
    prepaid = np.maximum(monthly, minimum)
    avg_balance = (replenish_point + 0.5) * prepaid
    return {
        "mean_weekly_trips": mean,
        "mean_monthly_spend": float(monthly.mean()),
        "median_prepaid": float(np.median(prepaid)),
        "share_at_minimum": float((monthly < minimum).mean()),
        "mean_avg_balance": float(avg_balance.mean()),
        "float_income_per_rider_year": float(avg_balance.mean() * rate),
        "breakage_per_rider_year": float(avg_balance.mean() * churn * unclaimed),
        "balance_as_months_of_spend": float(avg_balance.sum() / monthly.sum()),
    }


# ---------------------------------------------------------------------------
# Scenario grid for the write-up
# ---------------------------------------------------------------------------

MEANS = (3.0, 4.5, 6.0)   # weekly trips per full-fare rider; sets rider count
CHAINS = (0.0, 0.10, 0.25)  # same-day trip chaining, swept for the daily caps
CAP_SCENARIOS = {
    "monthly $138 (ETA, 46 rides)": dict(monthly_cap=138.0),
    "monthly $132 (old 30-Day price)": dict(monthly_cap=132.0),
    "monthly $120 (40 rides)": dict(monthly_cap=120.0),
    "annual $1,500 (500 rides)": dict(annual_cap=1500.0),
    "annual $1,656 (12 x $138)": dict(annual_cap=1656.0),
    "monthly $138 + annual $1,500": dict(monthly_cap=138.0, annual_cap=1500.0),
    "daily $6 (3rd ride free)": dict(daily_cap=6.0),
    "daily $7.50": dict(daily_cap=7.5),
    "daily $9 (4th ride free)": dict(daily_cap=9.0),
}


def cap_grid(n: int = 40_000) -> pd.DataFrame:
    """Every cap scenario at every assumed mean. $ scaled to observed full-fare trips."""
    obs = observed_weekly_full_fare_trips()
    rows = []
    for mean in MEANS:
        shape = calibrate_shape(mean)
        sq = simulate(mean, shape, n=n)
        sq_rev = obs * 52 * BASE * (1 - sq["share_taps_free_under_weekly_cap"])
        for name, kw in CAP_SCENARIOS.items():
            for chain in (CHAINS if "daily" in name else (0.0,)):
                # Chaining moves trips between days within a week, so the weekly
                # status quo (and sq_rev) is unchanged; the ratio is like for like.
                r = simulate(mean, shape, n=n, chain=chain, **kw)
                rows.append(_row(name, mean, chain, obs, sq_rev, r))
    return pd.DataFrame(rows)


def _row(name, mean, chain, obs, sq_rev, r) -> dict:
    return {
        "scenario": name, "mean_weekly_trips": mean, "chain": chain,
        "implied_riders_M": obs / mean / 1e6,
        "status_quo_revenue_$M": sq_rev / 1e6,
        "revenue_change_pct": (r["revenue_ratio"] - 1) * 100,
        "revenue_change_$M": (r["revenue_ratio"] - 1) * sq_rev / 1e6,
        "riders_benefiting_pct": r["share_riders_benefiting"] * 100,
        "trips_by_beneficiaries_pct": r["share_trips_by_beneficiaries"] * 100,
        "saving_per_beneficiary_$yr": r["mean_annual_saving_beneficiary"],
        "free_under_weekly_cap_pct": r["share_taps_free_under_weekly_cap"] * 100,
        "rider_days_3plus_pct": r["share_rider_days_3plus_trips"] * 100,
    }


def float_grid() -> pd.DataFrame:
    """Reserve-account float and breakage across means, replenishment points and rates."""
    obs = observed_weekly_full_fare_trips()
    rows = []
    for mean in MEANS:
        shape = calibrate_shape(mean)
        riders = obs / mean
        for rp in (0.10, 0.25, 0.50):
            for rate in (0.03, 0.0424, 0.05):
                f = reserve_float(mean, shape, replenish_point=rp, rate=rate)
                rows.append({
                    "mean_weekly_trips": mean, "implied_riders_M": riders / 1e6,
                    "replenish_point": rp, "rate": rate,
                    "median_prepaid_$": f["median_prepaid"],
                    "share_at_$25_minimum_pct": f["share_at_minimum"] * 100,
                    "total_float_$M": f["mean_avg_balance"] * riders / 1e6,
                    "interest_$M_yr": f["float_income_per_rider_year"] * riders / 1e6,
                    "breakage_$M_yr (illustrative)": f["breakage_per_rider_year"] * riders / 1e6,
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Card-processing fees: per tap vs batched vs reserve replenishment
# ---------------------------------------------------------------------------

# ILLUSTRATIVE fee schedules, (fixed $ per transaction, ad valorem share). The
# MTA's negotiated rates are undisclosed (it cites non-disclosure agreements —
# Husock, NY Post, 2025-04-02). Regulated debit is the Federal Reserve
# Regulation II cap for large issuers (21c + 0.05% + 1c fraud adjustment); check
# whether the Fed's 2023 proposed reduction has since been finalised.
CARD_FEES = {
    "regulated debit (Reg II: 21c + 0.05% + 1c)": (0.22, 0.0005),
    "credit, small-ticket style (1.65% + 4c)": (0.04, 0.0165),
    "credit, standard style (2.0% + 10c)": (0.10, 0.020),
}


def card_fee_scenarios(mean: float = 4.5, *, n: int = 30_000, seed: int = 1,
                       replenish_point: float = 0.25, minimum: float = 25.0) -> pd.DataFrame:
    """Card transactions and fees per year for full-fare riders, by settlement model.

    per_tap: one card charge per paid tap. daily / weekly: charges batched per
    rider-day / rider-week. reserve: E-ZPass-style top-ups of about a month's
    spend (see reserve_float). Assumes every full-fare dollar is paid by bank
    card — scale down by the true bank-card share, which is not public. How OMNY
    actually settles (per tap or batched) is also not public.
    """
    rng = np.random.default_rng(seed)
    shape = calibrate_shape(mean)
    lam = _gamma_rates(n, mean, shape, rng)
    weekly = rng.poisson(lam[:, None], size=(n, 52)).astype(np.int32)
    charged = _charges(_daily_trips(weekly, rng), None)
    spend = charged.sum(axis=1)
    k = observed_weekly_full_fare_trips() / mean / n
    tx = {
        "per_tap": np.ceil(charged / BASE - 1e-9).sum(),
        "daily_batch": (charged > 0).sum(),
        "weekly_batch": (charged.reshape(n, 52, 7).sum(axis=2) > 0).sum(),
        "reserve": (spend / np.maximum(spend / 12, minimum)).sum(),
    }
    total_spend = spend.sum() * k
    rows = []
    for model, count in tx.items():
        row = {"settlement": model, "transactions_M": count * k / 1e6,
               "avg_charge_$": spend.sum() / count}
        for name, (fixed, pct) in CARD_FEES.items():
            row[f"fees_$M | {name}"] = (fixed * count * k + pct * total_spend) / 1e6
        rows.append(row)
    out = pd.DataFrame(rows)
    out.attrs["full_fare_spend_$B"] = total_spend / 1e9
    return out
