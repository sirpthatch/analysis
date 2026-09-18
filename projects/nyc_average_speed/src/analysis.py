"""Analysis helpers shared by the Phase 3 research-question notebooks.

Everything here works off the date x hour x area panel and keeps summed miles
and minutes intact, so a distance-weighted speed can be recomputed at any level
of grouping rather than averaging averages.

Two conventions hold throughout:
  * Outage days are already dropped (`panel.drop_outages`).
  * "Speed" means distance-weighted mph: total miles / total hours.
"""

import numpy as np
import pandas as pd

from constants import TIME_BAND_LABELS, UNGA_GENERAL_DEBATE, unga_week
from panel import add_band


def wmph(frame):
    """Distance-weighted speed for a set of panel rows."""
    minutes = frame["minutes"].sum()
    return np.nan if minutes == 0 else frame["miles"].sum() / (minutes / 60.0)


def agg(frame, keys):
    """Group and carry sums through, so speed stays exactly weighted."""
    grouped = frame.groupby(keys, observed=True).agg(
        trips=("trips", "sum"),
        miles=("miles", "sum"),
        minutes=("minutes", "sum"),
    )
    grouped["mph"] = grouped["miles"] / (grouped["minutes"] / 60.0)
    return grouped.reset_index()


def daily(frame, area=None, weekdays_only=False, keys=()):
    """Collapse the panel to one row per day (optionally per band, area...)."""
    if area is not None:
        frame = frame[frame["area"] == area]

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["dow"] = frame["date"].dt.dayofweek
    if weekdays_only:
        frame = frame[frame["dow"] < 5]

    return agg(frame, ["date", *keys])


# --- RQ1: seasonality ------------------------------------------------------


def weekday_mix_adjusted(frame, area="midtown_core"):
    """Weekly speed with the weekday mix held constant.

    A week whose observed days skew toward weekends looks artificially fast.
    Averaging each day-of-week's speed within the week, then taking a simple
    mean across the five weekdays, removes that.
    """
    day = daily(frame, area=area, weekdays_only=True)
    day["iso_year"] = day["date"].dt.isocalendar().year
    day["iso_week"] = day["date"].dt.isocalendar().week
    day["dow"] = day["date"].dt.dayofweek

    per_dow = agg(day, ["iso_year", "iso_week", "dow"])
    weekly = (
        per_dow.groupby(["iso_year", "iso_week"])
        .agg(mph=("mph", "mean"), dows=("dow", "nunique"), trips=("trips", "sum"))
        .reset_index()
    )
    return weekly


def seasonal_profile(frame, area="midtown_core", years=None, min_dows=4):
    """Average speed by week-of-year across years, for the backbone chart."""
    weekly = weekday_mix_adjusted(frame, area=area)
    weekly = weekly[weekly["dows"] >= min_dows]
    if years is not None:
        weekly = weekly[weekly["iso_year"].isin(years)]

    return (
        weekly.groupby("iso_week")
        .agg(mph=("mph", "mean"), sd=("mph", "std"), n_years=("iso_year", "nunique"))
        .reset_index()
    )


def band_by_week(frame, area="midtown_core", years=None):
    """Week-of-year profile split by time band."""
    frame = frame[frame["area"] == area].copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[frame["date"].dt.dayofweek < 5]
    frame["iso_year"] = frame["date"].dt.isocalendar().year
    frame["iso_week"] = frame["date"].dt.isocalendar().week
    if years is not None:
        frame = frame[frame["iso_year"].isin(years)]

    per = agg(add_band(frame), ["iso_year", "iso_week", "band"])
    return (
        per.groupby(["iso_week", "band"], observed=True)
        .agg(mph=("mph", "mean"), n_years=("iso_year", "nunique"))
        .reset_index()
    )


# --- RQ2: UNGA -------------------------------------------------------------


def week_table(frame, area="midtown_core", dows=(0, 1, 2, 3, 4), min_days=4):
    """Speed per ISO week, restricted to the given days of the week."""
    frame = frame[frame["area"] == area].copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[frame["date"].dt.dayofweek.isin(dows)]
    frame["iso_year"] = frame["date"].dt.isocalendar().year
    frame["iso_week"] = frame["date"].dt.isocalendar().week

    day = agg(frame, ["iso_year", "iso_week", "date"])
    weekly = (
        day.groupby(["iso_year", "iso_week"])
        .agg(
            miles=("miles", "sum"),
            minutes=("minutes", "sum"),
            trips=("trips", "sum"),
            days=("date", "nunique"),
        )
        .reset_index()
    )
    weekly["mph"] = weekly["miles"] / (weekly["minutes"] / 60.0)
    return weekly[weekly["days"] >= min_days]


def unga_ranking(frame, area="midtown_core", dows=(0, 1, 2, 3, 4), min_days=4):
    """Where UNGA week lands among all weeks of its own year.

    Ranked slowest-first, so rank 1 means the slowest week of the year.
    """
    weekly = week_table(frame, area=area, dows=dows, min_days=min_days)

    rows = []
    for year in sorted(UNGA_GENERAL_DEBATE):
        iso = pd.Timestamp(unga_week(year)[0]).isocalendar()
        same_year = weekly[weekly["iso_year"] == year]
        if same_year.empty:
            continue

        match = same_year[same_year["iso_week"] == iso.week]
        median = same_year["mph"].median()
        if match.empty:
            rows.append(
                {
                    "year": year,
                    "iso_week": iso.week,
                    "mph": np.nan,
                    "rank": np.nan,
                    "weeks": len(same_year),
                    "median": median,
                    "gap": np.nan,
                    "trips": np.nan,
                }
            )
            continue

        mph = match["mph"].iloc[0]
        rank = int((same_year["mph"] < mph).sum() + 1)
        rows.append(
            {
                "year": year,
                "iso_week": iso.week,
                "mph": mph,
                "rank": rank,
                "weeks": len(same_year),
                "median": median,
                "gap": mph - median,
                "trips": int(match["trips"].iloc[0]),
            }
        )

    return pd.DataFrame(rows).set_index("year")


def slowest_weeks(frame, year, area="midtown_core", n=8, dows=(0, 1, 2, 3, 4)):
    """The slowest weeks of a year - what competes with UNGA."""
    weekly = week_table(frame, area=area, dows=dows)
    weekly = weekly[weekly["iso_year"] == year].nsmallest(n, "mph")

    # Label each week by the Monday it starts on, for readability.
    weekly["starts"] = [
        pd.Timestamp.fromisocalendar(int(y), int(w), 1).date()
        for y, w in zip(weekly["iso_year"], weekly["iso_week"])
    ]
    return weekly[["iso_week", "starts", "mph", "trips", "days"]]


def unga_vs_neighbours(frame, area="midtown_core", spread=3, dows=(0, 1, 2, 3, 4)):
    """UNGA week against the weeks either side of it, within the same year.

    Comparing locally rather than to the year median keeps the seasonal trend
    from doing the work.
    """
    weekly = week_table(frame, area=area, dows=dows)

    rows = []
    for year in sorted(UNGA_GENERAL_DEBATE):
        iso = pd.Timestamp(unga_week(year)[0]).isocalendar()
        same_year = weekly[weekly["iso_year"] == year]
        target = same_year[same_year["iso_week"] == iso.week]
        if target.empty:
            continue

        window = same_year[
            same_year["iso_week"].between(iso.week - spread, iso.week + spread)
            & (same_year["iso_week"] != iso.week)
        ]
        if window.empty:
            continue

        mph = target["mph"].iloc[0]
        neighbour = window["mph"].median()
        rows.append(
            {
                "year": year,
                "unga_mph": mph,
                "neighbour_mph": neighbour,
                "gap": mph - neighbour,
                "pct": (mph / neighbour - 1) * 100,
                "n_neighbours": len(window),
            }
        )

    return pd.DataFrame(rows).set_index("year")


# --- RQ3: congestion pricing ----------------------------------------------


def did_table(frame, treated="cbd", control="uptown", months=None, areas=None):
    """Yearly speed for treated and control areas, with differences.

    `months` restricts to the same months in every year, which matters because
    the last year of data is partial and December is the slowest month.
    """
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    if months is not None:
        frame = frame[frame["date"].dt.month.isin(months)]
    frame["year"] = frame["date"].dt.year

    areas = areas or [treated, control]
    out = agg(frame[frame["area"].isin(areas)], ["area", "year"])
    table = out.pivot(index="year", columns="area", values="mph")

    table["difference"] = table[treated] - table[control]
    return table


def did_estimate(table, pre, post, treated="cbd", control="uptown"):
    """Difference-in-differences between two periods, as mph and percent."""
    d_treated = table.loc[post, treated] - table.loc[pre, treated]
    d_control = table.loc[post, control] - table.loc[pre, control]
    return {
        "treated_change": d_treated,
        "control_change": d_control,
        "did_mph": d_treated - d_control,
        "did_pct": (d_treated - d_control) / table.loc[pre, treated] * 100,
    }


def volume_series(frame, area, freq="ME"):
    """Trip counts over time, for the de-trending work."""
    frame = frame[frame["area"] == area].copy()
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.set_index("date")["trips"].resample(freq).sum().rename(area)


def band_label(series):
    """Map band keys to display labels."""
    return series.map(TIME_BAND_LABELS)


# --- Volume de-trending, and whether it can be trusted ---------------------

VOLUME_SPECS = {
    "linear": "log_trips ~ t + C(month)",
    "quadratic": "log_trips ~ t + I(t**2) + C(month)",
}


def _volume_frame(frame, area):
    import numpy as np

    series = volume_series(frame, area).reset_index()
    series.columns = ["date", "trips"]
    series["t"] = (series["date"] - pd.Timestamp("2019-01-01")).dt.days / 365.25
    series["month"] = series["date"].dt.month
    series["log_trips"] = np.log(series["trips"])
    return series


def volume_residual(frame, area, spec, fit_start, fit_end, target_year):
    """Percent by which actual volume beat its own extrapolated trend.

    Fits monthly log volume on a time trend plus month-of-year effects using
    only data before `fit_end`, then measures `target_year` against it.
    """
    import numpy as np
    import statsmodels.formula.api as smf

    series = _volume_frame(frame, area)
    train = series[(series["date"] >= fit_start) & (series["date"] < fit_end)]
    model = smf.ols(spec, data=train).fit()

    target = series[series["date"].dt.year == target_year].copy()
    target["predicted"] = np.exp(model.predict(target))
    return ((target["trips"] / target["predicted"] - 1) * 100).mean()


def volume_did(
    frame, spec, fit_start, fit_end, target_year, treated="cbd", control="uptown"
):
    """De-trended volume difference-in-differences, in percent."""
    t = volume_residual(frame, treated, spec, fit_start, fit_end, target_year)
    c = volume_residual(frame, control, spec, fit_start, fit_end, target_year)
    return {"treated_pct": t, "control_pct": c, "did_pct": t - c}


def volume_placebo(
    frame, fit_starts=("2021-07-01", "2022-01-01"), placebo_year=2024, specs=None
):
    """Run the de-trending on a year with no toll, where the answer must be ~0.

    Any spread here is the method's own error, and bounds what a real estimate
    can claim.  This is the check that decides whether the volume question is
    answerable with yellow taxi data at all.
    """
    specs = specs or VOLUME_SPECS
    rows = []
    for name, spec in specs.items():
        for start in fit_starts:
            out = volume_did(frame, spec, start, f"{placebo_year}-01-01", placebo_year)
            rows.append({"spec": name, "fit_from": start[:7], **out})
    return pd.DataFrame(rows)
