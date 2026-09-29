"""Station-level monthly-pass concentration — the load-bearing question.

There is no rider identifier anywhere in these datasets: they are aggregated by
station, hour and fare class. So you CANNOT compute how many riders would reach
a 46-ride monthly cap. Any approach that tries is unsound.

The workaround is revealed preference. A rider who bought a 30-Day Unlimited
had already told the MTA they were high-frequency — that is what the purchase
was. So the 2024 window, grouped by station, gives the share of each station's
ridership taken on a monthly pass. That is a direct map of where the affected
riders board, with no trip-count model required.

The kill condition lives here. If monthly-pass share is roughly uniform across
stations, there is no geography, no equity angle, and the piece collapses to a
citywide arithmetic note. `dispersion_verdict` decides that explicitly instead
of leaving it to the eye.
"""

from __future__ import annotations

import pandas as pd

from . import collect, constants as C

# Stations below this ridership in the window are dropped before ranking:
# a complex with 300 rides can post a wild share on a handful of swipes.
MIN_WINDOW_RIDERSHIP = 5_000

# If the interquartile range of monthly-pass share is below this many
# percentage points, treat the distribution as flat and the geography as absent.
FLATNESS_IQR_THRESHOLD_PP = 2.0


def station_table(year: int = 2024, *, refresh: bool = False) -> pd.DataFrame:
    """Per-station monthly-pass share for one window, ranked."""
    rows = collect.station_fare_class(year, refresh=refresh)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"no station rows returned for {year}")

    total = (
        df.groupby(["station_complex_id", "station_complex", "borough"], as_index=False)
        ["ridership"].sum().rename(columns={"ridership": "total_ridership"})
    )
    monthly = (
        df[df["fare_class_category"] == C.MONTHLY_PASS_CATEGORY]
        .groupby("station_complex_id", as_index=False)["ridership"]
        .sum().rename(columns={"ridership": "monthly_pass_ridership"})
    )

    out = total.merge(monthly, on="station_complex_id", how="left")
    out["monthly_pass_ridership"] = out["monthly_pass_ridership"].fillna(0.0)
    out["monthly_pass_share_pct"] = (
        out["monthly_pass_ridership"] / out["total_ridership"] * 100
    ).round(2)
    out["year"] = year
    out["below_volume_floor"] = out["total_ridership"] < MIN_WINDOW_RIDERSHIP

    return out.sort_values("monthly_pass_share_pct", ascending=False).reset_index(drop=True)


def borough_rollup(year: int = 2024, *, refresh: bool = False) -> pd.DataFrame:
    """Monthly-pass share aggregated to borough. Weighted, not a mean of shares."""
    df = station_table(year, refresh=refresh)
    g = df.groupby("borough", as_index=False)[
        ["monthly_pass_ridership", "total_ridership"]
    ].sum()
    g["monthly_pass_share_pct"] = (
        g["monthly_pass_ridership"] / g["total_ridership"] * 100
    ).round(2)
    return g.sort_values("monthly_pass_share_pct", ascending=False)


def dispersion_verdict(year: int = 2024, *, refresh: bool = False) -> dict:
    """Decide whether the station-level geography is real or flat.

    Returns the spread statistics and an explicit verdict. A 'flat' verdict is
    a genuine finding that deflates the equity framing — report it, don't bury
    it and don't go looking for a subgroup that survives.
    """
    df = station_table(year, refresh=refresh)
    keep = df[~df["below_volume_floor"]]
    s = keep["monthly_pass_share_pct"]

    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    flat = iqr < FLATNESS_IQR_THRESHOLD_PP

    return {
        "year": year,
        "stations_ranked": int(len(keep)),
        "stations_dropped_below_floor": int(df["below_volume_floor"].sum()),
        "median_share_pct": round(float(s.median()), 2),
        "p25_share_pct": round(float(q1), 2),
        "p75_share_pct": round(float(q3), 2),
        "iqr_pp": round(float(iqr), 2),
        "min_share_pct": round(float(s.min()), 2),
        "max_share_pct": round(float(s.max()), 2),
        "verdict": "FLAT — no station geography; the piece is citywide arithmetic only"
        if flat
        else "DISPERSED — station geography is real; the map is worth building",
    }


def join_across_windows(*, refresh: bool = False) -> pd.DataFrame:
    """Join 2024 and 2026 station tables on station_complex_id, not name.

    Station complexes get renamed and merged over time, so a join on
    `station_complex` silently loses rows. Joining on the id and reporting the
    misses is the only honest way to do this.
    """
    a = station_table(2024, refresh=refresh)[
        ["station_complex_id", "station_complex", "borough",
         "total_ridership", "monthly_pass_share_pct"]
    ].rename(columns={"total_ridership": "total_2024",
                      "monthly_pass_share_pct": "monthly_pass_share_2024"})
    b = station_table(2026, refresh=refresh)[
        ["station_complex_id", "total_ridership"]
    ].rename(columns={"total_ridership": "total_2026"})

    merged = a.merge(b, on="station_complex_id", how="outer", indicator=True)
    merged.attrs["only_2024"] = int((merged["_merge"] == "left_only").sum())
    merged.attrs["only_2026"] = int((merged["_merge"] == "right_only").sum())
    merged.attrs["matched"] = int((merged["_merge"] == "both").sum())
    return merged
