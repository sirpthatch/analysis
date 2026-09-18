"""Collapse trip-level speed data into the panels the analysis actually uses.

Seven years of yellow taxi is ~250M trips - too much to hold in memory at once.
Every research question is answered at date x hour x area, so aggregate once and
work off that.  Sums of miles and minutes are carried through rather than means,
so a distance-weighted speed can be recomputed at any level of grouping:

    weighted mph = sum(miles) / (sum(minutes) / 60)

Two outputs:
  * speed_panel.parquet  - date x hour x area, with weather joined on
  * zone_pairs.parquet   - month x area x pu_zone x do_zone counts, for the
                           composition-drift check that Phase 2 hinges on

    python src/panel.py --service yellow --start 2019-01 --end 2026-07
"""

import argparse

import pandas as pd

from constants import PROCESSED_DIR
from speed import GEOGRAPHIES, build_month
from tlc import month_range

SPEED_PANEL_FILE = PROCESSED_DIR / "speed_panel.parquet"
ZONE_PAIRS_FILE = PROCESSED_DIR / "zone_pairs.parquet"

# Quantiles kept alongside the means, so we can see whether the distribution
# moved or just its tail.
QUANTILES = (0.25, 0.5, 0.75)


def _aggregate_area(trips, area):
    """One row per (date, hour) for trips wholly inside `area`."""
    subset = trips[trips[area]]
    if subset.empty:
        return None

    grouped = subset.groupby(["date", "hour"])
    panel = grouped.agg(
        trips=("mph", "size"),
        miles=("miles", "sum"),
        minutes=("minutes", "sum"),
        mean_mph=("mph", "mean"),
    )

    # Present only from 2025; carries the CBD toll at trip level, which beats
    # inferring exposure from the date alone.
    if "cbd_congestion_fee" in subset.columns:
        panel["tolled_trips"] = grouped["cbd_congestion_fee"].apply(
            lambda s: (s.fillna(0) > 0).sum()
        )
    else:
        panel["tolled_trips"] = 0

    quantiles = grouped["mph"].quantile(list(QUANTILES)).unstack()
    quantiles.columns = [f"p{int(q * 100)}_mph" for q in QUANTILES]

    panel = panel.join(quantiles).reset_index()
    panel["area"] = area
    return panel


def aggregate_month(trips):
    """Stack the per-area panels for one month of trips."""
    parts = [_aggregate_area(trips, area) for area in GEOGRAPHIES]
    return pd.concat([p for p in parts if p is not None], ignore_index=True)


def zone_pairs_month(trips, year, month):
    """Trip counts and mean speed per origin-destination pair, for drift checks."""
    parts = []
    for area in ("midtown_core", "midtown_ext"):
        subset = trips[trips[area]]
        if subset.empty:
            continue

        pairs = (
            subset.groupby(["pu_zone", "do_zone"])
            .agg(
                trips=("mph", "size"),
                miles=("miles", "sum"),
                minutes=("minutes", "sum"),
            )
            .reset_index()
        )
        pairs["area"] = area
        parts.append(pairs)

    if not parts:
        return None

    frame = pd.concat(parts, ignore_index=True)
    frame["year"] = year
    frame["month"] = month
    return frame


def weighted_mph(frame, by=None):
    """Distance-weighted speed: total miles over total hours.

    The honest aggregate - a simple mean of trip speeds over-weights the short
    crosstown hops that dominate midtown by count but not by distance.
    """
    if by is None:
        return frame["miles"].sum() / (frame["minutes"].sum() / 60.0)

    grouped = frame.groupby(by)
    return grouped["miles"].sum() / (grouped["minutes"].sum() / 60.0)


def attach_weather(panel):
    """Join hourly midtown weather onto the panel, if it has been fetched."""
    from constants import WEATHER_FILE

    if not WEATHER_FILE.exists():
        print("  no weather file; skipping weather join")
        return panel

    import weather

    columns = [
        "date",
        "hour",
        "temp_c",
        "precip_mm",
        "wind_kmh",
        "is_wet",
        "is_heavy_precip",
        "is_snowing",
        "has_snow_cover",
    ]
    hourly = weather.load()[columns]
    return panel.merge(hourly, on=["date", "hour"], how="left")


def build(service, start, end):
    """Stream every month through the aggregators and write both panels."""
    panels, pairs = [], []

    for year, month in month_range(start, end):
        trips = build_month(service, year, month)
        if trips is None:
            print(f"  skip {year:04d}-{month:02d}: no raw file")
            continue

        panels.append(aggregate_month(trips))

        pair_frame = zone_pairs_month(trips, year, month)
        if pair_frame is not None:
            pairs.append(pair_frame)

        core = trips[trips["midtown_core"]]
        print(
            f"  {year:04d}-{month:02d}: {len(trips):>9,} trips, "
            f"{len(core):>7,} core, core {weighted_mph(core):.2f} mph"
        )

    if not panels:
        raise SystemExit("no months built - run src/tlc.py trips first")

    panel = pd.concat(panels, ignore_index=True)
    panel = attach_weather(panel)
    panel.to_parquet(SPEED_PANEL_FILE, index=False)
    print(f"\n  wrote {SPEED_PANEL_FILE.name}: {len(panel):,} rows")

    pair_frame = pd.concat(pairs, ignore_index=True)
    pair_frame.to_parquet(ZONE_PAIRS_FILE, index=False)
    print(f"  wrote {ZONE_PAIRS_FILE.name}: {len(pair_frame):,} rows")

    return panel, pair_frame


def load_panel():
    panel = pd.read_parquet(SPEED_PANEL_FILE)
    panel["date"] = pd.to_datetime(panel["date"])
    return panel


def load_zone_pairs():
    return pd.read_parquet(ZONE_PAIRS_FILE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", default="yellow")
    parser.add_argument("--start", default="2019-01", metavar="YYYY-MM")
    parser.add_argument("--end", required=True, metavar="YYYY-MM")
    args = parser.parse_args()

    build(args.service, args.start, args.end)


if __name__ == "__main__":
    main()


def decompose_composition(pairs, area="midtown_core", baseline=(2019, 1)):
    """Separate "traffic got slower" from "the trip mix changed".

    Distance-weighted speed is a weighted harmonic mean over origin-destination
    pairs:

        S = 1 / sum_p (w_p / v_p)

    where v_p is pair p's own speed and w_p its share of total miles.  Holding
    w at a baseline month while letting v vary gives the speed-only series; the
    gap between that and the observed series is what the changing trip mix did.
    """
    subset = pairs[pairs["area"] == area].copy()
    subset["v"] = subset["miles"] / (subset["minutes"] / 60.0)
    subset["period"] = list(zip(subset["year"], subset["month"]))

    totals = subset.groupby("period")["miles"].transform("sum")
    subset["w"] = subset["miles"] / totals

    base = subset[subset["period"] == baseline].set_index(["pu_zone", "do_zone"])["w"]
    if base.empty:
        raise ValueError(f"baseline {baseline} not present for area {area}")

    rows = []
    for period, group in subset.groupby("period"):
        group = group.set_index(["pu_zone", "do_zone"])

        observed = 1.0 / (group["w"] / group["v"]).sum()

        # Restrict to pairs present in both, then renormalise the baseline
        # weights so they still sum to 1 over the shared pairs.
        shared = base.reindex(group.index).dropna()
        if shared.empty:
            continue
        weights = shared / shared.sum()
        fixed_mix = 1.0 / (weights / group.loc[shared.index, "v"]).sum()

        rows.append(
            {
                "year": period[0],
                "month": period[1],
                "observed_mph": observed,
                "fixed_mix_mph": fixed_mix,
                "composition_mph": observed - fixed_mix,
                "pair_coverage": weights.size / base.size,
            }
        )

    frame = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)
    frame["date"] = pd.to_datetime(
        dict(year=frame["year"], month=frame["month"], day=1)
    )
    return frame


def drop_outages(frame, date_column="date"):
    """Remove days where the TLC source file is missing most of its trips.

    See constants.TLC_OUTAGE_DATES for how these were identified.  Always call
    this before any speed comparison - four of the five land in UNGA weeks.
    """
    from constants import TLC_OUTAGE_DATES

    bad = pd.to_datetime(sorted(TLC_OUTAGE_DATES))
    return frame[~pd.to_datetime(frame[date_column]).isin(bad)]


def detect_outages(frame, area="cbd", window=15, threshold=0.5):
    """Re-run the outage scan, for when the data is extended past 2026-07.

    Flags days below `threshold` of their surrounding median.  The result still
    needs eyeballing against weather and holidays - a blizzard looks identical
    to an outage by volume alone.
    """
    daily = frame[frame["area"] == area].groupby("date")["trips"].sum().sort_index()
    daily = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="D"))
    baseline = daily.rolling(window, center=True, min_periods=5).median()
    ratio = daily / baseline

    flagged = ratio[ratio < threshold].dropna()
    return pd.DataFrame(
        {
            "trips": daily[flagged.index],
            "local_median": baseline[flagged.index].round(0),
            "ratio": flagged.round(3),
        }
    )


def add_band(frame, hour_column="hour"):
    """Label each panel row with its time-of-day band."""
    from constants import BAND_ORDER, TIME_BANDS

    edges = [TIME_BANDS[name][0] for name in BAND_ORDER] + [24]
    frame = frame.copy()
    frame["band"] = pd.cut(
        frame[hour_column],
        bins=edges,
        labels=BAND_ORDER,
        right=False,
        ordered=True,
    )
    return frame


def by_band(frame, extra_keys=(), area=None):
    """Distance-weighted speed per band, optionally grouped by more keys.

    The panel carries summed miles and minutes precisely so this stays exact -
    it is a true distance-weighted speed over the band, not an average of
    hourly averages.
    """
    if area is not None:
        frame = frame[frame["area"] == area]

    frame = add_band(frame)
    keys = list(extra_keys) + ["band"]

    grouped = frame.groupby(keys, observed=True).agg(
        trips=("trips", "sum"),
        miles=("miles", "sum"),
        minutes=("minutes", "sum"),
    )
    grouped["mph"] = grouped["miles"] / (grouped["minutes"] / 60.0)
    return grouped.reset_index()
