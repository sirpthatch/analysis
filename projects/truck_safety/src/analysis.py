"""Build the yearly series the side-guard question turns on.

    python src/analysis.py

Reads the cached pulls under `data/raw/` (run `src/collect.py all` first) and
writes `data/processed/`:

    refuse_harm_by_year.csv    refuse-truck crashes and the harm in them, per
                               year, normalized against citywide crash volume
    heavy_rate_by_year.csv     the same rate for every heavy-vehicle class, so
                               refuse can be read against a control group

Nothing here is narrowed to private carters.  `GARBAGE OR REFUSE` covers DSNY
municipal trucks as well, and the side-guard rule only binds licensed trade
waste vehicles - see `research/limitations.md`.  That separation is unresolved.
"""

import pandas as pd

from constants import (
    COMPLIANCE_DEADLINE_YEAR,
    PROCESSED_DIR,
    RAW_FILES,
    SERIES_START_YEAR,
)
from vehicle_codes import SIDE_IMPACT, SIDE_IMPACT_STRICT

HARM_COLUMNS = [
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
]


def citywide():
    frame = pd.read_csv(RAW_FILES["citywide_crashes_by_year"])
    frame["yr"] = frame["yr"].astype(int)
    frame["all_crashes"] = frame["all_crashes"].astype(int)
    return frame


def refuse_joined():
    """Refuse vehicle rows joined to their crash rows on `collision_id`."""
    vehicles = pd.read_csv(RAW_FILES["refuse_vehicles"], dtype=str, low_memory=False)
    crashes = pd.read_csv(RAW_FILES["refuse_crashes"], dtype=str, low_memory=False)

    frame = vehicles.merge(
        crashes, on="collision_id", how="left", suffixes=("_veh", "_crash")
    )
    frame["year"] = frame["crash_date_veh"].str[:4].astype(int)
    for column in HARM_COLUMNS:
        frame[column] = (
            pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
        )

    frame["impact"] = frame["point_of_impact"].fillna("").str.upper()
    frame["is_side"] = frame["impact"].isin(SIDE_IMPACT)
    frame["is_side_strict"] = frame["impact"].isin(SIDE_IMPACT_STRICT)
    return frame


def harm_by_year(frame, denominator):
    """Per-year crashes and harm, with the side-impact subset broken out.

    Harm is summed over the *crash*, not the vehicle: the collision file counts
    injuries per collision, so a crash with two refuse trucks in it would be
    double-counted if summed over vehicle rows.  De-duplicate on collision_id
    before summing.
    """
    crashes = frame.drop_duplicates("collision_id")

    totals = crashes.groupby("year").agg(
        crashes=("collision_id", "nunique"),
        **{column: (column, "sum") for column in HARM_COLUMNS},
    )

    side = (
        frame[frame["is_side"]]
        .drop_duplicates("collision_id")
        .groupby("year")
        .agg(
            side_crashes=("collision_id", "nunique"),
            side_cyclist_injured=("number_of_cyclist_injured", "sum"),
            side_cyclist_killed=("number_of_cyclist_killed", "sum"),
            side_ped_injured=("number_of_pedestrians_injured", "sum"),
            side_ped_killed=("number_of_pedestrians_killed", "sum"),
        )
    )

    strict = (
        frame[frame["is_side_strict"]]
        .drop_duplicates("collision_id")
        .groupby("year")
        .agg(door_impact_crashes=("collision_id", "nunique"))
    )

    out = totals.join(side).join(strict).fillna(0).astype(int).reset_index()
    out = out.merge(denominator, left_on="year", right_on="yr", how="left")
    out["per_10k_crashes"] = (out["crashes"] / out["all_crashes"] * 10_000).round(2)
    out["side_per_10k_crashes"] = (
        out["side_crashes"] / out["all_crashes"] * 10_000
    ).round(2)
    return out.drop(columns=["yr"])


def heavy_rate_by_year(denominator):
    """Every heavy-vehicle class as a rate, so refuse has something to be read against.

    If refuse rises per 10k citywide crashes while box trucks and tractor
    trailers hold flat, the rise is about refuse trucks.  If they all rise
    together, it is about what gets reported, not about the fleet.
    """
    frame = pd.read_csv(RAW_FILES["heavy_vehicles_by_year"])
    frame["yr"] = frame["yr"].str[:4].astype(int)
    frame["n"] = frame["n"].astype(int)

    wide = frame.pivot(index="yr", columns="vt", values="n").fillna(0).astype(int)
    wide = wide[wide.index >= SERIES_START_YEAR]
    rate = wide.div(denominator.set_index("yr")["all_crashes"], axis=0) * 10_000
    return rate.round(2).reset_index()


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    denominator = citywide()

    harm = harm_by_year(refuse_joined(), denominator)
    harm.to_csv(PROCESSED_DIR / "refuse_harm_by_year.csv", index=False)

    rate = heavy_rate_by_year(denominator)
    rate.to_csv(PROCESSED_DIR / "heavy_rate_by_year.csv", index=False)

    print("=== Refuse-truck crashes and harm, by year ===")
    print(harm.to_string(index=False))
    print(
        f"\n(Side-guard compliance deadline: {COMPLIANCE_DEADLINE_YEAR}.  "
        "2026 is partial - the public file ends 2026-06-11.)"
    )

    print("\n=== Heavy vehicles per 10,000 citywide crashes ===")
    print(rate.to_string(index=False))


if __name__ == "__main__":
    main()
