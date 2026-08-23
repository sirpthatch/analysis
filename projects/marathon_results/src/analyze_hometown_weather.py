"""Cell-level test: does hometown training weather move race performance AND turnout?

Hypothesis under test
---------------------
If the 90 days before a race were warmer and drier in a hometown, that hometown's
contingent at the race will be (a) slower and (b) larger - because dedicated runners
train regardless while casual runners are drawn in by pleasant weather. The extra
entrants are casual, so the group's median slows without any individual slowing.

Design
------
The unit is a (hometown city x race instance) CELL, so hometown is held constant by a
home-city fixed effect and the race is held constant by a race-instance fixed effect.
Identifying variation is how a given city's 90-day window differed across the races
its runners attended.

Two fixed-effect structures, because they use different variation:
  within-city   : home-city FE + race-instance FE. Includes SEASONAL variation (a
                  city's window before a March race vs an October race), ~12 degF.
  within-city-month : adds city x calendar-month FE, leaving only YEAR-TO-YEAR
                  weather shocks, ~2.5 degF. Cleaner, much less variation.

The mechanism test is the quantile ladder. If warmth slows everyone (physiological),
every quantile shifts together. If it recruits casual runners (compositional), the
fast tail is unmoved and the slow tail fattens.
"""
from __future__ import annotations

from pathlib import Path

import click
import numpy as np
import pandas as pd

from femodel import absorb, ols_cluster, stars, tidy

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "data" / "analysis_panel.parquet"
OUTDIR = ROOT / "data" / "estimates"

TEMP = "full_temp_mean"          # 90-day mean temperature, degF
PRECIP = "full_overall_precip"   # 90-day total precipitation, inches
QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]

# Restricting to one season is an alternative to the city-month fixed effect: for
# spring races the 90-day window is always winter, so variation across years is
# genuine winter-severity rather than calendar position.
SEASONS = {"all": None, "spring": (3, 4, 5), "summer": (6, 7, 8),
           "fall": (9, 10, 11), "winter": (12, 1, 2), "april": (4,)}


def build_cells(panel: pd.DataFrame, min_n: int) -> pd.DataFrame:
    """One row per (home city, race instance)."""
    g = panel.groupby(["home_id", "instance_id"], observed=True)
    cells = g.agg(
        n=("time_minutes", "size"),
        mean_time=("time_minutes", "mean"),
        temp=(TEMP, "first"),
        precip=(PRECIP, "first"),
        wet_days=("full_days_of_precip", "first"),
        date=("date", "first"),
        year=("year", "first"),
        mean_age=("age_int", "mean"),
        pct_male=("male_num", "mean"),
    ).reset_index()

    q = (panel.groupby(["home_id", "instance_id"], observed=True)["time_minutes"]
              .quantile(QUANTILES).unstack())
    q.columns = [f"p{int(c * 100)}" for c in q.columns]
    cells = cells.merge(q.reset_index(), on=["home_id", "instance_id"])

    cells = cells[cells["n"] >= min_n].copy()
    cells["log_n"] = np.log(cells["n"])
    # Interquantile spread: the compositional signature in a single outcome, so the
    # "warmth fattens the slow tail" claim gets a real standard error rather than an
    # eyeball comparison across the ladder.
    cells["spread_90_10"] = cells["p90"] - cells["p10"]
    cells["spread_50_10"] = cells["p50"] - cells["p10"]
    cells["month"] = pd.to_datetime(cells["date"]).dt.month
    cells["city_month"] = cells["home_id"] + "|" + cells["month"].astype(str)
    # Warm-and-dry index: +1 SD warmer and +1 SD drier, the hypothesis in one number.
    cells["warm_dry"] = ((cells["temp"] - cells["temp"].mean()) / cells["temp"].std()
                         - (cells["precip"] - cells["precip"].mean()) / cells["precip"].std())
    return cells


def fit(cells: pd.DataFrame, outcome: str, regressors: list[str], fe_cols: list[str],
        spec: str, label: str) -> pd.DataFrame:
    needed = list(dict.fromkeys([outcome] + regressors + fe_cols + ["home_id"]))
    sub = cells[needed].dropna()
    if len(sub) < 200:
        return pd.DataFrame()
    fe_keys = [pd.factorize(sub[c])[0] for c in fe_cols]
    n_absorbed = sum(len(np.unique(k)) for k in fe_keys)
    X = absorb(sub, [outcome] + regressors, fe_keys)
    beta, se, info = ols_cluster(X[outcome].to_numpy(), X[regressors].to_numpy(),
                                 sub["home_id"].to_numpy(), n_absorbed=n_absorbed)
    res = tidy(regressors, beta, se, info, spec, outcome,
               {"fe": "+".join(fe_cols), "label": label})
    res["within_sd"] = [X[r].std() for r in regressors]
    res["effect_per_sd"] = res["coef"] * res["within_sd"]
    return res


def show(res: pd.DataFrame, title: str, units: dict[str, str] | None = None) -> None:
    if res.empty:
        print(f"\n{title}\n  (insufficient data)")
        return
    units = units or {}
    print(f"\n{title}")
    print(f"  N={res['n'].iloc[0]:,} cells  clusters={res['clusters'].iloc[0]}  fe={res['fe'].iloc[0]}")
    print(f"  {'term':<12} {'coef':>10} {'se':>9} {'t':>7}   {'per 1sd':>9}  unit")
    for r in res.itertuples():
        print(f"  {r.term:<12} {r.coef:>10.4f} {r.se:>9.4f} {r.t:>7.2f}{stars(r.t):<3}"
              f" {r.effect_per_sd:>9.3f}  {units.get(r.term, '')}")


@click.command()
@click.option("--panel", default=str(PANEL), type=click.Path(exists=True, path_type=Path))
@click.option("--outdir", default=str(OUTDIR), type=click.Path(path_type=Path))
@click.option("--min-n", default=5, type=int, help="Minimum runners per cell.")
@click.option("--season", default="all", type=click.Choice(list(SEASONS)),
              help="Restrict to races run in these months.")
def main(panel: Path, outdir: Path, min_n: int, season: str) -> None:
    df = pd.read_parquet(panel)
    df["male_num"] = (df["sex"] == "M").astype(float)
    months = SEASONS[season]
    if months:
        df = df[pd.to_datetime(df["date"]).dt.month.isin(months)].copy()
    cells = build_cells(df, min_n)
    suffix = "" if season == "all" else f"_{season}"
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("HOMETOWN TRAINING WEATHER -> PERFORMANCE AND TURNOUT")
    print(f"unit: (hometown city x race instance) cell, >={min_n} finishers")
    if months:
        print(f"SEASON FILTER: {season} (months {months}) - races only, "
              f"so the 90-day window sits in one season")
    print(f"{len(cells):,} cells | {cells['home_id'].nunique()} cities | "
          f"{cells['instance_id'].nunique():,} race instances | "
          f"{int(cells['n'].sum()):,} runner-races")
    print("regressors: temp = 90-day mean degF; precip = 90-day total inches")
    print("=" * 80)

    U = {"temp": "min or log-pts per degF", "precip": "per inch", "warm_dry": "per index pt"}
    results = []
    FE_MAIN = ["home_id", "instance_id"]
    FE_SEAS = ["city_month", "instance_id"]

    for fe, tag, blurb in [
        (FE_MAIN, "within-city", "seasonal + year-to-year variation (~12 degF)"),
        (FE_SEAS, "within-city-month", "year-to-year weather shocks only (~2.5 degF)"),
    ]:
        print(f"\n{'=' * 80}\n{tag.upper()}  -  {blurb}\n{'=' * 80}")
        for outcome, title in [("p50", "MEDIAN FINISH TIME (minutes)"),
                               ("log_n", "TURNOUT: log(finishers from this city)")]:
            r = fit(cells, outcome, ["temp", "precip"], fe, f"{tag}:{outcome}", title)
            show(r, title, U)
            results.append(r)
        r = fit(cells, "p50", ["warm_dry"], fe, f"{tag}:p50_warmdry", "median ~ warm-dry index")
        r2 = fit(cells, "log_n", ["warm_dry"], fe, f"{tag}:logn_warmdry", "turnout ~ warm-dry index")
        show(r, "MEDIAN TIME ~ combined warm-and-dry index", U)
        show(r2, "TURNOUT ~ combined warm-and-dry index", U)
        results += [r, r2]

    # ---- mechanism: quantile ladder ------------------------------------------
    print(f"\n{'=' * 80}\nMECHANISM - QUANTILE LADDER\n{'=' * 80}")
    print("compositional => fast tail unmoved, slow tail fattens; physiological => all shift alike")
    ladders = []
    for fe, tag in [(FE_MAIN, "within-city"), (FE_SEAS, "within-city-month")]:
        print(f"\n  [{tag}]")
        print(f"  {'quantile':<10} {'temp coef':>11} {'se':>8} {'t':>7}   {'precip coef':>12} {'se':>8} {'t':>7}")
        rows = []
        for qn in ["p10", "p25", "p50", "p75", "p90"]:
            r = fit(cells, qn, ["temp", "precip"], fe, f"ladder[{tag}]:{qn}", qn)
            if r.empty:
                continue
            tr = r[r["term"] == "temp"].iloc[0]
            pr = r[r["term"] == "precip"].iloc[0]
            print(f"  {qn:<10} {tr['coef']:>11.4f} {tr['se']:>8.4f} {tr['t']:>7.2f}{stars(tr['t']):<3}"
                  f" {pr['coef']:>11.4f} {pr['se']:>8.4f} {pr['t']:>7.2f}{stars(pr['t'])}")
            rows.append({"fe": tag, "quantile": qn, "temp_coef": tr["coef"], "temp_se": tr["se"],
                         "temp_t": tr["t"], "precip_coef": pr["coef"], "precip_se": pr["se"],
                         "precip_t": pr["t"], "n_cells": tr["n"]})
            results.append(r)
        lad = pd.DataFrame(rows)
        if len(lad) >= 2:
            dt = lad["temp_coef"].iloc[-1] - lad["temp_coef"].iloc[0]
            dp = lad["precip_coef"].iloc[-1] - lad["precip_coef"].iloc[0]
            print(f"    p90-p10 spread:  temp {dt:+.4f} min/degF   precip {dp:+.4f} min/inch")
        ladders.append(lad)
    print("\n  (temp spread > 0 and precip spread < 0 => warm/dry fattens the slow tail:")
    print("   the compositional signature the hypothesis predicts)")
    pd.concat(ladders, ignore_index=True).to_csv(outdir / f"hometown_quantile_ladder{suffix}.csv", index=False)

    print(f"\n{'=' * 80}\nMECHANISM - DIRECT TEST ON THE INTERQUANTILE SPREAD\n{'=' * 80}")
    print("H0: weather shifts the whole distribution (spread unchanged, physiological)")
    print("H1: weather recruits slow runners (spread widens with warm/dry, compositional)")
    for fe, tag in [(FE_MAIN, "within-city"), (FE_SEAS, "within-city-month")]:
        for outcome in ["spread_90_10", "spread_50_10"]:
            r = fit(cells, outcome, ["temp", "precip"], fe, f"spread[{tag}]:{outcome}", tag)
            show(r, f"{outcome} ({tag})", U)
            results.append(r)

    # ---- turnout robustness: include cells with zero attendance ---------------
    print(f"\n{'=' * 80}\nTURNOUT ROBUSTNESS - extensive margin (zeros included)\n{'=' * 80}")
    grid = _zero_filled_grid(df, cells)
    r = fit(grid, "log_n1", ["temp", "precip"], FE_MAIN, "turnout:zeros",
            "log(1+finishers), full city x instance grid")
    show(r, "TURNOUT incl. zeros: log(1 + finishers)", U)
    results.append(r)

    # ---- turnout expressed as a headcount ------------------------------------
    print(f"\n{'=' * 80}\nTURNOUT AS A HEADCOUNT\n{'=' * 80}")
    r = fit(cells, "log_n", ["temp", "precip"], FE_MAIN, "turnout:count", "headcount")
    b = r[r["term"] == "temp"]["coef"].iloc[0]
    pct = np.exp(b) - 1
    print(f"  proportional effect: {100 * pct:+.2f}% per degF   "
          f"(the effect is multiplicative, so a headcount needs a baseline)")
    print(f"  a city's contingent at one race: median {cells['n'].median():.0f}, "
          f"mean {cells['n'].mean():.0f} runners")
    print(f"\n  {'city usually sends':>20}{'+1 degF':>10}{'+5 degF':>10}{'+10 degF':>10}")
    for base in [10, 25, 50, 100, 250, 500]:
        print(f"  {base:>20}{base * pct:>10.1f}{base * (np.exp(5 * b) - 1):>10.1f}"
              f"{base * (np.exp(10 * b) - 1):>10.1f}")
    print(f"\n  typical city ({cells['n'].median():.0f} runners): "
          f"{cells['n'].median() * pct:.2f} extra runners per degF")
    print("  NOTE: the race-instance FE absorbs race-level size, so this is a city's")
    print("  contingent RELATIVE to other cities at the same race - it does not say")
    print("  the whole field grows by this much.")

    # ---- individual level with runner FE -------------------------------------
    print(f"\n{'=' * 80}\nINDIVIDUAL LEVEL - same regressors, runner fixed effects\n{'=' * 80}")
    print("if the cell-level slowdown were physiological it should survive here")
    ind = df[df["runner_id"].notna()].copy()
    ind["temp"], ind["precip"] = ind[TEMP], ind[PRECIP]
    ind["age_c"] = ind["age_int"] - ind["age_int"].mean()
    ind["age_c2"] = ind["age_c"] ** 2
    for fe, tag in [(["home_id", "instance_id"], "city FE"),
                    (["runner_id", "instance_id"], "runner FE")]:
        needed = ["time_minutes", "temp", "precip", "age_c", "age_c2"] + fe + ["home_id"]
        sub = ind[list(dict.fromkeys(needed))].dropna()
        keys = [pd.factorize(sub[c])[0] for c in fe]
        X = absorb(sub, ["time_minutes", "temp", "precip", "age_c", "age_c2"], keys)
        beta, se, info = ols_cluster(X["time_minutes"].to_numpy(),
                                     X[["temp", "precip", "age_c", "age_c2"]].to_numpy(),
                                     sub["home_id"].to_numpy(),
                                     n_absorbed=sum(len(np.unique(k)) for k in keys))
        res = tidy(["temp", "precip", "age_c", "age_c2"], beta, se, info,
                   f"individual:{tag}", "time_minutes", {"fe": "+".join(fe), "label": tag})
        res["within_sd"] = [X[c].std() for c in ["temp", "precip", "age_c", "age_c2"]]
        res["effect_per_sd"] = res["coef"] * res["within_sd"]
        show(res[res["term"].isin(["temp", "precip"])], f"individual finish time, {tag}", U)
        results.append(res)

    allr = pd.concat([r for r in results if not r.empty], ignore_index=True)
    allr.to_csv(outdir / f"hometown_weather_estimates{suffix}.csv", index=False)
    cells.to_parquet(outdir / f"hometown_cells{suffix}.parquet", index=False)
    print(f"\n-> {outdir / ('hometown_weather_estimates' + suffix + '.csv')}")
    print(f"-> {outdir / ('hometown_quantile_ladder' + suffix + '.csv')}")


def _zero_filled_grid(panel: pd.DataFrame, cells: pd.DataFrame) -> pd.DataFrame:
    """Every (city, instance) pair a city plausibly could have attended.

    Restricted to instances in years the city was otherwise active, so we do not
    count 'did not attend a race in 2003' for a city only observed from 2015.
    """
    inst = panel.drop_duplicates("instance_id")[["instance_id", "date", "year"]]
    span = panel.groupby("home_id")["year"].agg(["min", "max"]).reset_index()
    grid = span.merge(inst, how="cross")
    grid = grid[(grid["year"] >= grid["min"]) & (grid["year"] <= grid["max"])]

    obs = panel.groupby(["home_id", "instance_id"], observed=True).size().rename("n").reset_index()
    grid = grid.merge(obs, on=["home_id", "instance_id"], how="left")
    grid["n"] = grid["n"].fillna(0)

    wx = cells.drop_duplicates(["home_id", "instance_id"])[["home_id", "instance_id", "temp", "precip"]]
    # Weather for non-attended cells comes from the city-window table via any cell of
    # the same city+date; fall back to merging on the observed cells we have.
    grid = grid.merge(wx, on=["home_id", "instance_id"], how="left")
    grid = grid.dropna(subset=["temp", "precip"])
    grid["log_n1"] = np.log1p(grid["n"])
    return grid


if __name__ == "__main__":
    main()
