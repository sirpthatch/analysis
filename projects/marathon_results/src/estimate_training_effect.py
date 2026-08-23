"""Stage 5 - estimate the effect of training weather on marathon performance.

Four specifications, reported together because the gaps between them are the
result:

  A  two-way FE (race-instance x home-city) - the causal core. Identified off
     year-to-year weather shocks in a runner's hometown.
  B  race-instance FE only, city effects reported - descriptive "hardy cities".
     Confounded by selection; that is the point of contrast with A.
  C  spec A within age/sex/ability subgroups - heterogeneity.
  D  race-instance x runner FE - strongest available; controls for ability
     directly, on the subsample with a recovered runner_id.

Plus two diagnostics: a placebo using post-race weather, and a check on whether
harsh training weather changes who shows up at all.
"""
from __future__ import annotations

import glob
from pathlib import Path

import click
import numpy as np
import pandas as pd

from femodel import absorb, ols_cluster, stars, tidy

ROOT = Path(__file__).resolve().parent.parent
PANEL = ROOT / "data" / "analysis_panel.parquet"
WINDOWS = ROOT / "data" / "city_window_features"
OUTDIR = ROOT / "data" / "estimates"

TRAIN = "full_temp_mean"      # 90-day mean temperature in the hometown
PEAK = "peak_temp_mean"       # 30-day mean temperature in the hometown
CONTROLS = ["age_c", "age_c2", "male"]


def prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["age_c"] = df["age_int"] - df["age_int"].mean()
    df["age_c2"] = df["age_c"] ** 2
    df["male"] = (df["sex"] == "M").astype(float)
    df["rday"] = df["race_day_temp_max"]
    # Interaction on centered terms so the main effect stays interpretable at the mean.
    df["train_x_rday"] = (df[TRAIN] - df[TRAIN].mean()) * (df["rday"] - df["rday"].mean())
    df["peak_x_rday"] = (df[PEAK] - df[PEAK].mean()) * (df["rday"] - df["rday"].mean())
    return df


def run_spec(df: pd.DataFrame, regressors: list[str], fe_cols: list[str],
             outcome: str, cluster_col: str, spec: str,
             label: str | None = None) -> pd.DataFrame:
    """Absorb `fe_cols`, regress `outcome` on `regressors`, cluster on `cluster_col`."""
    cols = [outcome] + regressors
    # fe_cols and cluster_col frequently overlap (e.g. both home_id); select once.
    needed = list(dict.fromkeys(cols + fe_cols + [cluster_col]))
    sub = df[needed].dropna()
    if len(sub) < 1000:
        return pd.DataFrame()

    fe_keys = [pd.factorize(sub[c])[0] for c in fe_cols]
    n_absorbed = sum(len(np.unique(k)) for k in fe_keys)
    X = absorb(sub, cols, fe_keys)

    beta, se, info = ols_cluster(X[outcome].to_numpy(),
                                 X[regressors].to_numpy(),
                                 sub[cluster_col].to_numpy(),
                                 n_absorbed=n_absorbed)
    # Within-transformed SD, so a coefficient can be read per 1-SD shock.
    sd = {f"sd_{r}": X[r].std() for r in regressors}
    res = tidy(regressors, beta, se, info, spec, outcome,
               {"fe": "+".join(fe_cols), "label": label or spec})
    res["within_sd"] = [sd[f"sd_{r}"] for r in regressors]
    res["effect_per_sd"] = res["coef"] * res["within_sd"]
    return res


def show(res: pd.DataFrame, title: str) -> None:
    if res.empty:
        print(f"\n{title}\n  (insufficient data)")
        return
    print(f"\n{title}")
    print(f"  N={res['n'].iloc[0]:,}  clusters={res['clusters'].iloc[0]:,}  fe={res['fe'].iloc[0]}")
    print(f"  {'term':<18} {'coef':>10} {'se':>9} {'t':>7}  {'per 1sd':>9}")
    for r in res.itertuples():
        print(f"  {r.term:<18} {r.coef:>10.4f} {r.se:>9.4f} {r.t:>7.2f}{stars(r.t):<3} {r.effect_per_sd:>8.3f}")


def placebo_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Post-race weather as a falsification test.

    A backward 90-day window evaluated at race_date + 90 covers exactly
    [race_date, race_date + 90) - the weather AFTER the race, which cannot affect
    it. A non-zero coefficient means the FE structure is leaking city-season effects.
    """
    win = pd.concat([pd.read_parquet(p, columns=["city", "state", "date", TRAIN])
                     for p in glob.glob(str(WINDOWS / "*" / "data.parquet"))],
                    ignore_index=True).rename(columns={TRAIN: "post_temp_mean"})
    d = df.copy()
    d["date_fwd"] = (pd.to_datetime(d["date"]) + pd.Timedelta(days=90)).dt.strftime("%Y-%m-%d")
    return d.merge(win, left_on=["city", "state", "date_fwd"],
                   right_on=["city", "state", "date"], how="inner", suffixes=("", "_w"))


def selection_check(df: pd.DataFrame) -> pd.DataFrame:
    """Does harsh training weather change WHO shows up?

    If a bad winter deters marginal (slower) runners, the surviving field is faster
    and beta is biased toward 'bad weather makes you faster'. Regress the log count
    of finishers per (home city, race instance) on training weather.
    """
    g = (df.groupby(["home_id", "instance_id"])
           .agg(n=("time_minutes", "size"), train=(TRAIN, "first"))
           .reset_index())
    g["log_n"] = np.log(g["n"])
    return run_spec(g, ["train"], ["instance_id", "home_id"], "log_n", "home_id",
                    "selection", "log finishers per city-instance")


@click.command()
@click.option("--panel", default=str(PANEL), type=click.Path(exists=True, path_type=Path))
@click.option("--outdir", default=str(OUTDIR), type=click.Path(path_type=Path))
@click.option("--spec", default="all",
              type=click.Choice(["all", "A", "B", "C", "D", "diagnostics"]))
def main(panel: Path, outdir: Path, spec: str) -> None:
    df = prep(pd.read_parquet(panel))
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    results: list[pd.DataFrame] = []

    print("=" * 78)
    print(f"TRAINING WEATHER AND MARATHON PERFORMANCE")
    print(f"panel: {len(df):,} runner-races | {df['instance_id'].nunique():,} race instances "
          f"| {df['home_id'].nunique():,} home cities | {df['year'].min()}-{df['year'].max()}")
    print(f"outcome: finish time in minutes; training weather: {TRAIN} (deg F)")
    print("=" * 78)

    if spec in ("all", "A"):
        r = run_spec(df, [TRAIN, "train_x_rday"] + CONTROLS,
                     ["instance_id", "home_id"], "time_minutes", "home_id", "A",
                     "two-way FE: race-instance x home-city")
        show(r, "SPEC A - causal core (race-instance FE + home-city FE)")
        results.append(r)

        r2 = run_spec(df, [PEAK, "peak_x_rday"] + CONTROLS,
                      ["instance_id", "home_id"], "time_minutes", "home_id", "A-peak",
                      "two-way FE, 30-day peak window")
        show(r2, "SPEC A (peak) - 30-day window instead of 90-day")
        results.append(r2)

        r3 = run_spec(df, [TRAIN, "train_x_rday"] + CONTROLS,
                      ["instance_id", "home_id"], "ag_z", "home_id", "A-agz",
                      "two-way FE, age-graded outcome")
        show(r3, "SPEC A (age-graded z outcome)")
        results.append(r3)

    if spec in ("all", "B"):
        # Absorb the race instance and controls, then read each city's mean residual:
        # its runners' average time relative to everyone else in the same races.
        cols = ["time_minutes"] + CONTROLS
        sub = df[cols + ["instance_id", "home_id"]].dropna()
        X = absorb(sub, cols, [pd.factorize(sub["instance_id"])[0]])
        b, _, _ = ols_cluster(X["time_minutes"].to_numpy(), X[CONTROLS].to_numpy(),
                              sub["home_id"].to_numpy())
        sub = sub.assign(resid=X["time_minutes"].to_numpy() - X[CONTROLS].to_numpy() @ b)
        city = (sub.groupby("home_id")["resid"]
                   .agg(effect_min="mean", n="size", sd="std").reset_index())
        city["se"] = city["sd"] / np.sqrt(city["n"])
        city = city[city["n"] >= 500].sort_values("effect_min")
        print("\nSPEC B - city effects (minutes vs. same-race peers; NEGATIVE = faster)")
        print("  CONFOUNDED BY SELECTION - measures who travels from a city as much as how they trained")
        print("\n  fastest 10:")
        for r in city.head(10).itertuples():
            print(f"    {r.home_id:<24} {r.effect_min:>7.2f} min  (n={r.n:,})")
        print("  slowest 10:")
        for r in city.tail(10).itertuples():
            print(f"    {r.home_id:<24} {r.effect_min:>7.2f} min  (n={r.n:,})")
        city.to_csv(outdir / "spec_B_city_effects.csv", index=False)

    if spec in ("all", "C"):
        print("\nSPEC C - heterogeneity (coefficient on 90-day training temp, min per degF)")
        rows = []
        df["fast"] = df.groupby("instance_id")["time_minutes"].transform(
            lambda s: s <= s.quantile(0.25))
        groups = ([("sex", s, df["sex"] == s) for s in ["M", "F"]]
                  + [("ability", lab, df["fast"] == val)
                     for lab, val in [("fastest quartile", True), ("rest of field", False)]]
                  + [("age", str(b), df["age_bracket"] == b)
                     for b in ["18-34", "35-39", "40-44", "45-49", "50-54",
                               "55-59", "60-64", "65-69", "70+"] if (df["age_bracket"] == b).sum() > 5000])
        for dim, lab, mask in groups:
            r = run_spec(df[mask], [TRAIN, "train_x_rday"] + CONTROLS,
                         ["instance_id", "home_id"], "time_minutes", "home_id",
                         f"C:{dim}={lab}", lab)
            if r.empty:
                continue
            row = r[r["term"] == TRAIN].iloc[0]
            rows.append({"dim": dim, "group": lab, "coef": row["coef"], "se": row["se"],
                         "t": row["t"], "n": row["n"]})
            print(f"  {dim:<9} {lab:<18} {row['coef']:>8.4f}  se {row['se']:.4f}  "
                  f"t {row['t']:>6.2f}{stars(row['t'])}  n={row['n']:,}")
            results.append(r)
        pd.DataFrame(rows).to_csv(outdir / "spec_C_heterogeneity.csv", index=False)

    if spec in ("all", "D"):
        d = df[df["runner_id"].notna()]
        r = run_spec(d, [TRAIN, "train_x_rday"] + CONTROLS,
                     ["instance_id", "runner_id"], "time_minutes", "home_id", "D",
                     "race-instance FE x runner FE")
        show(r, "SPEC D - runner fixed effects (identified off repeat runners)")
        results.append(r)

        # A and D otherwise differ in BOTH the FE structure and the sample. Rerun A
        # on D's sample so the contrast isolates the runner fixed effect alone.
        a_sub = run_spec(d, [TRAIN, "train_x_rday"] + CONTROLS,
                         ["instance_id", "home_id"], "time_minutes", "home_id",
                         "A|D-sample", "spec A restricted to repeat runners")
        show(a_sub, "SPEC A on D's sample - city FE, repeat runners only")
        results.append(a_sub)

        if not r.empty and not a_sub.empty:
            rd = r[r["term"] == TRAIN].iloc[0]
            ra = a_sub[a_sub["term"] == TRAIN].iloc[0]
            excluded = not (rd["ci_lo"] <= ra["coef"] <= rd["ci_hi"])
            print(f"\n  same sample, city FE {ra['coef']:+.3f} vs runner FE {rd['coef']:+.3f}"
                  f"  ->  city-FE estimate is "
                  f"{'EXCLUDED by' if excluded else 'consistent with'} the runner-FE CI "
                  f"[{rd['ci_lo']:+.3f}, {rd['ci_hi']:+.3f}]")
            v = d.groupby("runner_id")[TRAIN].std()
            print(f"  identifying variation: {(v > 0).sum():,} runners with within-runner "
                  f"training-temp variation (median SD {v.median():.2f} degF)")

    if spec in ("all", "diagnostics"):
        pl = placebo_frame(df)
        pl["post_x_rday"] = ((pl["post_temp_mean"] - pl["post_temp_mean"].mean())
                             * (pl["rday"] - pl["rday"].mean()))
        r = run_spec(pl, ["post_temp_mean", "post_x_rday"] + CONTROLS,
                     ["instance_id", "home_id"], "time_minutes", "home_id", "placebo",
                     "POST-race weather (should be ~0)")
        show(r, "PLACEBO - weather in the 90 days AFTER the race (should be ~0)")
        results.append(r)

        s = selection_check(df)
        show(s, "SELECTION - log finishers per city-instance vs training temp")
        results.append(s)

    if results:
        allr = pd.concat([r for r in results if not r.empty], ignore_index=True)
        allr.to_csv(outdir / "estimates.csv", index=False)
        print(f"\n-> {outdir / 'estimates.csv'}")


if __name__ == "__main__":
    main()
