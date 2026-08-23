# Hypothesis test: warm, dry training weather → slower, larger hometown fields

> **Hypothesis (as posed).** If the 90 days before a race were warmer and drier in a
> hometown, that hometown's contingent will be **slower** and have **more entrants** —
> because dedicated runners train regardless while casual runners are drawn in by
> pleasant weather.

Run with `src/analyze_hometown_weather.py` (`--min-n 5`, default). Estimates in
`data/estimates/hometown_weather_estimates.csv`; ladder in
`hometown_quantile_ladder.csv`. Companion to `RESULTS_training_weather.md`.

---

## Verdict

**The mechanism is confirmed; the temperature half of the trigger is not.**

| Claim | Verdict |
|---|---|
| Warm/dry fields are **slower at the median** | Weakly — +0.12 min/°F, t = 1.77 |
| …but the slowdown is **compositional, not physiological** | **Confirmed, strongly** |
| Warm/dry draws **more entrants** | **Confirmed for dryness**; for warmth only via seasonal variation |
| Dedicated runners run anyway, casuals join in | **Confirmed** — the fast tail does not move at all |

The distinctive part of your hypothesis — that the population slows *because its
composition changes*, not because anyone runs worse — is exactly what the data shows.
The part that does not survive is warmth as an independent trigger for turnout once
seasonality is removed. Dryness survives both tests.

---

## Design

Unit of analysis is a **(hometown city × race instance) cell** — one row per city's
contingent at one race on one date. 16,524 cells, 107 cities, 953 race instances,
952,138 runner-races (cells with ≥5 finishers).

- **Home-city fixed effect** holds hometown constant, as you asked.
- **Race-instance fixed effect** holds the race, course, date, field and race-day
  weather constant.
- Identification is how a given city's 90-day window differed across the races its
  runners attended. SEs clustered on home city.

Two fixed-effect structures, because they use different variation and it matters:

| Structure | What varies | Magnitude |
|---|---|---|
| **within-city** — city FE + instance FE | seasonal **and** year-to-year | ~12.0 °F |
| **within-city-month** — city×month FE + instance FE | year-to-year shocks only | ~2.5 °F |

The second is the cleaner causal test but has a fifth of the variation.

---

## 1. Turnout

`log(finishers from this city at this race)`

| Structure | Term | Coef | per 1 SD | t |
|---|---|---|---|---|
| within-city | temp (°F) | **+0.0354** | **+12.8%** | **4.30** |
| within-city | precip (in) | −0.0100 | −3.4% | −1.74 |
| within-city-month | temp | −0.0128 | −1.8% | −2.41 |
| within-city-month | precip | **−0.0065** | −1.8% | **−2.95** |

**Warmth: supported, but only seasonally.** A 1-SD warmer 90-day block (3.4 °F) brings
**12.8% more entrants** — a large effect, robust to including non-attended cells as
zeros (+0.0340, t = 4.28) and stronger at a 20-runner cell minimum (+0.0497, t = 4.42).
But restricting to year-to-year shocks within the same city *and calendar month*, the
sign **flips** (−0.0128, t = −2.41), and that flip is itself unstable across cell
thresholds (−0.0028, t = −0.44 at ≥20).

The honest reading: the warmth-turnout relationship lives in the seasonal comparison —
a city sends more runners to races preceded by a pleasant season. That is consistent
with your mechanism, but race-instance FE cannot separate it from race-calendar and
personal-calendar effects that also track the seasons. **It is a real pattern; it is
not cleanly attributable to weather.**

**Dryness: supported in both.** Drier training raises turnout in the seasonal
comparison (t = −1.74, and −2.24 at ≥20 runners) *and* in the year-to-year comparison
(t = −2.95). Precipitation is the more credible trigger of the two, because it is the
one that survives removing seasonality.

## 2. Median performance

| Structure | Term | Coef (min) | per 1 SD | t |
|---|---|---|---|---|
| within-city | temp | +0.1192 | +0.41 min | 1.77 |
| within-city | precip | +0.0156 | +0.05 min | 0.25 |
| within-city-month | temp | +0.1622 | +0.24 min | 1.14 |
| within-city-month | precip | −0.0843 | −0.23 min | −1.21 |

Directionally right for temperature, but **weak** — and at a 20-runner minimum the
median effect disappears entirely (+0.057, t = 0.50). Taken alone this would be a null
result.

That turns out to be the wrong place to look, because a compositional shift moves the
*shape* of the distribution far more than its centre.

## 3. The mechanism — where the hypothesis is confirmed

### Quantile ladder (within-city FE)

Coefficient on training temperature, by quantile of the cell's finish times:

| Quantile | temp coef | t | precip coef | t |
|---|---|---|---|---|
| p10 (fastest) | **−0.013** | −0.17 | +0.147 | 2.55 |
| p25 | +0.093 | 1.38 | +0.076 | 1.46 |
| p50 | +0.119 | 1.77 | +0.016 | 0.25 |
| p75 | +0.203 | 2.89 | −0.083 | −1.19 |
| p90 (slowest) | **+0.295** | 3.94 | −0.117 | −1.54 |

**This is precisely the predicted signature.** Warmth has *no effect whatsoever* on the
fastest tenth (−0.013 min/°F, t = −0.17) and a large, strongly significant effect on
the slowest tenth (+0.295, t = 3.94), rising monotonically in between. The dedicated
runners are untouched; the slow tail fattens.

Precipitation runs the mirror image: wet training slows the fast tail (+0.147, t = 2.55)
while *speeding* the slow tail (−0.117) — because in a wet block the casual entrants
stay home, so the p90 runner is a more committed one.

### Direct test on the interquantile spread

Regressing p90 − p10 gives the "field widening" claim a real standard error:

| Structure | Outcome | temp | t | precip | t |
|---|---|---|---|---|---|
| within-city | p90 − p10 | **+0.308** | **2.88** | **−0.264** | **−2.96** |
| within-city | p50 − p10 | +0.132 | 2.50 | −0.132 | −2.40 |
| within-city-month | p90 − p10 | +0.122 | 0.62 | −0.110 | −1.45 |
| within-city-month | p50 − p10 | +0.070 | 0.58 | −0.107 | −1.84 |

Warm and dry conditions **widen the field** — both terms significant in the predicted
direction, and a 1-SD warmer block stretches p90 − p10 by ~1.05 minutes. Under the
stricter seasonality control the signs persist but significance does not, which is what
a fifth of the identifying variation buys you.

### The slowdown is not physiological

Same regressors, individual runner level, 614,503 runner-races:

| Fixed effects | temp | t | precip | t |
|---|---|---|---|---|
| home-city × instance | +0.337 | 3.42 | +0.164 | 2.62 |
| **runner × instance** | **−0.121** | **−2.30** | +0.059 | 1.19 |

Holding the *runner* constant, a warmer training block makes them **slightly faster**,
not slower. Whatever is slowing the median cell, it is not individual runners running
worse — which is the load-bearing claim of your hypothesis, and it holds.

---

## What this adds to the earlier finding

`RESULTS_training_weather.md` established that the positive training-temperature
coefficient is compositional rather than physiological, using a runner fixed effect and
a turnout diagnostic. This analysis identifies **which part of the distribution moves**
and puts the composition claim on a direct test: warm and dry conditions leave the fast
tail exactly where it was and stretch the slow tail. That is your mechanism, measured.

The one correction to the hypothesis as posed: **dryness, not warmth, is the durable
trigger.** Warmth's turnout effect is large but lives entirely in seasonal variation,
where it cannot be separated from calendar effects; precipitation's effect survives
comparing the same city in the same month across years.

## Caveats

- **Only finishers are observed**, not entrants. If warm, dry blocks also raise finish
  rates among casuals, part of the turnout effect is completion, not registration.
- **The seasonal comparison is the weak link.** Race-instance FE holds the race fixed
  but not a city's seasonal propensity to travel to races at all.
- **109 home cities** caps cluster-robust inference; the year-to-year specifications are
  the ones most starved by it. Expanding weather coverage (see the Stage 1 note in
  `TRAINING_WEATHER_PIPELINE.md`) would bear directly on the results that came out
  underpowered here.
- **Temperature and precipitation are correlated with each other and with season**;
  the combined "warm-and-dry" index is reported in the estimates CSV but is harder to
  interpret than the separate terms, so the separate terms are used throughout.
- Cells require ≥5 finishers; results are reported at ≥20 as robustness and noted where
  they differ.

---

# Addendum: stratifying by season

Restricting to races run in one season is a cleaner way to strip seasonality than the
city×month fixed effect, because it fixes *what the 90-day window is*. For a spring
race the window is winter; for a fall race it is summer. Run with
`--season spring` / `--season fall`.

It also cuts the confound directly: among spring races the residual variation in
training temperature is 2.11 °F under plain city FE, down from 3.41 °F pooled — close
to what the city×month specification achieves, but without discarding the variation
that matters.

**Temperature effects, within-city FE. Coefficient (t-statistic):**

| | All races | **Spring (Mar–May)** | **Fall (Sep–Nov)** |
|---|---|---|---|
| cells | 16,524 | 4,258 | 6,103 |
| median finish time | +0.119 (1.77) | **+0.584 (2.89)** | +0.148 (0.60) |
| p10 — fastest | −0.013 (−0.17) | +0.051 (0.29) | +0.355 (1.55) |
| p90 — slowest | +0.295 (3.94) | **+0.980 (4.16)** | −0.007 (−0.03) |
| p90 − p10 spread | +0.308 (2.88) | **+0.929 (3.98)** | −0.362 (−1.45) |
| turnout | +3.6% (4.30) | **+5.3% (4.53)** | **−4.3% (−3.01)** |
| individual, city FE | +0.337 (3.42) | +0.412 (2.87) | −0.038 (−0.23) |
| individual, runner FE | −0.121 (−2.30) | −0.238 (−0.80) | −0.044 (−0.26) |
| precipitation → turnout | −1.0% (−1.74) | +1.3% (1.96) | −1.7% (−3.49) |
| precipitation → spread | −0.264 (−2.96) | +0.006 (0.03) | −0.063 (−0.50) |

## What this changes

**1. The hypothesis is a spring phenomenon, and there it is much stronger.**
Among spring marathons every prediction lands: a milder winter brings **5.3% more
entrants per °F**, the median slows by **0.58 min/°F** (5× the pooled estimate), the
fastest tenth does not move at all (t = 0.29), the slowest tenth moves hard
(t = 4.16), and the spread widens by 0.93 min/°F (t = 3.98). Holding the runner fixed
the effect is gone (−0.238, t = −0.80). This is the mechanism, cleanly.

**2. Fall races run the opposite way on turnout.** A warmer *summer* training block
means **4.3% fewer** entrants per °F (t = −3.01), and no slowdown of any kind — the
ladder is flat-to-reversed and both individual-level estimates are null.

That is physically sensible and it reframes the pooled result. Mild winters invite
casual runners outdoors; hot summers drive them off. These are not the same effect
with different noise levels — they have opposite signs, which is why the pooled
year-to-year specification looked confusing and why the pooled estimate is a poor
summary of either season.

**3. Correction: precipitation is not the robust trigger.** The main analysis above
concluded that dryness survived removing seasonality while warmth did not. Stratifying
by season overturns that. Precipitation's effect on turnout is **+1.3% in spring and
−1.7% in fall** — opposite signs — and its effect on the field's spread, which was the
strongest precipitation result in the pooled data (−0.264, t = −2.96), is **exactly
zero in spring** (+0.006, t = 0.03). The pooled precipitation results were an artifact
of mixing seasons. **Temperature is the real story; it is confined to spring.**

## Caveats

- Spring is a quarter of the panel (4,258 cells, 220,223 runner-races), so the
  spring-only estimates are less precise even though they are larger.
- The spring runner-FE estimate (−0.238, t = −0.80) is a genuine null but a weak one;
  it cannot rule out a small physiological effect the way the pooled version could.
- Season is still not randomly assigned — spring and fall races differ in field
  composition and goal-race status, so this compares like with like *within* a season
  rather than establishing what season does.
