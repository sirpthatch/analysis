# RQ3 — Did congestion pricing change midtown traffic?

> *"How has the introduction of congestion prices affected the traffic in
> midtown? The expectation is that trips would be faster and there would be
> fewer rides. Is that the case?"*

Notebook: `notebooks/congestion_pricing.ipynb`. CBD tolling began **5 January
2025**. CBD = Central Business District, MTA's term for the tolled area:
Manhattan south of 60th Street.

---

## The answer

**Faster trips: yes, but barely, and it did not last.** A +2.0% speed gain in
the first year against an untolled control, fully reversed by 2026.

**Fewer rides: we cannot say, and the honest answer is to stop trying with this
dataset.** The de-trending method fails its own placebo test. What the raw
counts show is the *opposite* of the expectation for taxis specifically.

## Setup

The toll is visible per trip, which confirms the treated/control split
independently of any geography we assigned:

| Area | Share of trips paying the CBD fee (2025+) |
|---|---|
| cbd | 97.6% |
| midtown_core | 96.9% |
| un_core | 97.3% |
| cbd_boundary | 25.4% |
| **uptown** | **0.2%** |

All comparisons use January–July so that the partial 2026 year is comparable and
December (the slowest month) never enters some years and not others.

## Speed: a small first-year gain that reverses

Distance-weighted mph, Jan–Jul:

| Year | CBD | Uptown | Gap |
|---|---|---|---|
| 2023 | 8.62 | 10.28 | −1.66 |
| 2024 | 8.18 | 10.18 | −2.00 |
| **2025** | **8.41** | **10.25** | **−1.83** |
| 2026 | 7.82 | 9.85 | −2.03 |

| Transition | CBD | Uptown | Difference-in-differences |
|---|---|---|---|
| 2024 → 2025 | +0.23 | +0.07 | **+0.16 mph (+2.00%)** |
| 2025 → 2026 | −0.59 | −0.39 | **−0.19 mph (−2.31%)** |
| **2024 → 2026** | −0.36 | −0.33 | **−0.03 mph (−0.38%)** |

**Two years on, the net effect is indistinguishable from zero.**

### Parallel trends — the assumption behind the estimate

The CBD−uptown gap was *already* moving before the toll: −1.11 (2022), −1.66
(2023), −2.00 (2024), i.e. changes of −0.55 and −0.34 per year. The gap was
widening at roughly 0.3–0.5 mph/year, then moved **+0.16** in the treatment year.

This cuts both ways and should be stated plainly:

- The +0.16 estimate is **conservative** — against a widening trend, simply
  stopping the slide is itself an effect.
- But a control whose gap was drifting that fast **violates strict parallel
  trends**, so the number carries real uncertainty and should be quoted as
  "about 2%", not "+0.163 mph".

### The effect appears at the right hours

A weekday daytime toll should move weekday daytime speed. It does:

| Band | Δ gap 2024→2025 | Δ gap 2025→2026 |
|---|---|---|
| Overnight | −0.25 | −0.45 |
| **Morning** | **+0.32** | −0.16 |
| **Midday** | **+0.31** | −0.29 |
| **Evening** | **+0.28** | −0.04 |
| Night | −0.01 | −0.39 |

Morning, midday and evening all gain about +0.3 mph relative to control in 2025;
overnight and night show nothing. That is the pattern a tolling scheme should
produce, and it is a genuine point in favour of the effect being real rather
than noise.

## Volume: a documented dead end

The plan was to fit the secular decline on pre-toll data, extrapolate, and read
the shortfall as the toll's effect. Implemented — then tested by running the
same method on **2024, a year with no toll**, where it must return zero.

**Placebo (truth = 0):**

| Spec | Fit from | DiD |
|---|---|---|
| linear | 2021-07 | **−14.0%** |
| linear | 2022-01 | **−12.1%** |
| quadratic | 2021-07 | +2.6% |
| quadratic | 2022-01 | −3.4% |

**Real estimate (2025):** ranges from **−9.4% to +10.1%** — the sign flips
between "fewer rides" and "more rides" depending on nothing but whether the
trend is fitted as linear or quadratic.

The method's own error is as large as its answer. The root cause is the
non-monotonic trend Phase 2 flagged: volume collapsed in 2020, recovered through
2025, fell again in 2026. No low-order trend extrapolates credibly across that.

**What the raw counts say, with no model at all** (Jan–Jul, indexed to 2019):

| Year | CBD | Uptown | Midtown core |
|---|---|---|---|
| 2024 | 0.420 | 0.400 | 0.394 |
| **2025** | **0.485** | 0.456 | 0.421 |
| 2026 | 0.429 | 0.443 | 0.344 |

CBD yellow taxi volume **rose** across the toll's introduction. That rejects the
strong form of "fewer rides" for taxis — and it is not surprising: a taxi pays a
small per-trip CBD fee where a private car pays far more to enter, so the toll
plausibly pushes riders *toward* taxis.

**"Fewer vehicles entering the CBD" is a different claim about a different
population.** Answering it needs MTA's own entry counts or the FHVHV files.
Cite MTA rather than this dataset.

## No sign of boundary toll-avoidance

If drivers were ending trips just north of 60th St to dodge the charge, the
straddling zones would gain share. They did not — 5.51% of trips in 2024, 5.21%
in 2025, 5.63% in 2026. No detectable avoidance behaviour at the boundary.

## Verdict against the stated expectation

| Expectation | Verdict |
|---|---|
| Trips would be **faster** | **Partly.** +2.0% in year one, at the right hours, against a control. Gone by 2026. |
| There would be **fewer rides** | **Not answerable here** — and for taxis specifically, the raw counts point the other way. |

## Limitations specific to RQ3

- Parallel trends is violated in the pre-period; the estimate is directionally
  informative, not precise.
- 2026 covers seven months.
- The control (above 60th St) may absorb spillover from displaced trips, which
  would bias the DiD toward zero.
- Taxis are not traffic. A toll aimed at private cars is being measured through
  a fleet that pays a different, much smaller charge.
- Yellow taxi is a shrinking and unrepresentative slice of vehicles.

## For the article

1. **Lead with the honest finding:** congestion pricing bought midtown about 2%
   more speed in its first year, and by 2026 that was gone.
2. **The part that will surprise readers:** taxi volume in the tolled zone went
   *up*, not down. The toll appears to have shifted people toward taxis.
3. **Say what we cannot answer.** Whether fewer *vehicles* enter the CBD is
   MTA's number to report, not ours — and explaining why is more interesting
   than faking a figure.
4. **The timing detail that supports causality:** the gain shows up in the
   morning, midday and evening, and not at all overnight.
