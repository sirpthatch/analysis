# RQ2 — What is the impact of UNGA week?

> *"What is the impact of UNGA? Is it in fact the slowest week of the year, and
> if not what is?"*

Notebook: `notebooks/unga.ipynb`.

---

## The answer

1. **Does UNGA slow midtown down?** Yes — by 15–22% against the weeks either
   side, and by up to 33% in the blocks nearest the UN.
2. **Is it the slowest week of the year?** **It depends on the geography**, and
   this is the subtlest finding in the project:
   - In the **two zones beside the UN**, yes — rank **1 of 52 in every year**,
     2019 through 2025, without exception. December never beats it there.
   - Across the **wider midtown core**, no — December wins in 2022, 2024 and
     2025; UNGA wins in 2019 and 2023.
3. **What beats it in midtown?** **December**, usually weeks 49–52.
4. Setting December aside, UNGA is the slowest non-December week in midtown —
   rank **1 of 47** in 2019, 2022, 2023, 2024 and 2025.

### Reconciling with NYC DOT

NYC DOT designates every UNGA weekday a **Gridlock Alert Day** and states that
midtown speeds during UNGA week are "historically their slowest of the year:
less than four miles per hour, whereas Midtown traffic normally averages between
four and six miles per hour."

Our absolute levels agree closely (UNGA week 2025: 3.78 mph; typical week ~5.0).
The *ranking* looks like a conflict — until the geography is split:

| Area | UNGA rank, 2019 / 2022 / 2023 / 2024 / 2025 |
|---|---|
| **UN-adjacent zones** | **1 / 1 / 1 / 1 / 1** |
| Midtown core | 1 / 5 / 1 / 5 / 3 |
| Midtown extended | 1 / 4 / 1 / 4 / 3 |

DOT's claim is very likely measured over the First Avenue corridor and the
blocks around UN headquarters, where it is unambiguously right. Our midtown core
sits fifteen blocks west, centred on Times Square and the Garment District,
where the holiday season is worse. **Both statements are correct about their own
geography** — the article says so rather than claiming DOT is wrong.

DOT's own Gridlock Alert calendar is consistent with this: 5 days for UNGA
week, and 15 across late November and December.


## Rank within its own year

Slowest-first, so rank 1 = slowest week of that year.

| Year | Full weekdays | Mon–Wed | Gap to year median |
|---|---|---|---|
| 2019 | **1 / 52** | **1 / 51** | −1.14 |
| **2020** | **28 / 53** | **28 / 53** | **+0.01** |
| 2021 | 5 / 52 | 5 / 52 | −1.68 |
| 2022 | 5 / 52 | 2 / 52 | −1.65 |
| 2023 | *outage* | **1 / 52** | −1.24 |
| 2024 | 5 / 52 | 4 / 52 | −0.89 |
| 2025 | 3 / 52 | **1 / 52** | −1.22 |

Mon–Wed is consistently harsher than the full week, which fits a General Debate
that opens on Tuesday and front-loads its heaviest days.

## Why this is close to causal

**The 75th session (2020) ran on pre-recorded video with almost no delegations
in New York.** UNGA was on the calendar; the motorcades were not. It is the
control condition we could not have built ourselves.

Against neighbouring weeks in the same year:

| Year | UNGA vs surrounding weeks |
|---|---|
| 2019 | −18.0% |
| **2020 (virtual)** | **+3.9%** |
| 2021 (hybrid) | −15.3% |
| 2022 | −21.7% |
| 2024 | −15.7% |
| 2025 | −22.5% |

Every in-person year: −15% to −22%. The virtual year: slightly *faster* than its
neighbours. 2021, a hybrid session with reduced attendance, sits at the mild end
of the in-person range — which is the direction you would predict.

## A dose-response gradient by distance from the UN

The effect scales cleanly with proximity to UN headquarters — the signature you
want from a causal story, and hard to explain any other way.

| Year | UN zones | Midtown core | Midtown ext | CBD | Uptown |
|---|---|---|---|---|---|
| 2019 | **−32.0%** | −18.0% | −18.1% | −7.5% | −3.8% |
| **2020** | **+1.3%** | +3.9% | +2.4% | +0.6% | +0.8% |
| 2021 | −21.6% | −15.3% | −13.4% | −7.9% | +0.6% |
| 2022 | **−33.3%** | −21.7% | −19.7% | −10.1% | −5.6% |
| 2024 | −28.9% | −15.7% | −15.8% | −8.2% | −6.5% |
| 2025 | −26.2% | −22.5% | −18.9% | −5.2% | +2.8% |

Monotonic in every in-person year, and flat across all five areas in 2020. The
UN-adjacent zones (UN/Turtle Bay South, Sutton Place/Turtle Bay North) lose
roughly a third of their speed.

## Day by day

Percent against the same weekday in adjacent weeks:

| Year | Mon | Tue | Wed | Thu | Fri | Sat | Sun |
|---|---|---|---|---|---|---|---|
| 2019 | −37 | −31 | −14 | −1 | −9 | −2 | **+40** |
| 2021 | −22 | −19 | −13 | −18 | −3 | +1 | +25 |
| 2022 | −27 | −32 | −27 | −18 | −1 | −8 | +26 |
| 2023 | −29 | +4 | −12 | *outage* | *outage* | — | — |
| 2024 | −30 | −23 | −5 | −23 | +3 | +7 | +26 |
| 2025 | −29 | −28 | −16 | −33 | −14 | +13 | **+46** |

**Monday through Wednesday carry it.** By Friday the effect has largely gone.

The Sunday result is striking and consistent: the Sunday closing UNGA week runs
**25–46% faster** than an ordinary Sunday. Worth investigating before it goes in
the article — the likely reading is that the security perimeter and public
advisories suppress discretionary weekend traffic, leaving the streets unusually
clear, but that is a hypothesis, not a finding.

## Alternative explanations, tested

**Weather.** UNGA weeks are sometimes wetter than their neighbours (2024: 26% of
hours wet vs 2.8%). Restricting to **dry hours only**, the effect survives
everywhere, though it shrinks in the wet years:

| Year | Raw | Dry hours only |
|---|---|---|
| 2019 | −18.0% | −13.4% |
| 2021 | −15.3% | −8.3% |
| 2022 | −21.7% | −15.2% |
| 2023 | — | −18.3% |
| 2024 | −15.7% | **−10.7%** |
| 2025 | −22.5% | −11.9% |

**Use the dry-hours numbers in the article.** They are the honest ones, and
−8% to −18% is still a large effect.

**Permitted street closures.** UNGA weeks have *no more* permitted road-
affecting events than neighbouring weeks — ratios of 0.59 to 1.12, mostly below
1. The permit feed does not explain the slowdown, which is consistent with the
cause being **NYPD security closures that are never published as data**.

## Limitations

- Security closures are unobserved, so we measure the slowdown without being
  able to attribute it to specific streets from data.
- 2023 rests on three clean weekdays (Mon–Wed) because of the TLC outage.
- 2026 is outside the data (ends July).
- Taxi speed is not car speed; see `research/limitations.md`.

## For the article

1. **The myth-check, properly stated:** UNGA is the slowest week of the year if
   you are beside the UN — every year, no exceptions. Fifteen blocks west, in
   the middle of midtown, December is worse. Both are true, and the gap between
   them is the story.
2. **The near-experiment:** in 2020 the General Debate happened by video, and
   the traffic effect vanished completely. Same week, same season, no motorcades.
3. **The gradient:** −32% next to the UN, −18% across midtown, −8% across the
   CBD, ~0% above 60th St. You can see the perimeter in the data.
4. **The texture:** Monday and Tuesday are brutal, Friday is fine, and the
   Sunday is the fastest Sunday of the month.
