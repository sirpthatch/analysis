# RQ1 — How does midtown speed change over the year?

> *"Over the course of a standard year, how does the average speed of surface
> streets change?"*

Notebook: `notebooks/seasonality.ipynb`. Midtown core, weekdays, distance-
weighted, outage days dropped, weekday-mix adjusted. Baseline years 2022–2025.

---

## The answer in one line

**Midtown speed falls by more than a third across the year** — 6.39 mph in
January to 4.10 mph in December, a **−36%** slide — with a sharp secondary dip
in late September that RQ2 identifies as UNGA.

## By month

| Month | mph | | Month | mph |
|---|---|---|---|---|
| Jan | **6.39** | | Jul | 5.39 |
| Feb | 6.20 | | Aug | 5.80 |
| Mar | 5.85 | | Sep | 5.00 |
| Apr | 5.71 | | Oct | 5.08 |
| May | 5.37 | | Nov | 4.88 |
| Jun | 5.37 | | Dec | **4.10** |

The shape is not a smooth sine wave. It is a **fast January**, a spring decline,
a **summer plateau** interrupted by an August recovery, then a **hard autumn
collapse** into December.

## By week

Fastest: weeks 3, 2, 27, 1, 4 (6.6–7.1 mph) — the first month of the year, plus
the week of 4 July.

Slowest: weeks 50, 49, 52, 51, 48 (4.07–4.77 mph) — the whole of December plus
the last week of November. Week 38 (UNGA) is the sixth slowest at 4.88 and the
only non-holiday week in the bottom six.

Indexing each week to its own year's median separates seasonality from the
downward trend. The ranking barely moves: weeks 50, 49, 52 and 51 sit at
**72–76% of their year's median**, and the standard deviation across years is
small (0.02–0.06), so this is a stable annual pattern, not one year's noise.

## August is genuinely faster

August (5.80) beats May, June and July (5.37–5.39) by roughly 8%. The "August
exodus" is real and measurable — worth a line in the article, because it runs
against the intuition that summer is uniformly quiet.

## Weather does not explain the shape

Restricting to dry hours only moves the monthly figures by at most 0.17 mph and
leaves the January→December collapse identical:

| | January | December | Change |
|---|---|---|---|
| All weather | 6.39 | 4.10 | −35.9% |
| Dry hours only | 6.46 | 4.13 | −36.0% |

Winter slowness is not rain and snow. It is traffic.

## The seasonal swing is far larger at some times of day

Peak-to-trough swing across the year, by band:

| Band | Range over the year | Swing |
|---|---|---|
| Overnight (12–7am) | 9.00 – 10.76 | 17.8% |
| Morning (7–10am) | 5.16 – 7.77 | 42.3% |
| Midday (10am–4pm) | 3.39 – 6.36 | 60.8% |
| **Evening (4–8pm)** | **3.19 – 6.71** | **69.9%** |
| Night (8pm–12am) | 4.40 – 8.60 | 60.2% |

Overnight barely has a season. The evening has an enormous one — a December
evening in midtown runs at **3.19 mph**, slower than walking pace, against 6.71
in the best week.

## The multi-year trend is not uniform across the day either

| Band | 2019 | 2026 | Change |
|---|---|---|---|
| Overnight (12–7am) | 10.45 | 8.89 | −14.9% |
| **Morning (7–10am)** | 6.15 | 5.64 | **−8.3%** |
| Midday (10am–4pm) | 5.23 | 4.38 | −16.2% |
| Evening (4–8pm) | 5.32 | 4.39 | −17.4% |
| **Night (8pm–12am)** | 7.33 | 5.98 | **−18.3%** |

Morning rush is twice as resilient as everything else. **Midtown's quiet hours
are eroding fastest** — evening and night are converging down onto the midday
floor.

## For the article

1. **The headline number:** midtown loses a third of its speed between January
   and December. 6.39 → 4.10 mph.
2. **The counter-intuitive one:** rush hour is the *least* damaged part of the
   day over seven years. The story is not "rush hour got worse", it is "midtown
   is becoming rush hour all day".
3. **The vivid one:** a December evening in midtown averages 3.19 mph. A brisk
   walk is about 3.5.
4. **The myth-check setup:** the slowest weeks of the year are December, not
   UNGA — which leads into RQ2.
