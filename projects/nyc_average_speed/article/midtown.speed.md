A Walk Through Midtown Traffic
------------------------------

It is common wisdom in NYC that it is faster to walk than take a car through midtown. Let's dig into the data on that folklore to unravel some data-driven truths.

The NYC Taxi & Limousine Commission publishes every yellow taxi trip — pickup time, dropoff time, distance, and which of 263 zones each end of the trip fell in. Seven and a half years of that, from January 2019 through July 2026, is 334 million trips. Look at only the ones that start *and* end inside midtown — so nothing escapes to the FDR or an airport — and you have a speedometer for midtown's surface streets, sampled a few thousand times a day.

The folklore is almost right. Midtown averages **5.8 miles per hour**. A brisk walk is about 3.5. So the cab still wins, but not by a lot.

Of course, that is not the whole story. The TLC data can give us a few more insights about midtown traffic.

# Midtown has a rush day, not a rush hour

![Midtown traffic barely outpaces a walk for most of the working day](lib/daily_curve.png)

Most cities have a morning peak, a midday lull, and an evening peak. Midtown doesn't. It has a cliff at 7am, a plateau of about 5.2 mph that lasts until 7pm, and a slow recovery overnight. Midtown NYC does not have a rush hour; it has a rush day.

# The January sprint and the December crawl

![Midtown loses a third of its speed between January and December](lib/seasonal_curve.png)

Midtown in January moves at 6.39 mph on average. Midtown in December moves at 4.10. That is a **36% collapse** across the year. The slowest weeks of the year are the same weeks every year, and the pattern survives when you index each week to its own year's median.

A few things fall out of this:

* **January is the fastest month**, not some summer lull. The city empties out after the holidays and the streets open up.
* **August is genuinely faster than June or July** — 5.80 against 5.37. This is probably due to people taking their August vacations.
* **The slowdown across the year is not weather.** I pulled hourly weather for midtown and re-ran everything on dry hours only. The January-to-December drop is 35.9% with all weather included and 36.0% with rain and snow excluded. Winter slowness in midtown is not precipitation. It is people.

# Midtown is slowing down year over year

Midtown has gotten slower since 2019 — from 6.18 mph to 4.94, about 20%. Uptown, above 60th Street, fell only 7.5% over the same period.

But after breaking the day into time bands and measuring the decline, it looks like morning is slowing down the least.

![Midtown's quiet hours are disappearing faster than its rush hour](lib/band_trend.png)

Morning rush lost 8% of its speed. Night lost 18%. Evening lost 17%. Overnight lost 15%. Note that the increase in speed in 2020 and 2021 is almost certainly an artifact of COVID times in NYC.

**The morning commute is getting slower but not as much as the rest of the day.** Everything else is degrading roughly twice as fast. The evening and night are converging downward onto the midday floor — the hours that used to be midtown's relief are quietly becoming indistinguishable from its worst.

If you want a one-line version: midtown is not getting a worse rush hour. Midtown is becoming rush hour all day.

# Should we blame the UN for slowness?

Every September, the UN General Assembly brings world leaders and their delegations to a few blocks of Turtle Bay, and every September New Yorkers cry about it being the worst traffic week of the year. The city agrees with them: NYC DOT designates every weekday of the conference a **Gridlock Alert Day**, and states that midtown speeds during UNGA week are "historically their slowest of the year."

The taxi data gives a little more nuance to that: it depends entirely on where you are standing (or driving).

![Across midtown, December beats UNGA week](lib/slowest_weeks.png)

For midtown broadly, the slowest weeks of the year are in **December**. In 2024 the four slowest weeks were all December; UNGA week came fifth. Christmas shopping, tourist crowds, and delivery trucks beat a hundred motorcades.

But in the two taxi zones **next to the UN itself**, UNGA week is the slowest week of the year in **every single year** — rank one out of fifty-two, 2019 through 2025, without exception. December never touches it there.

Both things are true at once, and the difference between them is about fifteen blocks. DOT's claim is very likely measured over the First Avenue corridor and the blocks around the UN, where it is unambiguously correct. Fifteen blocks west, in the heart of midtown, the holiday season is worse.

So the more nuanced version is: **UNGA is the worst week of the year if you live near the UN, and the worst week that isn't Christmas if you don't.**

# An unintended causal experiment

In 2020, the 75th session of the General Assembly was held **by pre-recorded video**, without the usual hustle and bustle that affects midtown traffic.

![When UNGA went virtual, midtown's traffic jam went with it](lib/unga_natural_experiment.png)

In every in-person year, UNGA week runs 15% to 22% slower than the weeks on either side of it. In 2020 it ran **3.9% faster**. In 2021 — a hybrid session with reduced attendance — the effect came back, but at the mild end of the range. This is about as close to a controlled experiment as an urban dataset ever gets.

# You can see the security perimeter in the data

We can dig deeper into how the UNGA affects traffic in different parts of midtown by measuring its impact as a function of distance from the UN area.

![You can see the UN security perimeter in the traffic data](lib/unga_gradient.png)

In the two taxi zones adjacent to UN headquarters, UNGA week runs **30% slower** than its neighboring weeks. Across midtown as a whole, 19%. Across all of Manhattan below 60th Street, 8%. Above 60th Street, 3%.

The effect decays cleanly with distance from a single point in Turtle Bay. And in the virtual years, the gradient flattens completely.

The day-level pattern fits the diplomatic calendar closely. Monday through Wednesday are brutal — 27% to 37% below normal. By Friday the effect has essentially gone. The General Debate opens on a Tuesday and front-loads its heaviest days.

# Congestion pricing: a real effect, and a small one

On January 5, 2025, New York began charging vehicles to enter Manhattan below 60th Street. A reasonable expectation from this was faster trips due to less congestion on the road.

The geography here is a gift for analysis. Everything below 60th Street is tolled; everything above it is not. Two halves of the same island, same weather, same tourists, same secular decline in yellow taxi ridership — one with a toll and one without. That is a control group.

![Congestion pricing bought midtown about 2% — for one year](lib/congestion_speed.png)

In the first year, the tolled zone gained about **2%** in speed relative to the untolled control. The gain shows up in the morning, midday, and evening — and not at all overnight, which is what you'd want to see if a weekday toll were doing the work rather than chance.

By 2026 (partial-year data), it is gone. Two years on, the net difference between the tolled zone and its control is indistinguishable from zero.

# What all this adds up to

Four things I'd take away:

1. **Midtown is slow in a way that averages hide.** Not a peak-hour problem — a ten-hour plateau at walking-adjacent speeds, and a 36% swing between January and December.
2. **The decline is concentrated in the hours nobody plans for.** Morning rush is holding up. Evenings and nights are collapsing toward the midday floor.
3. **UNGA is real, and how bad it is depends on your address.** Next to the UN it is the slowest week of the year, every year. Across midtown as a whole, December is worse. We know the effect is causal because the year they held it on Zoom, it vanished.
4. **Congestion pricing did something small, at the right hours, and then it faded.** We probably need the full 2026 data to say this definitively.

---

*Methodology: NYC TLC yellow taxi trip records, January 2019 – July 2026, restricted to trips beginning and ending within six midtown taxi zones (Midtown Center, East, North, South, Times Square, Garment District). Speed is distance-weighted — total miles divided by total hours — rather than an average of trip speeds, which over-weights short hops. Weather is hourly ERA5 reanalysis for midtown. Trips with implausible distance, duration or implied speed are excluded (about 8% of records). Every filter bound moves the headline by under 0.03 mph except the 0.2-mile minimum trip distance, which is worth +0.12 mph if raised to half a mile — and in the conservative direction, since a stricter floor makes midtown look faster. Five days in September 2022 and 2023 were dropped after TLC's published files turned out to be missing 96–98% of their trips — four of those days fell inside UNGA weeks.*

*A caveat worth stating plainly: taxis are not traffic. They stop for passengers, cruise for fares, and prefer avenues. Everything above describes how fast a taxi moves through midtown, which is a good proxy for how fast midtown moves, but it is a proxy.*
