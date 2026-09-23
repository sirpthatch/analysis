# Limitations

What the numbers in `data/processed/` cannot support. Written to be stated in
the post, not buried.

## 1. The harm counts are too small to test the rule directly

This is the binding constraint. Cyclists injured in *side-impact* refuse-truck
crashes run **0–5 per year** across the whole series; cyclists killed, 0–1.
Pedestrians injured, 0–4.

Side guards are claimed to cut fatalities by roughly 40%. Against a base of one
death a year, a 40% reduction is statistically invisible — you could not detect
it if it were real, and its absence is not evidence the rule failed. Any claim
that the rule "didn't move the curve" has to rest on the crash-rate series, not
on the death counts.

The crash-level series (5,402 refuse crashes since 2016) does carry enough
volume to talk about rates.

## 2. `GARBAGE OR REFUSE` does not map onto the regulated fleet

Verified against the rule text 2026-09-22 — see `research/research-log.md`. The
mismatch has three separate parts and they point in different directions.

**DSNY trucks are not exempt from side guards.** Both DSNY and private carters
were required to have guards by the same date, January 1, 2023, under the same
Administrative Code section (§16-526, added by Local Law 56 of 2015 and amended
by Local Law 108 of 2021). What differs is the administrator: DCAS runs the
requirement for the city fleet and city contractors, BIC runs it for licensed
trade waste haulers. Each has its own exemption pathway.

So for the question *"did the 2023 deadline move the crash curve?"*, mixing DSNY
and carters is **not** fatal — both halves faced the same deadline.

**For the waiver question it is still fatal.** Only the carter half is exposed to
the BIC waiver process — the one Streetsblog counted, and the one the Badger
truck used. DCAS reported its city-fleet installs complete in January 2023
(~4,000 vehicles), while BIC told Streetsblog roughly half the trade waste fleet
is exempted. If that holds, the two halves of the `GARBAGE OR REFUSE` bucket have
very different compliance rates, and the combined series averages them.

**The BIC-covered fleet is broader than `GARBAGE OR REFUSE`.** The rule covers
any vehicle over 10,000 lbs GVWR operated by a BIC licensee or registrant for the
collection, removal, transportation or disposal of trade waste. That is not just
garbage trucks — the Badger hydro-excavation truck in the news hook is a case in
point, and would not plausibly be coded `GARBAGE OR REFUSE` by NYPD. So the code
both **over-covers** the regulated fleet (it includes DSNY) and **under-covers**
it (it excludes non-refuse trade waste vehicles). Neither direction is quantified.

**The separation is an opportunity, not just a caveat.** A DSNY fleet that
complied on time and a carter fleet that was roughly half waived, both hitting
the same deadline, is a treated-versus-untreated comparison — a much stronger
design than a before/after on the combined series. It depends entirely on
separating them, which is unresolved. Approaches in order of promise:
`vehicle_make`, `state_registration`, and a spatial join against DSNY Commercial
Waste Zones (`8ev8-jjxq`). If none separate cleanly, present the combined figure
with the caveat — do not silently assume.

**A caution on the waiver counts.** A BIC waiver is valid for up to two years and
is renewable. The reported 312 waivers this year against 572 last year are
therefore a flow of determinations, not a stock of exempt vehicles, and a
year-over-year drop could be the renewal cycle rather than a tightening. Do not
read those two numbers as a trend without establishing which they are.

## 3. The public crash file froze on 2026-06-11

Confirmed live. The September 19, 2026 fatality that prompted the story is not
in the data and won't be for months.

The companion reconstruction (`../reconstruct_crashdata`) covers 2026-06-12 →
2026-09-13, but **carries no vehicle type at all** — `vehicle_type_code1` is null
on all 20,625 rows and `collision_type` is only `UNKNOWN`/`OTHER`. It can extend
the citywide denominator and the citywide cyclist/pedestrian injury series. It
cannot extend the refuse-truck series.

2026 is therefore a partial year in every table here, and is marked as such.

## 4. The rising rate is a ratio, and ratios have two ends

Refuse trucks per 10,000 citywide crashes nearly doubled from 2016 to 2025. The
denominator fell 63% over that period — much of it the 2020 collapse in driving,
which never fully recovered. A rising ratio means refuse trucks did not fall as
fast as everything else; it does not by itself mean more refuse-truck crashes
happened.

The control group is what makes it meaningful: box trucks and tractor trailers
held flat or fell on the same denominator while refuse rose. Present the control
group alongside the refuse series, always.

## 5. The 2016 series break is a schema artifact

The vehicle file holds 1 record in 2014 and 1 in 2015, then 552 in 2016. That is
a reporting-schema change, not an event. The series starts in 2016.

Whether the 2016→2019 rise (24.0 → 34.5 per 10k) is continued code adoption or
real is **not established**. The 2020-onward rise is on firmer ground — the
coding regime looks stable there and the control classes are flat across it.

## 6. `vehicle_type` is dirty and the blanks are large

Junk codes (`PK`, `DELV`, `UN/C`), duplicate concepts (`BIKE` vs `BICY`), and
tens of thousands of blank records. Every comparison wraps both sides in
`upper()`. The heavy-vehicle code list is in `src/vehicle_codes.py` and should
be published in the post. Quantify what is excluded rather than dropping it.

## 7. Crash records exist only when NYPD files a report

Minor truck-versus-cyclist contact that produces no report never appears. This
biases toward severe events, and the direction of any change in reporting
practice over the period is unknown.

## 8. Unverified claims inherited from reporting

Carried over from `research/handoff/truck-sideguards/VERIFIED-DATA.md` and still
**not independently confirmed** — all are Streetsblog reporting, not open data:
312 BIC waivers this year against 572 last year; roughly half the trade-waste
fleet exempted; the 40% fatality reduction figure; the Badger truck's 2023
exemption. Attribute them, don't assert them. The 40% figure should be traced to
the Volpe LPD literature before use.

## 9. "Off-route" does not mean "illegally off-route"

Applies to every route figure in `notebooks/truck_crash_profile.ipynb`.

Trucks are permitted to leave the designated network to reach a destination by
the most direct path — which describes most local delivery. The crash record
carries no origin or destination, so a permitted last-mile trip and an illegal
shortcut are indistinguishable here. The 37% off-route share is a statement
about **geography**, not about compliance, and must not be written up as one.

## 10. The truck route network is current, the crashes are historical

`jjja-shxy` was pulled 2026-09-22 and reflects today's network. Crashes run back
to 2016, and the network changed over that period. Early-year on/off-route calls
are therefore made against a network that did not exist in that form at the
time, biasing the off-route time series in an unknown direction. Read the
2016→2025 drift in the off-route share cautiously; the cross-sectional
comparison (trucks vs non-trucks, same network, same period) is unaffected.

## 11. Truck involvement is not truck fault

A crash is counted as truck-involved if any vehicle row on it is on the curated
list. Nothing in this says the truck caused the crash, and a crash with two
trucks counts once. Any causal language in a writeup has to come from somewhere
other than these counts.

## 12. Hex share maps suppress low-volume cells

The over-representation map omits hexes with fewer than 40 crashes, because a
share computed on a handful of crashes is noise. Sparse industrial areas with
genuinely high truck shares may therefore be missing from the map.
