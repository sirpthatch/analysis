# Research log

Every figure below came back from a live query. The verification standard
inherited from the handoff bundle carries forward: nothing is a fact until a
query returns it, and anything unverifiable is flagged rather than dropped.

## 2026-09-22 — project set up, blocking question answered

Ported the handoff bundle (`research/handoff/truck-sideguards/`) into the
standard project layout and re-ran its verification queries before using any
of its numbers.

### Data extent re-confirmed

| Dataset | Rows | `crash_date` range | `rowsUpdatedAt` |
|---|---|---|---|
| Crashes `h9gi-nx95` | 2,269,187 | 2012-07-01 → 2026-06-11 | 2026-06-15 23:05 |
| Vehicles `bm4k-52h4` | 4,551,002 | 2012-07-01 → 2026-06-11 | 2026-06-15 23:22 |

The crash row count and the 2026-06-11 end date match the bundle exactly. The
bundle listed a catalog `updatedAt` of 2026-07-31 for both; the API's
`rowsUpdatedAt` is 2026-06-15. Those are different metadata fields — a metadata
touch is not a data refresh — and the 2026-06-11 max crash date is what matters.
The file is still frozen.

### Step 1, BLOCKING: the citywide denominator (`queries.md` §4)

`data/raw/citywide_crashes_by_year.csv`.

| Year | All crashes | Refuse vehicle rows | Refuse per 10k crashes |
|---|---|---|---|
| 2016 | 229,833 | 552 | 24.0 |
| 2017 | 231,007 | 743 | 32.2 |
| 2018 | 231,564 | 785 | 33.9 |
| 2019 | 211,486 | 729 | 34.5 |
| 2020 | 112,918 | 385 | 34.1 |
| 2021 | 110,558 | 401 | 36.3 |
| 2022 | 103,887 | 429 | 41.3 |
| 2023 | 96,607 | 398 | **41.2** |
| 2024 | 91,316 | 406 | 44.5 |
| 2025 | 85,546 | 402 | 47.0 |
| 2026 | 36,424 | 191 | 52.4 (partial) |

The refuse counts reproduce the bundle's CSV exactly.

**This is NEXT-STEPS outcome #1, the one that strengthens the story.** Citywide
crashes fell 63% from 2016 to 2025 (229,833 → 85,546). Refuse-truck records fell
27% (552 → 402). Normalized, refuse trucks went from 24.0 to 47.0 per 10,000
citywide crashes — they roughly **doubled** in relative terms, and the rise runs
straight through the 2023 compliance deadline without a kink: 41.3 → 41.2 → 44.5
→ 47.0.

The story does not die here. The flat raw series was concealing a rising
relative rate, not a real decline.

### The control group (`queries.md` §5)

`data/raw/heavy_vehicles_by_year.csv`, rates in
`data/processed/heavy_rate_by_year.csv`. Per 10,000 citywide crashes:

| Class | 2016 | 2020 | 2023 | 2025 |
|---|---|---|---|---|
| **Garbage or refuse** | **24.0** | **34.1** | **41.2** | **47.0** |
| Box truck | 313.7 | 394.6 | 388.1 | 380.3 |
| Tractor truck diesel | 145.7 | 159.4 | 143.7 | 144.1 |
| Dump | 54.6 | 59.9 | 59.5 | 61.3 |
| Flat bed | 29.1 | 40.2 | 38.2 | 41.3 |
| Tow truck / wrecker | 19.0 | 19.8 | 26.5 | 28.3 |
| Tractor truck gasoline | 25.8 | 21.1 | 25.0 | 23.5 |

Every other heavy class is flat or declining from 2020 onward. Refuse rises
monotonically across the same period. That argues the rise is about refuse
trucks specifically, not about a drift in how heavy vehicles get coded — though
the 2016→2019 portion of the rise may still be code adoption.

### Step 3: the impact vocabulary, enumerated not guessed (`queries.md` §6)

`data/raw/impact_vocabulary.csv`. `point_of_impact` on refuse rows has 19
distinct values. The lateral ones, now in `src/vehicle_codes.py`:

- `LEFT SIDE DOORS`, `RIGHT SIDE DOORS` — the strict reading; a side guard sits
  between the axles and this is the location it most directly covers
- `LEFT/RIGHT FRONT QUARTER PANEL`, `LEFT/RIGHT REAR QUARTER PANEL` — these
  straddle the ends of the guard

`pre_crash` and `vehicle_damage` vocabularies pulled in the same file.

### Step 5: the join

5,421 refuse vehicle rows since 2016 across 5,402 distinct crashes, joined to
their crash rows on `collision_id`. Output in
`data/processed/refuse_harm_by_year.csv`.

Side-impact crashes as a rate also rise: 7.1 per 10k citywide crashes in 2016,
12.9 in 2020, 12.7 in 2023, 13.4 in 2025 — flat-to-rising since 2020, no break
at the deadline.

**But the harm counts inside those crashes are tiny.** Cyclists injured in
side-impact refuse crashes run 0–5 per year; cyclists killed, 0–1. See
`research/limitations.md` #1 — this is the finding that most constrains what
the post can claim.

### The companion reconstruction carries no vehicle type

`../reconstruct_crashdata/data/processed/reconstructed_crashes_gap.csv`, 20,625
collisions covering 2026-06-12 → 2026-09-13. Checked directly:

- `vehicle_type_code1` is **null on all 20,625 rows**
- `collision_type` is only `UNKNOWN` (15,801) or `OTHER` (4,824)

So the patched file **cannot identify truck-involved crashes** and cannot extend
the refuse series past 2026-06-11. What it can extend is the denominator and the
citywide cyclist/pedestrian injury series: it carries `injured_cyclist` (1,604
true), `injured_pedestrian` (1,926) and `fatality` (47) as booleans.

## Still not done

See `NEXT-STEPS.md` in the handoff bundle. The two that matter most:

1. **Separating DSNY municipal trucks from private carters.** Unresolved, and it
   decides whether the waiver framing attaches to these numbers at all.
2. **The FOIL to BIC** for the waiver list. There is no open dataset of which
   trucks hold exemptions.

## 2026-09-22 — who the rule and the waiver actually cover

Prompted by the question of whether the waiver reaches DSNY. Read from primary
sources rather than the handoff bundle's summary.

### The statute

Administrative Code **§16-526**, added by **Local Law 56 of 2015** and amended by
**Local Law 108 of 2021**. Verified in BIC's Notice of Adoption
(`notice-of-adoption-sideguards-08-10-2022.pdf`, Statement of Basis and Purpose).

Per DCAS's own opening remarks at the August 10, 2022 public hearing:

> In 2015, NYC passed Local Law 56 requiring truck side-guards for the City Fleet
> and Commercial Waste Trucks. […] Local Law 108, passed in October 2021, extends
> the truck side-guards requirements to City Contractors. […] By the new local
> law, all City trucks and commercial waste vehicles must be complete with
> side-guards by Jan 1, 2023 unless exempted according to these rules. **The
> Business Integrity Commission (BIC) is implementing and enforcing the law for
> commercial waste vehicles.**

So: **one deadline, two administrators.** DCAS for the city fleet and city
contractors (contracts ≥$2M with weekly truck use), BIC for licensed trade waste.

### The BIC side — what the waiver actually is

From the rule text, 17 RCNY §5-10(h) and §7-03(h):

- Covered: "trade waste hauling vehicle" = any motor vehicle **over 10,000 lbs
  GVWR owned or operated by an entity required to be licensed or registered by
  the Commission**, operated in NYC for the collection, removal, transportation
  or disposal of trade waste. Note this is **broader than garbage trucks**.
- The definition **excludes** any vehicle "on which side guard installation is
  deemed impractical by the Commission" — a waived vehicle is definitionally
  outside the covered class.
- Waiver ground: the licensee "can demonstrate that installation of side guards
  is impractical." Toolboxes, ladders and fuel tanks are the examples BIC gives.
- **A waiver is valid for up to two (2) years**, renewable for further two-year
  periods on a showing that installation "continues to be impractical." Denials
  are reconsiderable within 30 days.

BIC's own page describes the law as applying to "private trade waste carting
companies that are licensed by or registered with" the Commission.

### The DCAS side

Separate exemption pathway with **broader** grounds than BIC's impracticality
test — DCAS's hearing remarks list supply chain disruptions, non-contractor
delays, material effect on contract performance, emergencies, financial hardship,
and "any other circumstance, whether or not anticipated or found to be usual or
typical." DCAS replies in writing within 90 days.

DCAS reported the city-fleet requirement **complete in January 2023**, ~4,000
vehicles, "the largest side guard program in the U.S."
(Government Fleet, 2023-02-15).

### What this means for the analysis

DSNY is **not** exempt as a class, so the 2023 deadline applies to both halves of
the `GARBAGE OR REFUSE` bucket. But only the carter half is exposed to the BIC
waiver, and a complying DSNY fleet against a roughly half-waived carter fleet on
a shared deadline is a **natural experiment** if they can be separated.
Rewritten as `research/limitations.md` #2.

### Not verified

- **DSNY at 92% side-guard compliance.** Seen only in a search snippet; the FY22
  PMMR text does not contain it, and no primary source for it was found. Do not
  repeat it. The FY22 PMMR says only that the "City has a policy to install
  side-guards on all fleet units."
- The 312/572 waiver counts remain Streetsblog's reporting. Given the two-year
  waiver term, these are a **flow of determinations, not a stock of exempt
  vehicles** — establish which before reading them as a trend. The FOIL should
  ask for both.

### Sources read

- BIC Notice of Adoption, side guards, 2022-08-10 —
  https://www.nyc.gov/assets/bic/downloads/pdf/regulations/notice-of-adoption-sideguards-08-10-2022.pdf
- DCAS, Public Hearing Local Law 108 Truck Sideguards, Opening Remarks, 2022-08-10 —
  https://www.nyc.gov/assets/dcas/downloads/pdf/fleet/Public-Hearing-Local-Law-108-Truck-Sideguards-Opening-Remarks-DCAS-August-10-2022.pdf
- NYC Rules, Side Guard Requirements for Heavy Duty Trade Waste Vehicle —
  https://rules.cityofnewyork.us/rule/side-guard-requirements-for-heavy-duty-trade-waste-vehicle/
- BIC, Side Guard Law — https://www.nyc.gov/site/bic/industries/side-guard-law.page
- Government Fleet, "NYC DCAS Completes Truck Side Guard Requirement", 2023-02-15

## 2026-09-22 — research question 1: volume, geography, on/off route

Notebook: `notebooks/truck_crash_profile.ipynb`. Pipeline:
`src/collect_geo.py` (pulls) → `src/routes.py` (spatial join) → notebook (maps).

### Defining "truck"

The `vehicle_type` vocabulary over 2016-present is **2,165 distinct values** for
2.82M vehicle rows; **121 codes cover 99.7%** of them. Two artifacts shaped the
curated list in `src/vehicle_codes.py`:

- Some records are **truncated to five characters** — `TRACT`, `BOX T`, `TOW T`,
  `GARBA`, `SANIT`, `FIRET`, `FLAT`. Same vehicles as their spelled-out
  siblings; folded in. Missing them undercounts.
- The tail below the top 121 codes (~0.3% of rows) is excluded by omission.

Two tiers, reported separately because the choice moves the headline:
**core** (36 matched codes, 127,187 vehicle rows) = unambiguous heavy goods
vehicles; **light** (15 codes, 106,906 rows) = pickups, vans, small commercial.
Buses, ambulances, fire apparatus and forklifts are explicitly excluded.

### Volume — how many truck crashes

`data/processed/truck_crashes_by_year.csv`. Since 2016, **110,445 geocoded
truck-involved crashes** (core list), out of 1,407,499 geocoded crashes.

| Year | All crashes | Truck-involved | Truck share |
|---|---|---|---|
| 2016 | 192,153 | 13,806 | 7.18% |
| 2018 | 216,108 | 17,713 | 8.20% |
| 2019 | 193,820 | 16,078 | 8.30% |
| 2020 | 103,838 | 7,898 | 7.61% |
| 2023 | 89,014 | 6,816 | 7.66% |
| 2025 | 82,704 | 6,497 | 7.86% |

**Truck-involved crashes fell with everything else and their share never moved.**
The band is 7.18%–8.30% across the entire decade, with no trend. Trucks are in
about one NYC crash in thirteen, consistently.

This is a useful contrast with the refuse-specific finding above: refuse trucks
*rose* sharply as a share of crashes over the same period while trucks as a
whole held flat. Whatever is happening to refuse trucks is not happening to
trucks generally.

### Geography

Two maps, because they answer different questions. Raw density traces where
traffic is. The **share** map — truck-involved share of all crashes per hex,
diverging around the citywide 7.8% — traces freight: Hunts Point and the South
Bronx, the Bronx expressway corridors, the Brooklyn–Queens industrial belt
through Maspeth and Long Island City, the crossing approaches, and the Staten
Island Expressway. Midtown and downtown Brooklyn, dense with crashes overall,
sit at or below the citywide truck share.

### On or off the truck route network

DOT truck routes (`jjja-shxy`, pulled 2026-09-22): 32,939 segments — 19,411
Local, 13,305 Through, 223 Limited Local. Nearest-route distance computed in
UTM 18N; **"on-route" is a distance threshold, not a fact in the data**, so
every figure is reported at 15 / 30 / 50 m.

| Group | Crashes | ≤15 m | ≤30 m | ≤50 m |
|---|---|---|---|---|
| Truck (core) | 110,445 | 60.9% | **62.9%** | 65.0% |
| Truck (incl. light commercial) | 199,388 | 56.9% | 59.0% | 61.2% |
| No truck involved | 1,208,111 | 45.0% | **47.0%** | 49.4% |

**Truck crashes are 16 percentage points more likely to be on the designated
network than crashes with no truck in them — and 37% of them still happen off
it.** The gap holds at every threshold, so it is not an artifact of where the
line is drawn.

By borough (30 m), truck share vs other share:

| Borough | Truck | Other | Gap |
|---|---|---|---|
| Queens | 54.9% | 40.7% | **+14.2pp** |
| Bronx | 58.2% | 45.9% | +12.3pp |
| Brooklyn | 46.6% | 34.3% | +12.3pp |
| Manhattan | 62.5% | 58.7% | +3.8pp |
| Staten Island | 62.6% | 65.7% | **−3.1pp** |

Manhattan's small gap is mechanical — the network covers so much of the grid
that being on it says little. **Staten Island is the real exception**: truck
crashes there are marginally *less* on-route than everyone else's.

The off-route share drifted up over the decade, 35.9% (2016) → 37.6% (2025).

### The caveat that governs the whole route finding

**Off-route is not the same as illegal.** Trucks may leave the network to reach
a destination by the most direct path, which is most of what local delivery is.
Nothing in the crash record says where the truck was going, so this analysis
cannot separate a permitted last-mile trip from an illegal shortcut. What it
does establish is *where* truck crashes concentrate away from the network —
which is the input to research question 3.

### Data quality checks run

- **8.7% of crashes have no usable coordinates** (125,910 null, 7,588 zeroed)
  and are dropped from all maps.
- **Geocoding bias checked, and it is small**: 90.8% of truck crashes are
  geocoded against 91.4% of other crashes. A 0.6pp difference — not enough to
  move the on/off-route shares meaningfully, but recorded rather than assumed.

## 2026-09-22 — can crash metadata separate DSNY from private carters?

Short answer: **make and model cannot. Registration and time of day give bounds,
not a split.** And the attempt turned up a dataset that reframes the project.

### What the crash file actually carries on refuse rows (n = 5,421)

| Field | Usable? |
|---|---|
| `vehicle_model` | **No** — 98.3% null (5,330 of 5,421) |
| `vehicle_make` | Partly — 168 values, 13.4% null, dominated by Mack |
| `state_registration` | Yes, as a one-sided bound |
| `crash_time` | Yes, as a behavioural signal |
| day of week | **No** — see below |

### Make-based mixture estimation fails its own consistency check

The idea: BIC's fleet file gives the carter make mix; DSNY's differs; so the
crash-record mix is a weighted blend and the weight is solvable. Using each
"carter-only" make as a tracer against BIC rear/front-end loaders:

| Tracer | Carter fleet | Observed in crashes | Implied carter share |
|---|---|---|---|
| Peterbilt | 23.4% | 6.7% | 28.7% |
| Western Star | 1.0% | 0.8% | 77.6% |
| Kenworth | 3.0% | 3.3% | **107.0%** |
| Freightliner | 1.9% | 2.4% | **130.5%** |

The tracers disagree by 4.5x and **two return impossible values above 100%**.
Whatever the true split is, this method cannot find it. Do not use it.

Likely causes, none of them fixable from here: the BIC file is a 2026 snapshot
against crashes running back to 2016; NYPD's `GARBAGE OR REFUSE` does not map
cleanly onto BIC body types; and crash exposure is not proportional to fleet
count.

### What does carry signal

**Out-of-state registration — a hard floor.** An out-of-state refuse truck
cannot be a DSNY municipal vehicle. **14.6% (793 records) are definitively
private.** NY-registered is 4,308; that population is the mixture.

**Time of day — a real behavioural separation.** Known-private trucks crash
overnight at twice the rate of NY-registered ones:

| Band | NY-registered | Out-of-state (known private) |
|---|---|---|
| 00–05 overnight | 19.2% | **38.1%** |
| 06–09 early am | 32.0% | 20.4% |

Consistent with carters running overnight commercial collection and DSNY
running day-shift residential. But it only **bounds** the answer: if DSNY never
crashed overnight, carters would be at most ~50% of NY-registered rows, putting
the overall carter share somewhere between **14.6% and ~58%**. Too wide to
attribute anything.

**Day of week — dead.** DSNY not collecting Sundays does not show: Sunday is
4.5% of NY-registered rows and 5.5% of out-of-state rows. Discard this idea.

**`GLBEN` = Global, and it is a DSNY tracer.** 124 records, 0% out-of-state,
9.7% overnight. Cross-referenced against the city Vehicle Auction List
(`ynic-uz5i`), which lists surplus city vehicles as make `GLOBAL`, model `M4` —
a Global Environmental mechanical broom. So **street sweepers are being coded
`GARBAGE OR REFUSE` in the crash file.** Small (2.6%) but it means the code is
contaminated with vehicles that are neither refuse collection nor covered by the
side-guard rule (sweepers are expressly outside it).

### The dataset that changes the project: BIC `n84m-kx4j`

"Licensees and Registrants Fleet Information", exported 2026-09-21. **7,101
active vehicles across 1,559 licensees**, each with make, model, year, body
type, GVWR band, ownership — and **`vehicle_has_side_guard`**.

`NEXT-STEPS.md` step 0 says "There is no open dataset of which trucks hold
waivers. That's a FOIL to BIC." **That is wrong.** Per-vehicle side-guard status
is published. The FOIL is still worth filing for the *waiver determinations*
(basis, date, renewals), but the compliance picture no longer waits on it.

**Streetsblog's "about half exempted" checks out — in aggregate.** Among the
6,701 vehicles over the rule's 10,000 lb line, **48.2% have a side guard and
51.2% do not.**

**But it does not hold for garbage trucks.** Side-guard rate by body type,
covered vehicles:

| Body type | n | With side guard |
|---|---|---|
| **Rear End Loader** | 586 | **97.6%** |
| Tank | 128 | 70.3% |
| Delivery | 169 | 68.6% |
| Box Truck | 169 | 66.3% |
| Roll-off Truck | 534 | 63.3% |
| Dump Truck | 3,121 | 43.0% |
| Truck (generic) | 1,207 | 35.6% |
| **Tractor** | 380 | **7.1%** |

Rear and front-end loaders together: **97.6% equipped (n=591)**. Only 14 lack a
guard.

### What this does to the pitch

**It undercuts it as framed.** The argument was that the waiver neutralized the
rule for refuse trucks. The actual refuse collection vehicles are almost fully
equipped. The 51% non-compliance is concentrated in dump trucks, generic trucks
and tractors — the construction-and-demolition end of the trade waste fleet,
not the garbage end.

Two live stories survive, and both are better sourced than the original:

1. **Refuse-truck crash rates roughly doubled relative to citywide despite
   near-universal side-guard fitment on that body type.** Guards went on; the
   relative crash rate kept climbing. That is a harder and more interesting
   question than "the rule was waived away."
2. **The waivers are real but they are somewhere else** — 93% of BIC-licensed
   tractors and 57% of dump trucks have no guard. If the exemption is being used
   at scale, that is where to look, and nobody has reported it.

### Caveats on the BIC file — state these before using it

- **It is a snapshot exported 2026-09-21, not a 2023 compliance record.** A
  vehicle waived in 2023 and since replaced reads as compliant now. This file
  cannot establish what the fleet looked like at the deadline.
- **`vehicle_has_side_guard` is largely self-reported** by licensees on their
  BIC applications (per the dataset description).
- **"No" is not the same as "waived."** It may mean non-compliant, pending, or
  not required. The waiver determinations themselves are still not published.
- 70 of 7,101 rows are flagged `vehicle_information_complete = False`; excluding
  them moves the covered-fleet side-guard rate from 48.2% to 48.6%, so the
  headline is not sensitive to them.

## 2026-09-22 — correction: truck over-representation is NOT confined to industrial areas

An earlier entry and the article draft both claimed Midtown and downtown
Brooklyn sat "at or below the citywide truck share." **That is wrong.** Caught
by Thatcher reading the map against the text.

Truck share of crashes by ZIP (geocoded crashes 2016–2026, n>=1,500), against
the citywide 7.85%:

| ZIP | Area | Truck share |
|---|---|---|
| 10474 | Hunts Point | 33.5% |
| 11222 | Greenpoint | 20.5% |
| **10018** | **Garment District** | **19.7%** |
| **10013** | **Tribeca / Canal** | **16.2%** |
| **10001** | **Chelsea / Penn** | **16.1%** |
| **10036** | **Times Square** | **15.6%** |
| **11217** | **Park Slope / downtown Bklyn** | **13.2%** |
| **11201** | **Brooklyn Heights / downtown** | **12.3%** |
| 10310 | Staten Island (Port Richmond) | 1.7% |
| 11004 | Eastern Queens | 2.2% |

By borough: **Manhattan 11.19%** (highest), Brooklyn 7.47%, Bronx 7.06%,
Queens 5.31%, Staten Island 2.44% (lowest).

Four of the twelve highest-share ZIPs citywide are in Midtown or lower
Manhattan.

**The correct pattern is freight versus residential, not industrial versus
commercial.** Trucks are over-represented in two kinds of place: the industrial
zones (Hunts Point, Greenpoint, Maspeth, the Brooklyn waterfront) and the dense
commercial cores where every business takes deliveries. The low-share areas are
low-density residential.

This unifies the two route findings rather than splitting them: the commercial
cores are both the highest truck-share areas *and* the densest off-route
clusters, which is what last-mile delivery on non-designated streets looks like.

**Process note.** The error came from pattern-matching an expectation onto a
choropleth instead of reading values out of it. Any claim about what a map shows
gets checked against the underlying numbers before it goes in a draft.
