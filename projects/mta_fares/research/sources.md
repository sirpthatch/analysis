# Sources

## Primary documents

- **MTA board adoption of January 2026 fares and tolls.**
  https://www.mta.info/press-release/mta-board-adopts-fare-and-toll-increases-take-effect-january-2026
  Base $2.90→$3.00, reduced $1.45→$1.50, express bus $7.00→$7.25, weekly cap $35 /
  $17.50 reduced. States the 7-Day, 30-Day and Express Bus Plus unlimited passes
  "retire and be replaced with the automatic fare cap for all riders." **This is the
  load-bearing citation for the whole piece.** Fetched 2026-09-28.

- **Wikipedia, New York City transit fares.**
  https://en.wikipedia.org/wiki/New_York_City_transit_fares
  Corroborates that as of January 2026 the 7-day cap is the only unlimited option and
  30-day options were discontinued except on AirTrain JFK. Does **not** give the retired
  passes' prices. Secondary — use for orientation, cite the MTA document.

## News hooks

- **Streetsblog NYC, 2026-09-23 — "MTA Chair Open To New Fare Structure Ahead Of 2027
  Fare Hike"** (Dave Colon).
  https://nyc.streetsblog.org/2026/09/23/mta-chair-open-to-new-fare-structure-ahead-of-2027-fare-hike
  Janno Lieber open to caps beyond the weekly one, "different ways to use the
  technology," wants an "equity oriented fare structure." PCAC's Lisa Daglian: "A
  30-day fare cap is something we've heard a lot about." Also: LIRR family fare usage
  +38% after the age limit went 12→17. No distance-based, zone or free-bus proposals in
  the piece — the story is fare capping specifically.

- **Streetsblog NYC, 2026-02-18 — "Advocates to MTA: More Fare Caps Will Be Fairer For
  All."**
  https://nyc.streetsblog.org/2026/02/18/advocates-to-mta-more-fare-caps-will-be-fairer-for-all
  Effective Transit Alliance's Blair Lorenzo: "A cap of 46 rides over 30 days would at
  least restore the status quo," and "The end of the monthly MetroCard actually meant a
  massive fare hike for New York's most loyal riders." PCAC's Brian Fritsch on more
  caps. Cites monthly caps raising ridership ~4% on US systems, and LA / London /
  Milwaukee offering multiple cap tiers. **Contains no quantification of how many riders
  would benefit — that absence is the gap this project fills.**

## Prior art — what is already published

The *argument* that losing the monthly pass was a stealth fare hike is on the record
from ETA and PCAC, and Streetsblog has covered it at least twice (Feb and Sept 2026).
What has not been published: the share of ridership that was on monthly passes, where
those riders board, and the per-month cost of the change. No denominator anywhere in
the coverage.

## Not checked

- MTA board minutes / archived fare schedules for the retired passes' prices. **Open
  item 3.**
- Any MTA methodology note on the `wujg-7c2s` → `5wq4-mkjj` seam at 2025-01-01.
- The MTA data dictionaries for both ridership datasets (needed for the "Other"
  buckets).

## Added 2026-09-28 (first networked session)

### Primary documents (saved in `research/docs/`)

- **MTA, "New Fare Information Effective August 20, 2023."**
  https://www.mta.info/document/118601 — 30-Day $132.00 / $66.00 reduced; 7-Day
  $34.00 / $17.00; OMNY rolling 7-day cap $34, $17 reduced. **Sources the retired pass
  price (open item 3).**
- **MTA, 2026 Fare Change Materials (Board staff summary, approved 2025-09-30, with
  Attachment A fare tables and Title VI analysis).** https://www.mta.info/document/186881
  — effective "on or about January 4, 2026"; "No longer sell MetroCard fare media";
  the rolling 7-day cap made permanent (it was a pilot); Title VI: discontinuing the
  30-Day Unlimited has "no disparate impact or disproportionate burden"; average fare
  paid minority $2.26 / non-minority $2.56, low-income $2.30 / high-income $2.42.
- **MTA Subway Hourly Ridership Overview + Data Dictionary** (attachments to
  `5wq4-mkjj` and `wujg-7c2s`; the two overview PDFs are identical) — fare codes in
  each fare-class category; release notes 2024-10-15 and 2025-10-07.

### Datasets added (all data.ny.gov, columns confirmed from metadata and records)

| Dataset | Id | Used for |
|---|---|---|
| MTA Bus Hourly Ridership: 2020-2024 / Beginning 2025 | `kv7t-n8in` / `gxb3-akrn` | Q3 bus revenue, Q4 |
| MTA NYCT Subway Fare Evasion: Beginning 2018 | `6kj3-ijvb` | Q4 |
| MTA Bus Fare Evasion: Beginning 2019 | `uv5h-dfhp` | Q4 |
| MTA Statement of Operations: Beginning 2019 | `yg77-3tkj` | Q3 outside check |
| MTA Daily Ridership and Traffic: Beginning 2020 | `sayj-mze2` | paratransit trips; Q4 official-ridership check |
| MTA Subway O-D Ridership Estimate: 2024 / Beginning 2026 | `jsu2-fbtj` / `28vm-gjqr` | Q5 distance fare |

### Secondary

- **NY1, 2025-09-11, "Fare evasion cost MTA $1 billion in 2024, report says"**
  (reporting Citizens Budget Commission, "No Fare: The Costs of MTA Fare and Toll
  Evasion", Sept 2025). https://ny1.com/nyc/all-boroughs/news/2025/09/11/fare-evasion-cost-mta--1-billion-in-2024-report-says
  — $350M subway, $568M bus in 2024; 330 / 710 evaded fares per minute. CBC PDF
  (cbcny.org) blocks automated download; method not verified.
- **NY1, 2026-01-02** — fare increase takes effect Sunday Jan 4, 2026 (search result;
  consistent with doc 186881).

### Not checked

- Policy source for student OMNY card weekend / summer validity.
- What launched or was reclassified into "OMNY – Other" on 2026-07-31.

## Added 2026-09-29 (spec_fare_proposals.md)

- **NYS Thruway Authority, "Terms and Conditions of the Pre-Paid Commercial Charge
  Account Program"** (form TA-W68167A, rev. 02/2011).
  https://thruway.ny.gov/sites/default/files/2025-07/ta-w68167a.pdf — prepaid amount
  "sufficient to pay Account charges for a 30-day period"; replenished "when your
  Account balance decreases to or below the replenishment point specific to this
  plan"; "No interest will be paid on balances in your Account." Note: the
  COMMERCIAL program's terms; the consumer $25 minimum comes from the E-ZPass NY
  site via search, not this document. Saved in `research/docs/`.
- **FRED, DGS3MO** (3-month Treasury constant maturity), 4.24% on 2026-09-25.
  https://fred.stlouisfed.org/series/DGS3MO — interest rate for the float.
- **NYC Open Data, CDBG Eligibility by Census Tract** (`qmcw-ur37`, updated
  2025-07-11) — HUD Low and Moderate Income Summary Data, ACS 2016–2020, 2,327
  tracts. Income test for proposal 1.
- **Census 2023 Gazetteer, tracts, New York** —
  https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_gaz_tracts_36.txt
  (tract internal points). The ACS API itself now requires a key
  (redirects to missing_key.html); not used.
- **Gothamist, "The MTA Is Making Millions Off Your Old MetroCards"** (Rebecca
  Fishbein, 2014-01-17).
  https://gothamist.com/news/the-mta-is-making-millions-off-your-old-metrocards —
  says, citing the New York Times, that the MTA made "$200 million in the decade
  leading up to 2010" from unused MetroCard cash (~$20M/yr). Secondary; the NYT
  original has not been checked. Read 2026-09-29.
- **Gizmodo / I Quant NY, "Why You Should Put $19.05 on Your MetroCard to Outsmart
  the MTA"** (Ben Wellington, 2014-09-10).
  https://gizmodo.com/why-you-should-put-19-05-on-your-metrocard-to-outsmart-1632531219
  — shows vending-machine amounts left odd remainders on cards. **Contains no dollar
  total for unused balances.**
- **Correction (2026-09-29):** an earlier version of this file and of
  `research/fare-proposals.md`, `article/alternative_fare.md` and
  `farecap/proposals.py` cited "MetroCard breakage ~$40–95M a year, 2011–2015
  (Gothamist; iQuantNY)". That range came from a web-search summary, not from
  either article; neither contains it. The "$95M in 2011" and "$50M a year" figures
  remain unsourced and are not used.

Not found: any MTA publication of unique riders, trips per rider, or the share of
riders reaching the weekly cap. The rider-frequency distribution in
`farecap.proposals` is synthetic for that reason.
- **Howard Husock, "Inside the secret fees grabbing millions a month from our MTA
  fares"**, *New York Post*, 2025-04-02 (reprinted at aei.org). MTA cites NDAs and
  won't disclose card fees; CFO Jai Patel, Feb 2025 board meeting: $1M card fees
  within $11M first-month congestion-pricing operating costs. Its "$400M a year"
  extrapolation is NOT used. The CFO remark itself should be checked against the
  February 2025 board video/minutes before publishing.
  https://www.aei.org/op-eds/inside-the-secret-fees-grabbing-millions-a-month-from-our-mta-fares/
- **OMNY FAQ, "Detailed information on OMNY"** — https://omny.info/faq/detailed-information-on-omny
  — "each tap will be charged as a full fare" refers to multiple riders on one card;
  it says nothing about per-tap vs batched settlement. How OMNY settles is not public.
- **Federal Reserve Regulation II** debit interchange cap (21¢ + 0.05% + 1¢ fraud
  adjustment, large issuers) — from general knowledge; not fetched this session.
  Check the current cap and the status of the Fed's 2023 proposed reduction.
