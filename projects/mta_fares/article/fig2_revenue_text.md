<!-- Text to accompany lib/fig2_revenue_model_vs_actual.png (~220 words).
     Sources: MTA Statement of Operations yg77-3tkj (Actual, "Farebox Revenue",
     NYCT + MTABC + SIR); model in src/farecap/revenue.py; output/q3_monthly_check.csv.
     Figures computed 2026-09-29. -->

**How much does the MTA take in at the farebox?** In 2025, NYC Transit, MTA Bus and
Staten Island Railway together booked **$3.85 billion** in fare revenue, about
**$320 million a month**. This year is running ahead: **$2.64 billion** through
August, 3.7% more than the same months of 2025, or roughly **$330 million a month**.
Those are the MTA's own figures, from its Statement of Operations.

That ledger doesn't split subway from bus, so I rebuilt it from the ridership data.
Every tap in the MTA's hourly subway and bus datasets carries a fare class: full
fare, reduced fare, Fair Fares, student, or unlimited pass. I dropped free transfers
and priced each remaining entry at the fare in force that day ($2.75, then $2.90
from August 2023, then $3.00 from January 2026). Express buses got the express fare,
and passes their price spread over an assumed number of rides. Then I added
Access-A-Ride trips.

The result lands within about 2–3% of the MTA's number in a typical month, and
inside my low-to-high assumption band in 40 of 41 non-December months. (The
Decembers of 2023 and 2024 jump about $48 million above the model; that looks like
year-end accounting, not riders.) By this estimate the **subway brings in about 80%**
of fares, roughly $3.0 billion in 2025. Local buses bring in 18% and express buses 3%.
