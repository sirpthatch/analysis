**Objective**

This is a project directory to reconstruct the New York City public crash
dataset (`h9gi-nx95`) for the period it stopped covering. The public file froze
on 2026-06-11 while NYPD continued recording crashes; NYPD's TrafficStat
dashboard has continued to publish them. The project should a) establish the
extent and nature of the freeze, b) extract record-level collision data from
TrafficStat's undocumented API, c) recover the fields TrafficStat omits —
principally street name and ZIP — by spatial join against the city street
centerline, d) validate the reconstruction against the months both sources
cover, e) document precisely which Socrata fields can and cannot be recovered,
and f) produce a writeup for an NYC urban-development Substack.

**Research Questions**

1. How large is the gap, in crashes and in time, and is it still growing?
2. Does TrafficStat measure the same universe of events as the Socrata file?
   Over the months both cover, do their counts agree, and how closely?
3. Which of Socrata's 29 fields can be reconstructed, by what method, and at
   what measured fidelity? Which are permanently lost?
4. How accurately can street name and ZIP be recovered from coordinates alone,
   measured against records where the true values are known?
5. Does the reconstructed period differ from the pre-freeze period in ways that
   would have been visible — a trend, a spike, a shift in where crashes happen —
   had the file kept updating?
6. Why did the file stop, and what does the city say about it?

**Research Method**

1. Collect data
2. Profile and analyze it
3. Explore questions
4. Document limitations
5. Repeat 3 & 4 until satisfactory findings are made (with human input)
6. Publish results
