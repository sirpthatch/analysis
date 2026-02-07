# Approach

Data Manipulation
-----------------
The end goal is to get a feature set of run descriptor (age, sex), home (city, state), outcome (time), event descriptors (race, date), and weather conditioners (total rainfall, weekend rainfall, last month rainfall, temperature, etc.).

Along the way rows (and races) will have to be filtered.  I would like to keep a record of this in some way, to examine the results for bias.  As a goal, each operation that results in a filtering should output metadata on what is filtered (number of rows, reason, from what file/race/record).

Steps
-----
1) Iterate through race files, determine for each how to extract (age, sex, time, race, date, city, state)
-- Done: Stored in race_final/data.csv
2) Determine mapping of city, state -> OrderedList(weather stations) to use for weather data
-- Done: Used open meteo data, which allowed lookup for weather on city, state and simplified process
3) Featurize weather data to (weather station) -> List(weather conditioners)
-- Skip, we do not have weather station level data anymore
4) Join the data together to get starting data frame
-- Skip, need to reframe analysis plan


-- Amend: 1/30
Step 1 the same, use open-meteo to get location->weather time series for step 2
Step 3: Design final data frame schema
-- Done: See final schema below
Step 4: Iterate through race data and weather data to get history
-- Notes: This needs some thought to be most efficient.  Likely have to iterate by race, and then for each race find all of the cities that racers come from, and then for each city do the weather lookup.  To do that, we will likely want to load the weather data in memory, indexed by city and then date (time series weather data)


Final Data Frame Schema
Race/Runner Metadata: Run Description (age, sex), home (city, state), outcome (time), event descriptors (race, date)
Weather Descriptors (for N=(90, 30)):
    + N day min/max temperatures
    + N day median temperature
    + N day number of day with rain
    + N day total rainfall
    + N day weekends with 1 clear (no/marginal precip) day

Research
--------
Once we have the data manipulated into a form that is able to be researched, the goal is to explore a number of questions related to how, for a given runner, the weather in their "home town" before a race affects their race results.  And then some extra details around that, like "toughest" town, race with toughest runners, difference in age/sex on whether people are affected by weather


Questions:
* Which cities have the greatest concentration of marathon runners?
* Of the major cities - [NYC(+Brooklyn), Boston(+Cambridge), Chicago, Los Angeles, San Francisco, etc.]
    *  Which has the fastest runners
    *  Which is the most resilient to bad weather
* How does the average runner do against weather (temp and precip)
* How do top runners (10%, AG graded) do against weather?
* How does weather hardiness change with age/sex?