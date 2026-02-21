#!/usr/bin/env python3
"""
Analyze which race cities are missing from weather_data_v2.csv
and prioritize them by number of race results
"""

import pandas as pd

print("Loading data files...")
print("="*80)

# Load race locations
race_locations = pd.read_csv('race_locations_normalized.csv')
print(f"Race locations: {len(race_locations)} races")

# Load existing weather data to see which cities we already have
weather_data = pd.read_csv('data/weather_data_v2.csv')
weather_cities = weather_data[['city', 'state']].drop_duplicates()
print(f"Weather data: {len(weather_cities)} cities with weather")

# Load enriched race data (with race day weather)
race_data = pd.read_csv('data/featurized_race_data_v2_with_raceday_weather.csv')
print(f"Race data: {len(race_data):,} total race results")

print("\n" + "="*80)
print("Analysis:")
print("="*80)

# Get race cities from race_locations (actual race locations)
race_cities = race_locations[['city', 'state']].dropna().drop_duplicates()
print(f"\nUnique race cities: {len(race_cities)}")

# Find which race cities are missing from weather data
merged = race_cities.merge(
    weather_cities,
    on=['city', 'state'],
    how='left',
    indicator=True
)

missing_cities = merged[merged['_merge'] == 'left_only'][['city', 'state']]
print(f"Race cities missing weather: {len(missing_cities)}")

# Count race results per city
race_with_location = race_data.merge(
    race_locations,
    on='race',
    how='left',
    suffixes=('_runner', '_race')
)

city_counts = race_with_location.groupby(['city_race', 'state_race']).size().reset_index(name='result_count')
city_counts.rename(columns={'city_race': 'city', 'state_race': 'state'}, inplace=True)

# Add result counts to missing cities
missing_with_counts = missing_cities.merge(
    city_counts,
    on=['city', 'state'],
    how='left'
)

# Fill NaN with 0 and sort by result count
missing_with_counts['result_count'] = missing_with_counts['result_count'].fillna(0).astype(int)
missing_with_counts = missing_with_counts.sort_values('result_count', ascending=False)

print("\n" + "="*80)
print("Top 50 missing cities by number of race results:")
print("="*80)
print(missing_with_counts.head(50).to_string(index=False))

# Calculate coverage
total_results = len(race_data)
results_with_weather = len(race_data[race_data['race_day_temp_min'].notna()])

print("\n" + "="*80)
print("Coverage Statistics:")
print("="*80)
print(f"Total race results: {total_results:,}")
print(f"Results with race day weather: {results_with_weather:,} ({results_with_weather/total_results*100:.1f}%)")
print(f"Results missing weather: {total_results - results_with_weather:,} ({(total_results - results_with_weather)/total_results*100:.1f}%)")

# Calculate potential coverage if we add top N cities
cumulative_results = missing_with_counts['result_count'].cumsum()
total_missing = missing_with_counts['result_count'].sum()

print("\n" + "="*80)
print("Incremental Coverage Improvement:")
print("="*80)
for n in [10, 25, 50, 100, 200]:
    if n <= len(missing_with_counts):
        new_results = cumulative_results.iloc[n-1]
        new_coverage = (results_with_weather + new_results) / total_results * 100
        improvement = new_coverage - (results_with_weather/total_results*100)
        print(f"Add top {n:3d} cities: {new_coverage:.2f}% coverage (+{improvement:.2f}%)")

# Save missing cities to CSV for mapping
output_file = 'data/missing_race_cities.csv'
missing_with_counts.to_csv(output_file, index=False)
print(f"\n" + "="*80)
print(f"Saved missing cities to: {output_file}")
print("="*80)
