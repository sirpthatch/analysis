#!/usr/bin/env python3
"""
Script to enrich featurized race data with race day weather conditions.

Joins:
1. featurized_race_data_v2.csv with race_locations_normalized.csv to get actual race location
2. Result with weather_data_v2.csv to get race day weather (temp_min, temp_max, precip)

Output: featurized_race_data_v2_with_raceday_weather.csv with 5 new columns
"""

import pandas as pd
import sys

def load_data():
    """Load all required data files"""
    print("Loading data files...")
    print("="*80)

    # Load featurized race data
    print("1. Loading featurized_race_data_v2.csv...", end='', flush=True)
    featurized_df = pd.read_csv('data/featurized_race_data_v2.csv')
    print(f" ✓ {len(featurized_df):,} records")

    # Load race locations
    print("2. Loading race_locations_normalized.csv...", end='', flush=True)
    race_locations_df = pd.read_csv('race_locations_normalized.csv')
    print(f" ✓ {len(race_locations_df):,} races")

    # Load weather data
    print("3. Loading weather_data_v2.csv...", end='', flush=True)
    weather_df = pd.read_csv('data/weather_data_v2.csv')
    print(f" ✓ {len(weather_df):,} daily records")

    return featurized_df, race_locations_df, weather_df


def enrich_with_race_locations(featurized_df, race_locations_df):
    """Join featurized data with race locations"""
    print("\n" + "="*80)
    print("Step 1: Joining with race locations...")
    print("="*80)

    initial_count = len(featurized_df)

    # Join on 'race' column to get actual race location
    enriched = featurized_df.merge(
        race_locations_df,
        on='race',
        how='left',
        suffixes=('_runner', '_race')
    )

    # Rename columns for clarity
    enriched.rename(columns={
        'city_race': 'race_location_city',
        'state_race': 'race_location_state',
        'city_runner': 'city',  # Keep original as 'city' (runner hometown)
        'state_runner': 'state'  # Keep original as 'state' (runner home state)
    }, inplace=True)

    # Check join success
    matched = enriched['race_location_city'].notna()
    matched_count = matched.sum()

    print(f"Records with race location: {matched_count:,} ({matched_count/initial_count*100:.1f}%)")
    print(f"Records without race location: {(~matched).sum():,} ({(~matched).sum()/initial_count*100:.1f}%)")

    # Show some races without locations
    if (~matched).sum() > 0:
        unmapped_races = enriched[~matched]['race'].unique()
        print(f"\nUnique races without location: {len(unmapped_races)}")
        if len(unmapped_races) <= 10:
            print("Unmapped races:")
            for race in unmapped_races:
                print(f"  - {race}")
        else:
            print(f"First 10 unmapped races:")
            for race in unmapped_races[:10]:
                print(f"  - {race}")

    return enriched


def enrich_with_weather(enriched_df, weather_df):
    """Join with weather data to get race day conditions"""
    print("\n" + "="*80)
    print("Step 2: Joining with race day weather...")
    print("="*80)

    initial_count = len(enriched_df)

    # Join on race location (city, state) and date
    result = enriched_df.merge(
        weather_df,
        left_on=['race_location_city', 'race_location_state', 'date'],
        right_on=['city', 'state', 'date'],
        how='left',
        suffixes=('', '_weather_dup')
    )

    # Rename weather columns to be explicit
    result.rename(columns={
        'temp_min': 'race_day_temp_min',
        'temp_max': 'race_day_temp_max',
        'precip': 'race_day_precip'
    }, inplace=True)

    # Drop duplicate city/state columns from weather join
    cols_to_drop = [col for col in result.columns if col.endswith('_weather_dup')]
    if cols_to_drop:
        result.drop(columns=cols_to_drop, inplace=True)

    # Check join success
    has_weather = result['race_day_temp_min'].notna()
    weather_count = has_weather.sum()

    print(f"Records with race day weather: {weather_count:,} ({weather_count/initial_count*100:.1f}%)")
    print(f"Records without race day weather: {(~has_weather).sum():,} ({(~has_weather).sum()/initial_count*100:.1f}%)")

    # Analyze missing weather
    if (~has_weather).sum() > 0:
        missing_weather = result[~has_weather]

        # Check reasons for missing weather
        no_location = missing_weather['race_location_city'].isna()
        has_location_no_weather = missing_weather['race_location_city'].notna()

        print(f"\nBreakdown of missing weather:")
        print(f"  - No race location: {no_location.sum():,}")
        print(f"  - Has location but no weather: {has_location_no_weather.sum():,}")

        if has_location_no_weather.sum() > 0:
            # Show some examples
            examples = missing_weather[has_location_no_weather][['race', 'date', 'race_location_city', 'race_location_state']].drop_duplicates().head(10)
            print(f"\nSample races with location but no weather (likely international):")
            for _, row in examples.iterrows():
                print(f"  - {row['race']} on {row['date']} in {row['race_location_city']}, {row['race_location_state']}")

    return result


def validate_output(result_df, original_count):
    """Validate the enriched data"""
    print("\n" + "="*80)
    print("Data Validation:")
    print("="*80)

    # Check record count
    if len(result_df) != original_count:
        print(f"⚠ WARNING: Record count changed! Expected {original_count:,}, got {len(result_df):,}")
    else:
        print(f"✓ Record count preserved: {len(result_df):,}")

    # Check for new columns
    new_cols = ['race_location_city', 'race_location_state', 'race_day_temp_min',
                'race_day_temp_max', 'race_day_precip']
    for col in new_cols:
        if col in result_df.columns:
            print(f"✓ Column '{col}' added")
        else:
            print(f"✗ Column '{col}' missing!")

    # Check value ranges
    print("\nWeather value ranges:")
    if 'race_day_temp_min' in result_df.columns:
        temp_min = result_df['race_day_temp_min'].dropna()
        if len(temp_min) > 0:
            print(f"  Temperature min: {temp_min.min():.1f}°F to {temp_min.max():.1f}°F")
            if temp_min.min() < -50 or temp_min.max() > 120:
                print(f"    ⚠ WARNING: Unusual temperature range!")

    if 'race_day_temp_max' in result_df.columns:
        temp_max = result_df['race_day_temp_max'].dropna()
        if len(temp_max) > 0:
            print(f"  Temperature max: {temp_max.min():.1f}°F to {temp_max.max():.1f}°F")
            if temp_max.min() < -50 or temp_max.max() > 120:
                print(f"    ⚠ WARNING: Unusual temperature range!")

    if 'race_day_precip' in result_df.columns:
        precip = result_df['race_day_precip'].dropna()
        if len(precip) > 0:
            print(f"  Precipitation: {precip.min():.1f} to {precip.max():.1f} inches")
            if precip.min() < 0 or precip.max() > 20:
                print(f"    ⚠ WARNING: Unusual precipitation range!")


def show_sample(result_df):
    """Show sample of enriched data"""
    print("\n" + "="*80)
    print("Sample of Enriched Data:")
    print("="*80)

    # Try to find Boston Marathon as a sample
    boston = result_df[result_df['race'].str.contains('boston', na=False, case=False)]

    if len(boston) > 0:
        print("\nBoston Marathon sample (first 5 records):")
        cols_to_show = ['race', 'date', 'race_location_city', 'race_location_state',
                       'race_day_temp_min', 'race_day_temp_max', 'race_day_precip',
                       'city', 'state', 'age', 'sex', 'time']
        print(boston[cols_to_show].head().to_string(index=False))
    else:
        print("\nFirst 5 records:")
        cols_to_show = ['race', 'date', 'race_location_city', 'race_location_state',
                       'race_day_temp_min', 'race_day_temp_max', 'race_day_precip']
        print(result_df[cols_to_show].head().to_string(index=False))


def main():
    """Main enrichment workflow"""
    print("\n" + "="*80)
    print("Race Day Weather Enrichment")
    print("="*80 + "\n")

    # Load data
    featurized_df, race_locations_df, weather_df = load_data()
    original_count = len(featurized_df)

    # Step 1: Join with race locations
    enriched = enrich_with_race_locations(featurized_df, race_locations_df)

    # Step 2: Join with weather data
    result = enrich_with_weather(enriched, weather_df)

    # Validation
    validate_output(result, original_count)

    # Show sample
    show_sample(result)

    # Save output
    print("\n" + "="*80)
    print("Saving enriched data...")
    print("="*80)

    output_file = 'data/featurized_race_data_v2_with_raceday_weather.csv'
    result.to_csv(output_file, index=False)

    print(f"✓ Saved to: {output_file}")

    # Final statistics
    print("\n" + "="*80)
    print("Summary:")
    print("="*80)
    print(f"Total records: {len(result):,}")
    print(f"Records with complete enrichment: {result['race_day_temp_min'].notna().sum():,} ({result['race_day_temp_min'].notna().sum()/len(result)*100:.1f}%)")
    print(f"New columns added: 5")
    print(f"  - race_location_city")
    print(f"  - race_location_state")
    print(f"  - race_day_temp_min")
    print(f"  - race_day_temp_max")
    print(f"  - race_day_precip")

    print("\n" + "="*80)
    print("Enrichment complete!")
    print("="*80)


if __name__ == '__main__':
    main()
