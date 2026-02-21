#!/usr/bin/env python3
"""
Script to normalize race names in race_locations.csv to match the format
used in data/featurized_race_data_v2.csv
"""

import pandas as pd
import re

def normalize_race_name(race_name):
    """
    Normalize race name to match featurized data format:
    - Convert to lowercase
    - Replace spaces with underscores

    Args:
        race_name: Original race name

    Returns:
        Normalized race name
    """
    if pd.isna(race_name) or race_name == '':
        return race_name

    # Convert to lowercase
    normalized = race_name.lower()

    # Replace spaces with underscores
    normalized = normalized.replace(' ', '_')

    return normalized


def main():
    """Main function to normalize race locations"""

    print("Reading race_locations.csv...")
    df = pd.read_csv('race_locations.csv')

    print(f"Found {len(df)} races")

    # Show some examples before normalization
    print("\nExamples before normalization:")
    print(df.head(10)[['Race', 'City', 'State']].to_string(index=False))

    # Normalize race names
    print("\nNormalizing race names...")
    df['race'] = df['Race'].apply(normalize_race_name)

    # Also normalize city and state to lowercase for consistency
    df['city'] = df['City'].apply(lambda x: x.lower() if pd.notna(x) and x != '' else x)
    df['state'] = df['State'].apply(lambda x: x.lower() if pd.notna(x) and x != '' else x)

    # Create output dataframe with normalized columns
    output_df = df[['race', 'city', 'state']].copy()

    # Show examples after normalization
    print("\nExamples after normalization:")
    print(output_df.head(10).to_string(index=False))

    # Save to new file
    output_file = 'race_locations_normalized.csv'
    output_df.to_csv(output_file, index=False)

    print(f"\n{'='*80}")
    print(f"Normalization complete!")
    print(f"{'='*80}")
    print(f"Output saved to: {output_file}")
    print(f"Total races: {len(output_df)}")
    print(f"Races with city and state: {((output_df['city'] != '') & (output_df['state'] != '')).sum()}")

    # Test join with featurized data
    print(f"\n{'='*80}")
    print("Testing join with featurized data...")
    print(f"{'='*80}")

    featurized_df = pd.read_csv('data/featurized_race_data_v2.csv')

    print(f"Featurized data has {len(featurized_df):,} records")
    print(f"Unique races in featurized data: {featurized_df['race'].nunique()}")
    print(f"Unique races in race_locations: {output_df['race'].nunique()}")

    # Check how many races match
    featurized_races = set(featurized_df['race'].unique())
    location_races = set(output_df['race'].unique())

    matches = featurized_races & location_races
    only_in_featurized = featurized_races - location_races
    only_in_locations = location_races - featurized_races

    print(f"\nMatching races: {len(matches)}")
    print(f"Races only in featurized data: {len(only_in_featurized)}")
    print(f"Races only in race_locations: {len(only_in_locations)}")

    if len(only_in_featurized) > 0:
        print(f"\nFirst 10 races only in featurized data:")
        for race in sorted(only_in_featurized)[:10]:
            print(f"  - {race}")

    if len(only_in_locations) > 0:
        print(f"\nFirst 10 races only in race_locations:")
        for race in sorted(only_in_locations)[:10]:
            print(f"  - {race}")

    # Test the join
    print(f"\n{'='*80}")
    print("Testing actual join...")
    print(f"{'='*80}")

    # Sample join
    sample_featurized = featurized_df.head(1000)
    joined = sample_featurized.merge(output_df, on='race', how='left', suffixes=('', '_locations'))

    print(f"Sample of 1000 records from featurized data:")
    print(f"  - Records with matching location: {joined['city'].notna().sum()}")
    print(f"  - Records without matching location: {joined['city'].isna().sum()}")

    print("\nSample of joined data:")
    print(joined[['race', 'city', 'state', 'date']].head(10).to_string(index=False))


if __name__ == '__main__':
    main()
