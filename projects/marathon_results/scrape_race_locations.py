#!/usr/bin/env python3
"""
Script to scrape race location information from marathonguide.com
Reads race_mandates.csv and outputs race_locations.csv with Race, City, State
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import time
import sys

def get_race_location(url, race_name):
    """
    Fetch the race location from marathonguide.com

    Args:
        url: The results URL
        race_name: Name of the race for logging

    Returns:
        tuple: (city, state) or (None, None) if not found
    """
    try:
        print(f"  Fetching race data...", end='', flush=True)
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the __NEXT_DATA__ script tag which contains the race data as JSON
        next_data = soup.find('script', {'id': '__NEXT_DATA__'})
        if not next_data or not next_data.string:
            print(" ✗ No data found")
            return None, None

        # Parse the JSON data
        data = json.loads(next_data.string)

        # Extract location from the nested JSON structure
        race_data = data.get('props', {}).get('pageProps', {}).get('raceData', {})

        city = race_data.get('location_city')
        state = race_data.get('location_state')

        if city and state:
            print(f" ✓ {city}, {state}")
            return city, state
        elif city:
            print(f" ⚠ Only found city: {city}")
            return city, None
        elif state:
            print(f" ⚠ Only found state: {state}")
            return None, state
        else:
            print(" ✗ No location data found")
            return None, None

    except requests.exceptions.RequestException as e:
        print(f" ✗ Request error: {e}")
        return None, None
    except json.JSONDecodeError as e:
        print(f" ✗ JSON parse error: {e}")
        return None, None
    except Exception as e:
        print(f" ✗ Unexpected error: {e}")
        return None, None


def main():
    """Main function to process race mandates and scrape locations"""

    # Read the race_mandates.csv file
    print("Reading race_mandates.csv...")
    df = pd.read_csv('race_mandates.csv')

    print(f"Found {len(df)} total race entries")

    # Get unique races (group by race name and take first URL)
    unique_races = df.groupby('Race Name').agg({
        'Results URL': 'first',
        'Date': 'first'
    }).reset_index()

    print(f"Found {len(unique_races)} unique races")
    print(f"\nStarting scrape (this will take a while)...\n")

    # Create a list to store results
    results = []

    # Track progress
    success_count = 0
    fail_count = 0

    # Process each unique race
    for idx, row in unique_races.iterrows():
        race_name = row['Race Name']
        url = row['Results URL']

        # Progress indicator
        print(f"[{idx+1:4d}/{len(unique_races)}] {race_name[:60]:<60}", end=' ')

        city, state = get_race_location(url, race_name)

        results.append({
            'Race': race_name,
            'City': city if city else '',
            'State': state if state else ''
        })

        if city and state:
            success_count += 1
        else:
            fail_count += 1

        # Be polite to the server - wait between requests
        if (idx + 1) % 10 == 0:
            # Print progress update every 10 races
            print(f"\n  Progress: {success_count} successful, {fail_count} failed\n")

        time.sleep(1)  # 1 second between requests

    # Create DataFrame and save to CSV
    results_df = pd.DataFrame(results)
    output_file = 'race_locations.csv'
    results_df.to_csv(output_file, index=False)

    print(f"\n{'='*80}")
    print(f"Scraping complete!")
    print(f"{'='*80}")
    print(f"Results saved to: {output_file}")
    print(f"Total races processed: {len(results)}")
    print(f"Races with complete location data: {((results_df['City'] != '') & (results_df['State'] != '')).sum()}")
    print(f"Races with city only: {((results_df['City'] != '') & (results_df['State'] == '')).sum()}")
    print(f"Races with state only: {((results_df['City'] == '') & (results_df['State'] != '')).sum()}")
    print(f"Races with no location data: {((results_df['City'] == '') & (results_df['State'] == '')).sum()}")
    print(f"{'='*80}")

    # Show a sample of results
    print("\nFirst 20 results:")
    print(results_df.head(20).to_string(index=False))

    print("\nRaces missing location data:")
    missing = results_df[(results_df['City'] == '') & (results_df['State'] == '')]
    if len(missing) > 0:
        print(missing.head(10).to_string(index=False))
    else:
        print("  None!")


if __name__ == '__main__':
    main()
