#!/usr/bin/env python3

import pandas as pd
import numpy as np
import re
from pathlib import Path

def parse_median_income_csv(input_file="data/median-income-2023.csv",
                           output_file="data/census_tract_median_income.csv"):
    """
    Parse the median income CSV file to extract census tract FIPS codes and median income.

    The input file format has:
    - GEO_ID: Contains full geography ID like "1400000US36005015300"
    - NAME: Human readable name like "Census Tract 153; Bronx County; New York"
    - B19013_001E: Median household income estimate
    - B19013_001M: Margin of error

    We extract the 11-digit FIPS code from GEO_ID and clean the income data.
    """

    print("Loading median income data...")

    try:
        # Read the CSV file, skipping the descriptive second row
        df = pd.read_csv(input_file, encoding='utf-8-sig')  # Handle BOM if present
        print(f"✓ Loaded {len(df):,} records")

        # Clean column names (remove extra spaces/characters)
        df.columns = df.columns.str.strip()

        # Skip the header row if it contains descriptions
        if df.iloc[0]['GEO_ID'] == 'Geography':
            df = df.iloc[1:].reset_index(drop=True)
            print("✓ Skipped header description row")

        print(f"Data columns: {list(df.columns)}")
        print(f"Processing {len(df):,} census tract records...")

        # Extract FIPS code from GEO_ID
        # Format: "1400000US36005015300" -> extract the last 11 digits (state+county+tract)
        def extract_fips_code(geo_id):
            if pd.isna(geo_id) or geo_id == '':
                return None

            # Remove "1400000US" prefix and extract 11-digit FIPS
            match = re.search(r'1400000US(\d{11})', str(geo_id))
            if match:
                return match.group(1)
            else:
                print(f"Warning: Could not extract FIPS from {geo_id}")
                return None

        df['TRACT_FIPS'] = df['GEO_ID'].apply(extract_fips_code)

        # Clean median income data
        def clean_income(value):
            if pd.isna(value) or value == '' or value == '-' or value == '**':
                return None
            try:
                # Remove any non-numeric characters except decimal point
                cleaned = re.sub(r'[^\d.]', '', str(value))
                if cleaned == '':
                    return None
                return float(cleaned)
            except (ValueError, TypeError):
                return None

        df['MEDIAN_INCOME'] = df['B19013_001E'].apply(clean_income)
        df['INCOME_MARGIN_ERROR'] = df['B19013_001M'].apply(clean_income)

        # Extract borough/county information from NAME
        def extract_county_info(name):
            if pd.isna(name):
                return 'Unknown', 'Unknown'

            # Extract tract and county from format like "Census Tract 153; Bronx County; New York"
            parts = str(name).split(';')

            if len(parts) >= 2:
                tract_part = parts[0].strip()
                county_part = parts[1].strip()

                # Extract tract number
                tract_match = re.search(r'Census Tract ([\d.]+)', tract_part)
                tract_num = tract_match.group(1) if tract_match else 'Unknown'

                # Extract county name
                county_match = re.search(r'(.+) County', county_part)
                county_name = county_match.group(1) if county_match else county_part

                return tract_num, county_name

            return 'Unknown', 'Unknown'

        df[['TRACT_NUMBER', 'COUNTY_NAME']] = df['NAME'].apply(
            lambda x: pd.Series(extract_county_info(x))
        )

        # Map county names to boroughs for NYC
        COUNTY_TO_BOROUGH = {
            'New York': 'MANHATTAN',      # New York County = Manhattan
            'Kings': 'BROOKLYN',          # Kings County = Brooklyn
            'Queens': 'QUEENS',           # Queens County = Queens
            'Bronx': 'BRONX',             # Bronx County = Bronx
            'Richmond': 'STATEN ISLAND'   # Richmond County = Staten Island
        }

        df['BOROUGH'] = df['COUNTY_NAME'].map(COUNTY_TO_BOROUGH).fillna('OTHER')

        # Filter out records without valid FIPS codes or income data
        valid_records = df[
            df['TRACT_FIPS'].notna() &
            df['MEDIAN_INCOME'].notna()
        ].copy()

        print(f"✓ {len(valid_records):,} records with valid FIPS codes and income data")

        # Create final output dataframe
        output_df = valid_records[[
            'TRACT_FIPS',
            'MEDIAN_INCOME',
            'INCOME_MARGIN_ERROR',
            'TRACT_NUMBER',
            'COUNTY_NAME',
            'BOROUGH',
            'NAME'
        ]].copy()

        # Sort by FIPS code
        output_df = output_df.sort_values('TRACT_FIPS').reset_index(drop=True)

        # Data validation and summary
        print(f"\n📊 PARSED DATA SUMMARY:")
        print(f"{'─' * 40}")
        print(f"Total valid records: {len(output_df):,}")
        print(f"NYC boroughs found: {output_df[output_df['BOROUGH'] != 'OTHER']['BOROUGH'].nunique()}")
        print(f"Income range: ${output_df['MEDIAN_INCOME'].min():,.0f} - ${output_df['MEDIAN_INCOME'].max():,.0f}")
        print(f"Median income: ${output_df['MEDIAN_INCOME'].median():,.0f}")

        # Borough breakdown
        print(f"\nRecords by borough:")
        borough_counts = output_df['BOROUGH'].value_counts()
        for borough, count in borough_counts.items():
            pct = (count / len(output_df)) * 100
            if count > 0:
                avg_income = output_df[output_df['BOROUGH'] == borough]['MEDIAN_INCOME'].median()
                print(f"• {borough}: {count:,} tracts ({pct:.1f}%) - Median: ${avg_income:,.0f}")

        # Save to CSV
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output_df.to_csv(output_path, index=False)
        print(f"\n✅ Parsed data saved to: {output_path}")

        # Sample output
        print(f"\nSample records:")
        print(output_df.head(10)[['TRACT_FIPS', 'BOROUGH', 'MEDIAN_INCOME', 'TRACT_NUMBER']])

        return output_df

    except Exception as e:
        print(f"✗ Error parsing median income data: {e}")
        return None

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse median income CSV to extract FIPS codes and income data")
    parser.add_argument("--input", default="data/median-income-2023.csv",
                       help="Input median income CSV file")
    parser.add_argument("--output", default="data/census_tract_median_income.csv",
                       help="Output CSV file with FIPS codes and income")
    parser.add_argument("--preview", action="store_true",
                       help="Show preview of first few records without saving")

    args = parser.parse_args()

    # Run the parser
    result_df = parse_median_income_csv(args.input, args.output)

    if result_df is not None and args.preview:
        print(f"\n📋 PREVIEW MODE - First 20 records:")
        print(result_df.head(20)[['TRACT_FIPS', 'BOROUGH', 'MEDIAN_INCOME', 'COUNTY_NAME']])

    print(f"\n✅ Parsing complete!")