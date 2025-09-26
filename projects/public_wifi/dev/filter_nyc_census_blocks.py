#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
from pathlib import Path

def filter_nyc_census_blocks(input_shapefile="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp",
                           output_shapefile="nyc_census_blocks",
                           output_format="shapefile"):
    """
    Filter NY census blocks to only NYC (5 boroughs).

    Args:
        input_shapefile (str): Path to input NY census blocks shapefile
        output_shapefile (str): Output filename (without extension)
        output_format (str): Output format - "shapefile", "geojson", or "gpkg"
    """

    # NYC County FIPS codes (5 boroughs)
    NYC_COUNTIES = {
        '061': 'Manhattan (New York County)',
        '047': 'Brooklyn (Kings County)',
        '081': 'Queens (Queens County)',
        '005': 'Bronx (Bronx County)',
        '085': 'Staten Island (Richmond County)'
    }

    print("Loading New York census blocks shapefile...")
    print(f"Input: {input_shapefile}")

    # Load the full NY state census blocks
    gdf = gpd.read_file(input_shapefile)

    print(f"Total NY census blocks loaded: {len(gdf):,}")
    print(f"Coordinate system: {gdf.crs}")

    # Show all counties in the dataset
    all_counties = gdf['COUNTYFP20'].value_counts().sort_index()
    print(f"\nAll counties in dataset:")
    for county_fips, count in all_counties.items():
        borough_name = NYC_COUNTIES.get(county_fips, "Other NY County")
        marker = "🗽" if county_fips in NYC_COUNTIES else "  "
        print(f"{marker} {county_fips}: {count:,} blocks - {borough_name}")

    # Filter to NYC counties only
    print(f"\nFiltering to NYC counties: {list(NYC_COUNTIES.keys())}")
    nyc_gdf = gdf[gdf['COUNTYFP20'].isin(NYC_COUNTIES.keys())].copy()

    print(f"NYC census blocks after filtering: {len(nyc_gdf):,}")

    # Show breakdown by borough
    print("\nNYC breakdown by borough:")
    for county_fips in sorted(NYC_COUNTIES.keys()):
        count = len(nyc_gdf[nyc_gdf['COUNTYFP20'] == county_fips])
        print(f"  {county_fips}: {count:,} blocks - {NYC_COUNTIES[county_fips]}")

    # Determine output file extension and save
    if output_format.lower() == "geojson":
        output_file = f"{output_shapefile}.geojson"
        nyc_gdf.to_file(output_file, driver='GeoJSON')
    elif output_format.lower() == "gpkg":
        output_file = f"{output_shapefile}.gpkg"
        nyc_gdf.to_file(output_file, driver='GPKG')
    else:  # default to shapefile
        output_file = f"{output_shapefile}.shp"
        nyc_gdf.to_file(output_file)

    print(f"\nNYC census blocks saved to: {output_file}")

    # Calculate some statistics
    total_area_sq_meters = nyc_gdf.geometry.area.sum()
    total_area_sq_km = total_area_sq_meters / 1_000_000

    print(f"\nNYC Statistics:")
    print(f"Total census blocks: {len(nyc_gdf):,}")
    print(f"Total area: {total_area_sq_km:.2f} km²")
    print(f"Average block area: {total_area_sq_meters/len(nyc_gdf):.0f} m²")

    return nyc_gdf

def analyze_nyc_blocks(input_shapefile="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp"):
    """
    Analyze NYC census blocks without saving filtered data.
    """

    NYC_COUNTIES = {
        '061': 'Manhattan (New York County)',
        '047': 'Brooklyn (Kings County)',
        '081': 'Queens (Queens County)',
        '005': 'Bronx (Bronx County)',
        '085': 'Staten Island (Richmond County)'
    }

    print("Loading and analyzing NYC census blocks...")
    gdf = gpd.read_file(input_shapefile)
    nyc_gdf = gdf[gdf['COUNTYFP20'].isin(NYC_COUNTIES.keys())]

    print(f"\nNYC Census Blocks Analysis:")
    print(f"Total blocks: {len(nyc_gdf):,}")

    # Borough breakdown
    for county_fips in sorted(NYC_COUNTIES.keys()):
        borough_blocks = nyc_gdf[nyc_gdf['COUNTYFP20'] == county_fips]
        count = len(borough_blocks)
        area_km2 = borough_blocks.geometry.area.sum() / 1_000_000
        print(f"  {NYC_COUNTIES[county_fips]}: {count:,} blocks ({area_km2:.1f} km²)")

    # Show sample of the data
    print(f"\nSample of NYC census block data:")
    print(nyc_gdf[['GEOID20', 'COUNTYFP20', 'TRACTCE20', 'BLOCKCE20']].head(10))

    return nyc_gdf

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Filter NY census blocks to NYC only")
    parser.add_argument("--input", default="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp",
                       help="Input shapefile path")
    parser.add_argument("--output", default="nyc_census_blocks",
                       help="Output filename (without extension)")
    parser.add_argument("--format", choices=["shapefile", "geojson", "gpkg"],
                       default="shapefile", help="Output format")
    parser.add_argument("--analyze-only", action="store_true",
                       help="Only analyze data, don't save filtered output")

    args = parser.parse_args()

    if args.analyze_only:
        analyze_nyc_blocks(args.input)
    else:
        filter_nyc_census_blocks(args.input, args.output, args.format)