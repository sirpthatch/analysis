#!/usr/bin/env python3
"""
Script to analyze census block shapefile and identify fields for filtering water/non-land blocks
"""

import geopandas as gpd
import pandas as pd
import numpy as np

def analyze_census_blocks():
    # Load the shapefile
    shapefile_path = '/Users/thatcher/dev/analysis/projects/public_parks/res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp'
    print(f"Loading shapefile: {shapefile_path}")

    try:
        gdf = gpd.read_file(shapefile_path)

        print("\n" + "="*50)
        print("SHAPEFILE OVERVIEW")
        print("="*50)
        print(f"Shape: {gdf.shape}")
        print(f"CRS: {gdf.crs}")

        print("\n" + "="*50)
        print("COLUMN NAMES AND DATA TYPES")
        print("="*50)
        for col, dtype in gdf.dtypes.items():
            print(f"{col:<15} {dtype}")

        print("\n" + "="*50)
        print("FIRST 5 ROWS OF DATA")
        print("="*50)
        print(gdf.head())

        # Look for area-related fields (land vs water)
        area_fields = [col for col in gdf.columns if 'area' in col.lower() or 'land' in col.lower() or 'water' in col.lower()]
        print(f"\n" + "="*50)
        print("AREA/LAND/WATER RELATED FIELDS")
        print("="*50)
        if area_fields:
            for field in area_fields:
                print(f"Found field: {field}")
                print(f"  Data type: {gdf[field].dtype}")
                print(f"  Non-null count: {gdf[field].notna().sum()}")
                print(f"  Sample values: {gdf[field].head().tolist()}")
                print()
        else:
            print("No obvious area/land/water fields found by name pattern")

        # Check for common census field patterns
        print("\n" + "="*50)
        print("FIELD ANALYSIS FOR FILTERING")
        print("="*50)

        # Look for ALAND and AWATER fields specifically (common in census data)
        land_water_fields = [col for col in gdf.columns if col.upper().startswith(('ALAND', 'AWATER'))]
        if land_water_fields:
            print("Found land/water area fields:")
            for field in land_water_fields:
                values = gdf[field]
                print(f"\n{field}:")
                print(f"  Min: {values.min()}")
                print(f"  Max: {values.max()}")
                print(f"  Mean: {values.mean():.2f}")
                print(f"  Zero values: {(values == 0).sum()}")
                print(f"  Non-zero values: {(values > 0).sum()}")

        # Show statistics for all numeric fields
        print("\n" + "="*50)
        print("STATISTICS FOR NUMERIC FIELDS")
        print("="*50)
        numeric_cols = gdf.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            print(gdf[numeric_cols].describe())

        # Check for blocks with zero land area (potential water blocks)
        if any(col.upper().startswith('ALAND') for col in gdf.columns):
            aland_col = next(col for col in gdf.columns if col.upper().startswith('ALAND'))
            zero_land_blocks = (gdf[aland_col] == 0).sum()
            total_blocks = len(gdf)
            print(f"\n" + "="*50)
            print("LAND AREA ANALYSIS")
            print("="*50)
            print(f"Total census blocks: {total_blocks}")
            print(f"Blocks with zero land area: {zero_land_blocks}")
            print(f"Percentage with zero land area: {zero_land_blocks/total_blocks*100:.2f}%")

        return gdf

    except Exception as e:
        print(f"Error loading shapefile: {e}")
        return None

if __name__ == "__main__":
    gdf = analyze_census_blocks()