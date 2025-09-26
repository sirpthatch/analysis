#!/usr/bin/env python3

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
import numpy as np
import pandas as pd
import sys

def render_census_blocks_map(shapefile_path="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp",
                           output_file="ny_census_blocks_map.png",
                           figsize=(10, 8),
                           dpi=150,
                           county_filter=None,
                           census_tracts=None,
                           choropleth_data=None,
                           value_column=None,
                           fips_column=None,
                           colormap='viridis',
                           legend_title=None):
    """
    Render a map from NY census block shapefiles with optional choropleth coloring.

    Args:
        shapefile_path (str): Path to the shapefile
        output_file (str): Output filename for the map
        figsize (tuple): Figure size in inches
        dpi (int): Resolution for output image
        county_filter (list): List of county FIPS codes to filter (e.g., ['061'] for Manhattan)
        census_tracts (bool): If True, dissolve blocks into census tracts
        choropleth_data (pd.DataFrame): DataFrame with FIPS codes and values for coloring
        value_column (str): Column name in choropleth_data containing values to color by
        fips_column (str): Column name in choropleth_data containing FIPS codes
        colormap (str): Matplotlib colormap name for choropleth (default: 'viridis')
        legend_title (str): Title for the colorbar legend
    """

    print("Loading census block shapefile...")
    gdf = gpd.read_file(shapefile_path)

    print(f"Loaded {len(gdf)} census blocks")
    print(f"Coordinate system: {gdf.crs}")

    print(f"Filtering out water only blocks...")
    gdf = gdf[gdf['ALAND20'] > 100]
    print(f"Filtered to {len(gdf)} census blocks")

    if county_filter:
        print(f"Filtering to counties: {county_filter}")
        gdf = gdf[gdf['COUNTYFP20'].isin(county_filter)]
        print(f"Filtered to {len(gdf)} census blocks")

    fig, ax = plt.subplots(figsize=figsize)

    if census_tracts:
        print("Dissolving blocks into census tracts...")
        gdf = gdf.dissolve(by='TRACTCE20')
        gdf = gdf.reset_index()
        # Create full tract FIPS code (state + county + tract)
        gdf['TRACT_FIPS'] = (gdf['STATEFP20'] + gdf['COUNTYFP20'] + gdf['TRACTCE20']).astype('str')
        print(f"Created {len(gdf)} census tracts")

    # Handle choropleth coloring if data provided
    if choropleth_data is not None and value_column is not None and fips_column is not None:
        print(f"Applying choropleth coloring using {value_column} from choropleth data...")

        # Determine FIPS column to use for joining
        if census_tracts:
            # Use tract-level FIPS
            join_column = 'TRACT_FIPS'
        else:
            # Use block-level FIPS (full GEOID20)
            join_column = 'GEOID20'

        gdf['TRACT_FIPS'] = gdf['TRACT_FIPS'].astype(np.int64)
        
        # Merge choropleth data with shapefile
        gdf_merged = gdf.merge(
            choropleth_data[[fips_column, value_column]],
            left_on=join_column,
            right_on=fips_column,
            how='left'
        )

        # Check merge success
        matched_count = gdf_merged[value_column].notna().sum()
        print(f"Matched {matched_count} of {len(gdf)} geometries with choropleth data")

        # Plot with choropleth coloring
        gdf_merged.plot(ax=ax,
                       column=value_column,
                       cmap=colormap,
                       edgecolor='white',
                       linewidth=0.1,
                       alpha=0.8,
                       legend=True,
                       missing_kwds={'color': 'lightgray', 'alpha': 0.5})

        # Customize colorbar
        if legend_title:
            ax.get_figure().axes[-1].set_ylabel(legend_title, rotation=270, labelpad=20)

    else:
        # Default solid color plot
        gdf.plot(ax=ax,
                 color='lightblue',
                 edgecolor='white',
                 linewidth=0.1,
                 alpha=0.7)

    # Set appropriate title based on data type
    if census_tracts:
        title = 'New York Census Tracts (2024)'
    else:
        title = 'New York Census Blocks (2024)'

    if choropleth_data is not None and legend_title:
        title += f' - {legend_title}'

    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    plt.tight_layout()

    print(f"Saving map to {output_file}...")
    plt.savefig(output_file, dpi=dpi, bbox_inches='tight')
    #plt.show()

    print("Map rendering complete!")

def render_county_map(shapefile_path="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp",
                     output_file="ny_counties_map.png"):
    """
    Render a map showing counties with different colors.
    """

    print("Loading census block shapefile for county visualization...")
    gdf = gpd.read_file(shapefile_path)

    county_gdf = gdf.dissolve(by='COUNTYFP20')

    fig, ax = plt.subplots(figsize=(20, 16))

    county_gdf.plot(ax=ax,
                    column='COUNTYFP20',
                    cmap='Set3',
                    edgecolor='black',
                    linewidth=0.5,
                    legend=True)

    ax.set_title('New York Counties from Census Blocks (2024)', fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal')

    plt.tight_layout()

    print(f"Saving county map to {output_file}...")
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.show()

def get_county_info(shapefile_path="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp"):
    """
    Get information about counties in the shapefile.
    """
    print("Loading shapefile to get county information...")
    gdf = gpd.read_file(shapefile_path)

    counties = gdf.groupby('COUNTYFP20').agg({
        'GEOID20': 'count',
        'geometry': lambda x: x.iloc[0]
    }).rename(columns={'GEOID20': 'block_count'})

    print("\nCounties in New York shapefile:")
    print("County FIPS | Block Count")
    print("-" * 25)
    for county_fips, data in counties.iterrows():
        print(f"{county_fips:>11} | {data['block_count']:>10,}")

    return counties

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Render maps from NY census block shapefiles")
    parser.add_argument("--shapefile", default="res/tl_2024_36_tabblock20/tl_2024_36_tabblock20.shp",
                       help="Path to shapefile")
    parser.add_argument("--output", default="ny_census_blocks_map.png",
                       help="Output filename")
    parser.add_argument("--nyc", action="store_true", help="Filter to only NYC counties")
    parser.add_argument("--tracts", action="store_true", help="Render at census tract level")
    parser.add_argument("--county-map", action="store_true",
                       help="Render county-level map instead of blocks")
    parser.add_argument("--info", action="store_true",
                       help="Show county information only")
    parser.add_argument("--figsize", nargs=2, type=int, default=[20, 16],
                       help="Figure size in inches (width height)")
    parser.add_argument("--dpi", type=int, default=150,
                       help="Output resolution (DPI)")
    parser.add_argument("--choropleth", type=str,
                       help="Path to CSV file with FIPS codes and values for choropleth coloring")
    parser.add_argument("--fips-col", type=str, default="TRACT_FIPS",
                       help="Column name containing FIPS codes in choropleth data")
    parser.add_argument("--value-col", type=str, default="MEDIAN_INCOME",
                       help="Column name containing values to color by")
    parser.add_argument("--colormap", type=str, default="blues",
                       help="Matplotlib colormap name (e.g., viridis, RdYlBu, plasma)")
    parser.add_argument("--legend-title", type=str,
                       help="Title for the colorbar legend")
    print(sys.argv)
    args = parser.parse_args()

    counties = None
    if args.nyc:
        counties = set(['061',  # Manhattan
                        '047',  # Brooklyn
                        '081',  # Queens
                        '005',  # Bronx
                        '085']) # Staten Island

    # Load choropleth data if provided
    choropleth_data = None
    if args.choropleth:
        print(f"Loading choropleth data from {args.choropleth}...")
        choropleth_data = pd.read_csv(args.choropleth)
        print(f"✓ Loaded {len(choropleth_data):,} records")

        # Validate columns exist
        if args.fips_col not in choropleth_data.columns:
            print(f"✗ Error: Column '{args.fips_col}' not found in choropleth data")
            print(f"Available columns: {list(choropleth_data.columns)}")
            exit(1)
        if args.value_col not in choropleth_data.columns:
            print(f"✗ Error: Column '{args.value_col}' not found in choropleth data")
            print(f"Available columns: {list(choropleth_data.columns)}")
            exit(1)

    if args.info:
        get_county_info(args.shapefile)
    elif args.county_map:
        render_county_map(args.shapefile, args.output)
    else:
        render_census_blocks_map(
            shapefile_path=args.shapefile,
            output_file=args.output,
            figsize=tuple(args.figsize),
            dpi=args.dpi,
            county_filter=counties,
            census_tracts=args.tracts,
            choropleth_data=choropleth_data,
            value_column=args.value_col,
            fips_column=args.fips_col,
            colormap=args.colormap,
            legend_title=args.legend_title
        )

"""
 # NYC County FIPS codes (5 boroughs)
    NYC_COUNTIES = {
        '061': 'Manhattan (New York County)',
        '047': 'Brooklyn (Kings County)',
        '081': 'Queens (Queens County)',
        '005': 'Bronx (Bronx County)',
        '085': 'Staten Island (Richmond County)'
    }"""