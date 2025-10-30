import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
import numpy as np
import pandas as pd
import sys
from constants import GEOBLOCK_FILE

def render_census_blocks_map(shapefile_path=GEOBLOCK_FILE,
                           output_file=None,
                           figsize=(10, 8),
                           dpi=150,
                           census_tracts=None,
                           choropleth_data=None,
                           value_column=None,
                           fips_column="fips",
                           colormap='viridis',
                           legend_title=None):
    """
    Render a map from NY census block shapefiles with optional choropleth coloring.

    Args:
        shapefile_path (str): Path to the shapefile
        output_file (str): Output filename for the map (if none, will render with matplotlib)
        figsize (tuple): Figure size in inches
        dpi (int): Resolution for output image
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

    counties = set(['061',  # Manhattan
                        '047',  # Brooklyn
                        '081',  # Queens
                        '005',  # Bronx
                        '085']) # Staten Island
    print(f"Filtering to counties: {counties}")
    gdf = gdf[gdf['COUNTYFP20'].isin(counties)]
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

    if output_file:
        print(f"Saving map to {output_file}...")
        plt.savefig(output_file, dpi=dpi, bbox_inches='tight')
    else:
        plt.show()

    print("Map rendering complete!")