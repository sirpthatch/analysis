import pandas as pd
import os
import sys
import json


def load_median_income_data(file_path="data/census_tract_median_income.csv"):
    """
    Load median income dataset into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file containing median income data

    Returns:
        pd.DataFrame: DataFrame with median income data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Skip the first row which contains column descriptions
    df = pd.read_csv(file_path)
    # Clean up column names
    df.columns = df.columns.str.lower()

    # Convert median income to numeric, handling any non-numeric values
    df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
    df['income_margin_error'] = pd.to_numeric(df['income_margin_error'], errors='coerce')

    return df


def load_linknyc_data(file_path="output/linknyc_census_tract_analysis.csv"):
    """
    Load LinkNYC census tract analysis dataset into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file containing LinkNYC census tract analysis data

    Returns:
        pd.DataFrame: DataFrame with LinkNYC census tract analysis data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    # Clean column names by converting to lowercase
    df.columns = df.columns.str.lower()

    # Convert numeric columns to proper data types
    numeric_columns = ['total_kiosks', 'live_kiosks', 'live_percentage', 'pop20',
                      'land_area_sqmi', 'population_density', 'kiosks_per_sqmi',
                      'kiosks_per_1000_pop', 'link_5g_count', 'link_1_count']

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def generate_normalized_output(out_path:str):
    median_income = load_median_income_data()
    wifi = load_linknyc_data()

    filtered_median_income = median_income[["tract_fips", "median_income"]]
    filtered_wifi = wifi[["tract_geoid", "total_kiosks", "kiosks_per_sqmi", 
                          "kiosks_per_1000_pop", "link_5g_count", "link_1_count"]]
    land_features = wifi[["tract_geoid","pop20","land_area_sqmi"]]

    filtered_wifi.rename(columns={"tract_geoid":"tract_fips"})
    land_features.rename(columns={"tract_geoid":"tract_fips"})

    median_income_output = os.path.join(out_path, "f_median_income.csv")
    median_income_meta_output = os.path.join(out_path, "f_median_income_meta.json")
    filtered_median_income.to_csv(median_income_output, index=False)
    with open(median_income_meta_output, "w") as f:
        json.dump({"timestamp-year": "2023", "source": "acs"}, f)
    
    wifi_output = os.path.join(out_path, "f_wifi.csv")
    wifi_meta_output = os.path.join(out_path, "f_wifi_meta.json")
    filtered_wifi.to_csv(wifi_output, index=False)
    with open(wifi_meta_output, "w") as f:
        json.dump({"timestamp-date": "20250924", "source": "linknyc_kiosk_status_20250924"}, f)
    
    land_output = os.path.join(out_path, "f_land.csv")
    land_meta_output = os.path.join(out_path, "f_land_meta.json")
    land_features.to_csv(land_output, index=False)
    with open(land_meta_output, "w") as f:
        json.dump({"timestamp-year": "2024", "source": "https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2024&layergroup=Blocks+%282020%29"}, f)


    

    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_loader.py <output_path>")
        sys.exit(1)
    generate_normalized_output(sys.argv[1])