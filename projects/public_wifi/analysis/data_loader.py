import pandas as pd
import os


def load_median_income_data(file_path="data/median-income-2023.csv"):
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
    print(df.head())
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