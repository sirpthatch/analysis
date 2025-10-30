import pandas as pd
from pathlib import Path
from process import EtlModule



class OathEtlModule(EtlModule):

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["global"]) for p in file_paths])

    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:

        if not file_paths or len(file_paths) == 0:
            return dict()

        self.verify_consistent(file_paths)

        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            df = pd.read_csv(path)
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        mapper = self.get_fips_mapper()

        combined_df["fips"] = combined_df.apply(
            lambda r: mapper(r["Latitude"], r["Longitude"]),
            axis=1
        )
        print(combined_df.head())

        return {"global":combined_df}
    

"""
10-125	UNLAWFUL CONSUMPTION/POSSESSION OF ALCOHOLIC B...	132128	56.61	56.61
1	16-118(6)	PUBLIC URINATION	21107	9.04	65.65
2	19-176(b)	UNLAWFUL BICYCLE RIDING ON SIDEWALK	9704	4.16	69.81
3	19-190(B)	RIGHT OF WAY - FAILURE TO YIELD, PHYSICAL INJURY	6438	2.76	72.57
4	20-453	UNLICENSED GENERAL VENDOR	6061	2.60	75.17
5	17-503(D)(3)	SMOKING IN PARK OR ON OTHER PROPERTY UNDER JUR...	6019	2.58	77.75
6	19-176.2(B)	OPERATION OF MOTORIZED SCOOTER WITHIN THE CITY...	5255	2.25	80.00
7	17-315(E)	VEND IN BUS STOP, NEXT TO HOSPITAL/10 FT OF DR...	4757	2.04	82.04

"""
class OathFeaturizeModule(EtlModule):

    column_name_map = {
        "10-125":"oath_public_drinking",
        "16-118(6)":"oath_public_urination",
        "19-176(b)":"oath_bike_on_sidewalk",
        "20-453":"oath_unlicensed_vendor",
        "17-503(D)(3)":"oath_smoking_in_park",
        "19-176.2(B)":"oath_illegal_scooter",
        "17-315(E)":"oath_vendor_in_illegal_spot",
    }

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["monthly","annual","overall"]) for p in file_paths])
    
    def clean_df(self, starting_df:pd.DataFrame) -> pd.DataFrame:
        """
            Select the columns that we are interested in and clean up the names.

            Output dataframe will have a column named "law_friendly_name" for each row,
            and a formatted "date", "year", and "year_month" columns
        """
        df = starting_df.copy()
        starting_len = len(df)
        df = df[df["Law Code"].isin(self.column_name_map.keys())]
        filtered_len = len(df)

        removed = starting_len - filtered_len
        pct_removed = (removed / starting_len * 100) if starting_len > 0 else 0.0
        print(f"...Filtered {pct_removed:.2f}% of rows ({removed} rows removed); resulting size: {filtered_len} rows")

        df["law_friendly_name"] = df["Law Code"].apply(self.column_name_map.get)
        df['date'] = pd.to_datetime(df['OCCUR_DATE'], format='%m/%d/%Y')
        df['year'] = df['date'].dt.year
        df['year_month'] = df['date'].dt.to_period('M')
        df['fips'] = df['fips'].astype('Int64')

        return df


    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:

        if not file_paths or len(file_paths) == 0:
            return dict()

        self.verify_consistent(file_paths)

        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            df = pd.read_csv(path)
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        cleaned_frame = self.clean_df(combined_df)
        
        # 1. Overall aggregation: fips x LAW_DESC counts
        overall_df = cleaned_frame.groupby(['fips', 'law_friendly_name']).size().reset_index(name='count')
        overall_df = overall_df.pivot(index='fips', columns='law_friendly_name', values='count').fillna(0)
        overall_df = overall_df.astype(int)  # Convert to integer counts
        overall_df = overall_df.reset_index()

        # 2. Annual aggregation: (fips, year) x LAW_DESC counts
        annual_df = cleaned_frame.groupby(['fips', 'year', 'law_friendly_name']).size().reset_index(name='count')
        annual_df = annual_df.pivot_table(index=['fips', 'year'], columns='law_friendly_name', values='count', fill_value=0)
        annual_df = annual_df.astype(int)
        annual_df = annual_df.reset_index()

        # 3. Monthly aggregation: (fips, year_month) x LAW_DESC counts
        monthly_df = cleaned_frame.groupby(['fips', 'year_month', 'law_friendly_name']).size().reset_index(name='count')
        monthly_df = monthly_df.pivot_table(index=['fips', 'year_month'], columns='law_friendly_name', values='count', fill_value=0)
        monthly_df = monthly_df.astype(int)
        monthly_df = monthly_df.reset_index()
        # Convert period back to string for CSV compatibility
        monthly_df['year_month'] = monthly_df['year_month'].astype(str)

        return {
            "overall": overall_df,
            "annual": annual_df,
            "monthly": monthly_df
        }