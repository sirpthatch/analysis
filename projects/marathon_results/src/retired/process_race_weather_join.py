import pandas as pd
from pathlib import Path
from process import EtlModule
from datetime import datetime, timedelta



class RaceWeatherPartitionEtlModule(EtlModule):

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["global"]) for p in file_paths]) # have only one input partition, data is small

    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:
        print(f"processing {file_paths}")
        if not file_paths or len(file_paths) == 0:
            return dict()

        self.verify_consistent(file_paths) 
    
        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            dfs.append(pd.read_csv(path))
        combined_df = pd.concat(dfs, ignore_index=True)

        # Create dictionary of dataframes, keyed by (race, date) tuple
        race_date_dfs = {self.create_composite_index([race, date]): group \
                         for (race, date), group in combined_df.groupby(["race","date"])}
        return race_date_dfs

WEATHER_DATA = "data/weather_data.csv"

class RaceWeatherJoinEtlModule(EtlModule):

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["global"]) for p in file_paths]) # have only one input partition, data is small

    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:
        print(f"processing {file_paths}")
        if not file_paths or len(file_paths) == 0:
            return dict()

        self.verify_consistent(file_paths) 
    
        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            dfs.append(pd.read_csv(path))
        combined_df = pd.concat(dfs, ignore_index=True)

        # Create dictionary of dataframes, keyed by (race, date) tuple
        race_date_dfs = {(race, date): group \
                         for (race, date), group in combined_df.groupby(["race","date"])}
        
        weather_data = pd.read_csv(WEATHER_DATA, parse_dates=['date'])
        weather_dfs = {(city, state): group \
                         for (city, state), group in weather_data.groupby(["city","state"])}
        
        for (race,date), group in combined_df.groupby(["race","date"]):
            cities = group.groupby(["city","state"])
            print(f"examining {race}-{date}, {len(group)} records, {len(cities)} cities")
            [month, day, year] = date.split("_")
            race_date_dt = datetime(int("20"+year), int(month), int(day))
            if race_date_dt < datetime(2016,1,1):
                print("...skipping, race before 2016, no weather data")
                continue

            for (city, state), records in group.groupby(["city","state"]):
                print(f"looking up weather for {city} - {state}")
                lookup = (city.lower(), state.lower())
                if lookup not in weather_dfs:
                    print("--skipping, no weather data")
                    continue

                weather_records = weather_dfs[lookup]
                print(f"{len(weather_records)} weather records for {city} {state}")
                
                [month, day, year] = date.split("_")
                race_date_dt = datetime(int(year), int(month), int(day))
                full_training_early_date = race_date_dt - timedelta(days=90)
                peak_training_early_date = race_date_dt - timedelta(days=30)

                full_training_records = weather_records[weather_records["date"].between(full_training_early_date, race_date_dt, inclusive='left')]
                peak_training_records = weather_records[weather_records["date"].between(peak_training_early_date, race_date_dt, inclusive='left')]
                
                if len(full_training_records) <= 5 or len(peak_training_records) <= 5:
                    print(f"skipping, not enough weather data on {race_date_dt}")
                    continue
    
        return 1/0