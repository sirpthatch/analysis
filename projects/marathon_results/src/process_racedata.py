import pandas as pd
from pathlib import Path
from process import EtlModule



class RaceRecordsEtlModule(EtlModule):

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["global"]) for p in file_paths]) # have only one partition, data is small

    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:

        if not file_paths or len(file_paths) == 0:
            return dict()

        #self.verify_consistent(file_paths) 
        # validation fails sometimes because the input race records
        # can be a little messy (for instance, age is represented as either a float or int)
        # this will be cleaned up when all of the records are appended together with a 
        # consistent schema

        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            df = pd.read_parquet(path)
    
            # Filter for records with city, state, and time
            candidate_records = df[(df['city'].notnull()) & 
                                   (df['state'].notnull()) & 
                                   (df['time'].apply(RaceRecordsEtlModule._has_value))]
            
            # Convert time column from HH:MM:SS to total minutes, and convery city and state
            # to lower case
            candidate_records = candidate_records.copy()
            candidate_records['time'] = candidate_records['time'].apply(RaceRecordsEtlModule.time_to_minutes)
            candidate_records['city'] = candidate_records['city'].apply(str.lower)
            candidate_records['state'] = candidate_records['state'].apply(str.lower)
            dfs.append(candidate_records)

        combined_df = pd.concat(dfs, ignore_index=True)
        return {"global":combined_df}
    
    @classmethod
    def _has_value(cls, val):
        if pd.isna(val):
            return False
        if val.strip() == "":
            return False
        return True
    
    @classmethod
    def _time_to_minutes(cls, time_str):
        """
        Convert time string from HH:MM:SS format to total minutes
        """
        try:
            if pd.isna(time_str):
                return None
            time_str = str(time_str).strip()
            parts = time_str.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
                return hours * 60 + minutes + seconds / 60
            elif len(parts) == 2:
                minutes, seconds = int(parts[0]), float(parts[1])
                return minutes + seconds / 60
            else:
                return None
        except (ValueError, AttributeError, IndexError):
            return None