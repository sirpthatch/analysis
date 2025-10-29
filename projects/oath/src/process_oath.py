import pandas as pd
from pathlib import Path
from process import EtlModule

class OathEtlModule(EtlModule):

    def partition(self, file_paths:list[Path]) -> dict[Path, list[str]]:
        return dict([(p, ["global"]) for p in file_paths])

    def process_files(self, file_paths:list[Path]) -> dict[str, pd.DataFrame]:

        if not file_paths or len(file_paths) == 0:
            return dict()
        
        print(file_paths)
        self.verify_consistent(file_paths)

        dfs: list[pd.DataFrame] = []
        for path in file_paths:
            df = pd.read_csv(path)
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        print(combined_df.head())

        return {"global":combined_df}

