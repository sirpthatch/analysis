"""
JoinEtlModule: Full outer join of marathon_results.csv with race_final data.

This module combines data from two sources:
1. marathon_results.csv (475K records with runner names)
2. race_final/global/data.csv (7.4M normalized race records)

Join strategy:
- Normalize all fields (race names, dates, locations, times)
- Create composite join key from race + date + city + state
- Perform full outer join to preserve all records
- Add source tracking column
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from process import EtlModule
from etl_utils import DateParser, TextNormalizer, TimeConverter
from etl_config import JoinConfig


class JoinEtlModule(EtlModule):
    """
    Full outer join of marathon_results with race_final data.

    Normalizes and combines data from both sources into a unified dataset.
    Handles format differences (dates, times, casing, race names) and tracks
    the source of each record.
    """

    output_format = "parquet"

    def __init__(self):
        """Initialize with default configuration."""
        self.config = JoinConfig()

    def partition(self, file_paths: list[Path]) -> dict[Path, list[str]]:
        """
        Map files to partitions. Join operation uses single global partition.

        Args:
            file_paths: List of input file paths

        Returns:
            Dict mapping each file path to list of partition names
        """
        return {p: ["global"] for p in file_paths}

    def process_files(self, file_paths: list[Path]) -> dict[str, pd.DataFrame]:
        """
        Process and join marathon_results with race_final data.

        Steps:
        1. Load both datasets
        2. Normalize all fields
        3. Create join keys
        4. Perform full outer join
        5. Add source tracking column
        6. Return unified schema

        Args:
            file_paths: List of input file paths

        Returns:
            Dict with single 'global' key mapping to unified DataFrame
        """
        if not file_paths or len(file_paths) == 0:
            return {}

        # Separate files by expected name patterns
        marathon_files = []
        race_file = None

        for path in file_paths:
            if 'marathon_results' in path.name:
                marathon_files.append(path)
            elif 'race_final' in str(path) or 'data.csv' == path.name:
                race_file = path

        if len(marathon_files) == 0:
            raise ValueError(
                f"Expected marathon_results.csv in input files. "
                f"Found: {[p.name for p in file_paths]}"
            )
        if not race_file:
            raise ValueError(
                f"Expected race_final/.../data.csv in input files. "
                f"Found: {[p.name for p in file_paths]}"
            )

        print(f"Loading data files...")
        print(f"  Marathon results: {marathon_files}")
        print(f"  Race final: {race_file}")

        # Load data
        df_marathon = pd.concat([pd.read_csv(f) for f in marathon_files])
        df_marathon = df_marathon.drop_duplicates(keep='first')
        df_race = pd.read_csv(race_file)

        print(f"\nInitial record counts:")
        print(f"  Marathon results: {len(df_marathon):,}")
        print(f"  Race final: {len(df_race):,}")

        # Validate schemas
        self._validate_schemas(df_marathon, df_race)

        # Process marathon_results
        print(f"\nProcessing marathon_results...")
        df_marathon_processed = self._process_marathon_results(df_marathon)

        # Process race_final
        print(f"Processing race_final...")
        df_race_processed = self._process_race_final(df_race)

        # Combine datasets
        print(f"\nCombining datasets...")
        combined_df = pd.concat(
            [df_marathon_processed, df_race_processed],
            ignore_index=True
        )

        print(f"  Total combined records: {len(combined_df):,}")
        print(f"  Unique join keys: {combined_df['join_key'].nunique():,}")

        # Deduplicate based on join key
        print(f"\nDeduplicating records...")
        if self.config.DEDUP_STRATEGY == 'keep_first':
            combined_df_deduped = combined_df.drop_duplicates(
                subset=['join_key'],
                keep='first'
            )
        elif self.config.DEDUP_STRATEGY == 'keep_last':
            combined_df_deduped = combined_df.drop_duplicates(
                subset=['join_key'],
                keep='last'
            )
        else:  # keep_all
            combined_df_deduped = combined_df

        print(f"  After deduplication: {len(combined_df_deduped):,}")

        # Report source distribution
        source_counts = combined_df_deduped['source'].value_counts()
        print(f"\nSource distribution:")
        for source, count in source_counts.items():
            pct = (count / len(combined_df_deduped)) * 100
            print(f"  {source}: {count:,} ({pct:.1f}%)")

        return {"global": combined_df_deduped}

    def _validate_schemas(
        self,
        df_marathon: pd.DataFrame,
        df_race: pd.DataFrame
    ) -> None:
        """
        Validate that input dataframes have expected columns.

        Args:
            df_marathon: Marathon results DataFrame
            df_race: Race final DataFrame

        Raises:
            ValueError: If required columns are missing
        """
        marathon_required = {
            'name', 'age', 'sex', 'hometown_city', 'hometown_state',
            'time', 'race_name', 'race_date'
        }
        race_required = {'age', 'sex', 'time', 'race', 'date', 'city', 'state'}

        marathon_actual = set(df_marathon.columns)
        race_actual = set(df_race.columns)

        marathon_missing = marathon_required - marathon_actual
        race_missing = race_required - race_actual

        if marathon_missing:
            raise ValueError(
                f"marathon_results missing columns: {marathon_missing}"
            )
        if race_missing:
            raise ValueError(f"race_final missing columns: {race_missing}")

        print("✓ Schema validation passed")

    def _process_marathon_results(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Process and normalize marathon_results data.

        Args:
            df: Raw marathon_results DataFrame

        Returns:
            Processed DataFrame with normalized columns
        """
        df_processed = df.copy()

        # Normalize race name
        df_processed['race'] = df_processed['race_name'].apply(
            TextNormalizer.normalize_race_name
        )

        # Parse and normalize date
        df_processed['date'] = df_processed['race_date'].apply(
            DateParser.parse_marathon_results_date
        )

        # Normalize city and state
        df_processed['city'], df_processed['state'] = self._normalize_locations_vec(
            df_processed['hometown_city'], df_processed['hometown_state']
        )

        # Convert time to minutes
        df_processed['time_minutes'] = df_processed['time'].apply(
            TimeConverter.hms_to_minutes
        )

        # Create join key
        df_processed['join_key'] = self._create_join_keys_vec(df_processed)

        # Add source column
        df_processed['source'] = 'marathon_results'

        # Select final columns
        final_cols = [
            'race', 'date', 'city', 'state', 'age', 'sex',
            'time_minutes', 'join_key', 'source'
        ]
        df_result = df_processed[final_cols].copy()

        # Report statistics
        parse_failures = df_result['date'].isna().sum()
        if parse_failures > 0:
            pct = (parse_failures / len(df_result)) * 100
            print(f"  ⚠ Date parsing failures: {parse_failures:,} ({pct:.1f}%)")

        time_failures = df_result['time_minutes'].isna().sum()
        if time_failures > 0:
            pct = (time_failures / len(df_result)) * 100
            print(f"  ⚠ Time conversion failures: {time_failures:,} ({pct:.1f}%)")

        return df_result

    def _process_race_final(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and normalize race_final data.

        Args:
            df: Raw race_final DataFrame

        Returns:
            Processed DataFrame with normalized columns
        """
        df_processed = df.copy()

        # Normalize race name (already underscore format, but ensure consistency)
        df_processed['race'] = df_processed['race'].apply(
            TextNormalizer.normalize_race_name
        )

        # Parse and normalize date
        df_processed['date'] = df_processed['date'].apply(
            DateParser.parse_race_final_date
        )

        # Normalize city and state (should already be lowercase, but ensure)
        df_processed['city'], df_processed['state'] = self._normalize_locations_vec(
            df_processed['city'], df_processed['state']
        )

        # Time is already in minutes (float)
        df_processed['time_minutes'] = df_processed['time']

        # Create join key
        df_processed['join_key'] = self._create_join_keys_vec(df_processed)

        # Add source column
        df_processed['source'] = 'race_final'

        # Select final columns (matching marathon_results output schema)
        final_cols = [
            'race', 'date', 'city', 'state', 'age', 'sex',
            'time_minutes', 'join_key', 'source'
        ]
        df_result = df_processed[final_cols].copy()

        # Report statistics
        parse_failures = df_result['date'].isna().sum()
        if parse_failures > 0:
            pct = (parse_failures / len(df_result)) * 100
            print(f"  ⚠ Date parsing failures: {parse_failures:,} ({pct:.1f}%)")

        return df_result

    @staticmethod
    @staticmethod
    def _normalize_locations_vec(city, state):
        """Vectorized TextNormalizer.normalize_location - the row-wise apply was
        the bottleneck at 7.4M rows."""
        c = city.astype("string").str.lower().str.strip().fillna("")
        s = state.astype("string").str.lower().str.strip().fillna("")
        return c, s

    @staticmethod
    def _create_join_keys_vec(df: pd.DataFrame) -> pd.Series:
        """Vectorized equivalent of _create_join_key over a whole frame."""
        date_str = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        age = df["age"].map(lambda v: "" if pd.isna(v) else f"{v:g}")
        sex = df["sex"].astype("string").fillna("")
        tm = df["time_minutes"].map(lambda v: "" if pd.isna(v) else f"{v:g}")
        return (df["race"].astype("string").fillna("") + "__" + date_str + "__"
                + df["city"].astype("string").fillna("") + "__"
                + df["state"].astype("string").fillna("") + "__"
                + age + "__" + sex + "__" + tm)

    def _create_join_key(
        race: str,
        date: Optional[datetime],
        city: str,
        state: str,
        age=None,
        sex=None,
        time_minutes=None
    ) -> str:
        """
        Create composite join key from normalized components.

        The key identifies a single race ENTRY, not a race-city cell. It previously
        stopped at (race, date, city, state), which meant deduplicating on it
        collapsed every runner from the same hometown in the same race into one row
        - 7.4M race_final records became 2.05M (72% of runners silently lost).
        Age, sex and finish time restore record-level granularity.

        Args:
            race: Normalized race name
            date: Parsed datetime
            city: Normalized city
            state: Normalized state
            age: Runner age (optional)
            sex: Runner sex (optional)
            time_minutes: Finish time in minutes (optional)

        Returns:
            Join key string, e.g.
            "austin_marathon__2008-02-17__austin__tx__34__M__201.5"
        """
        date_str = date.strftime('%Y-%m-%d') if date else ""
        base = f"{race}__{date_str}__{city}__{state}"
        if age is None and sex is None and time_minutes is None:
            return base

        def _fmt(v):
            if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                return ""
            return f"{v:g}" if isinstance(v, (int, float)) else str(v)

        return f"{base}__{_fmt(age)}__{_fmt(sex)}__{_fmt(time_minutes)}"
