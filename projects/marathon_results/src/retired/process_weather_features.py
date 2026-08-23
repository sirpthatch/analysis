"""
WeatherFeatureEtlModule: Enrich race data with weather features from training windows.

This module calculates 14 weather features (7 metrics × 2 periods) for marathon runners
based on weather conditions during their training period before the race.

Features calculated:
- Temperature: min, max, median_min, median_max
- Precipitation: total, days_of_precip (>0.2"), weekend_days_of_precip

Periods:
- Full training: 90 days before race
- Peak training: 30 days before race
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from process import EtlModule
from etl_utils import DateParser
from etl_config import WeatherConfig


class WeatherFeatureEtlModule(EtlModule):
    """
    Enrich race data with weather features from training windows.

    Processes race records and weather data to calculate training period
    weather metrics. Uses per-race-date partitioning for efficient processing.
    """

    def __init__(self):
        """Initialize with default configuration."""
        self.config = WeatherConfig()

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
        Process race data and enrich with weather features.

        Strategy:
        1. Load race data and weather data
        2. Group race data by (race, date)
        3. For each group:
           - For each city/state in the group:
             - Look up weather data
             - Extract training windows
             - Calculate features
             - Append features to runner records
        4. Return partitioned results

        Args:
            file_paths: List of input file paths

        Returns:
            Dict mapping partition names to enriched DataFrames
        """
        if not file_paths or len(file_paths) == 0:
            return {}

        # Identify input files
        race_file = None
        weather_file = None

        for path in file_paths:
            if self._is_weather_file(path):
                weather_file = path
            elif self._is_race_file(path):
                race_file = path

            print(f"Checked {path.name}")
            print(f"Weather File: {weather_file}")
            print(f"Race file: {race_file}")

        if not race_file:
            raise ValueError(
                f"Expected race data file in inputs. "
                f"Found: {[p.name for p in file_paths]}"
            )
        if not weather_file:
            raise ValueError(
                f"Expected weather data file in inputs. "
                f"Found: {[p.name for p in file_paths]}"
            )

        print(f"\nLoading data files...")
        print(f"  Race data: {race_file}")
        print(f"  Weather data: {weather_file}")

        # Load data
        race_df = pd.read_csv(race_file)
        weather_df = pd.read_csv(weather_file)

        print(f"\nInitial record counts:")
        print(f"  Race records: {len(race_df):,}")
        print(f"  Weather records: {len(weather_df):,}")

        # Parse dates in race data
        print(f"\nParsing race dates...")
        race_df['date_parsed'] = race_df['date'].apply(self._parse_date)
        valid_dates = race_df['date_parsed'].notna().sum()
        print(f"  Valid dates: {valid_dates:,} ({valid_dates/len(race_df)*100:.1f}%)")
        if valid_dates < 10:
            raise ValueError("Problem parsing dates")
        
        # Filter to races with sufficient weather history
        min_race_date = (
            self.config.EARLIEST_WEATHER_DATE +
            timedelta(days=self.config.FULL_TRAINING_DAYS + 365)
        )
        print(f"\nFiltering races before {min_race_date.strftime('%Y-%m-%d')}...")
        race_df_filtered = race_df[race_df['date_parsed'] >= min_race_date].copy()
        print(f"  Remaining records: {len(race_df_filtered):,}")

        # Build weather lookup
        print(f"\nBuilding weather lookup...")
        weather_lookup = self._build_weather_lookup(weather_df)
        print(f"  Cities with weather data: {len(weather_lookup)}")

        # Process all records - don't filter by weather availability yet
        # We'll enrich what we can and keep all records
        print(f"\nProcessing weather features with parallel processing...")

        # Determine number of worker threads
        num_workers = min(os.cpu_count() or 4, 8)  # Cap at 8 threads
        print(f"  Using {num_workers} worker threads")

        # Group by race and date for parallel processing
        race_date_groups = list(race_df_filtered.groupby(["race","date_parsed"]))
        print(f"  Processing {len(race_date_groups)} race-date groups")

        all_enriched_records = []
        skipped_no_weather_for_city = 0
        skipped_not_enough_weather_history = 0
        missing_records_concordance = defaultdict(int)

        # Process groups in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self._process_race_date_group,
                    race,
                    race_date_dt,
                    group,
                    weather_lookup
                ): (race, race_date_dt)
                for (race, race_date_dt), group in race_date_groups
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(futures):
                race, race_date_dt = futures[future]
                try:
                    result = future.result()
                    all_enriched_records.extend(result['enriched_records'])
                    skipped_no_weather_for_city += result['skipped_no_weather']
                    skipped_not_enough_weather_history += result['skipped_insufficient']

                    # Merge missing city counts
                    for city_key, count in result['missing_cities'].items():
                        missing_records_concordance[city_key] += count

                    completed += 1
                    if completed % 100 == 0:
                        print(f"  Completed {completed}/{len(race_date_groups)} groups")

                except Exception as exc:
                    print(f"  Error processing {race}-{race_date_dt}: {exc}")

        print(f"  Completed all {len(race_date_groups)} groups")

        print(f"\nCombining all records...")
        if all_enriched_records:
            combined_df = pd.concat(all_enriched_records, ignore_index=True)
            print(f"  Total records: {len(combined_df):,}")
        else:
            combined_df = pd.DataFrame()
            print(f"  No records!")

        print(f"\n✓ Processing complete:")
        print(f"  Records processed: {len(race_df_filtered):,}")
        print(f"  Records without any weather data: {skipped_no_weather_for_city:,}")
        print(f"  Records without enough weather data: {skipped_not_enough_weather_history:,}")
        print(f"  Final record count: {len(combined_df):,}")

        top_5_missing_cities = sorted(missing_records_concordance.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nThe top 5 cities missing weather data:")
        for (city, state), count in top_5_missing_cities:
            print(f"  {city}, {state}: {count} occurrences")

        return {"global": combined_df}

    def _process_race_date_group(
        self,
        race: str,
        race_date_dt: datetime,
        group: pd.DataFrame,
        weather_lookup: Dict[Tuple[str, str], pd.DataFrame]
    ) -> Dict:
        """
        Process a single race-date group to enrich with weather features.

        Args:
            race: Race name
            race_date_dt: Race date
            group: DataFrame of runners for this race-date
            weather_lookup: Lookup dict mapping (city, state) to weather data

        Returns:
            Dict with enriched_records, skipped counts, and missing cities
        """
        enriched_records = []
        skipped_no_weather = 0
        skipped_insufficient = 0
        missing_cities = defaultdict(int)

        cities = group.groupby(["city","state"])

        for (city, state), records in cities:
            lookup = (city.lower().strip(), state.lower().strip())
            if lookup not in weather_lookup:
                skipped_no_weather += len(records)
                missing_cities[lookup] += 1
                continue

            weather_records = weather_lookup[lookup]
            full_training_early_date = race_date_dt - timedelta(days=90)
            peak_training_early_date = race_date_dt - timedelta(days=30)

            full_training_records = weather_records[weather_records["date"].between(
                full_training_early_date, race_date_dt, inclusive='left'
            )]
            peak_training_records = weather_records[weather_records["date"].between(
                peak_training_early_date, race_date_dt, inclusive='left'
            )]

            if len(full_training_records) <= 5 or len(peak_training_records) <= 5:
                skipped_insufficient += len(records)
                continue

            for period, weather_records in zip(["full","peak"], [full_training_records, peak_training_records]):
                overall_min = weather_records["temp_min"].min()
                overall_max = weather_records["temp_max"].max()
                overall_median_min = weather_records["temp_min"].median()
                overall_median_max = weather_records["temp_max"].median()
                overall_rain = weather_records["precip"].sum()
                overall_days_of_rain = len(weather_records[weather_records["precip"] > 0.2])

                weekend_days = weather_records[weather_records["date"].dt.dayofweek.isin([5, 6])]
                overall_weekend_days_with_rain = len(weekend_days[weekend_days["precip"] > 0.2])

                records[period+"_temp_min"] = overall_min
                records[period+"_temp_max"] = overall_max
                records[period+"_temp_median_min"] = overall_median_min
                records[period+"_temp_median_max"] = overall_median_max
                records[period+"_overall_precip"] = overall_rain
                records[period+"_overall_days_of_precip"] = overall_days_of_rain
                records[period+"_overall_weekend_days_of_precip"] = overall_weekend_days_with_rain

            enriched_records.append(records)

        return {
            'enriched_records': enriched_records,
            'skipped_no_weather': skipped_no_weather,
            'skipped_insufficient': skipped_insufficient,
            'missing_cities': dict(missing_cities)
        }

    def _build_weather_lookup(
        self,
        weather_df: pd.DataFrame
    ) -> Dict[Tuple[str, str], pd.DataFrame]:
        """
        Build lookup dict for efficient weather data access.

        Args:
            weather_df: Weather DataFrame with city, state, date columns

        Returns:
            Dict mapping (city, state) tuples to weather DataFrames
        """
        # Parse weather dates
        weather_df = weather_df.copy()
        weather_df['date'] = pd.to_datetime(weather_df['date'])

        # Create lookup by (city, state)
        lookup = {}
        for (city, state), group in weather_df.groupby(['city', 'state']):
            city_key = (
                str(city).lower().strip(),
                str(state).lower().strip()
            )
            lookup[city_key] = group.copy()

        return lookup

    def _get_training_windows(
        self,
        race_date: datetime,
        weather_records: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract full and peak training window weather data.

        Args:
            race_date: Date of the race
            weather_records: All weather records for the city

        Returns:
            Tuple of (full_training_records, peak_training_records)
        """
        full_start = race_date - timedelta(days=self.config.FULL_TRAINING_DAYS)
        peak_start = race_date - timedelta(days=self.config.PEAK_TRAINING_DAYS)

        # Exclusive of race date (training happens before race)
        full_records = weather_records[
            (weather_records['date'] >= full_start) &
            (weather_records['date'] < race_date)
        ]

        peak_records = weather_records[
            (weather_records['date'] >= peak_start) &
            (weather_records['date'] < race_date)
        ]

        return full_records, peak_records

    def _extract_weather_features(
        self,
        weather_records: pd.DataFrame,
        period_name: str
    ) -> Dict[str, float]:
        """
        Extract 7 weather metrics for a given period.

        Args:
            weather_records: DataFrame with weather data for the period
            period_name: 'full' or 'peak'

        Returns:
            Dict with keys like 'full_temp_min', 'peak_temp_max', etc.
        """
        features = {}

        # Temperature metrics
        features[f"{period_name}_temp_min"] = weather_records['temp_min'].min()
        features[f"{period_name}_temp_max"] = weather_records['temp_max'].max()
        features[f"{period_name}_temp_median_min"] = weather_records['temp_min'].median()
        features[f"{period_name}_temp_median_max"] = weather_records['temp_max'].median()

        # Precipitation metrics
        features[f"{period_name}_overall_precip"] = weather_records['precip'].sum()
        features[f"{period_name}_overall_days_of_precip"] = len(
            weather_records[weather_records['precip'] > self.config.PRECIP_THRESHOLD]
        )

        # Weekend precipitation (Saturday=5, Sunday=6 in dayofweek)
        weekend_records = weather_records[
            weather_records['date'].dt.dayofweek.isin([5, 6])
        ]
        features[f"{period_name}_overall_weekend_days_of_precip"] = len(
            weekend_records[weekend_records['precip'] > self.config.PRECIP_THRESHOLD]
        )

        return features

    @staticmethod
    def _parse_date(date_str: str) -> Optional[datetime]:
        """
        Parse date from MM_DD_YY format used in race_final data.

        Args:
            date_str: Date string

        Returns:
            datetime object or None
        """
        date_str = str(date_str).strip()
        parts = date_str.split('-')

        if len(parts) != 3:
            return None

        year, month, day = parts
        return datetime(int(year), int(month), int(day))

    @staticmethod
    def _is_race_file(path: Path) -> bool:
        """Check if path is a race data file."""
        return 'data.csv' == path.name.lower()

    @staticmethod
    def _is_weather_file(path: Path) -> bool:
        """Check if path is a weather data file."""
        return 'weather_data_v2.csv' in path.name.lower()
