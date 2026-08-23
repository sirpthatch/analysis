"""
Shared utilities for ETL modules.

Provides reusable helper classes for:
- Date parsing from different formats
- Text normalization (race names, locations)
- Time format conversions
"""

import pandas as pd
from datetime import datetime
from typing import Optional


class DateParser:
    """Utilities for parsing dates from different source formats."""

    @staticmethod
    def parse_marathon_results_date(date_str: str) -> Optional[datetime]:
        """
        Parse date from marathon_results.csv format (YYYY-MM-DD).

        Args:
            date_str: Date string in YYYY-MM-DD format

        Returns:
            datetime object or None if parsing fails

        Examples:
            >>> DateParser.parse_marathon_results_date("2008-02-17")
            datetime.datetime(2008, 2, 17, 0, 0)
        """
        try:
            if pd.isna(date_str):
                return None
            return datetime.strptime(str(date_str).strip(), '%Y-%m-%d')
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_race_final_date(date_str: str) -> Optional[datetime]:
        """
        Parse date from race_final format (MM_DD_YY).

        Args:
            date_str: Date string in MM_DD_YY format (e.g., "10_15_16")

        Returns:
            datetime object or None if parsing fails

        Examples:
            >>> DateParser.parse_race_final_date("10_15_16")
            datetime.datetime(2016, 10, 15, 0, 0)
        """
        try:
            if pd.isna(date_str):
                return None

            date_str = str(date_str).strip()
            parts = date_str.split('_')

            if len(parts) != 3:
                return None

            month, day, year = parts
            # Convert 2-digit year to 4-digit (assume 20xx)
            full_year = "20" + year

            return datetime(int(full_year), int(month), int(day))
        except (ValueError, AttributeError, IndexError):
            return None


class TextNormalizer:
    """Utilities for normalizing text data for consistent matching."""

    @staticmethod
    def normalize_race_name(name: str) -> str:
        """
        Normalize race name for consistent matching.

        Rules:
        - Convert to lowercase
        - Replace spaces with underscores
        - Remove apostrophes and quotes
        - Strip leading/trailing whitespace

        Args:
            name: Race name to normalize

        Returns:
            Normalized race name

        Examples:
            >>> TextNormalizer.normalize_race_name("Austin Marathon")
            'austin_marathon'
            >>> TextNormalizer.normalize_race_name("Dog Lake Marathon")
            'dog_lake_marathon'
            >>> TextNormalizer.normalize_race_name("Last Chance for Boston")
            'last_chance_for_boston'
        """
        if pd.isna(name):
            return ""

        normalized = str(name).lower().strip()
        normalized = normalized.replace(' ', '_')
        normalized = normalized.replace("'", '')
        normalized = normalized.replace('"', '')

        return normalized

    @staticmethod
    def normalize_location(city: Optional[str], state: Optional[str]) -> tuple[str, str]:
        """
        Normalize city and state for matching.

        Args:
            city: City name (any case)
            state: State code (any case)

        Returns:
            Tuple of (normalized_city, normalized_state) - both lowercase

        Examples:
            >>> TextNormalizer.normalize_location("Austin", "TX")
            ('austin', 'tx')
            >>> TextNormalizer.normalize_location("New York", "NY")
            ('new york', 'ny')
        """
        norm_city = str(city).lower().strip() if not pd.isna(city) else ""
        norm_state = str(state).lower().strip() if not pd.isna(state) else ""
        return norm_city, norm_state


class TimeConverter:
    """Utilities for converting time between different formats."""

    @staticmethod
    def hms_to_minutes(time_str: str) -> Optional[float]:
        """
        Convert time from HH:MM:SS or MM:SS format to decimal minutes.

        Args:
            time_str: Time string in HH:MM:SS or MM:SS format

        Returns:
            Time in decimal minutes or None if parsing fails

        Examples:
            >>> TimeConverter.hms_to_minutes("02:20:38")
            140.63333333333333
            >>> TimeConverter.hms_to_minutes("3:15:00")
            195.0
            >>> TimeConverter.hms_to_minutes("45:30")
            45.5
        """
        try:
            if pd.isna(time_str):
                return None

            time_str = str(time_str).strip()
            parts = time_str.split(':')

            if len(parts) == 3:
                # HH:MM:SS format
                hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
                return hours * 60 + minutes + seconds / 60
            elif len(parts) == 2:
                # MM:SS format
                minutes, seconds = int(parts[0]), float(parts[1])
                return minutes + seconds / 60
            else:
                return None
        except (ValueError, AttributeError, IndexError):
            return None

    @staticmethod
    def minutes_to_hms(minutes: float) -> str:
        """
        Convert decimal minutes to HH:MM:SS string format.

        Args:
            minutes: Time in decimal minutes

        Returns:
            Time in HH:MM:SS format

        Examples:
            >>> TimeConverter.minutes_to_hms(140.63333)
            '2:20:38'
            >>> TimeConverter.minutes_to_hms(195.0)
            '3:15:00'
        """
        if pd.isna(minutes):
            return ""

        total_seconds = int(round(minutes * 60))
        hours = total_seconds // 3600
        remaining = total_seconds % 3600
        mins = remaining // 60
        secs = remaining % 60

        return f"{hours}:{mins:02d}:{secs:02d}"
