"""
Unit tests for WeatherFeatureEtlModule.

Tests weather feature extraction, training windows, and data processing logic.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from process_weather_features import WeatherFeatureEtlModule
from etl_config import WeatherConfig


class TestWeatherFeatureExtraction:
    """Test weather feature calculation logic."""

    def test_extract_weather_features_basic(self):
        """Test basic weather feature extraction."""
        module = WeatherFeatureEtlModule()

        # Create sample weather data
        weather_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=30),
            'temp_min': [20] * 30,
            'temp_max': [40] * 30,
            'precip': [0.1] * 20 + [0.3] * 10
        })

        features = module._extract_weather_features(weather_data, 'full')

        assert features['full_temp_min'] == 20
        assert features['full_temp_max'] == 40
        assert features['full_temp_median_min'] == 20
        assert features['full_temp_median_max'] == 40
        assert features['full_overall_precip'] == pytest.approx(8.0, abs=0.1)
        assert features['full_overall_days_of_precip'] == 10  # Days with >0.2"

    def test_extract_weather_features_varying_temps(self):
        """Test feature extraction with varying temperatures."""
        module = WeatherFeatureEtlModule()

        weather_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=30),
            'temp_min': list(range(20, 50)),
            'temp_max': list(range(40, 70)),
            'precip': [0.0] * 30
        })

        features = module._extract_weather_features(weather_data, 'peak')

        assert features['peak_temp_min'] == 20
        assert features['peak_temp_max'] == 69
        assert features['peak_temp_median_min'] == 34.5
        assert features['peak_temp_median_max'] == 54.5
        assert features['peak_overall_precip'] == 0.0
        assert features['peak_overall_days_of_precip'] == 0

    def test_extract_weather_features_weekend_precip(self):
        """Test weekend precipitation counting."""
        module = WeatherFeatureEtlModule()

        # Create data with Saturdays and Sundays
        # 2020-01-04 is a Saturday, 2020-01-05 is a Sunday
        dates = pd.date_range('2020-01-01', periods=14)
        precip_values = [0.0] * 14
        precip_values[3] = 0.5  # Saturday
        precip_values[4] = 0.3  # Sunday
        precip_values[10] = 0.4  # Next Saturday

        weather_data = pd.DataFrame({
            'date': dates,
            'temp_min': [30] * 14,
            'temp_max': [50] * 14,
            'precip': precip_values
        })

        features = module._extract_weather_features(weather_data, 'full')

        # Should have 3 days with precip > 0.2
        assert features['full_overall_days_of_precip'] == 3
        # Should have 3 weekend days with precip > 0.2 (Sat, Sun, Sat)
        assert features['full_overall_weekend_days_of_precip'] == 3


class TestTrainingWindows:
    """Test training window extraction logic."""

    def test_get_training_windows(self):
        """Test training window selection."""
        module = WeatherFeatureEtlModule()

        # Create 100 days of weather data
        weather_df = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=100),
            'temp_min': list(range(100)),
            'temp_max': list(range(100, 200)),
            'precip': [0.1] * 100
        })

        race_date = datetime(2020, 4, 10)
        full, peak = module._get_training_windows(race_date, weather_df)

        # Full training: 90 days before race (exclusive of race date)
        assert len(full) == 90

        # Peak training: 30 days before race (exclusive of race date)
        assert len(peak) == 30

        # Verify dates are correct
        assert full['date'].max() < race_date
        assert peak['date'].max() < race_date

        # Verify window boundaries
        expected_full_start = race_date - timedelta(days=90)
        assert full['date'].min() >= expected_full_start

        expected_peak_start = race_date - timedelta(days=30)
        assert peak['date'].min() >= expected_peak_start

    def test_get_training_windows_insufficient_data(self):
        """Test training windows with insufficient data."""
        module = WeatherFeatureEtlModule()

        # Only 10 days of data
        weather_df = pd.DataFrame({
            'date': pd.date_range('2020-04-01', periods=10),
            'temp_min': [30] * 10,
            'temp_max': [50] * 10,
            'precip': [0.0] * 10
        })

        race_date = datetime(2020, 4, 10)
        full, peak = module._get_training_windows(race_date, weather_df)

        # Should return less than requested if data unavailable
        assert len(full) < 90
        assert len(peak) < 30


class TestWeatherLookup:
    """Test weather lookup dictionary building."""

    def test_build_weather_lookup(self):
        """Test building weather lookup dictionary."""
        module = WeatherFeatureEtlModule()

        weather_df = pd.DataFrame({
            'city': ['Austin', 'Austin', 'Dallas', 'Dallas'],
            'state': ['TX', 'TX', 'TX', 'TX'],
            'date': ['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02'],
            'temp_min': [30, 32, 28, 30],
            'temp_max': [50, 52, 48, 50],
            'precip': [0.0, 0.1, 0.0, 0.2]
        })

        lookup = module._build_weather_lookup(weather_df)

        # Should have 2 cities
        assert len(lookup) == 2

        # Should normalize to lowercase
        assert ('austin', 'tx') in lookup
        assert ('dallas', 'tx') in lookup

        # Each city should have 2 records
        assert len(lookup[('austin', 'tx')]) == 2
        assert len(lookup[('dallas', 'tx')]) == 2

        # Dates should be parsed
        assert pd.api.types.is_datetime64_any_dtype(lookup[('austin', 'tx')]['date'])


class TestFileTypeDetection:
    """Test file type detection methods."""

    def test_is_race_file(self):
        """Test race file detection."""
        assert WeatherFeatureEtlModule._is_race_file(Path("/data/race_results.csv"))
        assert WeatherFeatureEtlModule._is_race_file(Path("/data/data.csv"))
        assert WeatherFeatureEtlModule._is_race_file(Path("/data/featurized_race.csv"))
        assert not WeatherFeatureEtlModule._is_race_file(Path("/data/weather.csv"))

    def test_is_weather_file(self):
        """Test weather file detection."""
        assert WeatherFeatureEtlModule._is_weather_file(Path("/data/weather_data.csv"))
        assert WeatherFeatureEtlModule._is_weather_file(Path("/data/weather.csv"))
        assert not WeatherFeatureEtlModule._is_weather_file(Path("/data/race.csv"))


class TestWeatherConfig:
    """Test weather configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WeatherConfig()

        assert config.FULL_TRAINING_DAYS == 90
        assert config.PEAK_TRAINING_DAYS == 30
        assert config.MIN_WEATHER_RECORDS == 5
        assert config.PRECIP_THRESHOLD == 0.2
        assert config.EARLIEST_WEATHER_DATE == datetime(2000, 1, 1)

    def test_custom_config(self):
        """Test custom configuration values."""
        config = WeatherConfig(
            FULL_TRAINING_DAYS=60,
            PEAK_TRAINING_DAYS=20,
            PRECIP_THRESHOLD=0.3
        )

        assert config.FULL_TRAINING_DAYS == 60
        assert config.PEAK_TRAINING_DAYS == 20
        assert config.PRECIP_THRESHOLD == 0.3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
