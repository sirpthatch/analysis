"""
Unit tests for JoinEtlModule.

Tests normalization, validation, and join key generation functions.
"""

import pytest
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from process_join import JoinEtlModule
from etl_utils import DateParser, TextNormalizer, TimeConverter


class TestTextNormalizer:
    """Test text normalization utilities."""

    def test_normalize_race_name_basic(self):
        """Test basic race name normalization."""
        assert TextNormalizer.normalize_race_name("Austin Marathon") == "austin_marathon"
        assert TextNormalizer.normalize_race_name("Dog Lake Marathon") == "dog_lake_marathon"

    def test_normalize_race_name_with_quotes(self):
        """Test race name normalization with quotes."""
        assert TextNormalizer.normalize_race_name("Last Chance for 'Boston'") == "last_chance_for_boston"
        assert TextNormalizer.normalize_race_name('The "Big" Marathon') == "the_big_marathon"

    def test_normalize_race_name_multiple_spaces(self):
        """Test race name with multiple spaces."""
        assert TextNormalizer.normalize_race_name("New   York   Marathon") == "new___york___marathon"

    def test_normalize_race_name_empty(self):
        """Test empty or null race names."""
        assert TextNormalizer.normalize_race_name("") == ""
        assert TextNormalizer.normalize_race_name(None) == ""
        assert TextNormalizer.normalize_race_name(pd.NA) == ""

    def test_normalize_location(self):
        """Test location normalization."""
        assert TextNormalizer.normalize_location("Austin", "TX") == ("austin", "tx")
        assert TextNormalizer.normalize_location("New York", "NY") == ("new york", "ny")
        assert TextNormalizer.normalize_location("SAN FRANCISCO", "CA") == ("san francisco", "ca")

    def test_normalize_location_with_none(self):
        """Test location normalization with None values."""
        assert TextNormalizer.normalize_location(None, "TX") == ("", "tx")
        assert TextNormalizer.normalize_location("Austin", None) == ("austin", "")
        assert TextNormalizer.normalize_location(None, None) == ("", "")


class TestDateParser:
    """Test date parsing utilities."""

    def test_parse_marathon_results_date(self):
        """Test parsing YYYY-MM-DD format."""
        result = DateParser.parse_marathon_results_date("2008-02-17")
        assert result is not None
        assert result.year == 2008
        assert result.month == 2
        assert result.day == 17

    def test_parse_marathon_results_date_invalid(self):
        """Test invalid marathon results dates."""
        assert DateParser.parse_marathon_results_date("invalid") is None
        assert DateParser.parse_marathon_results_date("") is None
        assert DateParser.parse_marathon_results_date(None) is None
        assert DateParser.parse_marathon_results_date("2008-13-01") is None  # Invalid month

    def test_parse_race_final_date(self):
        """Test parsing MM_DD_YY format."""
        result = DateParser.parse_race_final_date("10_15_16")
        assert result is not None
        assert result.year == 2016
        assert result.month == 10
        assert result.day == 15

    def test_parse_race_final_date_edge_cases(self):
        """Test edge cases for race final dates."""
        # Year 2000
        result = DateParser.parse_race_final_date("01_01_00")
        assert result is not None
        assert result.year == 2000

        # Year 2099
        result = DateParser.parse_race_final_date("12_31_99")
        assert result is not None
        assert result.year == 2099

    def test_parse_race_final_date_invalid(self):
        """Test invalid race final dates."""
        assert DateParser.parse_race_final_date("invalid") is None
        assert DateParser.parse_race_final_date("") is None
        assert DateParser.parse_race_final_date(None) is None
        assert DateParser.parse_race_final_date("10-15-16") is None  # Wrong separator


class TestTimeConverter:
    """Test time conversion utilities."""

    def test_hms_to_minutes_hhmmss(self):
        """Test HH:MM:SS conversion."""
        result = TimeConverter.hms_to_minutes("02:20:38")
        assert result is not None
        assert abs(result - 140.63333) < 0.01

    def test_hms_to_minutes_mmss(self):
        """Test MM:SS conversion."""
        result = TimeConverter.hms_to_minutes("45:30")
        assert result is not None
        assert abs(result - 45.5) < 0.01

    def test_hms_to_minutes_whole_hours(self):
        """Test whole hours."""
        result = TimeConverter.hms_to_minutes("3:15:00")
        assert result is not None
        assert result == 195.0

    def test_hms_to_minutes_invalid(self):
        """Test invalid time formats."""
        assert TimeConverter.hms_to_minutes("invalid") is None
        assert TimeConverter.hms_to_minutes("") is None
        assert TimeConverter.hms_to_minutes(None) is None
        assert TimeConverter.hms_to_minutes("1:2:3:4") is None  # Too many parts

    def test_minutes_to_hms(self):
        """Test minutes to HH:MM:SS conversion."""
        assert TimeConverter.minutes_to_hms(140.63333) == "2:20:38"
        assert TimeConverter.minutes_to_hms(195.0) == "3:15:00"
        assert TimeConverter.minutes_to_hms(45.5) == "0:45:30"

    def test_minutes_to_hms_invalid(self):
        """Test invalid minutes values."""
        assert TimeConverter.minutes_to_hms(None) == ""
        assert TimeConverter.minutes_to_hms(pd.NA) == ""


class TestJoinEtlModule:
    """Test JoinEtlModule functionality."""

    def test_create_join_key(self):
        """Test join key generation."""
        key = JoinEtlModule._create_join_key(
            "austin_marathon",
            datetime(2008, 2, 17),
            "austin",
            "tx"
        )
        assert key == "austin_marathon__2008-02-17__austin__tx"

    def test_create_join_key_with_none_date(self):
        """Test join key with None date."""
        key = JoinEtlModule._create_join_key(
            "test_race",
            None,
            "test_city",
            "ts"
        )
        assert key == "test_race____test_city__ts"

    def test_create_join_key_with_empty_location(self):
        """Test join key with empty location."""
        key = JoinEtlModule._create_join_key(
            "test_race",
            datetime(2020, 1, 1),
            "",
            ""
        )
        assert key == "test_race__2020-01-01____"

    def test_create_join_key_record_level(self):
        """Full key identifies an entry, not a race-city cell."""
        key = JoinEtlModule._create_join_key(
            "austin_marathon", datetime(2008, 2, 17), "austin", "tx", 34, "M", 201.5
        )
        assert key == "austin_marathon__2008-02-17__austin__tx__34__M__201.5"

    def test_create_join_key_distinguishes_runners_in_same_city(self):
        """Regression: the location-only key collapsed 72% of race_final runners.

        Two different runners from the same hometown in the same race must not
        share a join key, or deduplication deletes one of them.
        """
        common = ("boston_marathon", datetime(2019, 4, 15), "boston", "ma")
        a = JoinEtlModule._create_join_key(*common, 34, "M", 201.5)
        b = JoinEtlModule._create_join_key(*common, 41, "F", 233.0)
        assert a != b

    def test_create_join_keys_vec_matches_scalar(self):
        """Vectorized path must agree with the scalar implementation."""
        df = pd.DataFrame({
            "race": ["austin_marathon", "boston_marathon"],
            "date": [datetime(2008, 2, 17), datetime(2019, 4, 15)],
            "city": ["austin", "boston"],
            "state": ["tx", "ma"],
            "age": [34, None],
            "sex": ["M", "F"],
            "time_minutes": [201.5, 233.0],
        })
        vec = JoinEtlModule._create_join_keys_vec(df).tolist()
        scalar = [
            JoinEtlModule._create_join_key(r.race, r.date, r.city, r.state,
                                           r.age, r.sex, r.time_minutes)
            for r in df.itertuples()
        ]
        assert vec == scalar

    def test_dedup_preserves_distinct_runners(self):
        """Deduplicating on the record-level key keeps distinct runners."""
        df = pd.DataFrame({
            "race": ["r"] * 3, "date": [datetime(2019, 4, 15)] * 3,
            "city": ["boston"] * 3, "state": ["ma"] * 3,
            "age": [34, 41, 34], "sex": ["M", "F", "M"],
            "time_minutes": [201.5, 233.0, 201.5],   # row 3 duplicates row 1
        })
        df["join_key"] = JoinEtlModule._create_join_keys_vec(df)
        assert df["join_key"].nunique() == 2

    def test_validate_schemas_success(self):
        """Test schema validation with valid DataFrames."""
        module = JoinEtlModule()

        df_marathon = pd.DataFrame({
            'name': ['John'],
            'age': [30],
            'sex': ['M'],
            'hometown_city': ['Austin'],
            'hometown_state': ['TX'],
            'time': ['2:30:00'],
            'race_name': ['Austin Marathon'],
            'race_date': ['2020-01-01']
        })

        df_race = pd.DataFrame({
            'age': [30],
            'sex': ['M'],
            'time': [150.0],
            'race': ['austin_marathon'],
            'date': ['01_01_20'],
            'city': ['austin'],
            'state': ['tx']
        })

        # Should not raise
        module._validate_schemas(df_marathon, df_race)

    def test_validate_schemas_missing_columns(self):
        """Test schema validation with missing columns."""
        module = JoinEtlModule()

        df_marathon = pd.DataFrame({'name': ['John']})
        df_race = pd.DataFrame({'age': [30]})

        with pytest.raises(ValueError) as exc_info:
            module._validate_schemas(df_marathon, df_race)

        assert "missing columns" in str(exc_info.value).lower()

    def test_partition(self):
        """Test partition method returns global partition."""
        module = JoinEtlModule()

        paths = [Path("/test/file1.csv"), Path("/test/file2.csv")]
        result = module.partition(paths)

        assert len(result) == 2
        assert all(partitions == ["global"] for partitions in result.values())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
