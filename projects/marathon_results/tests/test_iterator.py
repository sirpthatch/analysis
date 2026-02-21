import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil
from src.iterator import CheckpointIterator


class TestCheckpointIterator:
    """Test suite for CheckpointIterator class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_items(self):
        """Sample items for testing."""
        return list(range(1, 21))  # 1 to 20

    @pytest.fixture
    def simple_processor(self):
        """Simple processing function that squares numbers."""
        def process(n):
            return {"number": n, "square": n ** 2}
        return process

    def test_basic_processing(self, temp_dir, sample_items, simple_processor):
        """Test basic processing of items without checkpointing."""
        output_path = Path(temp_dir) / "output.csv"

        iterator = CheckpointIterator(
            items=sample_items,
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=100,  # High value to avoid checkpointing
            key_fields=["number"]
        )

        result_df = iterator.process()

        assert len(result_df) == 20
        assert list(result_df.columns) == ["number", "square"]
        assert result_df["square"].tolist() == [n**2 for n in sample_items]
        assert output_path.exists()

    def test_checkpoint_saving(self, temp_dir, sample_items, simple_processor):
        """Test that checkpoints are saved periodically."""
        output_path = Path(temp_dir) / "checkpoint_test.csv"

        iterator = CheckpointIterator(
            items=sample_items,
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=5,  # Save every 5 items
            key_fields=["number"]
        )

        result_df = iterator.process()

        # Verify final output
        assert len(result_df) == 20
        assert output_path.exists()

        # Load from file and verify
        saved_df = pd.read_csv(output_path)
        assert len(saved_df) == 20
        pd.testing.assert_frame_equal(result_df, saved_df)

    def test_resume_from_checkpoint(self, temp_dir, sample_items, simple_processor):
        """Test resuming from an existing checkpoint."""
        output_path = Path(temp_dir) / "resume_test.csv"

        # First run: process first 10 items
        first_items = sample_items[:10]
        iterator1 = CheckpointIterator(
            items=first_items,
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=5,
            key_fields=["number"]
        )
        iterator1.process()

        # Verify checkpoint exists with 10 items
        assert output_path.exists()
        checkpoint_df = pd.read_csv(output_path)
        assert len(checkpoint_df) == 10

        # Second run: process all 20 items (should skip first 10)
        iterator2 = CheckpointIterator(
            items=sample_items,  # All 20 items
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=5,
            key_fields=["number"]
        )
        result_df = iterator2.process()

        # Should have all 20 items now
        assert len(result_df) == 20
        assert result_df["number"].tolist() == sample_items

    def test_error_handling(self, temp_dir):
        """Test that errors in processing are handled gracefully."""
        output_path = Path(temp_dir) / "error_test.csv"
        items = [1, 2, 3, 4, 5]

        def faulty_processor(n):
            if n == 3:
                raise ValueError("Intentional error on item 3")
            return {"number": n, "double": n * 2}

        iterator = CheckpointIterator(
            items=items,
            item_key=lambda x: (x,),
            process_func=faulty_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["number"]
        )

        result_df = iterator.process()

        # Should have 4 items (skipped the one that errored)
        assert len(result_df) == 4
        assert 3 not in result_df["number"].tolist()

    def test_process_with_dict_items(self, temp_dir):
        """Test processing with dictionary items."""
        output_path = Path(temp_dir) / "dict_test.csv"

        items = [
            {"city": "New York", "state": "NY"},
            {"city": "Los Angeles", "state": "CA"},
            {"city": "Chicago", "state": "IL"}
        ]

        def process_city(item):
            return {
                "city": item["city"],
                "state": item["state"],
                "processed": True
            }

        iterator = CheckpointIterator(
            items=items,
            item_key=lambda x: (x["city"], x["state"]),
            process_func=process_city,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["city", "state"]
        )

        result_df = iterator.process()

        assert len(result_df) == 3
        assert all(result_df["processed"])
        assert result_df["city"].tolist() == ["New York", "Los Angeles", "Chicago"]

    def test_process_with_tuple_items(self, temp_dir):
        """Test processing with tuple items."""
        output_path = Path(temp_dir) / "tuple_test.csv"

        items = [
            ("New York", "NY"),
            ("Los Angeles", "CA"),
            ("Chicago", "IL")
        ]

        def process_tuple(item):
            city, state = item
            return {
                "city": city,
                "state": state,
                "length": len(city)
            }

        iterator = CheckpointIterator(
            items=items,
            item_key=lambda x: tuple(x),
            process_func=process_tuple,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["city", "state"]
        )

        result_df = iterator.process()

        assert len(result_df) == 3
        assert result_df["city"].tolist() == ["New York", "Los Angeles", "Chicago"]

    def test_none_results_are_skipped(self, temp_dir):
        """Test that None results from process_func are not added."""
        output_path = Path(temp_dir) / "none_test.csv"
        items = [1, 2, 3, 4, 5]

        def selective_processor(n):
            # Only process even numbers
            if n % 2 == 0:
                return {"number": n, "value": n * 10}
            return None

        iterator = CheckpointIterator(
            items=items,
            item_key=lambda x: (x,),
            process_func=selective_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["number"]
        )

        result_df = iterator.process()

        # Should only have 2 and 4 (even numbers)
        assert len(result_df) == 2
        assert result_df["number"].tolist() == [2, 4]

    def test_empty_items_list(self, temp_dir, simple_processor):
        """Test processing an empty items list."""
        output_path = Path(temp_dir) / "empty_test.csv"

        iterator = CheckpointIterator(
            items=[],
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=5,
            key_fields=["number"]
        )

        result_df = iterator.process()

        assert len(result_df) == 0
        assert output_path.exists()

    def test_completed_keys_tracking(self, temp_dir, simple_processor):
        """Test that completed keys are tracked correctly."""
        output_path = Path(temp_dir) / "tracking_test.csv"

        # Process first batch
        iterator1 = CheckpointIterator(
            items=[1, 2, 3],
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["number"]
        )
        iterator1.process()

        # Create new iterator with overlapping items
        iterator2 = CheckpointIterator(
            items=[2, 3, 4, 5],  # 2 and 3 already processed
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["number"]
        )

        # Check completed keys before processing
        assert (2,) in iterator2.completed_keys
        assert (3,) in iterator2.completed_keys
        assert (4,) not in iterator2.completed_keys

        result_df = iterator2.process()

        # Should have 1, 2, 3, 4, 5 (added 4 and 5)
        assert len(result_df) == 5
        assert sorted(result_df["number"].tolist()) == [1, 2, 3, 4, 5]

    def test_no_key_fields(self, temp_dir, simple_processor):
        """Test processing without key_fields (no deduplication)."""
        output_path = Path(temp_dir) / "no_key_test.csv"

        # Process items without key_fields
        iterator1 = CheckpointIterator(
            items=[1, 2, 3],
            item_key=None,
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=None
        )
        iterator1.process()

        # Process same items again - should add duplicates
        iterator2 = CheckpointIterator(
            items=[1, 2, 3],
            item_key=None,
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=None
        )
        result_df = iterator2.process()

        # Should have 6 items (duplicates)
        assert len(result_df) == 6

    def test_directory_creation(self, temp_dir, simple_processor):
        """Test that parent directories are created if they don't exist."""
        output_path = Path(temp_dir) / "subdir1" / "subdir2" / "output.csv"

        assert not output_path.parent.exists()

        iterator = CheckpointIterator(
            items=[1, 2, 3],
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["number"]
        )

        iterator.process()

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_large_checkpoint_interval(self, temp_dir, simple_processor):
        """Test with checkpoint interval larger than item count."""
        output_path = Path(temp_dir) / "large_interval.csv"

        iterator = CheckpointIterator(
            items=[1, 2, 3],
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=1000,  # Much larger than item count
            key_fields=["number"]
        )

        result_df = iterator.process()

        # Should still save at the end
        assert len(result_df) == 3
        assert output_path.exists()

    def test_checkpoint_every_one(self, temp_dir, simple_processor):
        """Test with checkpoint_every=1 (save after each item)."""
        output_path = Path(temp_dir) / "every_one.csv"

        iterator = CheckpointIterator(
            items=[1, 2, 3],
            item_key=lambda x: (x,),
            process_func=simple_processor,
            output_path=str(output_path),
            checkpoint_every=1,
            key_fields=["number"]
        )

        result_df = iterator.process()

        assert len(result_df) == 3
        assert output_path.exists()

    def test_pandas_groupby_processing(self, temp_dir):
        """Test processing pandas groupby groups with incremental aggregation."""
        output_path = Path(temp_dir) / "groupby_test.csv"

        # Create sample DataFrame similar to marathon data
        data = {
            'city': ['Boston', 'Boston', 'Boston', 'NYC', 'NYC', 'NYC',
                     'Chicago', 'Chicago', 'LA', 'LA', 'LA', 'LA'],
            'state': ['MA', 'MA', 'MA', 'NY', 'NY', 'NY',
                      'IL', 'IL', 'CA', 'CA', 'CA', 'CA'],
            'runner_id': range(1, 13),
            'time_minutes': [180, 195, 210, 200, 215, 190,
                           205, 220, 175, 185, 195, 200],
            'age': [25, 30, 35, 28, 32, 26,
                   29, 31, 24, 27, 33, 30]
        }
        df = pd.DataFrame(data)

        # Group by city and state
        grouped = df.groupby(['city', 'state'])

        # Convert groups to list of tuples: (group_key, group_dataframe)
        group_items = list(grouped)

        def process_group(group_tuple):
            """Process each group and compute statistics."""
            (city, state), group_df = group_tuple

            # Compute aggregated statistics for this group
            return {
                'city': city,
                'state': state,
                'runner_count': len(group_df),
                'avg_time': group_df['time_minutes'].mean(),
                'min_time': group_df['time_minutes'].min(),
                'max_time': group_df['time_minutes'].max(),
                'avg_age': group_df['age'].mean(),
                'total_runners_processed': len(group_df)
            }

        # Process groups with checkpointing
        iterator = CheckpointIterator(
            items=group_items,
            item_key=lambda x: (x[0][0], x[0][1]),  # Extract (city, state) tuple
            process_func=process_group,
            output_path=str(output_path),
            checkpoint_every=2,
            key_fields=["city", "state"]
        )

        result_df = iterator.process()

        # Verify results
        assert len(result_df) == 4  # 4 unique city/state combinations
        assert set(result_df['city'].tolist()) == {'Boston', 'NYC', 'Chicago', 'LA'}

        # Check Boston stats
        boston_row = result_df[result_df['city'] == 'Boston'].iloc[0]
        assert boston_row['runner_count'] == 3
        assert boston_row['avg_time'] == 195.0  # (180 + 195 + 210) / 3
        assert boston_row['min_time'] == 180
        assert boston_row['max_time'] == 210

        # Check LA stats
        la_row = result_df[result_df['city'] == 'LA'].iloc[0]
        assert la_row['runner_count'] == 4
        assert la_row['avg_time'] == 188.75  # (175 + 185 + 195 + 200) / 4

    def test_pandas_groupby_with_resume(self, temp_dir):
        """Test resuming pandas groupby processing from checkpoint."""
        output_path = Path(temp_dir) / "groupby_resume_test.csv"

        # Create sample DataFrame
        data = {
            'city': ['Boston', 'Boston', 'NYC', 'NYC', 'Chicago', 'Chicago', 'LA', 'LA'],
            'state': ['MA', 'MA', 'NY', 'NY', 'IL', 'IL', 'CA', 'CA'],
            'time_minutes': [180, 195, 200, 215, 205, 220, 175, 185]
        }
        df = pd.DataFrame(data)
        grouped = df.groupby(['city', 'state'])
        all_groups = list(grouped)

        def process_group(group_tuple):
            (city, state), group_df = group_tuple
            return {
                'city': city,
                'state': state,
                'count': len(group_df),
                'avg_time': group_df['time_minutes'].mean()
            }

        # First run: Process only first 2 groups
        first_groups = all_groups[:2]
        iterator1 = CheckpointIterator(
            items=first_groups,
            item_key=lambda x: (x[0][0], x[0][1]),
            process_func=process_group,
            output_path=str(output_path),
            checkpoint_every=1,
            key_fields=["city", "state"]
        )
        result1 = iterator1.process()
        assert len(result1) == 2

        # Second run: Process all groups (should skip first 2)
        iterator2 = CheckpointIterator(
            items=all_groups,
            item_key=lambda x: (x[0][0], x[0][1]),
            process_func=process_group,
            output_path=str(output_path),
            checkpoint_every=1,
            key_fields=["city", "state"]
        )
        result2 = iterator2.process()

        # Should have all 4 groups now
        assert len(result2) == 4
        assert set(result2['city'].tolist()) == {'Boston', 'NYC', 'Chicago', 'LA'}

    def test_pandas_groupby_complex_aggregation(self, temp_dir):
        """Test complex multi-level aggregation with groupby."""
        output_path = Path(temp_dir) / "complex_agg_test.csv"

        # Create more complex dataset
        data = {
            'city': ['Boston'] * 6 + ['NYC'] * 6,
            'state': ['MA'] * 6 + ['NY'] * 6,
            'year': [2020, 2020, 2021, 2021, 2022, 2022] * 2,
            'time_minutes': [180, 185, 190, 195, 175, 180, 200, 205, 210, 215, 195, 200],
            'age_group': ['young', 'old', 'young', 'old', 'young', 'old'] * 2
        }
        df = pd.DataFrame(data)

        # Group by city/state
        grouped = df.groupby(['city', 'state'])
        group_items = list(grouped)

        def complex_processor(group_tuple):
            """Compute complex nested statistics."""
            (city, state), group_df = group_tuple

            # Compute statistics by year
            yearly_stats = group_df.groupby('year')['time_minutes'].agg(['mean', 'min', 'max'])

            # Compute statistics by age group
            age_stats = group_df.groupby('age_group')['time_minutes'].mean()

            return {
                'city': city,
                'state': state,
                'total_count': len(group_df),
                'overall_avg': group_df['time_minutes'].mean(),
                'num_years': group_df['year'].nunique(),
                'young_avg': age_stats.get('young', None),
                'old_avg': age_stats.get('old', None),
                'fastest_year': yearly_stats['min'].idxmin(),
                'fastest_time': yearly_stats['min'].min()
            }

        iterator = CheckpointIterator(
            items=group_items,
            item_key=lambda x: (x[0][0], x[0][1]),
            process_func=complex_processor,
            output_path=str(output_path),
            checkpoint_every=1,
            key_fields=["city", "state"]
        )

        result_df = iterator.process()

        # Verify complex aggregations
        assert len(result_df) == 2

        boston_row = result_df[result_df['city'] == 'Boston'].iloc[0]
        assert boston_row['total_count'] == 6
        assert boston_row['num_years'] == 3
        assert boston_row['fastest_year'] == 2022  # 175 was the fastest
        assert boston_row['fastest_time'] == 175


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
