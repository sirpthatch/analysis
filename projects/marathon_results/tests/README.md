# Tests for Marathon Results Project

This directory contains test files for the marathon results analysis project.

## Running Tests

### Install Dependencies

First, ensure you have pytest installed:

```bash
pip install pytest pytest-cov
```

### Run All Tests

```bash
# From the project root directory
pytest tests/

# With verbose output
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=html
```

### Run Specific Test File

```bash
pytest tests/test_iterator.py -v
```

### Run Specific Test

```bash
pytest tests/test_iterator.py::TestCheckpointIterator::test_basic_processing -v
```

## Test Structure

- `test_iterator.py`: Tests for the CheckpointIterator class
  - Basic processing functionality
  - Checkpoint saving and resuming
  - Error handling
  - Different item types (dict, tuple, list)
  - Key tracking and deduplication
  - Edge cases (empty lists, None results, etc.)

## Adding New Tests

When adding new tests:

1. Create test files with the prefix `test_`
2. Create test classes with the prefix `Test`
3. Create test methods with the prefix `test_`
4. Use fixtures for common setup/teardown
5. Use descriptive test names that explain what is being tested
