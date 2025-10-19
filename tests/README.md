# MINDS Test Suite

This directory contains comprehensive tests for the MINDS framework.

## Test Structure

- `conftest.py` - Shared fixtures and test configuration
- `test_database.py` - DatabaseManager tests (connection, queries, schema operations)
- `test_update.py` - MINDSUpdater tests (GDC downloads, extraction)
- `test_aggregator.py` - Aggregator tests (manifest generation, API calls)
- `test_downloader.py` - Downloader tests (GDC, IDC, TCIA file downloads)
- `test_cohort.py` - Cohort class tests (initialization, downloads, stats)
- `test_api.py` - Main API tests (query, get_tables, build_cohort, etc.)
- `test_integration.py` - End-to-end workflow tests

## Running Tests

### Quick Run
```bash
./run_tests.sh
```

### All Tests with Verbose Output
```bash
pytest tests/ -v
```

### With Coverage Report
```bash
pytest tests/ --cov=app/med_minds --cov-report=html
```

### Specific Test File
```bash
pytest tests/test_database.py -v
```

### Specific Test
```bash
pytest tests/test_database.py::TestDatabaseManager::test_execute_query -v
```

### Stop on First Failure
```bash
pytest tests/ -x
```

## Test Categories

Tests can be run by marker:

```bash
# Unit tests only
pytest tests/ -m unit

# Integration tests only
pytest tests/ -m integration

# Skip slow tests
pytest tests/ -m "not slow"
```

## Coverage

After running tests with coverage, open `htmlcov/index.html` in your browser to view detailed coverage reports.

Current coverage target: 80%+

## Test Dependencies

- pytest
- pytest-cov (coverage reports)
- pytest-mock (mocking)
- responses (HTTP mocking)

Install with:
```bash
pip install -e ".[test]"
```

## Writing New Tests

1. Create test file in `tests/` directory with `test_` prefix
2. Create test classes with `Test` prefix
3. Create test methods with `test_` prefix
4. Use fixtures from `conftest.py` for common test data
5. Mock all external dependencies (database, APIs, file system)

Example:
```python
def test_my_feature(sample_cohort_data, temp_dir):
    """Test description."""
    # Arrange
    cohort = Cohort(sample_cohort_data, temp_dir)

    # Act
    result = cohort.some_method()

    # Assert
    assert result == expected_value
```

## Continuous Integration

Tests are designed to run without external dependencies (database, APIs) and can be integrated into CI/CD pipelines.

## Troubleshooting

If tests fail:

1. Ensure all dependencies are installed: `pip install -e ".[test]"`
2. Check that you're in the project root directory
3. Verify Python version (3.7+)
4. Clear pytest cache: `pytest --cache-clear`
5. Check for conflicting environment variables

For more help, see the main CLAUDE.md file or pytest documentation.
