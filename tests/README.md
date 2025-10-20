# Tee Test Suite

This directory contains comprehensive tests for the Tee framework, with a focus on incremental materialization functionality.

## Test Structure

```
tests/
├── conftest.py                           # Shared fixtures and configuration
├── run_incremental_tests.py              # Test runner script
├── README.md                             # This file
├── engine/                               # Core engine tests
│   ├── test_incremental_executor.py      # Incremental executor unit tests
│   └── test_incremental_adapter_interface.py  # Adapter interface tests
├── adapters/                             # Database adapter tests
│   ├── test_duckdb_incremental.py        # DuckDB-specific tests
│   └── test_metadata_propagation.py     # Metadata propagation tests
├── parser/                               # Parser tests
└── typing/                               # Type system tests
```

## Test Categories

### 1. Unit Tests (`test_incremental_executor.py`)

Tests the core incremental logic independently of database implementations:

- **Strategy Logic**: Tests for append, merge, and delete+insert strategies
- **Time Filtering**: Tests for time-based filtering and lookback logic
- **Variable Resolution**: Tests for CLI variable resolution (`@variable` and `{{ variable }}`)
- **State Management**: Tests for incremental state tracking
- **Error Handling**: Tests for error conditions and fallbacks

### 2. Adapter Interface Tests (`test_incremental_adapter_interface.py`)

Tests that verify adapters correctly implement the incremental interface:

- **Interface Compliance**: Tests that adapters implement required methods
- **Behavior Patterns**: Tests for expected adapter behavior
- **Error Handling**: Tests for adapter error handling
- **Fallback Behavior**: Tests for fallback when incremental methods aren't available
- **Data Types**: Tests for proper parameter type handling
- **Performance**: Tests for performance characteristics

### 3. Database-Specific Tests (`test_duckdb_incremental.py`)

Tests specific to DuckDB implementation (template for other adapters):

- **SQL Generation**: Tests for database-specific SQL generation
- **Data Operations**: Tests for actual data operations
- **Schema Handling**: Tests for schema qualification and table creation
- **Performance**: Tests for performance with large datasets
- **Error Scenarios**: Tests for database-specific error conditions

## Running Tests

### Using the Test Runner

```bash
# Run all incremental tests
uv run python tests/run_incremental_tests.py all

# Run unit tests only
uv run python tests/run_incremental_tests.py unit

# Run adapter interface tests
uv run python tests/run_incremental_tests.py adapter

# Run DuckDB integration tests
uv run python tests/run_incremental_tests.py duckdb

# Run performance tests
uv run python tests/run_incremental_tests.py performance

# Run with coverage
uv run python tests/run_incremental_tests.py coverage

# Run specific test
uv run python tests/run_incremental_tests.py specific --test-path tests/engine/test_incremental_executor.py::TestShouldRunIncremental
```

### Using pytest directly

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/engine/test_incremental_executor.py -v

# Run specific test class
uv run pytest tests/engine/test_incremental_executor.py::TestShouldRunIncremental -v

# Run specific test method
uv run pytest tests/engine/test_incremental_executor.py::TestShouldRunIncremental::test_no_state_exists_runs_full_load -v

# Run with coverage
uv run pytest tests/ --cov=tcli.engine.incremental_executor --cov-report=html

# Run only unit tests
uv run pytest tests/ -m unit -v

# Run only integration tests
uv run pytest tests/ -m integration -v

# Skip slow tests
uv run pytest tests/ -m "not slow" -v
```

## Test Markers

Tests are marked with categories for easy filtering:

- `@pytest.mark.unit`: Unit tests (fast, isolated)
- `@pytest.mark.integration`: Integration tests (require database)
- `@pytest.mark.slow`: Performance tests (may take longer)

## Test Fixtures

### Shared Fixtures (`conftest.py`)

- `temp_db_path`: Temporary DuckDB database file
- `temp_state_db_path`: Temporary state database file
- `duckdb_config`: DuckDB adapter configuration
- `duckdb_adapter`: Connected DuckDB adapter instance
- `state_manager`: Model state manager instance
- `sample_*_config`: Sample configurations for different strategies
- `sample_table_sql`: Sample table creation SQL
- `sample_data_sql`: Sample data insertion SQL

### Usage Example

```python
def test_my_feature(duckdb_adapter, sample_append_config):
    """Test using shared fixtures."""
    # Use duckdb_adapter and sample_append_config
    result = duckdb_adapter.execute_incremental_append("table", "SELECT 1")
    assert result is not None
```

## Writing New Tests

### For Core Logic

Add tests to `test_incremental_executor.py`:

```python
def test_new_feature(executor, sample_config):
    """Test new incremental feature."""
    result = executor.some_method(sample_config)
    assert result == expected_value
```

### For Adapter Interface

Add tests to `test_incremental_adapter_interface.py`:

```python
def test_new_adapter_behavior(mock_adapter):
    """Test new adapter behavior."""
    mock_adapter.some_method.return_value = expected_value
    result = mock_adapter.some_method("param")
    assert result == expected_value
```

### For Database-Specific Features

Add tests to `test_duckdb_incremental.py` (or create new adapter test file):

```python
def test_duckdb_specific_feature(duckdb_adapter, sample_table_sql):
    """Test DuckDB-specific feature."""
    duckdb_adapter.execute_query(sample_table_sql)
    result = duckdb_adapter.some_duckdb_method()
    assert result == expected_value
```

## Test Data

### Sample Data Structure

Tests use a consistent sample data structure:

```sql
CREATE TABLE test_schema.source_table (
    id INTEGER,
    name VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status VARCHAR
);

INSERT INTO test_schema.source_table VALUES
(1, 'Alice', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 'active'),
(2, 'Bob', '2024-01-02 11:00:00', '2024-01-02 11:00:00', 'active'),
(3, 'Charlie', '2024-01-03 12:00:00', '2024-01-03 12:00:00', 'inactive'),
(4, 'David', '2024-01-04 13:00:00', '2024-01-04 13:00:00', 'active'),
(5, 'Eve', '2024-01-05 14:00:00', '2024-01-05 14:00:00', 'active');
```

### Performance Test Data

For performance tests, use the `large_dataset_sql` fixture:

```python
def test_performance(duckdb_adapter, large_dataset_sql):
    """Test with large dataset."""
    duckdb_adapter.execute_query(large_dataset_sql)
    # Run performance test
```

## Best Practices

### 1. Test Isolation

- Each test should be independent
- Use fixtures for setup/teardown
- Clean up temporary files and databases

### 2. Test Naming

- Use descriptive test names
- Follow pattern: `test_what_behavior_when_condition`
- Group related tests in classes

### 3. Test Coverage

- Test happy path scenarios
- Test error conditions
- Test edge cases
- Test performance characteristics

### 4. Mock Usage

- Mock external dependencies
- Mock database operations in unit tests
- Use real database operations in integration tests

### 5. Assertions

- Use specific assertions
- Test both positive and negative cases
- Verify side effects (state changes, method calls)

## Continuous Integration

Tests are designed to run in CI environments:

- No external dependencies beyond test databases
- Temporary files are cleaned up automatically
- Tests are marked for different execution environments
- Performance tests can be skipped in fast CI runs

## Debugging Tests

### Running with Debug Output

```bash
# Run with maximum verbosity
uv run pytest tests/ -vvv --tb=long

# Run specific test with debug
uv run pytest tests/engine/test_incremental_executor.py::TestShouldRunIncremental::test_no_state_exists_runs_full_load -vvv --tb=long
```

### Using pytest Debugging

```python
def test_debug_example(duckdb_adapter):
    """Example of debugging test."""
    import pdb; pdb.set_trace()  # Set breakpoint
    result = duckdb_adapter.some_method()
    assert result == expected_value
```

### Logging in Tests

```python
import logging

def test_with_logging(duckdb_adapter):
    """Test with logging enabled."""
    logging.basicConfig(level=logging.DEBUG)
    result = duckdb_adapter.some_method()
    assert result == expected_value
```

## Contributing

When adding new tests:

1. **Follow existing patterns** in the test files
2. **Add appropriate markers** for test categorization
3. **Update this README** if adding new test categories
4. **Ensure tests are isolated** and don't depend on each other
5. **Add docstrings** explaining what each test verifies
6. **Use descriptive names** for test methods and variables
