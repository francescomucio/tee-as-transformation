"""
Pytest configuration and shared fixtures for TEE tests.
"""

import pytest
import tempfile
import os
from typing import Dict, Any

from tee.engine.config import AdapterConfig
from tee.engine.model_state import ModelStateManager
from tee.adapters.duckdb.adapter import DuckDBAdapter


@pytest.fixture
def temp_db_path():
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def temp_state_db_path():
    """Create a temporary state database file."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def duckdb_config():
    """Create DuckDB configuration."""
    return {
        "type": "duckdb",
        "path": ":memory:",
        "schema": "test_schema"
    }


@pytest.fixture
def duckdb_adapter(duckdb_config):
    """Create DuckDB adapter instance."""
    adapter = DuckDBAdapter(duckdb_config)
    adapter.connect()
    yield adapter
    adapter.disconnect()


@pytest.fixture
def state_manager(temp_state_db_path):
    """Create state manager instance."""
    manager = ModelStateManager(state_db_path=temp_state_db_path)
    yield manager
    manager.close()


@pytest.fixture
def sample_append_config() -> Dict[str, Any]:
    """Sample append configuration."""
    return {
        "time_column": "created_at",
        "start_date": "2024-01-01",
        "lookback": "7 days"
    }


@pytest.fixture
def sample_merge_config() -> Dict[str, Any]:
    """Sample merge configuration."""
    return {
        "unique_key": ["id"],
        "time_column": "updated_at",
        "start_date": "auto",
        "lookback": "3 hours"
    }


@pytest.fixture
def sample_delete_insert_config() -> Dict[str, Any]:
    """Sample delete+insert configuration."""
    return {
        "where_condition": "updated_at >= @start_date",
        "time_column": "updated_at",
        "start_date": "@start_date"
    }


@pytest.fixture
def sample_incremental_config() -> Dict[str, Any]:
    """Sample incremental configuration."""
    return {
        "strategy": "append",
        "append": {
            "time_column": "created_at",
            "start_date": "2024-01-01",
            "lookback": "7 days"
        }
    }


@pytest.fixture
def sample_table_sql():
    """Sample table creation SQL."""
    return """
    CREATE TABLE test_schema.source_table (
        id INTEGER,
        name VARCHAR,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        status VARCHAR
    )
    """


@pytest.fixture
def sample_data_sql():
    """Sample data insertion SQL."""
    return """
    INSERT INTO test_schema.source_table VALUES
    (1, 'Alice', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 'active'),
    (2, 'Bob', '2024-01-02 11:00:00', '2024-01-02 11:00:00', 'active'),
    (3, 'Charlie', '2024-01-03 12:00:00', '2024-01-03 12:00:00', 'inactive'),
    (4, 'David', '2024-01-04 13:00:00', '2024-01-04 13:00:00', 'active'),
    (5, 'Eve', '2024-01-05 14:00:00', '2024-01-05 14:00:00', 'active')
    """


@pytest.fixture
def large_dataset_sql():
    """Large dataset SQL for performance testing."""
    return """
    CREATE TABLE test_schema.large_source_table AS
    SELECT 
        row_number() OVER () as id,
        'User ' || row_number() OVER () as name,
        '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as created_at,
        '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as updated_at,
        CASE WHEN row_number() OVER () % 2 = 0 THEN 'active' ELSE 'inactive' END as status
    FROM generate_series(1, 1000)
    """


# Pytest configuration
def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers."""
    for item in items:
        # Add unit marker to tests in test_incremental_executor.py
        if "test_incremental_executor.py" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Add integration marker to tests in test_duckdb_incremental.py
        if "test_duckdb_incremental.py" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Add slow marker to performance tests
        if "performance" in item.name or "large_dataset" in item.name:
            item.add_marker(pytest.mark.slow)
