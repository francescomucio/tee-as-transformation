"""
Test cases for DuckDB incremental materialization.

These tests verify DuckDB-specific incremental functionality and can serve
as a template for testing other database adapters.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from typing import Dict, Any

from tee.adapters.duckdb.adapter import DuckDBAdapter
from tee.typing.metadata import IncrementalAppendConfig, IncrementalMergeConfig, IncrementalDeleteInsertConfig


class TestDuckDBIncremental:
    """Test cases for DuckDB incremental materialization."""
    
    @pytest.fixture
    def sample_table_sql(self):
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
    def sample_data_sql(self):
        """Sample data insertion SQL."""
        return """
        INSERT INTO test_schema.source_table VALUES
        (1, 'Alice', '2024-01-01 10:00:00', '2024-01-01 10:00:00', 'active'),
        (2, 'Bob', '2024-01-02 11:00:00', '2024-01-02 11:00:00', 'active'),
        (3, 'Charlie', '2024-01-03 12:00:00', '2024-01-03 12:00:00', 'inactive'),
        (4, 'David', '2024-01-04 13:00:00', '2024-01-04 13:00:00', 'active'),
        (5, 'Eve', '2024-01-05 14:00:00', '2024-01-05 14:00:00', 'active')
        """
    
    def setup_test_data(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Set up test data in the database."""
        # Create source table
        duckdb_adapter.execute_query(sample_table_sql)
        # Insert sample data
        duckdb_adapter.execute_query(sample_data_sql)
    
    def test_get_table_columns(self, duckdb_adapter, sample_table_sql):
        """Test getting table columns."""
        # Create schema first
        duckdb_adapter.execute_query("CREATE SCHEMA IF NOT EXISTS test_schema")
        
        # Create table
        duckdb_adapter.execute_query(sample_table_sql)
        
        # Get columns
        columns = duckdb_adapter.get_table_columns("test_schema.source_table")
        
        # Verify columns
        expected_columns = ["id", "name", "created_at", "updated_at", "status"]
        assert columns == expected_columns
    
    def test_get_table_columns_nonexistent_table(self, duckdb_adapter):
        """Test getting columns for non-existent table returns fallback."""
        columns = duckdb_adapter.get_table_columns("nonexistent_table")
        
        # Should return fallback columns
        expected_columns = ["id", "name", "created_at", "updated_at", "status"]
        assert columns == expected_columns
    
    def test_execute_incremental_append(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental append strategy."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table first
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT * FROM test_schema.source_table WHERE 1=0
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental append
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active' AND created_at >= '2024-01-01'
        """
        duckdb_adapter.execute_incremental_append("test_schema.target_table", source_sql)
        
        # Verify data was inserted
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # 4 active records
    
    def test_execute_incremental_merge(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental merge strategy."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table with initial data
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE id IN (1, 2)
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental merge
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        config = {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "1 hour"
        }
        duckdb_adapter.execute_incremental_merge("test_schema.target_table", source_sql, config)
        
        # Verify data was merged (should have all active records)
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # All 4 active records
    
    def test_execute_incremental_merge_with_updates(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental merge with updates to existing records."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table with initial data
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE id IN (1, 2)
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Update source data to simulate changes
        update_sql = """
        UPDATE test_schema.source_table
        SET name = 'Alice Updated', updated_at = '2024-01-06 15:00:00'
        WHERE id = 1
        """
        duckdb_adapter.execute_query(update_sql)
        
        # Execute incremental merge
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        config = {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "1 hour"
        }
        duckdb_adapter.execute_incremental_merge("test_schema.target_table", source_sql, config)
        
        # Verify the record was updated
        result = duckdb_adapter.execute_query("SELECT name FROM test_schema.target_table WHERE id = 1")
        name = result.fetchone()[0]
        assert name == "Alice Updated"
    
    def test_execute_incremental_delete_insert(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental delete+insert strategy."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table with initial data
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE id IN (1, 2, 3)
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental delete+insert
        delete_sql = "DELETE FROM test_schema.target_table WHERE updated_at >= '2024-01-02'"
        insert_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE updated_at >= '2024-01-02' AND status = 'active'
        """
        duckdb_adapter.execute_incremental_delete_insert("test_schema.target_table", delete_sql, insert_sql)
        
        # Verify data was deleted and re-inserted
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 3  # 3 active records from 2024-01-02 onwards
    
    def test_generate_merge_sql(self, duckdb_adapter):
        """Test MERGE SQL generation."""
        table_name = "test_schema.target_table"
        source_sql = "SELECT id, name, created_at, updated_at, status FROM test_schema.source_table"
        unique_key = ["id"]
        columns = ["id", "name", "created_at", "updated_at", "status"]
        
        merge_sql = duckdb_adapter._generate_merge_sql(table_name, source_sql, unique_key, columns)
        
        # Verify SQL structure
        assert "MERGE INTO" in merge_sql
        assert "USING" in merge_sql
        assert "ON target.id = source.id" in merge_sql
        assert "WHEN MATCHED THEN UPDATE SET" in merge_sql
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    
    def test_generate_merge_sql_with_multiple_unique_keys(self, duckdb_adapter):
        """Test MERGE SQL generation with multiple unique keys."""
        table_name = "test_schema.target_table"
        source_sql = "SELECT id, name, created_at, updated_at, status FROM test_schema.source_table"
        unique_key = ["id", "name"]
        columns = ["id", "name", "created_at", "updated_at", "status"]
        
        merge_sql = duckdb_adapter._generate_merge_sql(table_name, source_sql, unique_key, columns)
        
        # Verify SQL structure with multiple keys
        assert "ON target.id = source.id AND target.name = source.name" in merge_sql
    
    def test_incremental_append_with_existing_table(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental append when target table already exists."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table with some data
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE id IN (1, 2)
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental append
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE id IN (3, 4, 5) AND status = 'active'
        """
        duckdb_adapter.execute_incremental_append("test_schema.target_table", source_sql)
        
        # Verify data was appended
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # 2 original + 2 new active records
    
    def test_incremental_merge_with_empty_target(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental merge when target table is empty."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create empty target table
        target_sql = """
        CREATE TABLE test_schema.target_table (
            id INTEGER,
            name VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            status VARCHAR
        )
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental merge
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        config = {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "1 hour"
        }
        duckdb_adapter.execute_incremental_merge("test_schema.target_table", source_sql, config)
        
        # Verify data was inserted
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # All 4 active records
    
    def test_incremental_delete_insert_with_empty_target(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test incremental delete+insert when target table is empty."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create empty target table
        target_sql = """
        CREATE TABLE test_schema.target_table (
            id INTEGER,
            name VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            status VARCHAR
        )
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental delete+insert
        delete_sql = "DELETE FROM test_schema.target_table WHERE updated_at >= '2024-01-01'"
        insert_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE updated_at >= '2024-01-01' AND status = 'active'
        """
        duckdb_adapter.execute_incremental_delete_insert("test_schema.target_table", delete_sql, insert_sql)
        
        # Verify data was inserted
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # All 4 active records
    
    def test_error_handling_invalid_sql(self, duckdb_adapter):
        """Test error handling with invalid SQL."""
        with pytest.raises(Exception):
            duckdb_adapter.execute_incremental_append("test_table", "INVALID SQL")
    
    def test_error_handling_nonexistent_table(self, duckdb_adapter):
        """Test error handling with non-existent table."""
        with pytest.raises(Exception):
            duckdb_adapter.execute_incremental_merge("nonexistent_table", "SELECT 1", {"unique_key": ["id"]})
    
    def test_schema_qualification(self, duckdb_adapter, sample_table_sql, sample_data_sql):
        """Test that table names are properly qualified with schema."""
        # Set up test data
        self.setup_test_data(duckdb_adapter, sample_table_sql, sample_data_sql)
        
        # Create target table
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT * FROM test_schema.source_table WHERE 1=0
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental append
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        duckdb_adapter.execute_incremental_append("test_schema.target_table", source_sql)
        
        # Verify the operation succeeded (no exception raised)
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 4  # 4 active records


class TestDuckDBIncrementalPerformance:
    """Performance test cases for DuckDB incremental materialization."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
            temp_path = f.name
        yield temp_path
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.fixture
    def duckdb_config(self, temp_db_path):
        """Create DuckDB configuration."""
        return {
            "type": "duckdb",
            "path": temp_db_path,
            "schema": "test_schema"
        }
    
    @pytest.fixture
    def duckdb_adapter(self, duckdb_config):
        """Create DuckDB adapter instance."""
        adapter = DuckDBAdapter(duckdb_config)
        adapter.connect()
        yield adapter
        adapter.disconnect()
    
    def test_large_dataset_append_performance(self, duckdb_adapter):
        """Test append performance with large dataset."""
        # Create source table with many rows
        create_sql = """
        CREATE TABLE test_schema.source_table AS
        SELECT 
            row_number() OVER () as id,
            'User ' || row_number() OVER () as name,
            '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as created_at,
            '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as updated_at,
            CASE WHEN row_number() OVER () % 2 = 0 THEN 'active' ELSE 'inactive' END as status
        FROM generate_series(1, 1000)
        """
        duckdb_adapter.execute_query(create_sql)
        
        # Create target table
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT * FROM test_schema.source_table WHERE 1=0
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental append
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        
        import time
        start_time = time.time()
        duckdb_adapter.execute_incremental_append("test_schema.target_table", source_sql)
        end_time = time.time()
        
        # Verify data was inserted
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 500  # Half should be active
        
        # Performance should be reasonable (less than 1 second for 1000 rows)
        execution_time = end_time - start_time
        assert execution_time < 1.0, f"Execution took {execution_time:.2f} seconds, expected < 1.0"
    
    def test_large_dataset_merge_performance(self, duckdb_adapter):
        """Test merge performance with large dataset."""
        # Create source table with many rows
        create_sql = """
        CREATE TABLE test_schema.source_table AS
        SELECT 
            row_number() OVER () as id,
            'User ' || row_number() OVER () as name,
            '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as created_at,
            '2024-01-01'::timestamp + (row_number() OVER () * interval '1 hour') as updated_at,
            CASE WHEN row_number() OVER () % 2 = 0 THEN 'active' ELSE 'inactive' END as status
        FROM generate_series(1, 1000)
        """
        duckdb_adapter.execute_query(create_sql)
        
        # Create target table with some data
        target_sql = """
        CREATE TABLE test_schema.target_table AS
        SELECT * FROM test_schema.source_table WHERE id <= 100
        """
        duckdb_adapter.execute_query(target_sql)
        
        # Execute incremental merge
        source_sql = """
        SELECT id, name, created_at, updated_at, status
        FROM test_schema.source_table
        WHERE status = 'active'
        """
        config = {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "1 hour"
        }
        
        import time
        start_time = time.time()
        duckdb_adapter.execute_incremental_merge("test_schema.target_table", source_sql, config)
        end_time = time.time()
        
        # Verify data was merged
        result = duckdb_adapter.execute_query("SELECT COUNT(*) FROM test_schema.target_table")
        count = result.fetchone()[0]
        assert count == 500  # All active records
        
        # Performance should be reasonable
        execution_time = end_time - start_time
        assert execution_time < 2.0, f"Execution took {execution_time:.2f} seconds, expected < 2.0"
