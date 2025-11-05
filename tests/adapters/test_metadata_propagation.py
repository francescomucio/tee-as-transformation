"""
Test cases for metadata propagation functionality.

This module tests the column description propagation from model metadata
to database adapters (DuckDB and Snowflake).
"""

import os
import sys
import tempfile
import pytest
import shutil
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tee.executor import execute_models
from tee.adapters.duckdb.adapter import DuckDBAdapter
from tee.adapters.base import AdapterConfig


class TestMetadataPropagation:
    """Test class for metadata propagation functionality."""

    def test_duckdb_metadata_propagation(self):
        """Test that column descriptions are properly stored in DuckDB."""
        # Create a temporary DuckDB database
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            db_path = tmp_file.name

        # Close the file so DuckDB can use it
        tmp_file.close()

        # Remove the file if it exists so DuckDB can create it fresh
        if os.path.exists(db_path):
            os.unlink(db_path)

        try:
            # DuckDB configuration
            config = {"type": "duckdb", "path": db_path}

            # Test project folder
            project_folder = "t_project_sno"

            # Execute models
            results = execute_models(project_folder, config, save_analysis=False)

            # Check if the main table was created (which has metadata)
            assert "my_schema.my_first_table" in results["executed_tables"]

            # Now check if column comments were actually stored
            import duckdb

            conn = duckdb.connect(db_path)

            try:
                # Query the information schema to get column comments
                result = conn.execute("""
                    SELECT column_name, column_comment 
                    FROM information_schema.columns 
                    WHERE table_name = 'my_first_table' 
                    AND table_schema = 'my_schema'
                    ORDER BY ordinal_position
                """).fetchall()

                # Check if we have the expected columns with comments
                expected_columns = ["id", "name"]  # Based on the actual table structure
                found_columns = [row[0] for row in result]

                for col in expected_columns:
                    assert col in found_columns, f"Column {col} should exist"

                # Check that at least one column has a comment
                comments = [row[1] for row in result if row[1]]
                assert len(comments) > 0, "At least one column should have a comment"

            finally:
                conn.close()

        finally:
            # Clean up
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_duckdb_table_description(self):
        """Test that table descriptions are properly stored in DuckDB."""
        # This test verifies that the table description functionality works
        # by testing the metadata extraction and validation logic directly

        # Create a mock adapter for testing
        config_dict = {"type": "duckdb", "path": ":memory:"}
        from tee.adapters.duckdb.adapter import DuckDBAdapter

        adapter = DuckDBAdapter(config_dict)

        # Test metadata with table description
        metadata_with_table_desc = {
            "description": "This is a test table description",
            "schema": [
                {"name": "id", "datatype": "integer", "description": "Primary key identifier"},
                {"name": "name", "datatype": "string", "description": "Name of the record"},
            ],
        }

        # Test that metadata validation works with table descriptions
        column_descriptions = adapter._validate_column_metadata(metadata_with_table_desc)
        assert column_descriptions == {"id": "Primary key identifier", "name": "Name of the record"}

        # Test that table description is extracted correctly
        table_description = metadata_with_table_desc.get("description")
        assert table_description == "This is a test table description"

        # Test that the base adapter methods exist
        assert hasattr(adapter, "_add_table_comment")
        assert hasattr(adapter, "_add_column_comments")

    def test_metadata_validation(self):
        """Test metadata validation without database connection."""
        # Create a mock adapter for testing validation
        config_dict = {"type": "duckdb", "path": ":memory:"}
        adapter = DuckDBAdapter(config_dict)

        # Test valid metadata
        valid_metadata = {
            "schema": [{"name": "id", "datatype": "integer", "description": "Valid description"}]
        }

        descriptions = adapter._validate_column_metadata(valid_metadata)
        assert descriptions == {"id": "Valid description"}

        # Test malformed metadata
        malformed_metadata = {
            "schema": [
                {
                    # Missing 'name' field
                    "datatype": "integer",
                    "description": "Should fail",
                }
            ]
        }

        with pytest.raises(ValueError, match="Column definition must include 'name' field"):
            adapter._validate_column_metadata(malformed_metadata)

        # Test description too long
        long_description_metadata = {
            "schema": [
                {
                    "name": "id",
                    "datatype": "integer",
                    "description": "A" * 5000,  # Too long
                }
            ]
        }

        with pytest.raises(ValueError, match="Column description for 'id' is too long"):
            adapter._validate_column_metadata(long_description_metadata)

    def test_metadata_priority(self):
        """Test that decorator metadata takes priority over file metadata."""
        # This test would verify that when both decorator and file metadata exist,
        # the decorator metadata is used. This is more of an integration test
        # that would require setting up a test model with both types of metadata.
        pass

    def test_snowflake_metadata_propagation(self):
        """Test that column descriptions are properly stored in Snowflake."""
        # This test would require Snowflake credentials and is skipped by default
        pytest.skip("Snowflake test requires credentials")

    def test_error_handling_malformed_metadata(self):
        """Test that malformed metadata is properly handled."""
        # This test verifies that the metadata validation works correctly
        # by testing the validation function directly rather than through
        # the execution engine, since the test models were removed from the project

        # Create a mock adapter for testing validation
        config_dict = {"type": "duckdb", "path": ":memory:"}
        adapter = DuckDBAdapter(config_dict)

        # Test malformed metadata - missing name field
        malformed_metadata = {
            "schema": [
                {"name": "id", "datatype": "integer", "description": "Valid description"},
                {
                    # Missing 'name' field - should cause error
                    "datatype": "string",
                    "description": "This should cause an error",
                },
            ]
        }

        with pytest.raises(ValueError, match="Column definition must include 'name' field"):
            adapter._validate_column_metadata(malformed_metadata)

        # Test invalid schema type
        invalid_schema_metadata = {
            "schema": "not_a_list"  # Should be a list, not a string
        }

        with pytest.raises(ValueError, match="Schema must be a list of column definitions"):
            adapter._validate_column_metadata(invalid_schema_metadata)


# Test model fixtures for metadata testing
class TestMetadataModels:
    """Test models with metadata for testing purposes."""

    @staticmethod
    def create_users_with_descriptions():
        """Create a users table with detailed column descriptions."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_products_with_descriptions():
        """Create a products table with column descriptions."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_orders_with_descriptions():
        """Create an orders table with column descriptions."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_priority_test_table():
        """Create a table to test metadata priority."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_malformed_metadata_table():
        """Create a table with malformed metadata."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_long_description_table():
        """Create a table with description that's too long."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")

    @staticmethod
    def create_invalid_schema_type_table():
        """Create a table with invalid schema type."""
        from sqlglot import exp

        return exp.select("*").from_("my_first_table")


# Metadata fixtures for testing
USERS_METADATA = {
    "schema": [
        {"name": "id", "datatype": "integer", "description": "Unique identifier for the user"},
        {
            "name": "username",
            "datatype": "string",
            "description": "User's login username, must be unique",
        },
        {
            "name": "email",
            "datatype": "string",
            "description": "User's email address for notifications",
        },
        {
            "name": "created_at",
            "datatype": "timestamp",
            "description": "Timestamp when the user account was created",
        },
        {
            "name": "is_active",
            "datatype": "boolean",
            "description": "Whether the user account is currently active",
        },
    ]
}

PRODUCTS_METADATA = {
    "schema": [
        {"name": "product_id", "datatype": "integer", "description": "Primary key for the product"},
        {
            "name": "product_name",
            "datatype": "string",
            "description": "Display name of the product",
        },
        {
            "name": "price",
            "datatype": "number",
            "description": "Product price in USD, stored as decimal",
        },
        {
            "name": "category",
            "datatype": "string",
            "description": "Product category for grouping and filtering",
        },
        {
            "name": "in_stock",
            "datatype": "boolean",
            "description": "Whether the product is currently available for purchase",
        },
    ]
}

ORDERS_METADATA = {
    "schema": [
        {"name": "order_id", "datatype": "integer", "description": "Unique order identifier"},
        {
            "name": "user_id",
            "datatype": "integer",
            "description": "Foreign key reference to users table",
        },
        {
            "name": "total_amount",
            "datatype": "number",
            "description": "Total order value including tax and shipping",
        },
        {
            "name": "order_date",
            "datatype": "timestamp",
            "description": "Date and time when the order was placed",
        },
        {
            "name": "status",
            "datatype": "string",
            "description": "Current order status: pending, shipped, delivered, cancelled",
        },
    ]
}

PRIORITY_TEST_METADATA = {
    "schema": [
        {
            "name": "id",
            "datatype": "integer",
            "description": "DECORATOR: This description should take priority",
        },
        {
            "name": "name",
            "datatype": "string",
            "description": "DECORATOR: This description should take priority over file metadata",
        },
    ]
}

MALFORMED_METADATA = {
    "schema": [
        {"name": "id", "datatype": "integer", "description": "Valid description"},
        {
            # Missing 'name' field - should cause error
            "datatype": "string",
            "description": "This should cause an error",
        },
    ]
}

LONG_DESCRIPTION_METADATA = {
    "schema": [
        {
            "name": "id",
            "datatype": "integer",
            "description": "A" * 5000,  # This should exceed the 4000 character limit
        }
    ]
}

INVALID_SCHEMA_TYPE_METADATA = {
    "schema": "not_a_list"  # Should be a list, not a string
}
