"""
Unit tests for standard test implementations.
"""

import pytest
from unittest.mock import Mock, MagicMock

from tee.testing.base import TestSeverity, TestResult
from tee.testing.standard_tests import (
    NotNullTest,
    UniqueTest,
    AcceptedValuesTest,
    NoDuplicatesTest,
    RowCountGreaterThanZeroTest,
    RelationshipsTest,
)


class TestNotNullTest:
    """Test cases for NotNullTest."""

    def test_validate_params_no_column_name(self):
        """Test validation fails without column name in get_test_query."""
        test = NotNullTest()

        # NotNullTest doesn't validate column_name in validate_params, but in get_test_query
        # So we test get_test_query instead
        mock_adapter = Mock()
        with pytest.raises(ValueError, match="requires a column name"):
            test.get_test_query(mock_adapter, "my_table", column_name=None)

    def test_validate_params_with_column_name(self):
        """Test validation passes with column name."""
        test = NotNullTest()

        # Should not raise
        test.validate_params(column_name="id")
        test.validate_params(params={}, column_name="id")

    def test_validate_params_unknown_params(self):
        """Test validation fails with unknown parameters."""
        test = NotNullTest()

        with pytest.raises(ValueError, match="Unknown parameters"):
            test.validate_params(params={"unknown": "value"}, column_name="id")

    def test_get_test_query(self):
        """Test query generation."""
        test = NotNullTest()
        mock_adapter = Mock()
        mock_adapter.generate_not_null_test_query.return_value = (
            "SELECT COUNT(*) FROM table WHERE col IS NULL"
        )

        query = test.get_test_query(mock_adapter, "my_table", column_name="id")

        assert query == "SELECT COUNT(*) FROM table WHERE col IS NULL"
        mock_adapter.generate_not_null_test_query.assert_called_once_with("my_table", "id")

    def test_execute_success(self):
        """Test execution when no NULL values found."""
        test = NotNullTest()
        mock_adapter = Mock()
        mock_adapter.generate_not_null_test_query.return_value = (
            "SELECT COUNT(*) FROM table WHERE id IS NULL"
        )
        mock_adapter.execute_query.return_value = [(0,)]

        result = test.execute(mock_adapter, "my_table", column_name="id")

        assert result.passed is True
        assert result.rows_returned == 0

    def test_execute_failure(self):
        """Test execution when NULL values found."""
        test = NotNullTest()
        mock_adapter = Mock()
        mock_adapter.generate_not_null_test_query.return_value = (
            "SELECT COUNT(*) FROM table WHERE id IS NULL"
        )
        mock_adapter.execute_query.return_value = [(5,)]

        result = test.execute(mock_adapter, "my_table", column_name="id")

        assert result.passed is False
        assert result.rows_returned == 5


class TestUniqueTest:
    """Test cases for UniqueTest."""

    def test_validate_params_column_level(self):
        """Test validation for column-level test."""
        test = UniqueTest()

        # Should not raise
        test.validate_params(column_name="id")

    def test_validate_params_table_level(self):
        """Test validation for table-level test with composite columns."""
        test = UniqueTest()

        # Should not raise
        test.validate_params(params={"columns": ["col1", "col2"]}, column_name=None)

    def test_validate_params_conflict(self):
        """Test validation fails when both column_name and columns param provided."""
        test = UniqueTest()

        with pytest.raises(ValueError, match="cannot use 'columns' param when applied to a column"):
            test.validate_params(params={"columns": ["col1"]}, column_name="id")

    def test_validate_params_empty_columns(self):
        """Test validation fails with empty columns list."""
        test = UniqueTest()

        with pytest.raises(ValueError, match="must be a non-empty list"):
            test.validate_params(params={"columns": []}, column_name=None)

    def test_get_test_query_column_level(self):
        """Test query generation for column-level test."""
        test = UniqueTest()
        mock_adapter = Mock()
        mock_adapter.generate_unique_test_query.return_value = "SELECT COUNT(*) FROM ..."

        query = test.get_test_query(mock_adapter, "my_table", column_name="id")

        mock_adapter.generate_unique_test_query.assert_called_once_with("my_table", ["id"])

    def test_get_test_query_table_level(self):
        """Test query generation for table-level test."""
        test = UniqueTest()
        mock_adapter = Mock()
        mock_adapter.generate_unique_test_query.return_value = "SELECT COUNT(*) FROM ..."

        query = test.get_test_query(mock_adapter, "my_table", params={"columns": ["col1", "col2"]})

        mock_adapter.generate_unique_test_query.assert_called_once_with(
            "my_table", ["col1", "col2"]
        )


class TestAcceptedValuesTest:
    """Test cases for AcceptedValuesTest."""

    def test_validate_params_no_column_name(self):
        """Test validation fails without column name."""
        test = AcceptedValuesTest()

        with pytest.raises(ValueError, match="requires a column name"):
            test.validate_params(column_name=None)

    def test_validate_params_no_values(self):
        """Test validation fails without values parameter."""
        test = AcceptedValuesTest()

        with pytest.raises(ValueError, match="requires 'values' parameter"):
            test.validate_params(params={}, column_name="status")

    def test_validate_params_empty_values(self):
        """Test validation fails with empty values list."""
        test = AcceptedValuesTest()

        with pytest.raises(ValueError, match="must contain at least one value"):
            test.validate_params(params={"values": []}, column_name="status")

    def test_validate_params_not_list(self):
        """Test validation fails when values is not a list."""
        test = AcceptedValuesTest()

        with pytest.raises(ValueError, match="must be a list"):
            test.validate_params(params={"values": "not_a_list"}, column_name="status")

    def test_validate_params_valid(self):
        """Test validation passes with valid parameters."""
        test = AcceptedValuesTest()

        # Should not raise
        test.validate_params(params={"values": ["active", "inactive"]}, column_name="status")

    def test_get_test_query(self):
        """Test query generation."""
        test = AcceptedValuesTest()
        mock_adapter = Mock()
        mock_adapter.generate_accepted_values_test_query.return_value = "SELECT COUNT(*) FROM ..."

        query = test.get_test_query(
            mock_adapter,
            "my_table",
            column_name="status",
            params={"values": ["active", "inactive"]},
        )

        mock_adapter.generate_accepted_values_test_query.assert_called_once_with(
            "my_table", "status", ["active", "inactive"]
        )

    def test_execute_success(self):
        """Test execution when all values are accepted."""
        test = AcceptedValuesTest()
        mock_adapter = Mock()
        mock_adapter.generate_accepted_values_test_query.return_value = "SELECT COUNT(*) FROM ..."
        mock_adapter.execute_query.return_value = [(0,)]

        result = test.execute(
            mock_adapter,
            "my_table",
            column_name="status",
            params={"values": ["active", "inactive"]},
        )

        assert result.passed is True
        assert result.rows_returned == 0

    def test_execute_failure(self):
        """Test execution when invalid values found."""
        test = AcceptedValuesTest()
        mock_adapter = Mock()
        mock_adapter.generate_accepted_values_test_query.return_value = "SELECT COUNT(*) FROM ..."
        mock_adapter.execute_query.return_value = [(2,)]

        result = test.execute(
            mock_adapter,
            "my_table",
            column_name="status",
            params={"values": ["active", "inactive"]},
        )

        assert result.passed is False
        assert result.rows_returned == 2


class TestRelationshipsTest:
    """Test cases for RelationshipsTest."""

    def test_validate_params_no_column_name(self):
        """Test validation fails without column name."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="requires a column name"):
            test.validate_params(column_name=None)

    def test_validate_params_no_params(self):
        """Test validation fails without parameters."""
        test = RelationshipsTest()

        with pytest.raises(
            ValueError, match="requires 'to' and 'field' parameters|requires 'to' parameter"
        ):
            test.validate_params(params={}, column_name="user_id")

    def test_validate_params_missing_to(self):
        """Test validation fails without 'to' parameter."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="requires 'to' parameter"):
            test.validate_params(params={"field": "id"}, column_name="user_id")

    def test_validate_params_missing_field(self):
        """Test validation fails without 'field' or 'fields' parameter."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="requires 'field' or 'fields' parameter"):
            test.validate_params(params={"to": "users"}, column_name="user_id")

    def test_validate_params_empty_to(self):
        """Test validation fails with empty 'to' parameter."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="must be a non-empty string"):
            test.validate_params(params={"to": "", "field": "id"}, column_name="user_id")

    def test_validate_params_empty_field(self):
        """Test validation fails with empty 'field' parameter."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="must be a non-empty string"):
            test.validate_params(params={"to": "users", "field": ""}, column_name="user_id")

    def test_validate_params_valid(self):
        """Test validation passes with valid parameters."""
        test = RelationshipsTest()

        # Should not raise
        test.validate_params(params={"to": "my_schema.users", "field": "id"}, column_name="user_id")

    def test_get_test_query_single_column(self):
        """Test query generation for single column."""
        test = RelationshipsTest()
        mock_adapter = Mock()
        mock_adapter.generate_relationships_test_query.return_value = "SELECT COUNT(*) FROM ..."

        query = test.get_test_query(
            mock_adapter,
            "my_schema.orders",
            column_name="user_id",
            params={"to": "my_schema.users", "field": "id"},
        )

        mock_adapter.generate_relationships_test_query.assert_called_once_with(
            "my_schema.orders", ["user_id"], "my_schema.users", ["id"]
        )

    def test_get_test_query_composite_key(self):
        """Test query generation for composite key."""
        test = RelationshipsTest()
        mock_adapter = Mock()
        mock_adapter.generate_relationships_test_query.return_value = "SELECT COUNT(*) FROM ..."

        query = test.get_test_query(
            mock_adapter,
            "my_schema.order_items",
            column_name="order_id",  # Primary column (for backward compat)
            params={
                "to": "my_schema.products",
                "fields": ["order_id", "product_id"],
                "source_fields": ["order_id", "product_id"],
            },
        )

        mock_adapter.generate_relationships_test_query.assert_called_once_with(
            "my_schema.order_items",
            ["order_id", "product_id"],
            "my_schema.products",
            ["order_id", "product_id"],
        )

    def test_execute_success(self):
        """Test execution when all relationships are valid."""
        test = RelationshipsTest()
        mock_adapter = Mock()
        mock_adapter.generate_relationships_test_query.return_value = "SELECT COUNT(*) FROM ..."
        mock_adapter.execute_query.return_value = [(0,)]  # No orphaned rows

        result = test.execute(
            mock_adapter,
            "my_schema.orders",
            column_name="user_id",
            params={"to": "my_schema.users", "field": "id"},
        )

        assert result.passed is True
        assert result.rows_returned == 0

    def test_execute_failure(self):
        """Test execution when orphaned rows found."""
        test = RelationshipsTest()
        mock_adapter = Mock()
        mock_adapter.generate_relationships_test_query.return_value = "SELECT COUNT(*) FROM ..."
        mock_adapter.execute_query.return_value = [(3,)]  # 3 orphaned rows

        result = test.execute(
            mock_adapter,
            "my_schema.orders",
            column_name="user_id",
            params={"to": "my_schema.users", "field": "id"},
        )

        assert result.passed is False
        assert result.rows_returned == 3

    def test_validate_params_both_field_and_fields(self):
        """Test validation fails when both 'field' and 'fields' are provided."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="cannot use both 'field' and 'fields' parameters"):
            test.validate_params(
                params={"to": "users", "field": "id", "fields": ["id", "name"]},
                column_name="user_id",
            )

    def test_validate_params_composite_with_source_fields(self):
        """Test validation with composite key and source_fields."""
        test = RelationshipsTest()

        # Should not raise
        test.validate_params(
            params={
                "to": "my_schema.products",
                "fields": ["region_id", "country_id"],
                "source_fields": ["region_id", "country_id"],
            },
            column_name="region_id",
        )

    def test_validate_params_mismatched_source_fields_length(self):
        """Test validation fails when source_fields length doesn't match target."""
        test = RelationshipsTest()

        with pytest.raises(ValueError, match="must have the same length"):
            test.validate_params(
                params={
                    "to": "users",
                    "field": "id",
                    "source_fields": ["user_id", "other_id"],  # Length 2 vs 1
                },
                column_name="user_id",
            )


class TestNoDuplicatesTest:
    """Test cases for NoDuplicatesTest."""

    def test_validate_params_with_column_name(self):
        """Test validation fails when applied to column."""
        test = NoDuplicatesTest()

        with pytest.raises(ValueError, match="is a model-level test"):
            test.validate_params(column_name="id")

    def test_validate_params_valid(self):
        """Test validation passes for model-level test."""
        test = NoDuplicatesTest()

        # Should not raise
        test.validate_params(column_name=None)
        test.validate_params(params={}, column_name=None)

    def test_get_test_query(self):
        """Test query generation."""
        test = NoDuplicatesTest()
        mock_adapter = Mock()
        mock_adapter.get_table_columns.return_value = ["id", "name"]
        mock_adapter.generate_no_duplicates_test_query.return_value = "SELECT COUNT(*) FROM ..."
        mock_adapter.connection = True  # Mock connection

        query = test.get_test_query(mock_adapter, "my_table")

        mock_adapter.generate_no_duplicates_test_query.assert_called_once()


class TestRowCountGreaterThanZeroTest:
    """Test cases for RowCountGreaterThanZeroTest."""

    def test_validate_params_with_column_name(self):
        """Test validation fails when applied to column."""
        test = RowCountGreaterThanZeroTest()

        with pytest.raises(ValueError, match="is a model-level test"):
            test.validate_params(column_name="id")

    def test_check_passed_inverted_logic(self):
        """Test that check_passed uses inverted logic (count > 0 means pass)."""
        test = RowCountGreaterThanZeroTest()

        assert test.check_passed(0) is False  # Empty table = fail
        assert test.check_passed(1) is True  # Has data = pass
        assert test.check_passed(100) is True  # Has data = pass

    def test_format_message_passed(self):
        """Test message formatting for passed test."""
        test = RowCountGreaterThanZeroTest()

        message = test.format_message(True, 5)
        assert "passed" in message.lower()
        assert "5 row(s)" in message

    def test_format_message_failed(self):
        """Test message formatting for failed test."""
        test = RowCountGreaterThanZeroTest()

        message = test.format_message(False, 0)
        assert "failed" in message.lower()
        assert "empty" in message.lower()

    def test_execute_success(self):
        """Test execution when table has data."""
        test = RowCountGreaterThanZeroTest()
        mock_adapter = Mock()
        mock_adapter.generate_row_count_gt_0_test_query.return_value = "SELECT COUNT(*) FROM table"
        mock_adapter.execute_query.return_value = [(3,)]

        result = test.execute(mock_adapter, "my_table")

        assert result.passed is True
        assert result.rows_returned == 3
        assert "3 row(s)" in result.message

    def test_execute_failure(self):
        """Test execution when table is empty."""
        test = RowCountGreaterThanZeroTest()
        mock_adapter = Mock()
        mock_adapter.generate_row_count_gt_0_test_query.return_value = "SELECT COUNT(*) FROM table"
        mock_adapter.execute_query.return_value = [(0,)]

        result = test.execute(mock_adapter, "my_table")

        assert result.passed is False
        assert result.rows_returned == 0
        assert "empty" in result.message.lower()
