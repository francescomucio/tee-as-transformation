"""
Unit tests for NoDuplicatesTest.
"""

import pytest
from unittest.mock import Mock

from tee.testing.standard_tests import NoDuplicatesTest


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

