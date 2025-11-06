"""
Test cases for get_time_filter_condition method.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

from tests.engine.incremental.test_executor_base import TestIncrementalExecutor


class TestTimeFilterCondition(TestIncrementalExecutor):
    """Test cases for get_time_filter_condition method."""

    def test_with_last_processed_value(self, executor, sample_append_config):
        """Test time filter with last processed value."""
        result = executor.get_time_filter_condition(
            sample_append_config, last_processed_value="2024-01-15"
        )

        assert result == "created_at > '2024-01-15'"

    def test_auto_start_date_with_table_name(self, executor):
        """Test auto start_date with table name."""
        config = {"time_column": "updated_at", "start_date": "auto"}
        result = executor.get_time_filter_condition(config, table_name="my_schema.test_table")

        expected = "updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM my_schema.test_table)"
        assert result == expected

    def test_auto_start_date_with_lookback(self, executor):
        """Test auto start_date with lookback."""
        config = {"time_column": "updated_at", "start_date": "auto", "lookback": "3 hours"}

        result = executor.get_time_filter_condition(config, table_name="my_schema.test_table")

        expected = "updated_at > (SELECT COALESCE(MAX(updated_at) - INTERVAL '3 hours', '1900-01-01') FROM my_schema.test_table)"
        assert result == expected

    def test_current_date_start_date(self, executor, sample_append_config):
        """Test CURRENT_DATE start_date."""
        config = {"time_column": "created_at", "start_date": "CURRENT_DATE"}

        result = executor.get_time_filter_condition(config)

        assert result == "created_at >= CURRENT_DATE"

    def test_specific_date_start_date(self, executor, sample_append_config):
        """Test specific date start_date."""
        result = executor.get_time_filter_condition(sample_append_config)

        assert result == "created_at >= '2024-01-01'"

    def test_variable_resolution(self, executor):
        """Test variable resolution in start_date."""
        config = {"time_column": "created_at", "start_date": "@start_date"}
        variables = {"start_date": "2024-02-01"}

        result = executor.get_time_filter_condition(config, variables=variables)

        assert result == "created_at >= '2024-02-01'"

    def test_variable_resolution_with_braces(self, executor):
        """Test variable resolution with {{ }} syntax."""
        config = {"time_column": "created_at", "start_date": "{{ start_date }}"}
        variables = {"start_date": "2024-02-01"}

        result = executor.get_time_filter_condition(config, variables=variables)

        assert result == "created_at >= '2024-02-01'"

    def test_default_lookback_when_no_start_date(self, executor):
        """Test default 7-day lookback when no start_date specified."""
        config = {"time_column": "created_at"}

        with patch("tee.engine.incremental_executor.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15)
            mock_datetime.timedelta = timedelta

            result = executor.get_time_filter_condition(config)

            assert result == "created_at >= '2024-01-08'"  # 7 days before 2024-01-15

