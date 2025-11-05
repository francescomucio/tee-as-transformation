"""
Test cases for incremental materialization executor.

These tests focus on the core incremental logic and can be reused
across different database adapters.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any, Optional

from tee.engine.incremental_executor import IncrementalExecutor
from tee.engine.model_state import ModelStateManager, ModelState
from tee.typing.metadata import (
    IncrementalConfig,
    IncrementalAppendConfig,
    IncrementalMergeConfig,
    IncrementalDeleteInsertConfig,
)


class TestIncrementalExecutor:
    """Test cases for IncrementalExecutor core logic."""

    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock state manager."""
        state_manager = Mock(spec=ModelStateManager)
        state_manager.compute_sql_hash.return_value = "test_sql_hash"
        state_manager.compute_config_hash.return_value = "test_config_hash"
        return state_manager

    @pytest.fixture
    def executor(self, mock_state_manager):
        """Create an IncrementalExecutor instance."""
        return IncrementalExecutor(mock_state_manager)

    @pytest.fixture
    def sample_append_config(self) -> IncrementalAppendConfig:
        """Sample append configuration."""
        return {"time_column": "created_at", "start_date": "2024-01-01", "lookback": "7 days"}

    @pytest.fixture
    def sample_merge_config(self) -> IncrementalMergeConfig:
        """Sample merge configuration."""
        return {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "3 hours",
        }

    @pytest.fixture
    def sample_delete_insert_config(self) -> IncrementalDeleteInsertConfig:
        """Sample delete+insert configuration."""
        return {
            "where_condition": "updated_at >= @start_date",
            "time_column": "updated_at",
            "start_date": "@start_date",
        }

    @pytest.fixture
    def sample_incremental_config(self) -> IncrementalConfig:
        """Sample incremental configuration."""
        return {
            "strategy": "append",
            "append": {
                "time_column": "created_at",
                "start_date": "2024-01-01",
                "lookback": "7 days",
            },
        }


class TestShouldRunIncremental(TestIncrementalExecutor):
    """Test cases for should_run_incremental method."""

    def test_no_state_exists_runs_full_load(self, executor, sample_incremental_config):
        """Test that full load runs when no state exists."""
        executor.state_manager.get_model_state.return_value = None

        result = executor.should_run_incremental(
            "test_model", "SELECT * FROM test", sample_incremental_config
        )

        assert result is False
        executor.state_manager.get_model_state.assert_called_once_with("test_model")

    def test_unknown_hashes_runs_full_load(self, executor, sample_incremental_config):
        """Test that full load runs when hashes are unknown."""
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="unknown",
            config_hash="unknown",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value=None,
            strategy="append",
        )
        executor.state_manager.get_model_state.return_value = state

        result = executor.should_run_incremental(
            "test_model", "SELECT * FROM test", sample_incremental_config
        )

        assert result is False

    def test_model_definition_changed_runs_full_load(self, executor, sample_incremental_config):
        """Test that full load runs when model definition changes."""
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="old_hash",
            config_hash="old_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value="2024-01-01",
            strategy="append",
        )
        executor.state_manager.get_model_state.return_value = state
        executor.state_manager.compute_sql_hash.return_value = "new_hash"
        executor.state_manager.compute_config_hash.return_value = "old_hash"

        result = executor.should_run_incremental(
            "test_model", "SELECT * FROM test", sample_incremental_config
        )

        assert result is False

    def test_append_strategy_runs_incremental(self, executor, sample_incremental_config):
        """Test that append strategy runs incrementally."""
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_sql_hash",
            config_hash="test_config_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value="2024-01-01",
            strategy="append",
        )
        executor.state_manager.get_model_state.return_value = state

        result = executor.should_run_incremental(
            "test_model", "SELECT * FROM test", sample_incremental_config
        )

        assert result is True

    def test_merge_strategy_with_auto_start_date_runs_incremental(self, executor):
        """Test that merge strategy with auto start_date runs incrementally."""
        config = {
            "strategy": "merge",
            "merge": {"unique_key": ["id"], "time_column": "updated_at", "start_date": "auto"},
        }
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_sql_hash",
            config_hash="test_config_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value=None,
            strategy="merge",
        )
        executor.state_manager.get_model_state.return_value = state

        result = executor.should_run_incremental("test_model", "SELECT * FROM test", config)

        assert result is True

    def test_merge_strategy_with_variable_start_date_runs_incremental(self, executor):
        """Test that merge strategy with variable start_date runs incrementally."""
        config = {
            "strategy": "merge",
            "merge": {
                "unique_key": ["id"],
                "time_column": "updated_at",
                "start_date": "@start_date",
            },
        }
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_sql_hash",
            config_hash="test_config_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value=None,
            strategy="merge",
        )
        executor.state_manager.get_model_state.return_value = state

        result = executor.should_run_incremental("test_model", "SELECT * FROM test", config)

        assert result is True

    def test_merge_strategy_without_last_processed_value_runs_full_load(self, executor):
        """Test that merge strategy without last_processed_value runs full load."""
        config = {
            "strategy": "merge",
            "merge": {
                "unique_key": ["id"],
                "time_column": "updated_at",
                "start_date": "2024-01-01",
            },
        }
        state = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_hash",
            config_hash="test_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value=None,
            strategy="merge",
        )
        executor.state_manager.get_model_state.return_value = state

        result = executor.should_run_incremental("test_model", "SELECT * FROM test", config)

        assert result is False


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


class TestLookbackParsing(TestIncrementalExecutor):
    """Test cases for _parse_lookback method."""

    def test_parse_minutes(self, executor):
        """Test parsing minutes."""
        result = executor._parse_lookback("30 minutes")
        assert result == "'30 minutes'"

    def test_parse_hours(self, executor):
        """Test parsing hours."""
        result = executor._parse_lookback("3 hours")
        assert result == "'3 hours'"

    def test_parse_days(self, executor):
        """Test parsing days."""
        result = executor._parse_lookback("7 days")
        assert result == "'7 days'"

    def test_parse_weeks(self, executor):
        """Test parsing weeks."""
        result = executor._parse_lookback("2 weeks")
        assert result == "'14 days'"

    def test_parse_months(self, executor):
        """Test parsing months."""
        result = executor._parse_lookback("1 month")
        assert result == "'30 days'"

    def test_parse_invalid_format(self, executor):
        """Test parsing invalid format."""
        result = executor._parse_lookback("invalid")
        assert result is None


class TestVariableResolution(TestIncrementalExecutor):
    """Test cases for variable resolution methods."""

    def test_resolve_at_variable(self, executor):
        """Test resolving @variable syntax."""
        variables = {"start_date": "2024-01-01"}

        result = executor._resolve_variable("@start_date", variables)
        assert result == "2024-01-01"

    def test_resolve_brace_variable(self, executor):
        """Test resolving {{ variable }} syntax."""
        variables = {"start_date": "2024-01-01"}

        result = executor._resolve_variable("{{ start_date }}", variables)
        assert result == "2024-01-01"

    def test_resolve_variables_in_string(self, executor):
        """Test resolving multiple variables in a string."""
        variables = {"start_date": "2024-01-01", "end_date": "2024-01-31"}

        result = executor._resolve_variables_in_string(
            "created_at >= @start_date AND created_at <= @end_date", variables
        )
        assert result == "created_at >= 2024-01-01 AND created_at <= 2024-01-31"

    def test_resolve_variables_with_braces(self, executor):
        """Test resolving variables with {{ }} syntax."""
        variables = {"start_date": "2024-01-01"}

        result = executor._resolve_variables_in_string("created_at >= {{ start_date }}", variables)
        assert result == "created_at >= 2024-01-01"

    def test_missing_variable_fallback(self, executor):
        """Test fallback when variable is missing."""
        variables = {}

        result = executor._resolve_variables_in_string("created_at >= @missing_var", variables)
        assert result == "created_at >= @missing_var"


class TestDateCasting(TestIncrementalExecutor):
    """Test cases for _add_date_casting method."""

    def test_add_date_casting_with_quotes(self, executor):
        """Test adding date casting with quoted dates."""
        where_condition = "created_at >= '2024-01-01'"

        result = executor._add_date_casting(where_condition)

        assert result == "created_at >= CAST('2024-01-01' AS DATE)"

    def test_add_date_casting_without_quotes(self, executor):
        """Test adding date casting without quotes."""
        where_condition = "created_at >= 2024-01-01"

        result = executor._add_date_casting(where_condition)

        assert result == "created_at >= CAST('2024-01-01' AS DATE)"

    def test_add_date_casting_multiple_dates(self, executor):
        """Test adding date casting with multiple dates."""
        where_condition = "created_at >= '2024-01-01' AND updated_at <= '2024-01-31'"

        result = executor._add_date_casting(where_condition)

        expected = (
            "created_at >= CAST('2024-01-01' AS DATE) AND updated_at <= CAST('2024-01-31' AS DATE)"
        )
        assert result == expected

    def test_add_date_casting_no_dates(self, executor):
        """Test adding date casting with no dates."""
        where_condition = "status = 'active'"

        result = executor._add_date_casting(where_condition)

        assert result == "status = 'active'"


class TestStrategyExecution(TestIncrementalExecutor):
    """Test cases for strategy execution methods."""

    def test_append_strategy_execution(self, executor, sample_append_config):
        """Test append strategy execution."""
        mock_adapter = Mock()
        mock_adapter.execute_incremental_append = Mock()

        executor.execute_append_strategy(
            "test_model", "SELECT * FROM source", sample_append_config, mock_adapter, "test_table"
        )

        mock_adapter.execute_incremental_append.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()

    def test_merge_strategy_execution(self, executor, sample_merge_config):
        """Test merge strategy execution."""
        mock_adapter = Mock()
        mock_adapter.execute_incremental_merge = Mock()

        executor.execute_merge_strategy(
            "test_model", "SELECT * FROM source", sample_merge_config, mock_adapter, "test_table"
        )

        mock_adapter.execute_incremental_merge.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()

    def test_delete_insert_strategy_execution(self, executor, sample_delete_insert_config):
        """Test delete+insert strategy execution."""
        mock_adapter = Mock()
        mock_adapter.execute_incremental_delete_insert = Mock()

        executor.execute_delete_insert_strategy(
            "test_model",
            "SELECT * FROM source",
            sample_delete_insert_config,
            mock_adapter,
            "test_table",
            variables={"start_date": "2024-01-01"},
        )

        mock_adapter.execute_incremental_delete_insert.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()

    def test_fallback_to_regular_execution(self, executor, sample_append_config):
        """Test fallback to regular execution when adapter doesn't support incremental."""
        mock_adapter = Mock()
        mock_adapter.execute_incremental_append = None  # Not supported
        mock_adapter.create_table = Mock()

        executor.execute_append_strategy(
            "test_model", "SELECT * FROM source", sample_append_config, mock_adapter, "test_table"
        )

        mock_adapter.create_table.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()


class TestLookbackApplication(TestIncrementalExecutor):
    """Test cases for lookback application."""

    def test_apply_lookback_to_time_filter(self, executor, sample_append_config):
        """Test applying lookback to time filter."""
        time_filter = "created_at >= '2024-01-01'"

        result = executor._apply_lookback_to_time_filter(time_filter, sample_append_config)

        assert result == "created_at >= (CAST('2024-01-01' AS DATE) - INTERVAL '7 days')"

    def test_apply_lookback_with_hours(self, executor):
        """Test applying lookback with hours."""
        config = {"time_column": "created_at", "lookback": "3 hours"}
        time_filter = "created_at >= '2024-01-01'"

        result = executor._apply_lookback_to_time_filter(time_filter, config)

        assert result == "created_at >= (CAST('2024-01-01' AS DATE) - INTERVAL '3 hours')"

    def test_apply_lookback_no_lookback(self, executor):
        """Test applying lookback when no lookback specified."""
        config = {"time_column": "created_at"}
        time_filter = "created_at >= '2024-01-01'"

        result = executor._apply_lookback_to_time_filter(time_filter, config)

        assert result == "created_at >= '2024-01-01'"

    def test_apply_lookback_invalid_format(self, executor):
        """Test applying lookback with invalid format."""
        config = {"time_column": "created_at", "lookback": "invalid"}
        time_filter = "created_at >= '2024-01-01'"

        result = executor._apply_lookback_to_time_filter(time_filter, config)

        assert result == "created_at >= '2024-01-01'"


class TestIntegration(TestIncrementalExecutor):
    """Integration test cases."""

    def test_complete_append_workflow(self, executor, sample_append_config):
        """Test complete append workflow."""
        # Mock state manager
        executor.state_manager.get_model_state.return_value = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_sql_hash",
            config_hash="test_config_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value="2024-01-01",
            strategy="append",
        )

        # Mock adapter
        mock_adapter = Mock()
        mock_adapter.execute_incremental_append = Mock()

        # Test should_run_incremental
        should_run = executor.should_run_incremental(
            "test_model",
            "SELECT * FROM source",
            {"strategy": "append", "append": sample_append_config},
        )
        assert should_run is True

        # Test execution
        executor.execute_append_strategy(
            "test_model", "SELECT * FROM source", sample_append_config, mock_adapter, "test_table"
        )

        # Verify calls
        mock_adapter.execute_incremental_append.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()

    def test_complete_merge_workflow_with_auto_start_date(self, executor):
        """Test complete merge workflow with auto start_date."""
        config = {
            "strategy": "merge",
            "merge": {
                "unique_key": ["id"],
                "time_column": "updated_at",
                "start_date": "auto",
                "lookback": "1 hour",
            },
        }

        # Mock state manager
        executor.state_manager.get_model_state.return_value = ModelState(
            model_name="test_model",
            materialization="incremental",
            last_execution_timestamp=datetime.now(),
            sql_hash="test_sql_hash",
            config_hash="test_config_hash",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            last_processed_value=None,
            strategy="merge",
        )

        # Mock adapter
        mock_adapter = Mock()
        mock_adapter.execute_incremental_merge = Mock()

        # Test should_run_incremental
        should_run = executor.should_run_incremental("test_model", "SELECT * FROM source", config)
        assert should_run is True

        # Test execution
        executor.execute_merge_strategy(
            "test_model", "SELECT * FROM source", config["merge"], mock_adapter, "test_table"
        )

        # Verify calls
        mock_adapter.execute_incremental_merge.assert_called_once()
        executor.state_manager.update_processed_value.assert_called_once()
