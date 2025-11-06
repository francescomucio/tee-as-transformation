"""
Test cases for strategy execution methods.
"""

from unittest.mock import Mock

from tests.engine.incremental.test_executor_base import TestIncrementalExecutor


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

