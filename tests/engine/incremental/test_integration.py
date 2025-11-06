"""
Integration test cases for incremental executor.
"""

from datetime import datetime
from unittest.mock import Mock

from tests.engine.incremental.test_executor_base import TestIncrementalExecutor
from tee.engine.model_state import ModelState


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

