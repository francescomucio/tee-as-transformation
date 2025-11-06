"""
Base test class and fixtures for incremental executor tests.
"""

import pytest
from unittest.mock import Mock

from tee.engine.incremental_executor import IncrementalExecutor
from tee.engine.model_state import ModelStateManager
from tee.typing.metadata import (
    IncrementalConfig,
    IncrementalAppendConfig,
    IncrementalMergeConfig,
    IncrementalDeleteInsertConfig,
)


class TestIncrementalExecutor:
    """Base test class for IncrementalExecutor tests."""

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

