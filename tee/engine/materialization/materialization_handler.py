"""Materialization handler for different materialization types."""

import logging
from typing import Dict, Any, Optional

from tee.adapters.base.core import DatabaseAdapter

logger = logging.getLogger(__name__)


class MaterializationHandler:
    """Handles different materialization types (table, view, incremental, etc.)."""

    def __init__(self, adapter: DatabaseAdapter, state_manager, variables: Dict[str, Any]):
        """
        Initialize the materialization handler.

        Args:
            adapter: Database adapter instance
            state_manager: State manager instance
            variables: Variables dictionary for model execution
        """
        self.adapter = adapter
        self.state_manager = state_manager
        self.variables = variables

    def materialize(
        self,
        table_name: str,
        sql_query: str,
        materialization: str,
        metadata: Optional[Dict[str, Any]] = None,
        config: Optional[Any] = None,
    ) -> None:
        """
        Execute the appropriate materialization based on type.

        Args:
            table_name: Name of the table/view
            sql_query: SQL query to execute
            materialization: Materialization type
            metadata: Optional metadata dictionary
            config: Optional adapter config
        """
        if materialization == "view":
            self.adapter.create_view(table_name, sql_query, metadata)
        elif materialization == "materialized_view":
            if hasattr(self.adapter, "create_materialized_view"):
                self.adapter.create_materialized_view(table_name, sql_query)
            else:
                logger.warning(
                    f"Materialized views not supported by {self.adapter.__class__.__name__}, creating table instead"
                )
                self.adapter.create_table(table_name, sql_query, metadata)
        elif materialization == "external_table":
            if hasattr(self.adapter, "create_external_table"):
                # External tables need additional configuration
                external_location = (
                    config.extra.get("external_location") if config and config.extra else None
                )
                if external_location:
                    self.adapter.create_external_table(table_name, sql_query, external_location)
                else:
                    logger.warning(
                        "External table location not configured, creating table instead"
                    )
                    self.adapter.create_table(table_name, sql_query, metadata)
            else:
                logger.warning(
                    f"External tables not supported by {self.adapter.__class__.__name__}, creating table instead"
                )
                self.adapter.create_table(table_name, sql_query, metadata)
        elif materialization == "incremental":
            self._execute_incremental_materialization(table_name, sql_query, metadata)
        else:  # Default to table for "table" or any other type
            self.adapter.create_table(table_name, sql_query, metadata)

    def _execute_incremental_materialization(
        self, table_name: str, sql_query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Execute incremental materialization using the universal state manager."""
        from .incremental_executor import IncrementalExecutor
        from datetime import datetime, UTC

        # Use the universal state manager
        executor = IncrementalExecutor(self.state_manager)

        try:
            # Extract incremental configuration
            incremental_config = metadata.get("incremental") if metadata else None

            # If incremental config is not found, check if the metadata itself contains incremental info
            if (
                not incremental_config
                and metadata
                and metadata.get("materialization") == "incremental"
            ):
                # This is a flat metadata structure from SQL files, need to restructure it
                strategy = metadata.get("strategy", "append")  # Default to append if not specified
                incremental_config = {
                    "strategy": strategy,
                    strategy: {
                        "time_column": metadata.get("time_column"),
                        "start_date": metadata.get("start_date"),
                        "lookback": metadata.get("lookback"),
                        "unique_key": metadata.get("unique_key"),
                        "where_condition": metadata.get("where_condition"),
                    },
                }
                # Remove None values
                incremental_config[strategy] = {
                    k: v for k, v in incremental_config[strategy].items() if v is not None
                }

            if not incremental_config:
                logger.error("Incremental materialization requires incremental configuration")
                # Fallback to table
                self.adapter.create_table(table_name, sql_query, metadata)
                return

            strategy = incremental_config.get("strategy")
            if not strategy:
                logger.error("Incremental strategy is required")
                # Fallback to table
                self.adapter.create_table(table_name, sql_query, metadata)
                return

            # Check if we should run incrementally
            should_run_incremental = executor.should_run_incremental(
                table_name, sql_query, incremental_config
            )
            if not should_run_incremental:
                # Run as full load (create/replace table)
                self.adapter.create_table(table_name, sql_query, metadata)

                # Update state after full load to enable incremental runs
                current_time = datetime.now(UTC).isoformat()
                strategy = incremental_config.get("strategy") if incremental_config else None
                self.state_manager.update_processed_value(table_name, current_time, strategy)
            else:
                # Run incremental strategy
                if strategy == "append":
                    append_config = incremental_config.get("append")
                    if not append_config:
                        logger.error("Append strategy requires append configuration")
                        return
                    executor.execute_append_strategy(
                        table_name,
                        sql_query,
                        append_config,
                        self.adapter,
                        table_name,
                        self.variables,
                    )

                elif strategy == "merge":
                    merge_config = incremental_config.get("merge")
                    if not merge_config:
                        logger.error("Merge strategy requires merge configuration")
                        return
                    executor.execute_merge_strategy(
                        table_name,
                        sql_query,
                        merge_config,
                        self.adapter,
                        table_name,
                        self.variables,
                    )

                elif strategy == "delete_insert":
                    delete_insert_config = incremental_config.get("delete_insert")
                    if not delete_insert_config:
                        logger.error(
                            "Delete+insert strategy requires delete_insert configuration"
                        )
                        return
                    executor.execute_delete_insert_strategy(
                        table_name,
                        sql_query,
                        delete_insert_config,
                        self.adapter,
                        table_name,
                        self.variables,
                    )

                else:
                    logger.error(f"Unknown incremental strategy: {strategy}")
                    return

            # State is already saved by individual strategy methods

        except Exception as e:
            logger.error(f"Error executing incremental materialization: {e}")
            # Fallback to table creation
            self.adapter.create_table(table_name, sql_query, metadata)

