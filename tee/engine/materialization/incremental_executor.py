"""
Incremental execution logic for different strategies.

This module handles the execution of incremental materializations including:
- Append-only strategy
- Merge strategy
- Delete+insert strategy
- Time-based filtering
- State management integration
"""

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tee.adapters.base.core import DatabaseAdapter

import sqlglot
from sqlglot import expressions as exp

from tee.typing.metadata import (
    IncrementalAppendConfig,
    IncrementalConfig,
    IncrementalDeleteInsertConfig,
    IncrementalMergeConfig,
)

from ..model_state import ModelStateManager

logger = logging.getLogger(__name__)


class IncrementalExecutor:
    """Handles execution of incremental materializations."""

    def __init__(self, state_manager: ModelStateManager) -> None:
        """Initialize the incremental executor."""
        self.state_manager = state_manager

    def should_run_incremental(
        self, model_name: str, sql_query: str, config: IncrementalConfig
    ) -> bool:
        """Determine if a model should run incrementally or as a full load."""
        state = self.state_manager.get_model_state(model_name)

        if state is None:
            logger.info(f"No state exists for {model_name}, running full load")
            return False

        # Check if model definition changed
        current_sql_hash = self.state_manager.compute_sql_hash(sql_query)
        current_config_hash = self.state_manager.compute_config_hash(config)

        # Check if hashes match (debug logging removed for cleaner output)

        # Check if we have unknown hashes (from rebuilt state)
        if state.sql_hash == "unknown" or state.config_hash == "unknown":
            logger.info(f"Model state has unknown hashes for {model_name}, running full load")
            return False

        if state.sql_hash != current_sql_hash or state.config_hash != current_config_hash:
            logger.info(f"Model definition changed for {model_name}, running full load")
            return False

        # For append strategy, we don't need last_processed_value - we use time-based filtering
        strategy = config.get("strategy", "append")
        if strategy == "append":
            logger.info(f"Running incremental append for {model_name} with time-based filtering")
            return True

        # For merge and delete_insert strategies, check if we have a last processed value
        # unless start_date is "auto" or a variable reference (which uses external values)
        strategy_config = config.get(strategy, {})
        start_date = strategy_config.get("start_date")

        if start_date == "auto" or (
            start_date
            and (
                start_date.startswith("@")
                or (start_date.startswith("{{") and start_date.endswith("}}"))
            )
        ):
            logger.info(
                f"Running incremental {strategy} for {model_name} with {start_date} start_date"
            )
            return True

        if not state.last_processed_value:
            logger.info(f"No last processed value for {model_name}, running full load")
            return False

        logger.info(f"Running incremental load for {model_name}")
        return True

    def get_time_filter_condition(
        self,
        config: IncrementalAppendConfig | IncrementalMergeConfig | IncrementalDeleteInsertConfig,
        last_processed_value: str | None = None,
        variables: dict[str, Any] | None = None,
        table_name: str | None = None,
    ) -> str:
        """Generate time-based filter condition for incremental loading."""
        time_column = config["time_column"]
        start_date = config.get("start_date")

        # If we have a last processed value, use it (subsequent runs)
        if last_processed_value:
            return f"{time_column} > '{last_processed_value}'"

        # Handle different start_date values for first run
        if start_date == "auto":
            if table_name:
                # Apply lookback to the auto start_date if specified
                lookback = config.get("lookback")
                if lookback:
                    lookback_interval = self._parse_lookback(lookback)
                    if lookback_interval:
                        return f"{time_column} > (SELECT COALESCE(MAX({time_column}) - INTERVAL {lookback_interval}, '1900-01-01') FROM {table_name})"
                return f"{time_column} > (SELECT COALESCE(MAX({time_column}), '1900-01-01') FROM {table_name})"
            else:
                # Fallback to a default date if table_name is not provided
                return f"{time_column} >= '1900-01-01'"
        elif start_date == "CURRENT_DATE":
            return f"{time_column} >= CURRENT_DATE"
        elif start_date:
            # Check if start_date is a variable reference
            if start_date.startswith("@") or (
                start_date.startswith("{{") and start_date.endswith("}}")
            ):
                # Resolve variable
                resolved_start_date = self._resolve_variable(start_date, variables)
                if resolved_start_date:
                    logger.info(f"Resolved variable {start_date} to {resolved_start_date}")
                    return f"{time_column} >= '{resolved_start_date}'"
                else:
                    # Fallback to default if variable not found
                    logger.warning(f"Variable {start_date} not found, using default 7-day lookback")
                    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                    return f"{time_column} >= '{seven_days_ago}'"
            else:
                return f"{time_column} >= '{start_date}'"
        else:
            # Default to last 7 days if no start_date specified
            seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            return f"{time_column} >= '{seven_days_ago}'"

    def _apply_lookback_to_time_filter(
        self,
        time_filter: str,
        config: IncrementalAppendConfig | IncrementalMergeConfig | IncrementalDeleteInsertConfig,
    ) -> str:
        """Apply lookback to the time filter to handle late-arriving data."""
        lookback = config.get("lookback")
        if not lookback:
            return time_filter

        time_column = config["time_column"]
        lookback_interval = self._parse_lookback(lookback)
        if lookback_interval is None:
            logger.warning(f"Could not parse lookback: {lookback}")
            return time_filter

        # Extract the date from the time filter
        import re

        match = re.match(rf"{re.escape(time_column)}\s*([><=]+)\s*'([^']+)'", time_filter)
        if not match:
            logger.warning(f"Could not parse time filter for lookback: {time_filter}")
            return time_filter

        operator = match.group(1)
        start_date = match.group(2)

        # Apply lookback to the start_date
        return f"{time_column} {operator} (CAST('{start_date}' AS DATE) - INTERVAL {lookback_interval})"

    def _parse_lookback(self, lookback: str) -> str | None:
        """Parse lookback string to SQL interval format."""
        lookback = lookback.lower().strip()

        # Define time unit mappings
        time_units = {
            "minute": ("minutes", 1),
            "hour": ("hours", 1),
            "day": ("days", 1),
            "week": ("days", 7),  # Convert weeks to days
            "month": ("days", 30),  # Approximate months as 30 days
        }

        # Find matching time unit and extract value
        for unit, (sql_unit, multiplier) in time_units.items():
            if unit in lookback:
                try:
                    value = int(lookback.split()[0])
                    converted_value = value * multiplier
                    return f"'{converted_value} {sql_unit}'"
                except (ValueError, IndexError):
                    continue

        return None

    def _resolve_variable(
        self, variable_ref: str, variables: dict[str, Any] | None = None
    ) -> str | None:
        """Resolve a variable reference to its value."""
        if not variables:
            return None

        # Handle @variable_name syntax
        if variable_ref.startswith("@"):
            var_name = variable_ref[1:]
            return variables.get(var_name)

        # Handle {{ variable_name }} syntax
        if variable_ref.startswith("{{") and variable_ref.endswith("}}"):
            var_name = variable_ref[2:-2].strip()
            return variables.get(var_name)

        return None

    def _resolve_variables_in_string(self, text: str, variables: dict[str, Any]) -> str:
        """Resolve all variable references in a string."""
        import re

        # Handle @variable syntax
        def replace_at_vars(match: re.Match[str]) -> str:
            var_name = match.group(1)
            return str(variables.get(var_name, f"@{var_name}"))

        text = re.sub(r"@(\w+)", replace_at_vars, text)

        # Handle {{ variable }} syntax
        def replace_brace_vars(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            return str(variables.get(var_name, f"{{{{ {var_name} }}}}"))

        text = re.sub(r"\{\{\s*(\w+)\s*\}\}", replace_brace_vars, text)

        return text

    def _add_date_casting(self, where_condition: str) -> str:
        """Add proper date casting for date comparisons in WHERE conditions."""
        import re

        # Pattern to match date comparisons like "column >= 2024-01-01" or "column >= '2024-01-01'"
        # This will match: column_name >= YYYY-MM-DD or column_name >= 'YYYY-MM-DD' etc.
        pattern = r"(\w+)\s*([><=]+)\s*(\'?)(\d{4}-\d{2}-\d{2})\3"

        def replace_date_comparison(match: re.Match[str]) -> str:
            column = match.group(1)
            operator = match.group(2)
            match.group(3)
            date_value = match.group(4)
            return f"{column} {operator} CAST('{date_value}' AS DATE)"

        return re.sub(pattern, replace_date_comparison, where_condition)

    def execute_append_strategy(
        self,
        model_name: str,
        sql_query: str,
        config: IncrementalAppendConfig,
        adapter: DatabaseAdapter,
        table_name: str,
        variables: dict[str, Any] | None = None,
    ) -> None:
        """Execute append-only incremental strategy."""
        # Executing append strategy

        # Get current state
        state = self.state_manager.get_model_state(model_name)
        last_processed_value = state.last_processed_value if state else None

        # Get time filter condition
        time_filter = self.get_time_filter_condition(
            config, last_processed_value, variables, table_name
        )

        # Apply lookback to the time filter if needed
        time_filter_with_lookback = self._apply_lookback_to_time_filter(time_filter, config)

        # Add the combined time filter to the query
        filtered_sql = self._add_where_clause(sql_query, time_filter_with_lookback)

        # Execute the filtered query and insert into target table
        if hasattr(adapter, "execute_incremental_append") and callable(
            adapter.execute_incremental_append
        ):
            adapter.execute_incremental_append(table_name, filtered_sql)
        else:
            # Fallback to regular table creation
            adapter.create_table(table_name, filtered_sql)

        # Update state with current max time value
        current_time = datetime.now(UTC).isoformat()
        self.state_manager.update_processed_value(model_name, current_time, strategy="append")

    def execute_merge_strategy(
        self,
        model_name: str,
        sql_query: str,
        config: IncrementalMergeConfig,
        adapter: DatabaseAdapter,
        table_name: str,
        variables: dict[str, Any] | None = None,
    ) -> None:
        """Execute merge incremental strategy."""
        # Executing merge strategy

        # Get current state
        state = self.state_manager.get_model_state(model_name)
        last_processed_value = state.last_processed_value if state else None

        # Apply time filter
        time_filter = self.get_time_filter_condition(
            config, last_processed_value, variables, table_name
        )
        filtered_sql = self._add_where_clause(sql_query, time_filter)

        # Delegate to adapter for database-specific merge logic
        if hasattr(adapter, "execute_incremental_merge") and callable(
            adapter.execute_incremental_merge
        ):
            adapter.execute_incremental_merge(table_name, filtered_sql, config)
        else:
            # Fallback to regular execution
            logger.warning(
                f"Adapter {type(adapter).__name__} does not support incremental merge, falling back to regular execution"
            )
            adapter.execute_query(filtered_sql)

        # Update state
        current_time = datetime.now(UTC).isoformat()
        self.state_manager.update_processed_value(model_name, current_time, strategy="merge")

    def execute_delete_insert_strategy(
        self,
        model_name: str,
        sql_query: str,
        config: IncrementalDeleteInsertConfig,
        adapter: DatabaseAdapter,
        table_name: str,
        variables: dict[str, Any] | None = None,
    ) -> None:
        """Execute delete+insert incremental strategy."""
        # Executing delete+insert strategy

        # Get current state
        state = self.state_manager.get_model_state(model_name)
        last_processed_value = state.last_processed_value if state else None

        # Apply time filter
        time_filter = self.get_time_filter_condition(
            config, last_processed_value, variables, table_name
        )
        filtered_sql = self._add_where_clause(sql_query, time_filter)

        # Generate DELETE + INSERT statements
        where_condition = config["where_condition"]
        # Resolve variables in where_condition
        if variables:
            where_condition = self._resolve_variables_in_string(where_condition, variables)

        # Add proper date casting for date comparisons
        where_condition = self._add_date_casting(where_condition)

        # Use adapter's qualification method if available, otherwise construct manually
        if hasattr(adapter, "_qualify_object_name"):
            qualified_table_name = adapter._qualify_object_name(table_name)
        else:
            qualified_table_name = table_name

        delete_sql = f"DELETE FROM {qualified_table_name} WHERE {where_condition}"

        # Execute delete and insert
        if hasattr(adapter, "execute_incremental_delete_insert") and callable(
            adapter.execute_incremental_delete_insert
        ):
            adapter.execute_incremental_delete_insert(table_name, delete_sql, filtered_sql)
        else:
            # Fallback to regular execution
            adapter.execute_query(delete_sql)
            adapter.execute_query(filtered_sql)

        # Update state
        current_time = datetime.now(UTC).isoformat()
        self.state_manager.update_processed_value(
            model_name, current_time, strategy="delete_insert"
        )

    def _add_where_clause(self, sql_query: str, where_condition: str) -> str:
        """Add WHERE clause to SQL query."""
        try:
            parsed = sqlglot.parse_one(sql_query)

            # Find existing WHERE clause
            where_clause = None
            for node in parsed.walk():
                if isinstance(node, exp.Where):
                    where_clause = node
                    break

            # Parse the where condition as a proper SQL expression
            where_expr = sqlglot.parse_one(where_condition)

            if where_clause:
                # Add to existing WHERE clause
                existing_condition = where_clause.this
                new_condition = exp.and_(existing_condition, where_expr)
                where_clause.set("this", new_condition)
            else:
                # Add new WHERE clause
                where_clause = exp.Where(this=where_expr)
                # Find the main SELECT and add WHERE
                for node in parsed.walk():
                    if isinstance(node, exp.Select):
                        node.set("where", where_clause)
                        break

            return str(parsed)
        except Exception as e:
            logger.warning(f"Could not add WHERE clause: {e}")
            # Fallback: simple string concatenation
            return f"{sql_query} WHERE {where_condition}"
