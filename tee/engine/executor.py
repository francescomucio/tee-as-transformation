"""
Enhanced Model Executor with pluggable database adapters.

This module provides the high-level executor that uses the new adapter system
for database-agnostic SQL model execution.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from .execution_engine import ExecutionEngine
from .config import load_database_config
from ..adapters import AdapterConfig


class ModelExecutor:
    """Enhanced executor that uses the new adapter system for database-agnostic execution."""

    def __init__(
        self,
        project_folder: str,
        config: Optional[Union[AdapterConfig, Dict[str, Any]]] = None,
        config_name: str = "default",
    ):
        """
        Initialize the ModelExecutor.

        Args:
            project_folder: Path to the project folder containing SQL models
            config: Database adapter configuration (AdapterConfig or dict, if None, loads from config files)
            config_name: Configuration name to load (if config is None)
        """
        self.project_folder = project_folder

        # Handle configuration
        if config is None:
            self.config = load_database_config(config_name, project_folder)
        elif isinstance(config, dict):
            # Convert dict to AdapterConfig, handling adapter-specific fields
            from ..adapters.base import AdapterConfig
            from pathlib import Path

            # Resolve relative paths relative to project folder
            if "path" in config and config["path"] and not Path(config["path"]).is_absolute():
                config["path"] = str(Path(project_folder) / config["path"])

            # Handle adapter-specific fields by moving them to 'extra'
            extra_fields = {}
            adapter_specific_fields = ["account"]  # Add more as needed

            for field in adapter_specific_fields:
                if field in config:
                    extra_fields[field] = config.pop(field)

            # Add existing extra fields
            if "extra" in config and config["extra"]:
                extra_fields.update(config["extra"])

            # Set extra field if we have any
            if extra_fields:
                config["extra"] = extra_fields

            self.config = AdapterConfig(**config)
        else:
            self.config = config

        self.execution_engine = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_models(
        self,
        parser,
        variables: Optional[Dict[str, Any]] = None,
        parsed_models: Optional[Dict[str, Any]] = None,
        execution_order: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Execute the parsed SQL models using the enhanced execution engine.

        Args:
            parser: Parser instance that has collected models and execution order
            variables: Optional dictionary of variables to inject into Python model functions
            parsed_models: Optional pre-filtered models dict (overrides parser.collect_models())
            execution_order: Optional pre-filtered execution order (overrides parser.get_execution_order())

        Returns:
            Dictionary containing execution results
        """
        self.logger.info("Starting model execution with enhanced adapter system")

        # Create execution engine
        self.execution_engine = ExecutionEngine(
            self.config, project_folder=self.project_folder, variables=variables
        )

        try:
            # Connect to database
            self.execution_engine.connect()
            self.logger.info("Connected to database successfully")

            # Get parsed models and execution order from parser (or use provided filtered versions)
            if parsed_models is None:
                parsed_models = parser.collect_models()
            if execution_order is None:
                execution_order = parser.get_execution_order()

            # Evaluate Python models before SQL execution
            # This ensures all Python models have their SQLGlot expressions ready
            parsed_models = parser.orchestrator.evaluate_python_models(parsed_models, variables)

            self.logger.info(f"Executing {len(execution_order)} models in dependency order")
            self.logger.info(f"Execution order: {' -> '.join(execution_order)}")

            # Log database and dialect information
            db_info = self.execution_engine.get_database_info()
            self.logger.info(f"Using adapter: {db_info['adapter_type']}")
            self.logger.info(f"Database type: {db_info['database_type']}")
            if db_info.get("source_dialect") and db_info.get("target_dialect"):
                self.logger.info(
                    f"SQL dialect conversion: {db_info['source_dialect']} -> {db_info['target_dialect']}"
                )

            # Execute all models
            results = self.execution_engine.execute_models(parsed_models, execution_order)

            # Update Python parser's cached models with qualified SQL from execution
            # This ensures the qualified_sql is saved back to the Python parser
            parser.orchestrator.update_python_models_with_qualified_sql(parsed_models)

            # Update the main parser's cached models with qualified SQL from execution
            # This ensures the qualified_sql is saved back to the main parser for JSON output
            parser.update_parsed_models(parsed_models)

            # Log results
            self.logger.info(f"Successfully executed: {len(results['executed_tables'])} tables")
            self.logger.info(f"Failed: {len(results['failed_tables'])} tables")
            if results.get("warnings"):
                self.logger.info(f"Warnings: {len(results['warnings'])} warnings")

            # Log dialect conversion summary
            if results.get("dialect_conversions"):
                self.logger.info(
                    f"Performed {len(results['dialect_conversions'])} SQL dialect conversions"
                )

            if results["executed_tables"]:
                self.logger.info("Successfully executed tables:")
                for table in results["executed_tables"]:
                    table_info = results["table_info"].get(table, {})
                    row_count = table_info.get("row_count", 0)
                    execution_log = next(
                        (log for log in results["execution_log"] if log["table"] == table), {}
                    )
                    materialization = execution_log.get("materialization", "table")
                    self.logger.info(f"  - {table}: {row_count} rows ({materialization})")

            if results["failed_tables"]:
                self.logger.error("Failed tables:")
                for failure in results["failed_tables"]:
                    self.logger.error(f"  - {failure['table']}: {failure['error']}")

            return results

        except Exception as e:
            self.logger.error(f"Error during execution: {e}")
            raise
        finally:
            # Always disconnect
            if self.execution_engine:
                self.execution_engine.disconnect()
                self.logger.info("Disconnected from database")

    def get_database_info(self) -> Optional[Dict[str, Any]]:
        """Get database connection information."""
        if self.execution_engine:
            return self.execution_engine.get_database_info()
        return None

    def test_connection(self) -> bool:
        """
        Test the database connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.execution_engine = ExecutionEngine(self.config, project_folder=self.project_folder)
            self.execution_engine.connect()
            self.logger.info("Database connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
        finally:
            if self.execution_engine:
                self.execution_engine.disconnect()

    def list_supported_materializations(self) -> List[str]:
        """Get list of supported materialization types for the current adapter."""
        try:
            self.execution_engine = ExecutionEngine(self.config, project_folder=self.project_folder)
            return [m.value for m in self.execution_engine.adapter.get_supported_materializations()]
        except Exception as e:
            self.logger.error(f"Could not get supported materializations: {e}")
            return []
        finally:
            if self.execution_engine:
                self.execution_engine.disconnect()
