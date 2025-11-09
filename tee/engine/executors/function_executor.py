"""Function execution logic."""

import logging
from typing import Dict, Any, List

from tee.adapters.base.core import DatabaseAdapter
from tee.parser.shared.types import ParsedFunction
from ..metadata.metadata_extractor import MetadataExtractor

logger = logging.getLogger(__name__)


class FunctionExecutor:
    """Handles function execution logic."""

    def __init__(
        self,
        adapter: DatabaseAdapter,
        project_folder: str,
        metadata_extractor: MetadataExtractor,
    ):
        """
        Initialize the function executor.

        Args:
            adapter: Database adapter instance
            project_folder: Project folder path
            metadata_extractor: Metadata extractor instance
        """
        self.adapter = adapter
        self.project_folder = project_folder
        self.metadata_extractor = metadata_extractor
        # Track schemas that have been processed for tag attachment
        self._processed_schemas: Dict[str, Dict[str, Any]] = {}

    def execute(
        self, parsed_functions: Dict[str, ParsedFunction], execution_order: List[str]
    ) -> Dict[str, Any]:
        """
        Execute user-defined functions in dependency order.

        Functions are always created/overwritten (no versioning).
        Execution order should include functions before models that depend on them.

        Args:
            parsed_functions: Dictionary mapping function names to parsed function data
            execution_order: List of all objects (functions and models) in execution order

        Returns:
            Dictionary with execution results and status
        """
        results = {
            "executed_functions": [],
            "failed_functions": [],
            "execution_log": [],
            "warnings": [],
        }

        # Filter execution order to get only functions
        function_order = [
            name for name in execution_order if name in parsed_functions
        ]

        if not function_order:
            logger.info("No functions to execute")
            return results

        logger.info(
            f"Starting execution of {len(function_order)} functions using {self.adapter.__class__.__name__}"
        )

        for function_name in function_order:
            # Skip test nodes
            if function_name.startswith("test:"):
                logger.debug(f"Skipping test node: {function_name}")
                continue

            try:
                logger.info(f"Executing function: {function_name}")

                if function_name not in parsed_functions:
                    logger.warning(f"Function {function_name} not found in parsed functions")
                    results["failed_functions"].append(
                        {"function": function_name, "error": "Function not found in parsed functions"}
                    )
                    continue

                function_data = parsed_functions[function_name]

                # Extract function SQL
                function_sql = self._extract_function_sql(function_data, function_name)
                if not function_sql:
                    results["failed_functions"].append(
                        {"function": function_name, "error": "No SQL found for function"}
                    )
                    continue

                # Extract metadata
                metadata = self.metadata_extractor.extract_function_metadata(function_data)

                # Extract schema name and attach schema-level tags if needed
                schema_name = self._extract_schema_name(function_name)
                if schema_name:
                    self._attach_schema_tags_if_needed(schema_name)

                # Execute function creation (always CREATE OR REPLACE)
                # No need to check if function exists - CREATE OR REPLACE handles replacement
                # All supported databases (DuckDB, Snowflake, PostgreSQL, BigQuery) support CREATE OR REPLACE
                self.adapter.create_function(function_name, function_sql, metadata)

                results["executed_functions"].append(function_name)
                results["execution_log"].append(
                    {
                        "function": function_name,
                        "status": "success",
                    }
                )

                logger.info(f"Successfully executed function: {function_name}")

            except Exception as e:
                error_msg = f"Error executing function {function_name}: {str(e)}"
                logger.error(error_msg)
                results["failed_functions"].append({"function": function_name, "error": str(e)})
                results["execution_log"].append(
                    {"function": function_name, "status": "failed", "error": str(e)}
                )
                # Continue with other functions even if one fails
                # Functions are independent, so one failure shouldn't stop others

        logger.info(
            f"Function execution completed. {len(results['executed_functions'])} successful, "
            f"{len(results['failed_functions'])} failed"
        )
        return results

    def _extract_function_sql(self, function_data: Dict[str, Any], function_name: str) -> str:
        """
        Extract SQL query from function data.

        Args:
            function_data: Parsed function data
            function_name: Name of the function

        Returns:
            SQL query string or empty string if not found
        """
        code_data = function_data.get("code", {})
        if not code_data or "sql" not in code_data:
            logger.error(f"No code.sql found for function {function_name}")
            return ""

        sql_data = code_data["sql"]
        # For functions, we use original_sql (the CREATE OR REPLACE FUNCTION statement)
        if "original_sql" in sql_data:
            return sql_data["original_sql"]

        logger.error(f"No original_sql found for function {function_name}")
        return ""

    def _extract_schema_name(self, function_name: str) -> str:
        """
        Extract schema name from function name.

        Args:
            function_name: Full function name (e.g., "my_schema.function_name")

        Returns:
            Schema name or None if no schema in function name
        """
        if "." in function_name:
            return function_name.split(".", 1)[0]
        return None

    def _attach_schema_tags_if_needed(self, schema_name: str) -> None:
        """
        Attach tags to schema if not already processed and if schema-level tags are configured.

        Args:
            schema_name: Name of the schema
        """
        # Skip if already processed
        if schema_name in self._processed_schemas:
            return

        # Load schema-level tags from project config
        schema_metadata = self.metadata_extractor.load_schema_metadata(
            schema_name, self.project_folder
        )
        if not schema_metadata:
            # Mark as processed even if no tags to avoid repeated checks
            self._processed_schemas[schema_name] = {}
            return

        # Attach tags to schema if adapter supports it
        if hasattr(self.adapter, "attach_tags") and hasattr(self.adapter, "_create_schema_if_needed"):
            try:
                # Use the adapter's _create_schema_if_needed with metadata to attach tags
                # This will create the schema if needed and attach tags
                self.adapter._create_schema_if_needed(f"{schema_name}.dummy", schema_metadata)
                logger.info(f"Attached tags to schema: {schema_name}")
            except Exception as e:
                logger.warning(f"Could not attach tags to schema {schema_name}: {e}")

        # Mark as processed
        self._processed_schemas[schema_name] = schema_metadata

