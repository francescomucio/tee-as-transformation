"""
High-level orchestration for parsing and analysis workflows.
"""

import logging
from pathlib import Path
from typing import Any

from tee.parser.analysis import DependencyGraphBuilder, TableResolver
from tee.parser.output import JSONExporter, ReportGenerator
from tee.parser.parsers import FunctionPythonParser, FunctionSQLParser, ParserFactory
from tee.parser.processing import FileDiscovery, substitute_sql_variables, validate_sql_variables
from tee.parser.shared.exceptions import ParserError
from tee.parser.shared.function_utils import standardize_parsed_function
from tee.parser.shared.types import (
    ConnectionConfig,
    DependencyGraph,
    ParsedFunction,
    ParsedModel,
    Variables,
)

# Configure logging
logger = logging.getLogger(__name__)


class ParserOrchestrator:
    """High-level orchestrator for parsing and analysis workflows."""

    def __init__(
        self,
        project_folder: str,
        connection: ConnectionConfig,
        variables: Variables | None = None,
        project_config: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the orchestrator.

        Args:
            project_folder: Path to the project folder containing SQL files
            connection: Connection configuration dict with 'type' key
            variables: Optional dictionary of variables for SQL substitution
            project_config: Optional project configuration for OTS export
        """
        self.project_folder = Path(project_folder)
        self.connection = connection
        self.variables = variables or {}
        self.models_folder = self.project_folder / "models"
        self.functions_folder = self.project_folder / "functions"
        self.project_config = project_config or {}

        # Initialize components
        self.file_discovery = FileDiscovery(self.models_folder, functions_folder=self.functions_folder)
        self.table_resolver = TableResolver(connection)
        self.dependency_builder = DependencyGraphBuilder()
        self.json_exporter = JSONExporter(
            self.project_folder / "output", 
            project_config, 
            project_folder=self.project_folder
        )
        self.report_generator = ReportGenerator(self.project_folder / "output")
        self.transformer = project_config is not None  # Flag to enable OTS export

        # Cached results
        self._parsed_models: dict[str, ParsedModel] | None = None
        self._parsed_functions: dict[str, ParsedFunction] | None = None
        self._dependency_graph: DependencyGraph | None = None

    def discover_and_parse_models(self) -> dict[str, ParsedModel]:
        """
        Discover and parse all model files in the project.

        Returns:
            Dict mapping full_table_name to parsed model data
        """
        if self._parsed_models is not None:
            return self._parsed_models

        try:
            logger.info("Starting model discovery and parsing")

            # Discover all files
            files = self.file_discovery.discover_all_files()
            logger.info(
                f"Discovered {len(files['sql'])} SQL files, "
                f"{len(files['python'])} Python files, "
                f"and {len(files['ots'])} OTS module files"
            )

            parsed_models = {}

            # Load OTS modules first (imported modules - will be merged during compile)
            # Note: During normal parsing, we just collect them for later compilation
            # Conflict detection and merging happens during compile phase
            if files["ots"]:
                logger.info("Found imported OTS modules (will be processed during compile phase)")
                # Store OTS file paths for later processing during compile
                # For now, we'll skip loading them here - they'll be loaded during compile
                # This prevents conflicts during regular parsing

            # Parse SQL files
            for sql_file in files["sql"]:
                try:
                    logger.debug(f"Processing SQL file: {sql_file}")

                    # Read SQL content
                    with open(sql_file, encoding="utf-8") as f:
                        sql_content = f.read()

                    # Apply variable substitution if variables are provided
                    if self.variables:
                        try:
                            validate_sql_variables(sql_content, self.variables)
                            sql_content = substitute_sql_variables(sql_content, self.variables)
                            logger.debug(f"Applied variable substitution to {sql_file}")
                        except Exception as e:
                            logger.error(f"Variable substitution error in {sql_file}: {e}")
                            continue

                    # Generate full table name
                    full_table_name = self.table_resolver.generate_full_table_name(
                        sql_file, self.models_folder
                    )

                    # Parse with appropriate parser
                    parser = ParserFactory.create_parser(sql_file)
                    parsed_args = parser.parse(
                        sql_content, file_path=sql_file, table_name=full_table_name
                    )

                    parsed_models[full_table_name] = parsed_args
                    logger.debug(f"Successfully parsed SQL model: {full_table_name}")

                except Exception as e:
                    logger.error(f"Error processing SQL file {sql_file}: {e}")
                    continue

            # Parse Python files
            for python_file in files["python"]:
                try:
                    logger.debug(f"Processing Python file: {python_file}")

                    # Read Python content
                    with open(python_file, encoding="utf-8") as f:
                        python_content = f.read()

                    # Parse with Python parser
                    parser = ParserFactory.create_parser(python_file)
                    python_models = parser.parse(python_content, file_path=python_file)

                    # Add each model to the result with proper table naming
                    for table_name, model_data in python_models.items():
                        # For Python models, use the table_name from the decorator directly
                        # Only generate full table name if it doesn't already contain a schema
                        if "." in table_name:
                            # Table name already includes schema (e.g., "my_schema.incremental_example")
                            full_table_name = table_name
                        else:
                            # Generate full table name for unqualified table names
                            fake_file_path = python_file.parent / f"{table_name}.py"
                            full_table_name = self.table_resolver.generate_full_table_name(
                                fake_file_path, self.models_folder
                            )

                        parsed_models[full_table_name] = model_data
                        logger.debug(f"Successfully parsed Python model: {full_table_name}")

                except Exception as e:
                    logger.error(f"Error processing Python file {python_file}: {e}")
                    continue

            # Evaluate Python models to populate their code data
            logger.info("Evaluating Python models to populate code data")
            parsed_models = self.evaluate_python_models(parsed_models, self.variables)

            # Cache the result
            self._parsed_models = parsed_models
            logger.info(f"Successfully parsed {len(parsed_models)} models")

            return parsed_models

        except Exception as e:
            raise ParserError(f"Failed to discover and parse models: {e}") from e

    def discover_and_parse_functions(self) -> dict[str, ParsedFunction]:
        """
        Discover and parse all function files in the project.

        Returns:
            Dict mapping qualified_function_name to parsed function data
        """
        if self._parsed_functions is not None:
            return self._parsed_functions

        try:
            logger.info("Starting function discovery and parsing")

            # Discover all function files
            function_files = self.file_discovery.discover_function_files()
            logger.info(
                f"Discovered {len(function_files['sql'])} SQL function files, "
                f"{len(function_files['python'])} Python function files, "
                f"and {len(function_files['database_overrides'])} database override files"
            )

            parsed_functions = {}

            # Determine current database type for override matching
            current_db_type = None
            if self.connection and isinstance(self.connection, dict):
                current_db_type = self.connection.get("type", "").lower()
            elif hasattr(self, "connection") and hasattr(self.connection, "type"):
                current_db_type = str(self.connection.type).lower()

            # Build a map of function names to their override files
            # Format: {base_function_name: {database: override_file_path}}
            override_map = {}
            for override_file in function_files["database_overrides"]:
                # Extract database name from filename (e.g., "calculate_percentage.snowflake.sql" -> "snowflake")
                stem = override_file.stem  # filename without extension
                if "." in stem:
                    parts = stem.split(".")
                    if len(parts) >= 2:
                        db_name = parts[-1].lower()
                        base_name = ".".join(parts[:-1])  # Everything before the database name
                        if db_name not in override_map:
                            override_map[db_name] = {}
                        override_map[db_name][base_name] = override_file

            # Parse SQL functions
            sql_parser = FunctionSQLParser(connection=self.connection, project_config=self.project_config)
            for sql_file in function_files["sql"]:
                try:
                    logger.debug(f"Processing SQL function file: {sql_file}")

                    # Check if there's a database override for this function
                    sql_stem = sql_file.stem  # filename without extension
                    override_file = None
                    if current_db_type and current_db_type in override_map:
                        if sql_stem in override_map[current_db_type]:
                            override_file = override_map[current_db_type][sql_stem]
                            logger.debug(f"Found database override for {sql_stem}: {override_file}")

                    # Use override file if available, otherwise use generic SQL file
                    file_to_parse = override_file if override_file else sql_file

                    # Read SQL content
                    with open(file_to_parse, encoding="utf-8") as f:
                        sql_content = f.read()

                    # Parse SQL function
                    function_results = sql_parser.parse(sql_content, file_path=file_to_parse)

                    # Process each function found in the file
                    for function_name, function_data in function_results.items():
                        # Generate qualified function name
                        qualified_name = self.table_resolver.generate_full_function_name(
                            sql_file, self.functions_folder, function_data["function_metadata"]
                        )

                        # Standardize function structure
                        standardized = standardize_parsed_function(
                            function_data=function_data,
                            function_name=function_name,
                            file_path=str(file_to_parse),
                            is_python_function=False,
                        )

                        parsed_functions[qualified_name] = standardized
                        logger.debug(f"Successfully parsed SQL function: {qualified_name}")

                except Exception as e:
                    logger.error(f"Error processing SQL function file {sql_file}: {e}")
                    continue

            # Also process database override files that don't have a generic SQL file
            # (standalone database-specific functions)
            if current_db_type and current_db_type in override_map:
                for base_name, override_file in override_map[current_db_type].items():
                    # Check if we already processed this function (from generic SQL file)
                    # We need to check by qualified name, so we'll parse it first
                    try:
                        logger.debug(f"Processing standalone database override: {override_file}")

                        # Read SQL content
                        with open(override_file, encoding="utf-8") as f:
                            sql_content = f.read()

                        # Parse SQL function
                        function_results = sql_parser.parse(sql_content, file_path=override_file)

                        # Process each function found in the file
                        for function_name, function_data in function_results.items():
                            # Generate qualified function name
                            qualified_name = self.table_resolver.generate_full_function_name(
                                override_file, self.functions_folder, function_data["function_metadata"]
                            )

                            # Only add if not already present (generic SQL takes precedence if both exist)
                            if qualified_name not in parsed_functions:
                                # Standardize function structure
                                standardized = standardize_parsed_function(
                                    function_data=function_data,
                                    function_name=function_name,
                                    file_path=str(override_file),
                                    is_python_function=False,
                                )

                                parsed_functions[qualified_name] = standardized
                                logger.debug(f"Successfully parsed standalone database override function: {qualified_name}")

                    except Exception as e:
                        logger.error(f"Error processing database override file {override_file}: {e}")
                        continue

            # Parse Python functions
            python_parser = FunctionPythonParser()
            for python_file in function_files["python"]:
                try:
                    logger.debug(f"Processing Python function file: {python_file}")

                    # Read Python content
                    with open(python_file, encoding="utf-8") as f:
                        python_content = f.read()

                    # Parse Python functions
                    function_results = python_parser.parse(python_content, file_path=python_file)

                    # Process each function found in the file
                    for function_name, function_data in function_results.items():
                        # Generate qualified function name
                        qualified_name = self.table_resolver.generate_full_function_name(
                            python_file, self.functions_folder, function_data["function_metadata"]
                        )

                        # Check if this function already exists (e.g., from SQL file)
                        if qualified_name in parsed_functions:
                            existing_function = parsed_functions[qualified_name]
                            # If existing function has code but Python function doesn't,
                            # this is likely a metadata-only Python file for a SQL function
                            if existing_function.get("code") and not function_data.get("code"):
                                # Merge metadata from Python file into existing function
                                existing_metadata = existing_function.get("function_metadata", {})
                                new_metadata = function_data.get("function_metadata", {})
                                # Merge metadata (Python metadata takes precedence)
                                merged_metadata = {**existing_metadata, **new_metadata}
                                # Update the existing function with merged metadata
                                existing_function["function_metadata"] = merged_metadata
                                logger.debug(f"Merged metadata from Python file into existing SQL function: {qualified_name}")
                                continue

                        # Standardize function structure
                        standardized = standardize_parsed_function(
                            function_data=function_data,
                            function_name=function_name,
                            file_path=str(python_file),
                            is_python_function=True,
                        )

                        parsed_functions[qualified_name] = standardized
                        logger.debug(f"Successfully parsed Python function: {qualified_name}")

                except Exception as e:
                    logger.error(f"Error processing Python function file {python_file}: {e}")
                    continue

            # Cache the result
            self._parsed_functions = parsed_functions
            logger.info(f"Successfully parsed {len(parsed_functions)} functions")

            return parsed_functions

        except Exception as e:
            raise ParserError(f"Failed to discover and parse functions: {e}") from e

    def build_dependency_graph(self) -> DependencyGraph:
        """
        Build a dependency graph from the parsed models.

        Returns:
            Dict containing dependency graph information
        """
        if self._dependency_graph is not None:
            return self._dependency_graph

        try:
            # Use cached parsed models, discover if not available
            if self._parsed_models is None:
                parsed_models = self.discover_and_parse_models()
            else:
                parsed_models = self._parsed_models

            logger.info("Building dependency graph")

            # Get parsed functions if available
            parsed_functions = None
            if self._parsed_functions is not None:
                parsed_functions = self._parsed_functions
            else:
                # Try to discover and parse functions
                try:
                    parsed_functions = self.discover_and_parse_functions()
                except Exception as e:
                    logger.debug(f"Could not parse functions for dependency graph: {e}")
                    parsed_functions = {}

            # Store parsed_functions for report generation (before building graph)
            if parsed_functions:
                self._parsed_functions = parsed_functions

            # Build the graph (pass project_folder for test discovery and parsed_functions)
            self._dependency_graph = self.dependency_builder.build_graph(
                parsed_models, 
                self.table_resolver, 
                project_folder=Path(self.project_folder),
                parsed_functions=parsed_functions
            )

            logger.info(f"Built dependency graph with {len(self._dependency_graph['nodes'])} nodes")

            return self._dependency_graph

        except Exception as e:
            raise ParserError(f"Failed to build dependency graph: {e}") from e

    def export_all(self) -> dict[str, Path]:
        """
        Export all parsed models, dependency graph, and OTS modules.

        Returns:
            Dict mapping export type to file path
        """
        try:
            # Ensure we have parsed models and dependency graph
            parsed_models = self.discover_and_parse_models()
            dependency_graph = self.build_dependency_graph()

            # Export JSON files
            json_results = self.json_exporter.export_all(parsed_models, dependency_graph)

            # Export OTS modules and test library
            ots_results = {}
            test_library_path = None
            if self.transformer:
                try:
                    # Export test library first
                    project_name = self.project_config.get("project_folder", self.project_folder.name)
                    test_library_path = self.json_exporter.export_test_library(project_name)

                    # Export OTS modules (will include test_library_path reference)
                    ots_results = self.json_exporter.export_ots_modules(
                        parsed_models, 
                        test_library_path=test_library_path
                    )
                    logger.info(f"Exported {len(ots_results)} OTS modules")
                except Exception as e:
                    logger.warning(f"Failed to export OTS modules: {e}")

            # Generate reports (pass parsed_functions for function identification)
            parsed_functions = self._parsed_functions or {}
            report_results = self.report_generator.generate_all_reports(dependency_graph, parsed_functions)

            # Combine results
            all_results = {**json_results, **ots_results, **report_results}

            # Add test library if exported
            if test_library_path:
                all_results["test_library"] = test_library_path

            logger.info(f"Exported {len(all_results)} files")

            return all_results

        except Exception as e:
            raise ParserError(f"Failed to export all data: {e}") from e

    def evaluate_python_models(
        self, parsed_models: dict[str, Any], variables: Variables | None = None
    ) -> dict[str, Any]:
        """
        Evaluate all Python models that need evaluation.

        Args:
            parsed_models: Dictionary of parsed models
            variables: Optional variables for model evaluation

        Returns:
            Updated models with evaluated Python models
        """
        try:
            python_parser = ParserFactory.create_parser(Path("dummy.py"))
            return python_parser.evaluate_all_models(parsed_models, variables)
        except Exception as e:
            logger.error(f"Error evaluating Python models: {e}")
            return parsed_models

    def update_python_models_with_resolved_sql(self, updated_models: dict[str, Any]) -> None:
        """
        Update Python parser's cached models with resolved SQL from execution.

        Args:
            updated_models: Updated model data with resolved SQL
        """
        try:
            python_parser = ParserFactory.create_parser(Path("dummy.py"))
            python_parser.update_models_with_resolved_sql(updated_models)
        except Exception as e:
            logger.error(f"Error updating Python models with resolved SQL: {e}")

    def get_execution_order(self) -> list[str]:
        """Get the execution order for all tables based on dependencies."""
        graph = self.build_dependency_graph()
        return graph["execution_order"]

    def get_table_dependencies(self, table_name: str) -> list[str]:
        """Get direct dependencies for a specific table."""
        graph = self.build_dependency_graph()
        return graph["dependencies"].get(table_name, [])

    def get_table_dependents(self, table_name: str) -> list[str]:
        """Get tables that depend on a specific table."""
        graph = self.build_dependency_graph()
        return graph["dependents"].get(table_name, [])
