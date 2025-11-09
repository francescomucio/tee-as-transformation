"""
Dependency graph building and analysis functionality.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from graphlib import TopologicalSorter

from tee.parser.shared.types import ParsedModel, ParsedFunction, DependencyGraph, DependencyInfo, ExecutionOrder, GraphCycles
from tee.parser.shared.exceptions import DependencyError

logger = logging.getLogger(__name__)


class DependencyGraphBuilder:
    """Handles dependency graph construction and analysis."""

    def build_graph(
        self,
        parsed_models: Dict[str, ParsedModel],
        table_resolver,
        project_folder: Optional[Path] = None,
        parsed_functions: Optional[Dict[str, ParsedFunction]] = None,
    ) -> DependencyGraph:
        """
        Build a dependency graph from the parsed SQL models, functions, and tests.

        Args:
            parsed_models: Parsed SQL models
            table_resolver: TableResolver instance
            project_folder: Optional project folder path for discovering tests
            parsed_functions: Optional parsed functions dict

        Returns:
            Dict containing dependency graph information

        Raises:
            DependencyError: If graph building fails
        """
        parsed_functions = parsed_functions or {}
        
        try:
            # Extract dependencies from parsed models and functions
            dependencies = {}
            all_nodes = set()

            # First, extract dependencies from functions (consistent with models)
            for function_name, function_info in parsed_functions.items():
                all_nodes.add(function_name)
                function_deps = set()

                # Get dependencies from code["sql"] (consistent with models)
                code_data = function_info.get("code", {})
                if code_data and "sql" in code_data:
                    # Extract table dependencies (like models)
                    source_tables = code_data["sql"].get("source_tables", [])
                    for referenced_table in source_tables:
                        # Resolve table reference
                        full_table_name = table_resolver.resolve_table_reference(
                            referenced_table, parsed_models
                        )
                        if full_table_name:
                            function_deps.add(full_table_name)

                    # Extract function dependencies (like models)
                    source_functions = code_data["sql"].get("source_functions", [])
                    for func_ref in source_functions:
                        # Resolve function reference
                        full_func_name = table_resolver.resolve_function_reference(
                            func_ref, parsed_functions
                        )
                        if full_func_name and full_func_name != function_name:
                            function_deps.add(full_func_name)

                dependencies[function_name] = list(function_deps)

            # Extract dependencies from parsed models
            for table_name, model_info in parsed_models.items():
                all_nodes.add(table_name)
                table_deps = set()

                # Get dependencies from the parsed tables
                code_data = model_info.get("code", {})
                if code_data and "sql" in code_data:
                    source_tables = code_data["sql"].get("source_tables", [])
                    for referenced_table in source_tables:
                        # Convert referenced table to full table name if needed
                        full_ref_name = table_resolver.resolve_table_reference(
                            referenced_table, parsed_models
                        )
                        if full_ref_name and full_ref_name != table_name:
                            table_deps.add(full_ref_name)

                    # Read pre-extracted function dependencies (extracted during model parsing)
                    source_functions = code_data["sql"].get("source_functions", [])
                    for func_ref in source_functions:
                        full_func_name = table_resolver.resolve_function_reference(
                            func_ref, parsed_functions
                        )
                        if full_func_name:
                            table_deps.add(full_func_name)

                dependencies[table_name] = list(table_deps)

            # Parse tests and add them to the graph
            if project_folder:
                test_dependencies = self._parse_test_dependencies(
                    parsed_models, project_folder, table_resolver
                )
                # Add test nodes and their dependencies
                for test_node, test_deps in test_dependencies.items():
                    all_nodes.add(test_node)
                    dependencies[test_node] = test_deps

            # Build reverse dependencies (dependents)
            dependents = {node: [] for node in all_nodes}
            for node, deps in dependencies.items():
                for dep in deps:
                    if dep in dependents:
                        dependents[dep].append(node)

            # Build edges for graph representation
            edges = []
            for table, deps in dependencies.items():
                for dep in deps:
                    edges.append((dep, table))  # dep -> table (dependency direction)

            # Detect cycles and build execution order using graphlib
            cycles = self._detect_cycles_with_graphlib(dependencies)
            execution_order = (
                self._topological_sort_with_graphlib(dependencies, parsed_functions) if not cycles else []
            )

            return {
                "nodes": list(all_nodes),
                "edges": edges,
                "dependencies": dependencies,
                "dependents": dependents,
                "execution_order": execution_order,
                "cycles": cycles,
            }
        except Exception as e:
            raise DependencyError(f"Failed to build dependency graph: {e}")

    def _parse_test_dependencies(
        self,
        parsed_models: Dict[str, ParsedModel],
        project_folder: Path,
        table_resolver,
    ) -> Dict[str, List[str]]:
        """
        Parse test SQL files to extract table dependencies using sqlglot.
        Creates separate nodes for each test instance.

        Args:
            parsed_models: Parsed SQL models
            project_folder: Project folder path
            table_resolver: TableResolver instance

        Returns:
            Dict mapping test_node -> list of table dependencies
            Test nodes are named: test:{table_name}.{test_name} for table-level
            or test:{table_name}.{column_name}.{test_name} for column-level
        """
        test_dependencies = {}

        try:
            # Import here to avoid circular dependencies
            from ...testing.test_discovery import TestDiscovery
            from ...parser.parsers.sql_parser import SQLParser

            # Discover SQL tests
            test_discovery = TestDiscovery(project_folder)
            discovered_tests = test_discovery.discover_tests()

            # Parse test SQL to extract dependencies
            sql_parser = SQLParser()

            # Process each model to find test instances
            for table_name, model_info in parsed_models.items():
                metadata = self._extract_metadata(model_info)
                if not metadata:
                    continue

                # Process column-level tests
                if "schema" in metadata and metadata["schema"]:
                    for column_def in metadata["schema"]:
                        if "tests" in column_def and column_def["tests"]:
                            column_name = column_def.get("name")
                            if not column_name:
                                continue

                            for test_def in column_def["tests"]:
                                test_name = (
                                    test_def if isinstance(test_def, str) else test_def.get("name")
                                )
                                if not test_name:
                                    continue

                                # Create column-level test node
                                test_node = f"test:{table_name}.{column_name}.{test_name}"
                                test_deps = self._parse_test_instance_dependencies(
                                    test_name=test_name,
                                    table_name=table_name,
                                    column_name=column_name,
                                    discovered_tests=discovered_tests,
                                    sql_parser=sql_parser,
                                    table_resolver=table_resolver,
                                    parsed_models=parsed_models,
                                )
                                test_dependencies[test_node] = test_deps
                                logger.debug(
                                    f"Column-level test {test_node} depends on: {test_deps}"
                                )

                # Process table-level tests
                if "tests" in metadata and metadata["tests"]:
                    for test_def in metadata["tests"]:
                        test_name = (
                            test_def if isinstance(test_def, str) else test_def.get("name")
                        )
                        if not test_name:
                            continue

                        # Create table-level test node
                        test_node = f"test:{table_name}.{test_name}"
                        test_deps = self._parse_test_instance_dependencies(
                            test_name=test_name,
                            table_name=table_name,
                            column_name=None,
                            discovered_tests=discovered_tests,
                            sql_parser=sql_parser,
                            table_resolver=table_resolver,
                            parsed_models=parsed_models,
                        )
                        test_dependencies[test_node] = test_deps
                        logger.debug(
                            f"Table-level test {test_node} depends on: {test_deps}"
                        )

        except ImportError as e:
            logger.warning(f"Could not import test discovery: {e}")
        except Exception as e:
            logger.warning(f"Error parsing test dependencies: {e}")

        return test_dependencies

    def _parse_test_instance_dependencies(
        self,
        test_name: str,
        table_name: str,
        column_name: Optional[str],
        discovered_tests: Dict[str, Any],
        sql_parser,
        table_resolver,
        parsed_models: Dict[str, ParsedModel],
    ) -> List[str]:
        """
        Parse dependencies for a single test instance.

        Args:
            test_name: Name of the test
            table_name: Table this test is applied to
            column_name: Column name (None for table-level tests)
            discovered_tests: Dictionary of discovered SQL tests
            sql_parser: SQLParser instance
            table_resolver: TableResolver instance
            parsed_models: Parsed SQL models

        Returns:
            List of table dependencies for this test instance
        """
        test_deps = set()

        # Standard tests (not SQL tests) only depend on the table being tested
        if test_name not in discovered_tests:
            # Standard test - depends only on the table
            test_deps.add(table_name)
            return list(test_deps)

        # SQL test - parse the SQL to find dependencies
        sql_test = discovered_tests[test_name]
        try:
            # Load test SQL
            test_sql = sql_test._load_sql_content()

            # Substitute @table_name and {{ table_name }} with actual table
            substituted_sql = test_sql.replace("@table_name", table_name)
            substituted_sql = substituted_sql.replace("{{ table_name }}", table_name)
            substituted_sql = substituted_sql.replace("{{table_name}}", table_name)

            # Substitute @column_name if this is a column-level test
            if column_name:
                substituted_sql = substituted_sql.replace("@column_name", column_name)
                substituted_sql = substituted_sql.replace("{{ column_name }}", column_name)
                substituted_sql = substituted_sql.replace("{{column_name}}", column_name)

            # Parse with SQLParser (reuses existing sqlglot code!)
            parsed = sql_parser.parse(
                substituted_sql, file_path=str(sql_test.sql_file_path)
            )

            # Extract source tables from parsed result
            source_tables = (
                parsed.get("code", {}).get("sql", {}).get("source_tables", [])
            )

            # Resolve table references
            for ref_table in source_tables:
                full_ref = table_resolver.resolve_table_reference(
                    ref_table, parsed_models
                )
                if full_ref and full_ref != table_name:
                    test_deps.add(full_ref)

            # Test depends on the table it's testing
            test_deps.add(table_name)

        except Exception as e:
            logger.warning(
                f"Failed to parse test {test_name} for table {table_name}: {e}"
            )
            # Still add the table as a dependency even if parsing fails
            test_deps.add(table_name)

        return list(test_deps)

    def _extract_metadata(self, model_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract metadata from model data."""
        try:
            # First, try to get metadata from model_metadata
            model_metadata = model_data.get("model_metadata", {})
            if model_metadata and "metadata" in model_metadata:
                nested_metadata = model_metadata["metadata"]
                if nested_metadata:
                    return nested_metadata

            # Fallback to any other metadata in the model data
            if "metadata" in model_data:
                file_metadata = model_data["metadata"]
                if file_metadata:
                    return file_metadata

            return None
        except Exception as e:
            logger.debug(f"Error extracting metadata: {e}")
            return None

    def _detect_cycles(self, dependencies: DependencyInfo) -> GraphCycles:
        """
        Detect circular dependencies using DFS.

        Args:
            dependencies: Dict mapping table -> list of dependencies

        Returns:
            List of cycles found (empty if no cycles)
        """
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node, path):
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return

            if node in visited:
                return

            visited.add(node)
            rec_stack.add(node)

            for neighbor in dependencies.get(node, []):
                dfs(neighbor, path + [node])

            rec_stack.remove(node)

        for node in dependencies:
            if node not in visited:
                dfs(node, [])

        return cycles

    def _detect_cycles_with_graphlib(self, dependencies: DependencyInfo) -> GraphCycles:
        """
        Detect cycles using graphlib.TopologicalSorter.

        Args:
            dependencies: Dict mapping table -> list of dependencies

        Returns:
            List of cycles found (empty if no cycles)
        """
        try:
            # Create a TopologicalSorter
            ts = TopologicalSorter()

            # Add all nodes and their dependencies
            for node, deps in dependencies.items():
                if deps:
                    ts.add(node, *deps)
                else:
                    ts.add(node)

            # Try to get static order - this will raise ValueError if there are cycles
            list(ts.static_order())
            return []  # No cycles found

        except ValueError:
            # Cycles detected, fall back to custom cycle detection for detailed info
            return self._detect_cycles(dependencies)


    def _topological_sort_with_graphlib(
        self, dependencies: DependencyInfo, parsed_functions: Optional[Dict[str, ParsedFunction]] = None
    ) -> ExecutionOrder:
        """
        Perform topological sort using graphlib.TopologicalSorter.
        The topological sort naturally respects dependencies, so functions that depend
        on tables will come after those tables. Functions without dependencies will
        come first.

        Args:
            dependencies: Dict mapping node -> list of dependencies
            parsed_functions: Optional parsed functions dict (for reference, not used in sorting)

        Returns:
            List of nodes in dependency order (dependencies first)
        """
        # Create a TopologicalSorter
        ts = TopologicalSorter()

        # Add all nodes and their dependencies
        for node, deps in dependencies.items():
            if deps:
                ts.add(node, *deps)
            else:
                ts.add(node)

        # Get the static order (topological sort)
        # This already respects dependencies - functions depending on tables will come after tables
        try:
            return list(ts.static_order())
        except ValueError:
            # This should not happen if we check for cycles first
            return []
