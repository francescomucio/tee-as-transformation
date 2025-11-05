"""
Dependency graph building and analysis functionality.
"""

from typing import Dict, Any, List
from graphlib import TopologicalSorter

from ..shared.types import ParsedModel, DependencyGraph, DependencyInfo, ExecutionOrder, GraphCycles
from ..shared.exceptions import DependencyError


class DependencyGraphBuilder:
    """Handles dependency graph construction and analysis."""

    def build_graph(self, parsed_models: Dict[str, ParsedModel], table_resolver) -> DependencyGraph:
        """
        Build a dependency graph from the parsed SQL models.

        Args:
            parsed_models: Parsed SQL models
            table_resolver: TableResolver instance

        Returns:
            Dict containing dependency graph information

        Raises:
            DependencyError: If graph building fails
        """
        try:
            # Extract dependencies from parsed models
            dependencies = {}
            all_tables = set()

            for table_name, model_info in parsed_models.items():
                all_tables.add(table_name)
                table_deps = set()

                # Get dependencies from the parsed tables
                if (
                    "sqlglot" in model_info
                    and model_info["sqlglot"]
                    and "tables" in model_info["sqlglot"]
                ):
                    for referenced_table in model_info["sqlglot"]["tables"]:
                        # Convert referenced table to full table name if needed
                        full_ref_name = table_resolver.resolve_table_reference(
                            referenced_table, parsed_models
                        )
                        if full_ref_name and full_ref_name != table_name:
                            table_deps.add(full_ref_name)

                dependencies[table_name] = list(table_deps)

            # Build reverse dependencies (dependents)
            dependents = {table: [] for table in all_tables}
            for table, deps in dependencies.items():
                for dep in deps:
                    if dep in dependents:
                        dependents[dep].append(table)

            # Build edges for graph representation
            edges = []
            for table, deps in dependencies.items():
                for dep in deps:
                    edges.append((dep, table))  # dep -> table (dependency direction)

            # Detect cycles and build execution order using graphlib
            cycles = self._detect_cycles_with_graphlib(dependencies)
            execution_order = (
                self._topological_sort_with_graphlib(dependencies) if not cycles else []
            )

            return {
                "nodes": list(all_tables),
                "edges": edges,
                "dependencies": dependencies,
                "dependents": dependents,
                "execution_order": execution_order,
                "cycles": cycles,
            }
        except Exception as e:
            raise DependencyError(f"Failed to build dependency graph: {e}")

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

    def _topological_sort_with_graphlib(self, dependencies: DependencyInfo) -> ExecutionOrder:
        """
        Perform topological sort using graphlib.TopologicalSorter.

        Args:
            dependencies: Dict mapping table -> list of dependencies

        Returns:
            List of tables in dependency order (dependencies first)
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
        try:
            return list(ts.static_order())
        except ValueError:
            # This should not happen if we check for cycles first
            return []
