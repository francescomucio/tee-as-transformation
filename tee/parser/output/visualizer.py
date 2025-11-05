"""
Visualization functionality for dependency graphs.
"""

from typing import Dict, Any
from pathlib import Path

from ..shared.types import DependencyGraph
from ..shared.exceptions import OutputGenerationError


class DependencyVisualizer:
    """Handles visualization of dependency graphs."""

    def generate_mermaid_diagram(self, graph: DependencyGraph) -> str:
        """
        Generate a Mermaid diagram representation of the dependency graph.

        Args:
            graph: The dependency graph

        Returns:
            Mermaid diagram as a string
        """
        mermaid_lines = ["graph TD"]

        # Add nodes
        for node in sorted(graph["nodes"]):
            # Escape special characters in node names for Mermaid
            safe_node = self._escape_mermaid_node(node)
            mermaid_lines.append(f'    {safe_node}["{node}"]')

        # Add edges (dependencies)
        for dep, table in graph["edges"]:
            safe_dep = self._escape_mermaid_node(dep)
            safe_table = self._escape_mermaid_node(table)
            mermaid_lines.append(f"    {safe_dep} --> {safe_table}")

        # Add execution order information as comments
        if graph["execution_order"]:
            mermaid_lines.append("")
            mermaid_lines.append("    %% Execution Order:")
            for i, table in enumerate(graph["execution_order"], 1):
                safe_table = self._escape_mermaid_node(table)
                mermaid_lines.append(f"    %% {i}. {table}")

        # Add cycle information if present
        if graph["cycles"]:
            mermaid_lines.append("")
            mermaid_lines.append("    %% Circular Dependencies Detected:")
            for cycle in graph["cycles"]:
                cycle_str = " → ".join(cycle) + f" → {cycle[0]}"
                mermaid_lines.append(f"    %% {cycle_str}")

        return "\n".join(mermaid_lines)

    def save_mermaid_diagram(
        self, graph: DependencyGraph, output_file: str = "output/dependency_graph.mmd"
    ) -> None:
        """
        Save the dependency graph as a Mermaid diagram file.

        Args:
            graph: The dependency graph
            output_file: Output file path
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            mermaid_content = self.generate_mermaid_diagram(graph)

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(mermaid_content)

            print(f"Mermaid diagram saved to {output_file}")
            print(f"Total nodes: {len(graph['nodes'])}")
            print(f"Total dependencies: {len(graph['edges'])}")
            if graph["cycles"]:
                print(f"⚠️  Warning: {len(graph['cycles'])} circular dependencies detected!")
        except Exception as e:
            raise OutputGenerationError(f"Failed to save Mermaid diagram: {e}")

    def _escape_mermaid_node(self, node_name: str) -> str:
        """
        Escape special characters in node names for Mermaid compatibility.

        Args:
            node_name: The node name to escape

        Returns:
            Escaped node name safe for Mermaid
        """
        # Replace special characters that might cause issues in Mermaid
        escaped = node_name.replace(".", "_")
        escaped = escaped.replace("-", "_")
        escaped = escaped.replace(" ", "_")
        escaped = escaped.replace("(", "_")
        escaped = escaped.replace(")", "_")
        escaped = escaped.replace("[", "_")
        escaped = escaped.replace("]", "_")
        escaped = escaped.replace("{", "_")
        escaped = escaped.replace("}", "_")
        escaped = escaped.replace(":", "_")
        escaped = escaped.replace(";", "_")
        escaped = escaped.replace(",", "_")
        escaped = escaped.replace("'", "_")
        escaped = escaped.replace('"', "_")

        # Ensure it starts with a letter or underscore
        if escaped and not escaped[0].isalpha() and escaped[0] != "_":
            escaped = "_" + escaped

        return escaped
