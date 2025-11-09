"""
Visualization functionality for dependency graphs.
"""

from typing import Dict, Any, Optional
from pathlib import Path

from tee.parser.shared.types import DependencyGraph
from tee.parser.shared.exceptions import OutputGenerationError


class DependencyVisualizer:
    """Handles visualization of dependency graphs."""

    def generate_mermaid_diagram(self, graph: DependencyGraph, parsed_functions: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a Mermaid diagram representation of the dependency graph.

        Args:
            graph: The dependency graph
            parsed_functions: Optional dict of parsed functions to identify function nodes

        Returns:
            Mermaid diagram as a string
        """
        parsed_functions = parsed_functions or {}
        function_names = set(parsed_functions.keys())
        
        mermaid_lines = ["graph LR"]  # Left-to-right layout

        # Add style definitions for test and function nodes
        mermaid_lines.append("    classDef testNode fill:#e1f5ff,stroke:#01579b,stroke-width:2px")
        mermaid_lines.append("    classDef functionNode fill:#fff3e0,stroke:#e65100,stroke-width:2px")
        mermaid_lines.append("")

        # Add nodes
        for node in sorted(graph["nodes"]):
            # Escape special characters in node names for Mermaid
            safe_node = self._escape_mermaid_node(node)
            if node.startswith("test:"):
                # Test nodes: use different shape and style
                # Extract display name: test:table.test_name -> "table.test_name (test)"
                # or test:table.column.test_name -> "table.column.test_name (test)"
                test_display = node.replace("test:", "")
                mermaid_lines.append(f'    {safe_node}["{test_display} (test)"]:::testNode')
            elif node in function_names:
                # Function nodes: use different style
                mermaid_lines.append(f'    {safe_node}["{node} (function)"]:::functionNode')
            else:
                # Regular table nodes
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
        self, graph: DependencyGraph, output_file: str = "output/dependency_graph.mmd", parsed_functions: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Save the dependency graph as a Mermaid diagram file.

        Args:
            graph: The dependency graph
            output_file: Output file path
            parsed_functions: Optional dict of parsed functions to identify function nodes
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            mermaid_content = self.generate_mermaid_diagram(graph, parsed_functions)

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
