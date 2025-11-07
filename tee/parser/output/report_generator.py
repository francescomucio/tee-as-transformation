"""
Report generation functionality for dependency graphs.
"""

from pathlib import Path
from typing import Dict, Any, Optional

from .visualizer import DependencyVisualizer
from tee.parser.shared.types import DependencyGraph
from tee.parser.shared.exceptions import OutputGenerationError
from tee.parser.shared.constants import OUTPUT_FILES


class ReportGenerator:
    """Handles generation of markdown reports with Mermaid diagrams."""

    def __init__(self, output_folder: Path):
        """
        Initialize the report generator.

        Args:
            output_folder: Path to the output folder
        """
        self.output_folder = output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.visualizer = DependencyVisualizer()

    def generate_markdown_report(
        self, graph: DependencyGraph, output_file: Optional[str] = None
    ) -> Path:
        """
        Generate a comprehensive markdown report with Mermaid diagram.

        Args:
            graph: The dependency graph
            output_file: Optional custom output file path

        Returns:
            Path to the generated report

        Raises:
            OutputGenerationError: If report generation fails
        """
        try:
            if output_file is None:
                output_file = self.output_folder / OUTPUT_FILES["markdown_report"]
            else:
                output_file = Path(output_file)

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            mermaid_diagram = self.visualizer.generate_mermaid_diagram(graph)

            # Separate test nodes from table nodes
            test_nodes = [node for node in graph["nodes"] if node.startswith("test:")]
            table_nodes = [node for node in graph["nodes"] if not node.startswith("test:")]

            markdown_content = f"""# Dependency Graph Report

## Overview

This report provides a comprehensive analysis of the SQL model dependencies.

## Statistics

- **Total Tables**: {len(table_nodes)}
- **Total Tests**: {len(test_nodes)}
- **Total Nodes**: {len(graph["nodes"])}
- **Total Dependencies**: {len(graph["edges"])}
- **Circular Dependencies**: {len(graph["cycles"])}

## Visual Diagram

```mermaid
{mermaid_diagram}
```

## Execution Order

"""

            if graph["execution_order"]:
                for i, table in enumerate(graph["execution_order"], 1):
                    markdown_content += f"{i}. `{table}`\n"
            else:
                markdown_content += "No valid execution order (circular dependencies detected)\n"

            # Add Tests Details section if there are any tests
            if test_nodes:
                markdown_content += "\n## Tests Details\n\n"
                markdown_content += "The following tests are defined and integrated into the dependency graph:\n\n"

                for test_node in sorted(test_nodes):
                    # Parse test node: test:table.test_name or test:table.column.test_name
                    test_parts = test_node.replace("test:", "").split(".")
                    if len(test_parts) == 2:
                        # Table-level test: test:table.test_name
                        table_name, test_name = test_parts
                        test_display = f"`{test_name}` on `{table_name}`"
                        test_type = "Table-level"
                    elif len(test_parts) == 3:
                        # Column-level test: test:table.column.test_name
                        table_name, column_name, test_name = test_parts
                        test_display = f"`{test_name}` on `{table_name}.{column_name}`"
                        test_type = "Column-level"
                    else:
                        # Fallback for unexpected format
                        test_display = test_node.replace("test:", "")
                        test_type = "Test"
                    
                    deps = graph["dependencies"][test_node]
                    dependents = graph["dependents"][test_node]

                    markdown_content += f"### {test_display}\n\n"
                    markdown_content += f"**Type**: {test_type}\n\n"

                    if deps:
                        # Filter out test nodes from dependencies
                        table_deps = [dep for dep in deps if not dep.startswith("test:")]
                        test_deps = [dep for dep in deps if dep.startswith("test:")]
                        
                        if table_deps:
                            markdown_content += f"**Depends on tables**: {', '.join([f'`{dep}`' for dep in table_deps])}\n\n"
                        if test_deps:
                            test_names = [dep.replace("test:", "") for dep in test_deps]
                            markdown_content += f"**Depends on tests**: {', '.join([f'`{name}`' for name in test_names])}\n\n"
                    else:
                        markdown_content += "**No dependencies**\n\n"

                    if dependents:
                        # Filter out test nodes from dependents
                        table_dependents = [dep for dep in dependents if not dep.startswith("test:")]
                        test_dependents = [dep for dep in dependents if dep.startswith("test:")]
                        
                        if table_dependents:
                            markdown_content += f"**Used by tables**: {', '.join([f'`{dep}`' for dep in table_dependents])}\n\n"
                        if test_dependents:
                            test_names = [dep.replace("test:", "") for dep in test_dependents]
                            markdown_content += f"**Used by tests**: {', '.join([f'`{name}`' for name in test_names])}\n\n"
                    else:
                        markdown_content += "**No dependents**\n\n"

            markdown_content += "\n## Transformation Details\n\n"

            # Only include table nodes in the transformation details section (exclude tests)
            for table in sorted(table_nodes):
                deps = graph["dependencies"][table]
                dependents = graph["dependents"][table]

                markdown_content += f"### `{table}`\n\n"

                if deps:
                    # Filter out test nodes from dependencies display
                    table_deps = [dep for dep in deps if not dep.startswith("test:")]
                    test_deps = [dep for dep in deps if dep.startswith("test:")]
                    
                    if table_deps:
                        markdown_content += (
                            f"**Depends on**: {', '.join([f'`{dep}`' for dep in table_deps])}\n\n"
                        )
                    if test_deps:
                        test_names = [dep.replace("test:", "") for dep in test_deps]
                        markdown_content += (
                            f"**Has tests**: {', '.join([f'`{name}`' for name in test_names])}\n\n"
                        )
                else:
                    markdown_content += "**No dependencies** (base table)\n\n"

                if dependents:
                    # Filter out test nodes from dependents display
                    table_dependents = [dep for dep in dependents if not dep.startswith("test:")]
                    test_dependents = [dep for dep in dependents if dep.startswith("test:")]
                    
                    if table_dependents:
                        markdown_content += (
                            f"**Used by**: {', '.join([f'`{dep}`' for dep in table_dependents])}\n\n"
                        )
                    if test_dependents:
                        test_names = [dep.replace("test:", "") for dep in test_dependents]
                        markdown_content += (
                            f"**Tested by**: {', '.join([f'`{name}`' for name in test_names])}\n\n"
                        )
                else:
                    markdown_content += "**No dependents** (leaf table)\n\n"

            if graph["cycles"]:
                markdown_content += "## ⚠️ Circular Dependencies\n\n"
                for i, cycle in enumerate(graph["cycles"], 1):
                    cycle_str = " → ".join([f"`{table}`" for table in cycle]) + f" → `{cycle[0]}`"
                    markdown_content += f"{i}. {cycle_str}\n\n"

            markdown_content += "---\n\n*Generated by Project Parser*"

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown_content)

            print(f"Markdown report saved to {output_file}")
            print(f"Includes Mermaid diagram and detailed analysis")

            return output_file

        except Exception as e:
            raise OutputGenerationError(f"Failed to generate markdown report: {e}")

    def generate_mermaid_diagram(
        self, graph: DependencyGraph, output_file: Optional[str] = None
    ) -> Path:
        """
        Generate a standalone Mermaid diagram file.

        Args:
            graph: The dependency graph
            output_file: Optional custom output file path

        Returns:
            Path to the generated Mermaid file

        Raises:
            OutputGenerationError: If diagram generation fails
        """
        try:
            if output_file is None:
                output_file = self.output_folder / OUTPUT_FILES["mermaid_diagram"]
            else:
                output_file = Path(output_file)

            # Use the visualizer to save the Mermaid diagram
            self.visualizer.save_mermaid_diagram(graph, str(output_file))

            return output_file

        except Exception as e:
            raise OutputGenerationError(f"Failed to generate Mermaid diagram: {e}")

    def generate_all_reports(self, graph: DependencyGraph) -> Dict[str, Path]:
        """
        Generate all available reports.

        Args:
            graph: The dependency graph

        Returns:
            Dict mapping report type to file path

        Raises:
            OutputGenerationError: If report generation fails
        """
        try:
            results = {}

            # Generate markdown report
            results["markdown_report"] = self.generate_markdown_report(graph)

            # Generate Mermaid diagram
            results["mermaid_diagram"] = self.generate_mermaid_diagram(graph)

            return results

        except Exception as e:
            raise OutputGenerationError(f"Failed to generate all reports: {e}")
