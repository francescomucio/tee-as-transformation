"""
Documentation site generator for t4t projects.
"""

import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from tee.parser.output.visualizer import DependencyVisualizer
from tee.parser.shared.exceptions import OutputGenerationError
from tee.parser.shared.types import DependencyGraph, ParsedModel


class DocsGenerator:
    """Generates static HTML documentation site with dependency graph."""

    def __init__(
        self,
        project_path: Path,
        output_path: Path,
        parsed_models: dict[str, ParsedModel],
        parsed_functions: dict[str, Any],
        dependency_graph: DependencyGraph,
    ) -> None:
        """
        Initialize the documentation generator.

        Args:
            project_path: Path to the project root
            output_path: Path where documentation will be generated
            parsed_models: Dictionary of parsed models
            parsed_functions: Dictionary of parsed functions
            dependency_graph: Dependency graph structure
        """
        self.project_path = project_path
        self.output_path = output_path
        self.parsed_models = parsed_models
        self.parsed_functions = parsed_functions
        self.dependency_graph = dependency_graph
        self.visualizer = DependencyVisualizer()

        # Set up Jinja2 template environment
        templates_dir = Path(__file__).parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=False,  # We're generating HTML, not user content
        )

    def generate(self) -> None:
        """Generate the complete documentation site."""
        try:
            # Create output directory
            self.output_path.mkdir(parents=True, exist_ok=True)

            # Generate index page
            self._generate_index_page()

            # Generate model detail pages
            for model_name, model in self.parsed_models.items():
                self._generate_model_page(model_name, model)

            # Generate graph data JSON for interactive features
            self._generate_graph_data()

        except Exception as e:
            raise OutputGenerationError(f"Failed to generate documentation: {e}") from e

    def _generate_index_page(self) -> None:
        """Generate the main interactive index page with graph."""
        template = self.env.get_template("interactive_index.html")

        # Prepare models data as a dict for easy JS access
        models_dict = {}
        for model_name in self.parsed_models:
            model = self.parsed_models[model_name]
            metadata = model.get("model_metadata", {}).get("metadata", {})
            models_dict[model_name] = {
                "name": model_name,
                "safe_name": self._safe_filename(model_name),
                "description": metadata.get("description", "No description"),
                "materialization": metadata.get("materialization", "table"),
            }

        # Prepare functions data
        functions_dict = {}
        for func_name in self.parsed_functions:
            func = self.parsed_functions[func_name]
            func_metadata = func.get("function_metadata", {}).get("metadata", {})
            functions_dict[func_name] = {
                "name": func_name,
                "type": func_metadata.get("type", "scalar"),
            }

        # Prepare graph data JSON
        graph_data_json = json.dumps(
            {
                "nodes": self.dependency_graph["nodes"],
                "edges": self.dependency_graph["edges"],
                "dependencies": self.dependency_graph["dependencies"],
                "dependents": self.dependency_graph["dependents"],
                "execution_order": self.dependency_graph.get("execution_order", []),
                "cycles": self.dependency_graph.get("cycles", []),
                "functions": functions_dict,
            }
        )

        models_data_json = json.dumps(models_dict)

        html = template.render(
            models_count=len(self.parsed_models),
            nodes_count=len(self.dependency_graph["nodes"]),
            edges_count=len(self.dependency_graph["edges"]),
            graph_data_json=graph_data_json,
            models_data_json=models_data_json,
        )

        index_path = self.output_path / "index.html"
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(html)

    def _generate_model_page(self, model_name: str, model: ParsedModel) -> None:
        """Generate a detail page for a specific model."""
        template = self.env.get_template("model.html")

        metadata = model.get("model_metadata", {}).get("metadata", {})
        description = metadata.get("description", "No description provided.")
        schema = metadata.get("schema", [])
        materialization = metadata.get("materialization", "table")
        tests = metadata.get("tests", [])
        incremental = metadata.get("incremental")
        file_path = model.get("model_metadata", {}).get("file_path", "Unknown")

        # Get dependencies and dependents
        dependencies = self.dependency_graph.get("dependencies", {}).get(model_name, [])
        dependents = self.dependency_graph.get("dependents", {}).get(model_name, [])

        # Prepare schema data
        schema_data = []
        for col in schema:
            col_tests = col.get("tests", [])
            schema_data.append(
                {
                    "name": col.get("name", ""),
                    "datatype": col.get("datatype", ""),
                    "description": col.get("description"),
                    "tests": col_tests if isinstance(col_tests, list) else [],
                }
            )

        # Prepare dependencies data
        deps_data = []
        for dep in dependencies:
            deps_data.append(
                {
                    "name": dep,
                    "safe_name": self._safe_filename(dep),
                    "is_model": dep in self.parsed_models,
                }
            )

        # Prepare dependents data
        dependents_data = []
        for dep in dependents:
            dependents_data.append(
                {
                    "name": dep,
                    "safe_name": self._safe_filename(dep),
                    "is_model": dep in self.parsed_models,
                }
            )

        # Prepare tests data
        tests_data = []
        for test in tests:
            if isinstance(test, dict):
                test_name = test.get("name") or test.get("test", "unknown")
                test_params = test.get("params", {})
                test_severity = test.get("severity", "error")
                tests_data.append(
                    {
                        "name": test_name,
                        "params": test_params,
                        "params_json": json.dumps(test_params) if test_params else "",
                        "severity": test_severity,
                    }
                )
            else:
                tests_data.append(
                    {
                        "name": str(test),
                        "params": {},
                        "params_json": "",
                        "severity": "error",
                    }
                )

        # Prepare incremental data
        incremental_data = None
        if incremental:
            incremental_data = {
                "strategy": incremental.get("strategy", "unknown"),
                "unique_key": incremental.get("unique_key", []),
                "merge_key": incremental.get("merge_key", []),
            }

        # Get code data (original and resolved SQL)
        code_data = model.get("code", {})
        original_sql = ""
        resolved_sql = ""
        if code_data and "sql" in code_data:
            sql_data = code_data["sql"]
            original_sql = sql_data.get("original_sql", "")
            resolved_sql = sql_data.get("resolved_sql", original_sql)

        html = template.render(
            model_name=model_name,
            description=description,
            schema=schema_data,
            materialization=materialization,
            file_path=file_path,
            dependencies=deps_data,
            dependents=dependents_data,
            tests=tests_data,
            incremental=incremental_data,
            original_sql=original_sql,
            resolved_sql=resolved_sql,
        )

        # Create safe filename
        safe_name = self._safe_filename(model_name)
        model_path = self.output_path / f"model_{safe_name}.html"
        with open(model_path, "w", encoding="utf-8") as f:
            f.write(html)

    def _generate_graph_data(self) -> None:
        """Generate JSON file with graph data for interactive features."""
        graph_data = {
            "nodes": self.dependency_graph["nodes"],
            "edges": self.dependency_graph["edges"],
            "dependencies": self.dependency_graph["dependencies"],
            "dependents": self.dependency_graph["dependents"],
            "execution_order": self.dependency_graph.get("execution_order", []),
            "cycles": self.dependency_graph.get("cycles", []),
        }

        graph_path = self.output_path / "graph_data.json"
        with open(graph_path, "w", encoding="utf-8") as f:
            json.dump(graph_data, f, indent=2)

    def _safe_filename(self, name: str) -> str:
        """Convert model name to safe filename."""
        # Replace special characters
        safe = name.replace(".", "_")
        safe = safe.replace("/", "_")
        safe = safe.replace("\\", "_")
        safe = safe.replace(":", "_")
        safe = safe.replace("*", "_")
        safe = safe.replace("?", "_")
        safe = safe.replace('"', "_")
        safe = safe.replace("<", "_")
        safe = safe.replace(">", "_")
        safe = safe.replace("|", "_")
        return safe
