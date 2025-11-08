"""
JSON export functionality for parsed models and dependency graphs.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Literal
import yaml

from tee.parser.shared.types import ParsedModel, DependencyGraph
from tee.parser.shared.exceptions import OutputGenerationError
from tee.parser.shared.constants import OUTPUT_FILES
from .ots_transformer import OTSTransformer
from .test_library_exporter import TestLibraryExporter
from tee.typing.metadata import OTSModule

# Configure logging
logger = logging.getLogger(__name__)


class JSONExporter:
    """Handles JSON export of parsed models and dependency graphs."""

    def __init__(self, output_folder: Path, project_config: Optional[Dict[str, Any]] = None, project_folder: Optional[Path] = None):
        """
        Initialize the JSON exporter.

        Args:
            output_folder: Path to the output folder
            project_config: Optional project configuration for OTS transformer
            project_folder: Optional project folder path for test library export
        """
        self.output_folder = output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.project_config = project_config or {}
        self.project_folder = project_folder
        self.transformer = OTSTransformer(self.project_config) if self.project_config else None

    def export_parsed_models(
        self, parsed_models: Dict[str, ParsedModel], output_file: Optional[str] = None
    ) -> Path:
        """
        Export parsed models to JSON file.

        Args:
            parsed_models: Parsed models to export
            output_file: Optional custom output file path

        Returns:
            Path to the exported file

        Raises:
            OutputGenerationError: If export fails
        """
        try:
            if output_file is None:
                output_file = self.output_folder / OUTPUT_FILES["parsed_models"]
            else:
                output_file = Path(output_file)

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(parsed_models, f, indent=2, ensure_ascii=False)

            print(f"Parsed models saved to {output_file}")
            print(f"Found {len(parsed_models)} models")

            return output_file

        except Exception as e:
            raise OutputGenerationError(f"Failed to export parsed models: {e}")

    def export_dependency_graph(
        self, graph: DependencyGraph, output_file: Optional[str] = None
    ) -> Path:
        """
        Export dependency graph to JSON file.

        Args:
            graph: Dependency graph to export
            output_file: Optional custom output file path

        Returns:
            Path to the exported file

        Raises:
            OutputGenerationError: If export fails
        """
        try:
            if output_file is None:
                output_file = self.output_folder / OUTPUT_FILES["dependency_graph"]
            else:
                output_file = Path(output_file)

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(graph, f, indent=2, ensure_ascii=False)

            print(f"Dependency graph saved to {output_file}")
            print(f"Found {len(graph['nodes'])} tables")
            print(f"Execution order: {' -> '.join(graph['execution_order'])}")
            if graph["cycles"]:
                print(f"Warning: Found {len(graph['cycles'])} circular dependencies!")
                for cycle in graph["cycles"]:
                    print(f"  Cycle: {' -> '.join(cycle)}")

            return output_file

        except Exception as e:
            raise OutputGenerationError(f"Failed to export dependency graph: {e}")

    def export_all(
        self, parsed_models: Dict[str, ParsedModel], graph: DependencyGraph
    ) -> Dict[str, Path]:
        """
        Export both parsed models and dependency graph.

        Args:
            parsed_models: Parsed models to export
            graph: Dependency graph to export

        Returns:
            Dict mapping export type to file path

        Raises:
            OutputGenerationError: If export fails
        """
        try:
            results = {}

            # Export parsed models
            results["parsed_models"] = self.export_parsed_models(parsed_models)

            # Export dependency graph
            results["dependency_graph"] = self.export_dependency_graph(graph)

            return results

        except Exception as e:
            raise OutputGenerationError(f"Failed to export all data: {e}")

    def export_ots_modules(
        self, 
        parsed_models: Dict[str, ParsedModel], 
        test_library_path: Optional[Path] = None,
        format: Literal["json", "yaml"] = "json"
    ) -> Dict[str, Path]:
        """
        Export parsed models as OTS Modules.

        One file per schema module will be created.

        Args:
            parsed_models: Parsed models to export
            test_library_path: Optional path to test library file
            format: Output format ("json" or "yaml")

        Returns:
            Dictionary mapping module names to output file paths

        Raises:
            OutputGenerationError: If export fails
        """
        if not self.transformer:
            raise OutputGenerationError(
                "OTS transformer not initialized. Provide project_config when creating JSONExporter."
            )

        try:
            logger.info("Transforming models to OTS Modules")
            modules = self.transformer.transform_to_ots_modules(
                parsed_models, 
                test_library_path=test_library_path
            )

            results = {}
            for module_name, module_data in modules.items():
                # Create filename with double underscore between database and schema
                # e.g., "t_project.my_schema" -> "t_project__my_schema.ots.json" or ".ots.yaml"
                if format == "yaml":
                    filename = f"{module_name.replace('.', '__')}.ots.yaml"
                else:
                    filename = f"{module_name.replace('.', '__')}.ots.json"
                output_file = self.output_folder / filename

                # Ensure output directory exists
                output_file.parent.mkdir(parents=True, exist_ok=True)

                # Write module to file in the specified format
                with open(output_file, "w", encoding="utf-8") as f:
                    if format == "yaml":
                        yaml.dump(module_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
                    else:
                        json.dump(module_data, f, indent=2, ensure_ascii=False)

                results[module_name] = output_file
                logger.info(f"Exported OTS module '{module_name}' to {output_file} ({format.upper()})")
                print(f"✅ OTS module '{module_name}' saved to {output_file} ({format.upper()})")
                print(f"   Contains {len(module_data['transformations'])} transformations")

            print(f"\n✨ Exported {len(results)} OTS module(s) ({format.upper()})")

            return results

        except Exception as e:
            raise OutputGenerationError(f"Failed to export OTS modules: {e}")

    def export_test_library(self, project_name: str) -> Optional[Path]:
        """
        Export discovered SQL tests to OTS test library format.

        Args:
            project_name: Project name (for filename generation)

        Returns:
            Path to the exported test library file, or None if no tests found

        Raises:
            OutputGenerationError: If export fails
        """
        if not self.project_folder:
            logger.debug("No project folder provided, skipping test library export")
            return None

        try:
            exporter = TestLibraryExporter(self.project_folder, project_name)
            return exporter.export_test_library(self.output_folder, format="json")
        except Exception as e:
            logger.warning(f"Failed to export test library: {e}")
            return None
