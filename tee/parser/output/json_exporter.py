"""
JSON export functionality for parsed models and dependency graphs.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional

from ..shared.types import ParsedModel, DependencyGraph
from ..shared.exceptions import OutputGenerationError
from ..shared.constants import OUTPUT_FILES


class JSONExporter:
    """Handles JSON export of parsed models and dependency graphs."""
    
    def __init__(self, output_folder: Path):
        """
        Initialize the JSON exporter.
        
        Args:
            output_folder: Path to the output folder
        """
        self.output_folder = output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
    
    def export_parsed_models(self, parsed_models: Dict[str, ParsedModel], 
                           output_file: Optional[str] = None) -> Path:
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
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(parsed_models, f, indent=2, ensure_ascii=False)
            
            print(f"Parsed models saved to {output_file}")
            print(f"Found {len(parsed_models)} models")
            
            return output_file
            
        except Exception as e:
            raise OutputGenerationError(f"Failed to export parsed models: {e}")
    
    def export_dependency_graph(self, graph: DependencyGraph, 
                              output_file: Optional[str] = None) -> Path:
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
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(graph, f, indent=2, ensure_ascii=False)
            
            print(f"Dependency graph saved to {output_file}")
            print(f"Found {len(graph['nodes'])} tables")
            print(f"Execution order: {' -> '.join(graph['execution_order'])}")
            if graph['cycles']:
                print(f"Warning: Found {len(graph['cycles'])} circular dependencies!")
                for cycle in graph['cycles']:
                    print(f"  Cycle: {' -> '.join(cycle)}")
            
            return output_file
            
        except Exception as e:
            raise OutputGenerationError(f"Failed to export dependency graph: {e}")
    
    def export_all(self, parsed_models: Dict[str, ParsedModel], 
                  graph: DependencyGraph) -> Dict[str, Path]:
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
