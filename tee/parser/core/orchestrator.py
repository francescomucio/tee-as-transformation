"""
High-level orchestration for parsing and analysis workflows.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional

from ..parsers import ParserFactory
from ..analysis import DependencyGraphBuilder, TableResolver
from ..processing import FileDiscovery, substitute_sql_variables, validate_sql_variables
from ..output import JSONExporter, ReportGenerator
from ..shared.types import ParsedModel, DependencyGraph, ConnectionConfig, Variables
from ..shared.exceptions import ParserError

# Configure logging
logger = logging.getLogger(__name__)


class ParserOrchestrator:
    """High-level orchestrator for parsing and analysis workflows."""
    
    def __init__(self, project_folder: str, connection: ConnectionConfig, 
                 variables: Optional[Variables] = None):
        """
        Initialize the orchestrator.
        
        Args:
            project_folder: Path to the project folder containing SQL files
            connection: Connection configuration dict with 'type' key
            variables: Optional dictionary of variables for SQL substitution
        """
        self.project_folder = Path(project_folder)
        self.connection = connection
        self.variables = variables or {}
        self.models_folder = self.project_folder / "models"
        
        # Initialize components
        self.file_discovery = FileDiscovery(self.models_folder)
        self.table_resolver = TableResolver(connection)
        self.dependency_builder = DependencyGraphBuilder()
        self.json_exporter = JSONExporter(self.project_folder / "output")
        self.report_generator = ReportGenerator(self.project_folder / "output")
        
        # Cached results
        self._parsed_models: Optional[Dict[str, ParsedModel]] = None
        self._dependency_graph: Optional[DependencyGraph] = None
    
    def discover_and_parse_models(self) -> Dict[str, ParsedModel]:
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
            logger.info(f"Discovered {files['sql']} SQL files and {files['python']} Python files")
            
            parsed_models = {}
            
            # Parse SQL files
            for sql_file in files['sql']:
                try:
                    logger.debug(f"Processing SQL file: {sql_file}")
                    
                    # Read SQL content
                    with open(sql_file, 'r', encoding='utf-8') as f:
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
                    full_table_name = self.table_resolver.generate_full_table_name(sql_file, self.models_folder)
                    
                    # Parse with appropriate parser
                    parser = ParserFactory.create_parser(sql_file)
                    parsed_args = parser.parse(sql_content, file_path=sql_file, table_name=full_table_name)
                    
                    parsed_models[full_table_name] = parsed_args
                    logger.debug(f"Successfully parsed SQL model: {full_table_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing SQL file {sql_file}: {e}")
                    continue
            
            # Parse Python files
            for python_file in files['python']:
                try:
                    logger.debug(f"Processing Python file: {python_file}")
                    
                    # Read Python content
                    with open(python_file, 'r', encoding='utf-8') as f:
                        python_content = f.read()
                    
                    # Parse with Python parser
                    parser = ParserFactory.create_parser(python_file)
                    python_models = parser.parse(python_content, file_path=python_file)
                    
                    # Add each model to the result with proper table naming
                    for table_name, model_data in python_models.items():
                        # Generate full table name for Python models
                        fake_file_path = python_file.parent / f"{table_name}.py"
                        full_table_name = self.table_resolver.generate_full_table_name(fake_file_path, self.models_folder)
                        
                        parsed_models[full_table_name] = model_data
                        logger.debug(f"Successfully parsed Python model: {full_table_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing Python file {python_file}: {e}")
                    continue
            
            # Evaluate Python models to populate their sqlglot data
            logger.info("Evaluating Python models to populate SQLGlot data")
            parsed_models = self.evaluate_python_models(parsed_models, self.variables)
            
            # Cache the result
            self._parsed_models = parsed_models
            logger.info(f"Successfully parsed {len(parsed_models)} models")
            
            return parsed_models
            
        except Exception as e:
            raise ParserError(f"Failed to discover and parse models: {e}")
    
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
            
            # Build the graph
            self._dependency_graph = self.dependency_builder.build_graph(parsed_models, self.table_resolver)
            
            logger.info(f"Built dependency graph with {len(self._dependency_graph['nodes'])} nodes")
            
            return self._dependency_graph
            
        except Exception as e:
            raise ParserError(f"Failed to build dependency graph: {e}")
    
    def export_all(self) -> Dict[str, Path]:
        """
        Export all parsed models and dependency graph.
        
        Returns:
            Dict mapping export type to file path
        """
        try:
            # Ensure we have parsed models and dependency graph
            parsed_models = self.discover_and_parse_models()
            dependency_graph = self.build_dependency_graph()
            
            # Export JSON files
            json_results = self.json_exporter.export_all(parsed_models, dependency_graph)
            
            # Generate reports
            report_results = self.report_generator.generate_all_reports(dependency_graph)
            
            # Combine results
            all_results = {**json_results, **report_results}
            
            logger.info(f"Exported {len(all_results)} files")
            
            return all_results
            
        except Exception as e:
            raise ParserError(f"Failed to export all data: {e}")
    
    def evaluate_python_models(self, parsed_models: Dict[str, Any], variables: Optional[Variables] = None) -> Dict[str, Any]:
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
    
    def update_python_models_with_qualified_sql(self, updated_models: Dict[str, Any]) -> None:
        """
        Update Python parser's cached models with qualified SQL from execution.
        
        Args:
            updated_models: Updated model data with qualified SQL
        """
        try:
            python_parser = ParserFactory.create_parser("py")
            python_parser.update_models_with_qualified_sql(updated_models)
        except Exception as e:
            logger.error(f"Error updating Python models with qualified SQL: {e}")
    
    def refresh_all(self) -> Dict[str, Any]:
        """
        Force refresh all cached data and re-parse everything.
        
        Returns:
            Dict containing both parsed models and dependency graph
        """
        try:
            # Clear all caches
            self._parsed_models = None
            self._dependency_graph = None
            self.file_discovery.clear_cache()
            
            # Re-discover and parse
            parsed_models = self.discover_and_parse_models()
            dependency_graph = self.build_dependency_graph()
            
            return {
                "parsed_models": parsed_models,
                "dependency_graph": dependency_graph
            }
            
        except Exception as e:
            raise ParserError(f"Failed to refresh all data: {e}")
    
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
