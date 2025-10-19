"""
Enhanced execution engine with pluggable database adapters.

This module provides the main execution engine that uses the new adapter system
for database-agnostic SQL model execution with automatic dialect conversion.
"""

from typing import Dict, Any, List, Optional, Union
import logging

from ..adapters import get_adapter, AdapterConfig
from .config import load_database_config


class ExecutionEngine:
    """
    Enhanced execution engine with pluggable database adapters.
    
    This engine supports:
    - Automatic SQL dialect conversion using SQLglot
    - Pluggable database adapters
    - Configuration management from pyproject.toml and environment variables
    - Database-specific optimizations and features
    """
    
    def __init__(self, config: Optional[Union[AdapterConfig, Dict[str, Any]]] = None, config_name: str = "default"):
        """
        Initialize the execution engine.
        
        Args:
            config: Database adapter configuration (AdapterConfig or dict, if None, loads from config files)
            config_name: Configuration name to load (if config is None)
        """
        self.config = config or load_database_config(config_name)
        self.adapter = get_adapter(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def connect(self) -> None:
        """Establish connection to the database."""
        self.adapter.connect()
    
    def disconnect(self) -> None:
        """Close the database connection."""
        self.adapter.disconnect()
    
    def execute_models(self, parsed_models: Dict[str, Any], execution_order: List[str]) -> Dict[str, Any]:
        """
        Execute SQL models in the specified order with dialect conversion.
        
        Args:
            parsed_models: Dictionary mapping table names to parsed SQL arguments
            execution_order: List of table names in execution order
            
        Returns:
            Dictionary with execution results and status
        """
        results = {
            "executed_tables": [],
            "failed_tables": [],
            "execution_log": [],
            "table_info": {},
            "dialect_conversions": [],
            "warnings": []
        }
        
        self.logger.info(f"Starting execution of {len(execution_order)} models using {self.adapter.__class__.__name__}")
        
        # Build table mapping for resolving references
        table_mapping = {}
        for table_name in execution_order:
            if table_name in parsed_models:
                model_data = parsed_models[table_name]
                tables = model_data.get("tables", [])
                for table in tables:
                    # Find the full table name that matches this short name
                    for full_name in execution_order:
                        if full_name.endswith(f".{table}") or full_name == table:
                            table_mapping[table] = full_name
                            break
        
        self.logger.debug(f"Table mapping: {table_mapping}")
        
        for table_name in execution_order:
            try:
                self.logger.info(f"Executing model: {table_name}")
                
                if table_name not in parsed_models:
                    self.logger.warning(f"Model {table_name} not found in parsed models")
                    results["failed_tables"].append({
                        "table": table_name,
                        "error": "Model not found in parsed models"
                    })
                    continue
                
                model_data = parsed_models[table_name]
                
                # Get SQL query (prefer qualified_sql, fallback to sql_content)
                sql_query = self._extract_sql_query(model_data, table_name)
                if not sql_query:
                    results["failed_tables"].append({
                        "table": table_name,
                        "error": "No SQL query found"
                    })
                    continue
                
                # Log dialect conversion if applicable
                if self.adapter.config.source_dialect and self.adapter.config.source_dialect != self.adapter.get_default_dialect():
                    results["dialect_conversions"].append({
                        "table": table_name,
                        "from_dialect": self.adapter.config.source_dialect,
                        "to_dialect": self.adapter.get_default_dialect()
                    })
                
                # Execute based on materialization type
                materialization = self._get_materialization_type(model_data)
                self._execute_materialization(table_name, sql_query, materialization)
                
                # Get table information
                table_info = self.adapter.get_table_info(table_name)
                
                results["executed_tables"].append(table_name)
                results["table_info"][table_name] = table_info
                results["execution_log"].append({
                    "table": table_name,
                    "status": "success",
                    "row_count": table_info["row_count"],
                    "materialization": materialization
                })
                
                self.logger.info(f"Successfully executed {table_name} with {table_info['row_count']} rows")
                
            except Exception as e:
                error_msg = f"Error executing {table_name}: {str(e)}"
                self.logger.error(error_msg)
                results["failed_tables"].append({
                    "table": table_name,
                    "error": str(e)
                })
                results["execution_log"].append({
                    "table": table_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        self.logger.info(f"Execution completed. {len(results['executed_tables'])} successful, {len(results['failed_tables'])} failed")
        return results
    
    def execute_single_model(self, table_name: str, sql_query: str, materialization: str = "table") -> Dict[str, Any]:
        """
        Execute a single SQL model.
        
        Args:
            table_name: Name of the table to create
            sql_query: SQL query to execute
            materialization: Materialization type (table, view, materialized_view, etc.)
            
        Returns:
            Dictionary with execution results
        """
        try:
            self.logger.info(f"Executing single model: {table_name}")
            
            # Execute the materialization
            self._execute_materialization(table_name, sql_query, materialization)
            
            # Get table information
            table_info = self.adapter.get_table_info(table_name)
            
            return {
                "status": "success",
                "table": table_name,
                "table_info": table_info,
                "materialization": materialization
            }
            
        except Exception as e:
            error_msg = f"Error executing {table_name}: {str(e)}"
            self.logger.error(error_msg)
            return {
                "status": "failed",
                "table": table_name,
                "error": str(e)
            }
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get information about the current database connection and adapter."""
        return self.adapter.get_database_info()
    
    def _extract_sql_query(self, model_data: Dict[str, Any], table_name: str) -> str:
        """Extract SQL query from model data."""
        # Get qualified SQL from the sqlglot object (should be generated during parsing)
        if "sqlglot" in model_data and "qualified_sql" in model_data["sqlglot"]:
            return model_data["sqlglot"]["qualified_sql"]
        else:
            self.logger.warning(f"No qualified SQL found for {table_name}, falling back to sql_content")
            if "sqlglot" in model_data and "sql_content" in model_data["sqlglot"]:
                return model_data["sqlglot"]["sql_content"]
            else:
                return ""
    
    def _get_materialization_type(self, model_data: Dict[str, Any]) -> str:
        """
        Extract materialization type from model data.
        
        Args:
            model_data: Parsed model data containing metadata
            
        Returns:
            Materialization type ("table", "view", "materialized_view") or "table" as default
        """
        try:
            # Check if metadata exists and has materialization field
            if "model_metadata" in model_data and "metadata" in model_data["model_metadata"]:
                metadata = model_data["model_metadata"]["metadata"]
                if isinstance(metadata, dict) and "materialization" in metadata:
                    materialization = metadata["materialization"]
                    # Check if the adapter supports this materialization type
                    supported_types = [m.value for m in self.adapter.get_supported_materializations()]
                    if materialization in supported_types:
                        return materialization
                    else:
                        self.logger.warning(
                            f"Materialization type '{materialization}' not supported by {self.adapter.__class__.__name__}, "
                            f"falling back to 'table'. Supported types: {supported_types}"
                        )
            
            # Default to table if no materialization specified or invalid type
            return "table"
            
        except Exception as e:
            self.logger.warning(f"Error extracting materialization type: {e}, defaulting to 'table'")
            return "table"
    
    def _execute_materialization(self, table_name: str, sql_query: str, materialization: str) -> None:
        """Execute the appropriate materialization based on type."""
        if materialization == "view":
            self.adapter.create_view(table_name, sql_query)
        elif materialization == "materialized_view":
            if hasattr(self.adapter, 'create_materialized_view'):
                self.adapter.create_materialized_view(table_name, sql_query)
            else:
                self.logger.warning(f"Materialized views not supported by {self.adapter.__class__.__name__}, creating table instead")
                self.adapter.create_table(table_name, sql_query)
        elif materialization == "external_table":
            if hasattr(self.adapter, 'create_external_table'):
                # External tables need additional configuration
                external_location = self.config.extra.get("external_location") if self.config.extra else None
                if external_location:
                    self.adapter.create_external_table(table_name, sql_query, external_location)
                else:
                    self.logger.warning("External table location not configured, creating table instead")
                    self.adapter.create_table(table_name, sql_query)
            else:
                self.logger.warning(f"External tables not supported by {self.adapter.__class__.__name__}, creating table instead")
                self.adapter.create_table(table_name, sql_query)
        else:  # Default to table for "table" or any other type
            self.adapter.create_table(table_name, sql_query)
