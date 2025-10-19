"""
Enhanced execution engine with pluggable database adapters.

This module provides the main execution engine that uses the new adapter system
for database-agnostic SQL model execution with automatic dialect conversion.
"""

from typing import Dict, Any, List, Optional, Union
import logging
from datetime import datetime

from ..adapters import get_adapter, AdapterConfig
from .config import load_database_config
from .model_state import ModelStateManager


class ExecutionEngine:
    """
    Enhanced execution engine with pluggable database adapters.
    
    This engine supports:
    - Automatic SQL dialect conversion using SQLglot
    - Pluggable database adapters
    - Configuration management from pyproject.toml and environment variables
    - Database-specific optimizations and features
    """
    
    def __init__(self, config: Optional[Union[AdapterConfig, Dict[str, Any]]] = None, config_name: str = "default", project_folder: str = ".", variables: Optional[Dict[str, Any]] = None):
        """
        Initialize the execution engine.
        
        Args:
            config: Database adapter configuration (AdapterConfig or dict, if None, loads from config files)
            config_name: Configuration name to load (if config is None)
            project_folder: Project folder path for state management
            variables: Optional dictionary of variables for model execution
        """
        self.config = config or load_database_config(config_name)
        self.adapter = get_adapter(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.project_folder = project_folder
        self.variables = variables or {}
        
        # Initialize state manager
        self.state_manager = ModelStateManager(project_folder=project_folder)
    
    def connect(self) -> None:
        """Establish connection to the database."""
        self.adapter.connect()
    
    def disconnect(self) -> None:
        """Close the database connection."""
        self.adapter.disconnect()
        self.state_manager.close()
    
    def _generate_sql_hash(self, sql_query: str) -> str:
        """Generate a hash for SQL content using the centralized state manager."""
        return self.state_manager.compute_sql_hash(sql_query)
    
    def _generate_config_hash(self, metadata: Optional[Dict[str, Any]]) -> str:
        """Generate a hash for model configuration using the centralized state manager."""
        if not metadata:
            return self.state_manager.compute_config_hash({})
        return self.state_manager.compute_config_hash(metadata)
    
    def _load_flags(self) -> Dict[str, Any]:
        """Load flags from project configuration."""
        try:
            from .config import load_database_config
            config = load_database_config("default", self.project_folder)
            if hasattr(config, 'extra') and config.extra:
                return config.extra.get('flags', {})
        except Exception as e:
            self.logger.debug(f"Could not load flags: {e}")
        
        return {}
    
    def _check_model_state(self, table_name: str, materialization: str, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Check model state for materialization changes and database existence.
        
        Args:
            table_name: Name of the model
            materialization: Materialization type
            metadata: Model metadata
        """
        # Check if model exists in state
        state = self.state_manager.get_model_state(table_name)
        
        if state is None:
            # Model doesn't exist in state, check if it exists in database
            if self.state_manager.check_database_existence(self.adapter, table_name):
                self.logger.warning(f"Model {table_name} exists in database but not in state. Rebuilding state...")
                self.state_manager.rebuild_state_from_database(self.adapter, table_name)
        else:
            # Check for materialization changes
            flags = self._load_flags()
            behavior = flags.get('materialization_change_behavior', 'warn')
            self.state_manager.check_materialization_change(table_name, materialization, behavior)
    
    def _save_model_state(self, table_name: str, materialization: str, sql_query: str, metadata: Optional[Dict[str, Any]]) -> None:
        """
        Save model state after successful execution.
        
        Args:
            table_name: Name of the model
            materialization: Materialization type
            sql_query: SQL query content
            metadata: Model metadata
        """
        sql_hash = self._generate_sql_hash(sql_query)
        
        # Extract incremental-specific data if applicable
        last_processed_value = None
        strategy = None
        config_hash = None
        
        if materialization == "incremental" and metadata:
            incremental_config = metadata.get('incremental', {})
            if incremental_config:
                strategy = incremental_config.get('strategy')
                # For incremental models, compute config hash from incremental config only
                config_hash = self._generate_config_hash(incremental_config)
                # For incremental models, we might want to track the last processed value
                # This would be set by the incremental executor
            else:
                config_hash = self._generate_config_hash(metadata)
        else:
            config_hash = self._generate_config_hash(metadata)
        
        self.state_manager.save_model_state(
            model_name=table_name,
            materialization=materialization,
            sql_hash=sql_hash,
            config_hash=config_hash,
            last_processed_value=last_processed_value,
            strategy=strategy
        )
        
        self.logger.debug(f"Saved state for model: {table_name}")
    
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
                metadata = self._extract_metadata(model_data)
                
                # Check for materialization changes and database existence
                self._check_model_state(table_name, materialization, metadata)
                
                # Execute the model
                self._execute_materialization(table_name, sql_query, materialization, metadata)
                
                # Save model state after successful execution
                self._save_model_state(table_name, materialization, sql_query, metadata)
                
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
    
    def _execute_materialization(self, table_name: str, sql_query: str, materialization: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Execute the appropriate materialization based on type."""
        if materialization == "view":
            self.adapter.create_view(table_name, sql_query, metadata)
        elif materialization == "materialized_view":
            if hasattr(self.adapter, 'create_materialized_view'):
                self.adapter.create_materialized_view(table_name, sql_query)
            else:
                self.logger.warning(f"Materialized views not supported by {self.adapter.__class__.__name__}, creating table instead")
                self.adapter.create_table(table_name, sql_query, metadata)
        elif materialization == "external_table":
            if hasattr(self.adapter, 'create_external_table'):
                # External tables need additional configuration
                external_location = self.config.extra.get("external_location") if self.config.extra else None
                if external_location:
                    self.adapter.create_external_table(table_name, sql_query, external_location)
                else:
                    self.logger.warning("External table location not configured, creating table instead")
                    self.adapter.create_table(table_name, sql_query, metadata)
            else:
                self.logger.warning(f"External tables not supported by {self.adapter.__class__.__name__}, creating table instead")
                self.adapter.create_table(table_name, sql_query, metadata)
        elif materialization == "incremental":
            self._execute_incremental_materialization(table_name, sql_query, metadata)
        else:  # Default to table for "table" or any other type
            self.adapter.create_table(table_name, sql_query, metadata)
    
    def _execute_incremental_materialization(self, table_name: str, sql_query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Execute incremental materialization using the universal state manager."""
        from .incremental_executor import IncrementalExecutor
        
        # Use the universal state manager
        executor = IncrementalExecutor(self.state_manager)
        
        try:
            # Extract incremental configuration
            incremental_config = metadata.get('incremental') if metadata else None
            
            # If incremental config is not found, check if the metadata itself contains incremental info
            if not incremental_config and metadata and metadata.get('materialization') == 'incremental':
                # This is a flat metadata structure from SQL files, need to restructure it
                strategy = metadata.get('strategy', 'append')  # Default to append if not specified
                incremental_config = {
                    'strategy': strategy,
                    strategy: {
                        'time_column': metadata.get('time_column'),
                        'start_date': metadata.get('start_date'),
                        'lookback': metadata.get('lookback'),
                        'unique_key': metadata.get('unique_key'),
                        'where_condition': metadata.get('where_condition')
                    }
                }
                # Remove None values
                incremental_config[strategy] = {k: v for k, v in incremental_config[strategy].items() if v is not None}
            
            if not incremental_config:
                self.logger.error("Incremental materialization requires incremental configuration")
                # Fallback to table
                self.adapter.create_table(table_name, sql_query, metadata)
                return
            
            strategy = incremental_config.get('strategy')
            if not strategy:
                self.logger.error("Incremental strategy is required")
                # Fallback to table
                self.adapter.create_table(table_name, sql_query, metadata)
                return
            
            # Check if we should run incrementally
            should_run_incremental = executor.should_run_incremental(table_name, sql_query, incremental_config)
            if not should_run_incremental:
                # Run as full load (create/replace table)
                self.adapter.create_table(table_name, sql_query, metadata)
                
                # Update state after full load to enable incremental runs
                current_time = datetime.utcnow().isoformat()
                strategy = incremental_config.get('strategy') if incremental_config else None
                self.state_manager.update_processed_value(table_name, current_time, strategy)
            else:
                # Run incremental strategy
                if strategy == "append":
                    append_config = incremental_config.get('append')
                    if not append_config:
                        self.logger.error("Append strategy requires append configuration")
                        return
                    executor.execute_append_strategy(table_name, sql_query, append_config, self.adapter, table_name, self.variables)
                
                elif strategy == "merge":
                    merge_config = incremental_config.get('merge')
                    if not merge_config:
                        self.logger.error("Merge strategy requires merge configuration")
                        return
                    executor.execute_merge_strategy(table_name, sql_query, merge_config, self.adapter, table_name, self.variables)
                
                elif strategy == "delete_insert":
                    delete_insert_config = incremental_config.get('delete_insert')
                    if not delete_insert_config:
                        self.logger.error("Delete+insert strategy requires delete_insert configuration")
                        return
                    executor.execute_delete_insert_strategy(table_name, sql_query, delete_insert_config, self.adapter, table_name, self.variables)
                
                else:
                    self.logger.error(f"Unknown incremental strategy: {strategy}")
                    return
            
            # State is already saved by individual strategy methods
            
        except Exception as e:
            self.logger.error(f"Error executing incremental materialization: {e}")
            # Fallback to table creation
            self.adapter.create_table(table_name, sql_query, metadata)
        finally:
            pass  # State manager is managed by the execution engine
    
    def _extract_metadata(self, model_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from model data, prioritizing decorator metadata over file metadata.
        
        Args:
            model_data: Dictionary containing model data
            
        Returns:
            Metadata dictionary with column descriptions, or None if no metadata found
        """
        try:
            # Debug: Log the entire model_data structure
            
            # First, try to get metadata from model_metadata (from decorator)
            model_metadata = model_data.get("model_metadata", {})
            if model_metadata and "metadata" in model_metadata:
                decorator_metadata = model_metadata["metadata"]
                
                # Check if this is incremental materialization
                if "materialization" in decorator_metadata and decorator_metadata["materialization"] == "incremental":
                    return decorator_metadata
                
                if decorator_metadata and "schema" in decorator_metadata:
                    # Prioritize file metadata description over decorator description
                    if "description" in model_metadata and "description" not in decorator_metadata:
                        decorator_metadata["description"] = model_metadata["description"]
                    return decorator_metadata
                elif decorator_metadata and "metadata" in decorator_metadata and "schema" in decorator_metadata["metadata"]:
                    # Handle nested metadata structure
                    nested_metadata = decorator_metadata["metadata"]
                    if "description" in model_metadata:
                        nested_metadata["description"] = model_metadata["description"]
                    return nested_metadata
            
            # Fallback to any other metadata in the model data
            if "metadata" in model_data:
                file_metadata = model_data["metadata"]
                if file_metadata and "schema" in file_metadata:
                    # Use file metadata description if available
                    if "description" in model_metadata:
                        file_metadata["description"] = model_metadata["description"]
                    # If no decorator description, file metadata description is already there
                    return file_metadata
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error extracting metadata: {e}")
            return None
