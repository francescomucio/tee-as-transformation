"""
Table name resolution and generation functionality.
"""

from pathlib import Path
from typing import Dict, Any, Optional

from ..shared.types import ParsedModel, ConnectionConfig
from ..shared.exceptions import TableResolutionError


class TableResolver:
    """Handles table name generation and resolution based on connection type."""
    
    def __init__(self, connection: ConnectionConfig):
        """
        Initialize the TableResolver.
        
        Args:
            connection: Connection configuration dict with 'type' key
        """
        self.connection = connection
    
    def generate_full_table_name(self, sql_file: Path, models_folder: Path) -> str:
        """
        Generate the full table name based on the connection type and file path.
        
        Args:
            sql_file: Path to the SQL file
            models_folder: Path to the models folder
            
        Returns:
            Full table name string
            
        Raises:
            TableResolutionError: If table name generation fails
        """
        try:
            # Handle both dict and AdapterConfig
            connection_type = self.connection.get("type") if hasattr(self.connection, 'get') else self.connection.type
            if connection_type == "duckdb":
                # For DuckDB: first parent folder in models + "." + file_name_without_sql
                relative_path = sql_file.relative_to(models_folder)
                path_parts = relative_path.parts
                
                if len(path_parts) >= 2:
                    # First parent folder (schema) + file name without extension
                    schema_name = path_parts[0]
                    file_name = sql_file.stem  # filename without .sql extension
                    return f"{schema_name}.{file_name}"
                else:
                    # If file is directly in models folder
                    return sql_file.stem
            else:
                # For other connection types, use file path but remove all extensions
                relative_path = sql_file.relative_to(models_folder)
                # Convert path separators to dots and remove all file extensions
                table_name = str(relative_path).replace('/', '.').replace('\\', '.')
                # Remove any file extension (not just .sql)
                if '.' in table_name:
                    table_name = table_name.rsplit('.', 1)[0]
                return table_name
        except Exception as e:
            raise TableResolutionError(f"Failed to generate table name for {sql_file}: {e}")
    
    def resolve_table_reference(self, table_ref: str, parsed_models: Dict[str, ParsedModel]) -> Optional[str]:
        """
        Resolve a table reference to its full table name.
        
        Args:
            table_ref: The referenced table name
            parsed_models: Parsed models dict
            
        Returns:
            Full table name if found, None otherwise
        """
        # Direct match
        if table_ref in parsed_models:
            return table_ref
        
        # Try to find by partial name (without schema)
        table_name_only = table_ref.split('.')[-1]
        for full_name in parsed_models.keys():
            if full_name.split('.')[-1] == table_name_only:
                return full_name
        
        return None
    
    def resolve_all_table_references(self, parsed_models: Dict[str, ParsedModel]) -> Dict[str, str]:
        """
        Resolve all table references in the parsed models.
        
        Args:
            parsed_models: Parsed models dict
            
        Returns:
            Dict mapping unresolved references to resolved full names
        """
        resolved = {}
        
        for table_name, model_info in parsed_models.items():
            if "sqlglot" in model_info and model_info["sqlglot"] and "tables" in model_info["sqlglot"]:
                for referenced_table in model_info["sqlglot"]["tables"]:
                    if referenced_table not in resolved:
                        resolved_ref = self.resolve_table_reference(referenced_table, parsed_models)
                        if resolved_ref:
                            resolved[referenced_table] = resolved_ref
        
        return resolved
