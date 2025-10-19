"""
DuckDB adapter with SQLglot integration.

This adapter provides DuckDB-specific functionality including:
- SQL dialect conversion
- DuckDB-specific optimizations
- Connection management
- Materialization support
"""

from typing import Dict, Any, List, Optional
import logging

try:
    import duckdb
except ImportError:
    duckdb = None

from ..base import DatabaseAdapter, AdapterConfig, MaterializationType
from ..registry import register_adapter


class DuckDBAdapter(DatabaseAdapter):
    """DuckDB database adapter with SQLglot integration."""
    
    def __init__(self, config: AdapterConfig):
        if duckdb is None:
            raise ImportError("DuckDB is not installed. Install it with: uv add duckdb")
        
        super().__init__(config)
    
    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for DuckDB."""
        return "duckdb"
    
    def get_supported_materializations(self) -> List[MaterializationType]:
        """Get list of supported materialization types for DuckDB."""
        return [
            MaterializationType.TABLE,
            MaterializationType.VIEW,
            MaterializationType.MATERIALIZED_VIEW,
        ]
    
    def connect(self) -> None:
        """Establish connection to DuckDB database."""
        db_path = self.config.path or ":memory:"
        self.connection = duckdb.connect(db_path)
        self.logger.info(f"Connected to DuckDB database: {db_path}")
    
    def disconnect(self) -> None:
        """Close the DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from DuckDB database")
    
    def execute_query(self, query: str) -> Any:
        """Execute a SQL query and return results."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            result = self.connection.execute(query).fetchall()
            self.logger.debug(f"Executed query: {query[:100]}...")
            return result
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def create_table(self, table_name: str, query: str) -> None:
        """Create a table from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)
        
        # Extract schema and table name
        if '.' in table_name:
            schema_name, _ = table_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Wrap the query in a CREATE TABLE statement
        create_query = f"CREATE OR REPLACE TABLE {table_name} AS {converted_query}"
        
        try:
            self.connection.execute(create_query)
            self.logger.info(f"Created table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def create_view(self, view_name: str, query: str) -> None:
        """Create a view from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)
        
        # Extract schema and view name
        if '.' in view_name:
            schema_name, _ = view_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Wrap the query in a CREATE VIEW statement
        create_query = f"CREATE OR REPLACE VIEW {view_name} AS {converted_query}"
        
        try:
            self.connection.execute(create_query)
            self.logger.info(f"Created view: {view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create view {view_name}: {e}")
            raise
    
    def create_materialized_view(self, view_name: str, query: str) -> None:
        """Create a materialized view from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)
        
        # Extract schema and view name
        if '.' in view_name:
            schema_name, _ = view_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # DuckDB doesn't have native materialized views, so we create a table
        # In a real implementation, you might want to implement refresh logic
        create_query = f"CREATE OR REPLACE TABLE {view_name} AS {converted_query}"
        
        try:
            self.connection.execute(create_query)
            self.logger.info(f"Created materialized view (as table): {view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create materialized view {view_name}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            result = self.connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            ).fetchone()
            return result[0] > 0
        except Exception:
            return False
    
    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            # Get table schema
            schema_query = f"DESCRIBE {table_name}"
            schema_result = self.connection.execute(schema_query).fetchall()
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.connection.execute(count_query).fetchone()
            
            return {
                "schema": [{"column": row[0], "type": row[1]} for row in schema_result],
                "row_count": count_result[0] if count_result else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    def validate_connection_string(self, connection_string: str) -> bool:
        """Validate DuckDB connection string format."""
        if not connection_string or not connection_string.strip():
            return False
        
        # DuckDB connection strings can be:
        # - File path: "/path/to/database.db"
        # - Memory: ":memory:"
        # - Special: ":temp:" or other DuckDB special identifiers
        return True  # DuckDB is very permissive with connection strings
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get DuckDB-specific database information."""
        base_info = super().get_database_info()
        
        if self.connection:
            try:
                # Get DuckDB version
                version_result = self.connection.execute("SELECT version()").fetchone()
                base_info["duckdb_version"] = version_result[0] if version_result else "unknown"
                
                # Get database path
                base_info["database_path"] = self.config.path or ":memory:"
                
            except Exception as e:
                self.logger.warning(f"Could not get DuckDB-specific info: {e}")
        
        return base_info


# Register the adapter
register_adapter("duckdb", DuckDBAdapter)
