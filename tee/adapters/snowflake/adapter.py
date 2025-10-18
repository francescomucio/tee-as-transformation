"""
Snowflake adapter with SQLglot integration.

This adapter provides Snowflake-specific functionality including:
- SQL dialect conversion from other dialects to Snowflake
- Snowflake-specific optimizations and features
- Connection management with warehouse and role support
- Materialization support including external tables
"""

from typing import Dict, Any, List, Optional
import logging

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
except ImportError:
    snowflake = None
    DictCursor = None

from ..base import DatabaseAdapter, AdapterConfig, MaterializationType
from ..registry import register_adapter


class SnowflakeAdapter(DatabaseAdapter):
    """Snowflake database adapter with SQLglot integration."""
    
    def __init__(self, config: AdapterConfig):
        if snowflake is None:
            raise ImportError("Snowflake connector is not installed. Install it with: uv add snowflake-connector-python")
        
        super().__init__(config)
    
    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for Snowflake."""
        return "snowflake"
    
    def get_supported_materializations(self) -> List[MaterializationType]:
        """Get list of supported materialization types for Snowflake."""
        return [
            MaterializationType.TABLE,
            MaterializationType.VIEW,
            MaterializationType.MATERIALIZED_VIEW,
            MaterializationType.EXTERNAL_TABLE,
        ]
    
    def connect(self) -> None:
        """Establish connection to Snowflake database."""
        if not all([self.config.host, self.config.user, self.config.password, self.config.database]):
            raise ValueError("Snowflake connection requires host, user, password, and database")
        
        connection_params = {
            "account": self.config.host,
            "user": self.config.user,
            "password": self.config.password,
            "database": self.config.database,
            "warehouse": self.config.warehouse,
            "role": self.config.role,
            "schema": self.config.schema,
        }
        
        # Remove None values
        connection_params = {k: v for k, v in connection_params.items() if v is not None}
        
        try:
            self.connection = snowflake.connector.connect(**connection_params)
            self.logger.info(f"Connected to Snowflake: {self.config.host}/{self.config.database}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from Snowflake database")
    
    def execute_query(self, query: str) -> Any:
        """Execute a SQL query and return results."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
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
            schema_name, table_only = table_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Wrap the query in a CREATE TABLE statement
        create_query = f"CREATE OR REPLACE TABLE {table_name} AS {converted_query}"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            cursor.close()
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
            schema_name, view_only = view_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Wrap the query in a CREATE VIEW statement
        create_query = f"CREATE OR REPLACE VIEW {view_name} AS {converted_query}"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            cursor.close()
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
            schema_name, view_only = view_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Snowflake materialized view syntax
        create_query = f"CREATE OR REPLACE MATERIALIZED VIEW {view_name} AS {converted_query}"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            cursor.close()
            self.logger.info(f"Created materialized view: {view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create materialized view {view_name}: {e}")
            raise
    
    def create_external_table(self, table_name: str, query: str, external_location: str) -> None:
        """Create an external table from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)
        
        # Extract schema and table name
        if '.' in table_name:
            schema_name, table_only = table_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")
        
        # Snowflake external table syntax (simplified - would need proper column definitions)
        create_query = f"CREATE OR REPLACE EXTERNAL TABLE {table_name} LOCATION = '{external_location}' AS {converted_query}"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            cursor.close()
            self.logger.info(f"Created external table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create external table {table_name}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            )
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0
        except Exception:
            return False
    
    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.close()
            self.logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            cursor = self.connection.cursor()
            
            # Get table schema
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = ?
                ORDER BY ordinal_position
            """, [table_name])
            schema_result = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cursor.fetchone()
            
            cursor.close()
            
            return {
                "schema": [{"column": row[0], "type": row[1]} for row in schema_result],
                "row_count": count_result[0] if count_result else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    def validate_connection_string(self, connection_string: str) -> bool:
        """Validate Snowflake connection string format."""
        if not connection_string or not connection_string.strip():
            return False
        
        # Snowflake connection strings should be in format:
        # snowflake://user:password@account/database?warehouse=wh&role=role
        return connection_string.startswith("snowflake://")
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get Snowflake-specific database information."""
        base_info = super().get_database_info()
        
        if self.connection:
            try:
                cursor = self.connection.cursor()
                
                # Get Snowflake version
                cursor.execute("SELECT CURRENT_VERSION()")
                version_result = cursor.fetchone()
                base_info["snowflake_version"] = version_result[0] if version_result else "unknown"
                
                # Get current warehouse
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                warehouse_result = cursor.fetchone()
                base_info["current_warehouse"] = warehouse_result[0] if warehouse_result else "unknown"
                
                # Get current role
                cursor.execute("SELECT CURRENT_ROLE()")
                role_result = cursor.fetchone()
                base_info["current_role"] = role_result[0] if role_result else "unknown"
                
                cursor.close()
                
            except Exception as e:
                self.logger.warning(f"Could not get Snowflake-specific info: {e}")
        
        return base_info


# Register the adapter
register_adapter("snowflake", SnowflakeAdapter)
