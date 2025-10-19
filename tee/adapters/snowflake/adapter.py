"""
Snowflake adapter with SQLglot integration.

This adapter provides Snowflake-specific functionality including:
- SQL dialect conversion from other dialects to Snowflake
- Snowflake-specific optimizations and features
- Connection management with warehouse and role support
- Materialization support including external tables
"""

from typing import Dict, Any, List, Optional

try:
    import snowflake.connector
except ImportError:
    snowflake = None

try:
    import sqlglot
except ImportError:
    sqlglot = None

from ..base import DatabaseAdapter, AdapterConfig, MaterializationType
from ..registry import register_adapter


class SnowflakeAdapter(DatabaseAdapter):
    """Snowflake database adapter with SQLglot integration."""
    
    # Snowflake-specific required fields
    REQUIRED_FIELDS = ['type', 'account', 'user', 'password', 'database']
    
    def __init__(self, config_dict: Dict[str, Any]):
        if snowflake is None:
            raise ImportError("Snowflake connector is not installed. Install it with: uv add snowflake-connector-python")
        
        super().__init__(config_dict)
    
    def _validate_field_values(self, config_dict: Dict[str, Any]) -> None:
        """Validate Snowflake-specific field values."""
        super()._validate_field_values(config_dict)
        
        # Validate account format
        if 'account' in config_dict:
            account = config_dict['account']
            if not isinstance(account, str) or not account.strip():
                raise ValueError("Snowflake account must be a non-empty string")
        
        # Validate warehouse if provided
        if 'warehouse' in config_dict and config_dict['warehouse']:
            if not isinstance(config_dict['warehouse'], str) or not config_dict['warehouse'].strip():
                raise ValueError("Snowflake warehouse must be a non-empty string")
        
        # Validate role if provided
        if 'role' in config_dict and config_dict['role']:
            if not isinstance(config_dict['role'], str) or not config_dict['role'].strip():
                raise ValueError("Snowflake role must be a non-empty string")
    
    def _create_adapter_config(self, config_dict: Dict[str, Any]) -> AdapterConfig:
        """Create Snowflake-specific AdapterConfig."""
        # Prepare extra fields for Snowflake-specific settings
        extra_fields = {}
        if config_dict.get("account"):
            extra_fields["account"] = config_dict.get("account")
        if config_dict.get("extra"):
            extra_fields.update(config_dict.get("extra"))
        
        return AdapterConfig(
            type=config_dict['type'],
            host=config_dict.get('host'),
            port=config_dict.get('port'),
            database=config_dict.get('database'),
            user=config_dict.get('user'),
            password=config_dict.get('password'),
            path=config_dict.get('path'),
            source_dialect=config_dict.get('source_dialect'),
            target_dialect=config_dict.get('target_dialect'),
            connection_timeout=config_dict.get('connection_timeout', 30),
            query_timeout=config_dict.get('query_timeout', 300),
            schema=config_dict.get('schema'),
            warehouse=config_dict.get('warehouse'),
            role=config_dict.get('role'),
            project=config_dict.get('project'),
            extra=extra_fields if extra_fields else None,
        )
    
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
        
        # Get account from extra fields or use host
        account = self.config.host
        if self.config.extra and "account" in self.config.extra:
            account = self.config.extra["account"]
        
        connection_params = {
            "account": account,
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
    
    def create_table(self, table_name: str, query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Create a table from a qualified SQL query with optional column metadata."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Create schema if needed (use 3-part naming for Snowflake)
        if '.' in table_name:
            schema_name, _ = table_name.split('.', 1)
            database_name = self.config.database
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")
                cursor.close()
                self.logger.debug(f"Created schema: {database_name}.{schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {database_name}.{schema_name}: {e}")
        
        # Use 3-part naming for the table (DATABASE.SCHEMA.TABLE)
        database_name = self.config.database
        qualified_table_name = f"{database_name}.{table_name}"
        create_query = f"CREATE OR REPLACE TABLE {qualified_table_name} AS {query}"
        
        # Log the SQL being executed at DEBUG level
        self.logger.debug(f"Executing SQL for table {table_name}:")
        self.logger.debug(f"  Original query: {query}")
        self.logger.debug(f"  Full CREATE statement: {create_query}")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            cursor.close()
            self.logger.info(f"Created table: {table_name}")
            
            # Add table and column comments if metadata is provided
            if metadata:
                try:
                    # Add table comment if description is provided
                    table_description = metadata.get('description')
                    if table_description:
                        self._add_table_comment(qualified_table_name, table_description)
                    
                    # Add column comments
                    column_descriptions = self._validate_column_metadata(metadata)
                    if column_descriptions:
                        self._add_column_comments(qualified_table_name, column_descriptions)
                except ValueError as e:
                    self.logger.error(f"Invalid metadata for table {table_name}: {e}")
                    raise
                except Exception as e:
                    self.logger.warning(f"Could not add comments for table {table_name}: {e}")
                    # Don't raise here - table creation succeeded, comments are optional
                    
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def create_view(self, view_name: str, query: str) -> None:
        """Create a view from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Create schema if needed (use 3-part naming for Snowflake)
        if '.' in view_name:
            schema_name, _ = view_name.split('.', 1)
            database_name = self.config.database
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")
                cursor.close()
                self.logger.debug(f"Created schema: {database_name}.{schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {database_name}.{schema_name}: {e}")
        
        # Use 3-part naming for the view (DATABASE.SCHEMA.VIEW)
        database_name = self.config.database
        qualified_view_name = f"{database_name}.{view_name}"
        create_query = f"CREATE OR REPLACE VIEW {qualified_view_name} AS {query}"
        
        # Log the SQL being executed at DEBUG level
        self.logger.debug(f"Executing SQL for view {view_name}:")
        self.logger.debug(f"  Original query: {query}")
        self.logger.debug(f"  Full CREATE statement: {create_query}")
        
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
            schema_name, _ = view_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                cursor.close()
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
            schema_name, _ = table_name.split('.', 1)
            # Create schema if it doesn't exist
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                cursor.close()
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
            
            # Use 3-part naming for Snowflake (DATABASE.SCHEMA.TABLE)
            database_name = self.config.database
            if '.' in table_name:
                qualified_table_name = f"{database_name}.{table_name}"
            else:
                qualified_table_name = f"{database_name}.{table_name}"
            
            # Get table schema - extract just the table name for the query
            if '.' in table_name:
                _, table_name_only = table_name.split('.', 1)
            else:
                table_name_only = table_name
                
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name_only,))
            schema_result = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {qualified_table_name}")
            count_result = cursor.fetchone()
            
            cursor.close()
            
            return {
                "schema": [{"column": row[0], "type": row[1]} for row in schema_result],
                "row_count": count_result[0] if count_result else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    
    def qualify_table_references(self, sql: str, schema: Optional[str] = None) -> str:
        """
        Qualify table references with schema names for Snowflake.
        
        Args:
            sql: SQL query to qualify
            schema: Schema name to use for qualification
            
        Returns:
            SQL with qualified table references
        """
        if not sql or not sql.strip() or not schema:
            return sql
        
        if sqlglot is None:
            self.logger.warning("SQLglot not available, skipping table qualification")
            return sql
        
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, read=self.target_dialect)
            
            # For Snowflake, we need to manually qualify table references
            # because the qualify optimizer quotes schema names incorrectly
            qualified_tables = []
            for table in parsed.find_all(sqlglot.exp.Table):
                if not table.name.startswith(schema + '.'):
                    # Create a new qualified table reference
                    qualified_name = f"{schema.upper()}.{table.name}"
                    # Replace the table name in the AST
                    table.replace(sqlglot.exp.Table(this=qualified_name))
                    qualified_tables.append(qualified_name)
            
            # Convert back to SQL
            qualified_sql = parsed.sql(dialect=self.target_dialect)
            
            # Log qualification
            if qualified_tables:
                self.logger.debug(f"Qualified tables: {qualified_tables}")
            
            return qualified_sql
            
        except Exception as e:
            self.logger.warning(f"Failed to qualify table references: {e}")
            return sql

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
    
    def _add_table_comment(self, table_name: str, description: str) -> None:
        """
        Add a comment to a table using Snowflake's COMMENT ON TABLE syntax.
        
        Args:
            table_name: Fully qualified table name (DATABASE.SCHEMA.TABLE)
            description: Description of the table
        """
        cursor = self.connection.cursor()
        try:
            # Snowflake uses COMMENT ON TABLE syntax
            comment_query = f"COMMENT ON TABLE {table_name} IS '{description.replace("'", "''")}'"
            cursor.execute(comment_query)
            self.logger.debug(f"Added comment to table {table_name}")
        except Exception as e:
            self.logger.warning(f"Could not add comment to table {table_name}: {e}")
            # Don't raise here - table creation succeeded, comments are optional
        finally:
            cursor.close()
    
    def _add_column_comments(self, table_name: str, column_descriptions: Dict[str, str]) -> None:
        """
        Add column comments to a table using Snowflake's COMMENT ON COLUMN syntax.
        
        Args:
            table_name: Fully qualified table name (DATABASE.SCHEMA.TABLE)
            column_descriptions: Dictionary mapping column names to descriptions
        """
        cursor = self.connection.cursor()
        try:
            for col_name, description in column_descriptions.items():
                try:
                    # Snowflake uses COMMENT ON COLUMN syntax
                    comment_query = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{description.replace("'", "''")}'"
                    cursor.execute(comment_query)
                    self.logger.debug(f"Added comment to column {table_name}.{col_name}")
                except Exception as e:
                    self.logger.warning(f"Could not add comment to column {table_name}.{col_name}: {e}")
                    # Continue with other columns even if one fails
        finally:
            cursor.close()


# Register the adapter
register_adapter("snowflake", SnowflakeAdapter)
