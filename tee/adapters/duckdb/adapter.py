"""
DuckDB adapter with SQLglot integration.

This adapter provides DuckDB-specific functionality including:
- SQL dialect conversion
- DuckDB-specific optimizations
- Connection management
- Materialization support
"""

from typing import Dict, Any, List, Optional

try:
    import duckdb
except ImportError:
    duckdb = None

from tee.adapters.base import DatabaseAdapter, AdapterConfig, MaterializationType
from tee.adapters.registry import register_adapter


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
            MaterializationType.INCREMENTAL,
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

    def create_table(
        self, table_name: str, query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a table from a qualified SQL query with optional column metadata."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Create schema if needed
        self._create_schema_if_needed(table_name)

        # Convert SQL and qualify table references
        converted_query = self._convert_and_qualify_sql(query)

        # Wrap the query in a CREATE TABLE statement
        create_query = f"CREATE OR REPLACE TABLE {table_name} AS {converted_query}"

        try:
            self._execute_query(create_query)
            self.logger.info(f"Created table: {table_name}")

            # Add table and column comments if metadata is provided
            if metadata:
                try:
                    # Add table comment if description is provided
                    table_description = metadata.get("description")
                    if table_description:
                        self._add_table_comment(table_name, table_description)

                    # Add column comments
                    column_descriptions = self._validate_column_metadata(metadata)
                    if column_descriptions:
                        self._add_column_comments(table_name, column_descriptions)
                except ValueError as e:
                    self.logger.error(f"Invalid metadata for table {table_name}: {e}")
                    raise
                except Exception as e:
                    self.logger.warning(f"Could not add comments for table {table_name}: {e}")
                    # Don't raise here - table creation succeeded, comments are optional

        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def create_view(
        self, view_name: str, query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a view from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Create schema if needed
        self._create_schema_if_needed(view_name)

        # Convert SQL and qualify table references
        converted_query = self._convert_and_qualify_sql(query)

        # Wrap the query in a CREATE VIEW statement
        create_query = f"CREATE OR REPLACE VIEW {view_name} AS {converted_query}"

        try:
            self._execute_query(create_query)
            self.logger.info(f"Created view: {view_name}")

            # Add view and column comments if metadata is provided
            if metadata:
                # Add view description
                if "description" in metadata and metadata["description"]:
                    self._add_table_comment(view_name, metadata["description"])

                # Add column comments
                if "schema" in metadata and metadata["schema"]:
                    column_descriptions = self._validate_column_metadata(metadata)
                    if column_descriptions:
                        self._add_column_comments(view_name, column_descriptions)

        except Exception as e:
            self.logger.error(f"Failed to create view {view_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            result = self.connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table_name]
            ).fetchone()
            return result[0] > 0
        except Exception:
            return False

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            self._execute_query(f"DROP TABLE IF EXISTS {table_name}")
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
                "row_count": count_result[0] if count_result else 0,
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise

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

    def _create_schema_if_needed(self, object_name: str) -> None:
        """Create schema if needed for the given object name."""
        if "." in object_name:
            schema_name, _ = object_name.split(".", 1)
            try:
                self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")

    def _convert_and_qualify_sql(self, query: str) -> str:
        """Convert SQL dialect and qualify table references if schema is specified."""
        converted_query = self.convert_sql_dialect(query)
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)
        return converted_query

    def _execute_query(self, query: str) -> None:
        """Execute a simple query with error handling."""
        try:
            self.connection.execute(query)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def execute_incremental_append(self, table_name: str, sql_query: str) -> None:
        """Execute incremental append operation."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Convert SQL and qualify table references
        converted_query = self._convert_and_qualify_sql(sql_query)

        # Insert into existing table
        insert_query = f"INSERT INTO {table_name} {converted_query}"

        try:
            self._execute_query(insert_query)
            self.logger.info(f"Executed incremental append for table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error executing incremental append for {table_name}: {e}")
            raise

    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            # Use DESCRIBE to get column information
            result = self.connection.execute(f"DESCRIBE {table_name}").fetchall()
            return [row[0] for row in result]  # First column is the column name
        except Exception as e:
            self.logger.error(f"Error getting columns for table {table_name}: {e}")
            # Fallback to a default set of columns
            return ["id", "name", "created_at", "updated_at", "status"]

    def _generate_merge_sql(
        self, table_name: str, source_sql: str, unique_key: List[str], columns: List[str]
    ) -> str:
        """Generate DuckDB-specific MERGE SQL statement."""
        key_conditions = []
        for key in unique_key:
            key_conditions.append(f"target.{key} = source.{key}")

        key_condition = " AND ".join(key_conditions)

        # Generate UPDATE SET clause (exclude unique key columns)
        update_clauses = []
        for col in columns:
            if col not in unique_key:
                update_clauses.append(f"{col} = source.{col}")

        update_set = (
            ", ".join(update_clauses)
            if update_clauses
            else f"{unique_key[0]} = source.{unique_key[0]}"
        )

        # Generate INSERT clause
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING ({source_sql}) AS source
        ON {key_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """

        return merge_sql.strip()

    def execute_incremental_merge(
        self, table_name: str, source_sql: str, config: Dict[str, Any]
    ) -> None:
        """Execute incremental merge operation."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Get table columns dynamically
        columns = self.get_table_columns(table_name)
        unique_key = config["unique_key"]

        # Generate DuckDB-specific merge SQL
        merge_sql = self._generate_merge_sql(table_name, source_sql, unique_key, columns)

        # Convert SQL and qualify table references
        converted_query = self._convert_and_qualify_sql(merge_sql)

        # Log the generated SQL (debug level for cleaner output)
        self.logger.debug(f"Generated DuckDB merge SQL for {table_name}: {converted_query}")

        try:
            self._execute_query(converted_query)
            self.logger.info("Executed incremental merge")
        except Exception as e:
            self.logger.error(f"Error executing incremental merge: {e}")
            raise

    def generate_no_duplicates_test_query(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> str:
        """
        Generate SQL query for no_duplicates test (DuckDB-specific).

        DuckDB supports GROUP BY * which makes this efficient.
        """
        if columns and len(columns) > 0:
            # Use provided columns
            column_list = ", ".join(columns)
            return f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT {column_list}, COUNT(*) as row_count
                    FROM {table_name}
                    GROUP BY {column_list}
                    HAVING COUNT(*) > 1
                ) AS duplicate_groups
            """

        # DuckDB supports GROUP BY * for grouping by all columns
        return f"""
            SELECT COUNT(*) 
            FROM (
                SELECT *, COUNT(*) as row_count
                FROM {table_name}
                GROUP BY *
                HAVING COUNT(*) > 1
            ) AS duplicate_groups
        """

    def execute_incremental_delete_insert(
        self, table_name: str, delete_sql: str, insert_sql: str
    ) -> None:
        """Execute incremental delete+insert operation."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Convert SQL and qualify table references
        converted_delete = self._convert_and_qualify_sql(delete_sql)
        converted_insert = self._convert_and_qualify_sql(insert_sql)

        # Log the generated SQL (debug level for cleaner output)
        self.logger.debug(f"Generated DuckDB delete+insert SQL for {table_name}:")
        self.logger.debug(f"DELETE: {converted_delete}")
        self.logger.debug(f"INSERT: {converted_insert}")

        try:
            # Execute delete
            self._execute_query(converted_delete)
            self.logger.info(f"Executed delete for table: {table_name}")

            # Execute insert
            insert_query = f"INSERT INTO {table_name} {converted_insert}"
            self._execute_query(insert_query)
            self.logger.info(f"Executed insert for table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error executing incremental delete+insert for {table_name}: {e}")
            raise


# Register the adapter
register_adapter("duckdb", DuckDBAdapter)
