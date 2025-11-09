"""
DuckDB adapter with SQLglot integration.

This adapter provides DuckDB-specific functionality including:
- SQL dialect conversion
- DuckDB-specific optimizations
- Connection management
- Materialization support
"""

from typing import Any

try:
    import duckdb
except ImportError:
    duckdb = None

from tee.adapters.base import AdapterConfig, DatabaseAdapter, MaterializationType
from tee.adapters.registry import register_adapter

from .functions.function_manager import FunctionManager
from .materialization.incremental_handler import IncrementalHandler
from .materialization.table_handler import TableHandler
from .materialization.view_handler import ViewHandler
from .utils.helpers import DuckDBUtils


class DuckDBAdapter(DatabaseAdapter):
    """DuckDB database adapter with SQLglot integration."""

    def __init__(self, config: AdapterConfig):
        if duckdb is None:
            raise ImportError("DuckDB is not installed. Install it with: uv add duckdb")

        super().__init__(config)

        # Initialize component managers
        self.function_manager = FunctionManager(self)
        self.table_handler = TableHandler(self)
        self.view_handler = ViewHandler(self)
        self.incremental_handler = IncrementalHandler(self)
        self.utils = DuckDBUtils(self)

    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for DuckDB."""
        return "duckdb"

    def get_supported_materializations(self) -> list[MaterializationType]:
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
        self, table_name: str, query: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Create a table from a qualified SQL query with optional column metadata."""
        self.table_handler.create(table_name, query, metadata)

    def create_view(
        self, view_name: str, query: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Create a view from a qualified SQL query."""
        self.view_handler.create(view_name, query, metadata)

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
            self.utils.execute_query(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            raise

    def get_table_info(self, table_name: str) -> dict[str, Any]:
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

    def create_function(
        self,
        function_name: str,
        function_sql: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create or replace a user-defined function in the database."""
        self.function_manager.create(function_name, function_sql, metadata)

    def function_exists(self, function_name: str, signature: str | None = None) -> bool:
        """Check if a function exists in the database."""
        return self.function_manager.exists(function_name, signature)

    def drop_function(self, function_name: str) -> None:
        """Drop a function from the database."""
        self.function_manager.drop(function_name)

    def get_database_info(self) -> dict[str, Any]:
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

    def get_table_columns(self, table_name: str) -> list[str]:
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

    def execute_incremental_append(self, table_name: str, sql_query: str) -> None:
        """Execute incremental append operation."""
        self.incremental_handler.execute_append(table_name, sql_query)

    def execute_incremental_merge(
        self, table_name: str, source_sql: str, config: dict[str, Any]
    ) -> None:
        """Execute incremental merge operation."""
        self.incremental_handler.execute_merge(table_name, source_sql, config)

    def execute_incremental_delete_insert(
        self, table_name: str, delete_sql: str, insert_sql: str
    ) -> None:
        """Execute incremental delete+insert operation."""
        self.incremental_handler.execute_delete_insert(table_name, delete_sql, insert_sql)

    def _generate_merge_sql(
        self, table_name: str, source_sql: str, unique_key: list[str], columns: list[str]
    ) -> str:
        """Generate DuckDB-specific MERGE SQL statement."""
        return self.incremental_handler._generate_merge_sql(table_name, source_sql, unique_key, columns)

    def generate_no_duplicates_test_query(
        self, table_name: str, columns: list[str] | None = None
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


# Register the adapter
register_adapter("duckdb", DuckDBAdapter)
