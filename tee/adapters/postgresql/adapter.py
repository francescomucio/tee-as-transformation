"""
PostgreSQL adapter implementation.

This module provides PostgreSQL-specific functionality including:
- SQL dialect conversion
- PostgreSQL-specific optimizations
- Connection management
- Materialization support
"""

from typing import Any, Optional, Dict
from tee.adapters.base import DatabaseAdapter, AdapterConfig, MaterializationType
from tee.adapters.registry import register_adapter


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter with SQLglot integration."""

    def __init__(self, config: AdapterConfig):
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 is not installed. Install it with: uv add psycopg2-binary")

        super().__init__(config)

    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for PostgreSQL."""
        return "postgresql"

    def get_supported_materializations(self) -> list[MaterializationType]:
        """Get list of supported materialization types for PostgreSQL."""
        return [
            MaterializationType.TABLE,
            MaterializationType.VIEW,
            MaterializationType.MATERIALIZED_VIEW,
        ]

    def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        if not all(
            [self.config.host, self.config.user, self.config.password, self.config.database]
        ):
            raise ValueError("PostgreSQL connection requires host, user, password, and database")

        connection_params = {
            "host": self.config.host,
            "port": self.config.port or 5432,
            "database": self.config.database,
            "user": self.config.user,
            "password": self.config.password,
        }

        try:
            import psycopg2

            self.connection = psycopg2.connect(**connection_params)
            self.logger.info(
                f"Connected to PostgreSQL: {self.config.host}:{self.config.port}/{self.config.database}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def disconnect(self) -> None:
        """Close the PostgreSQL connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from PostgreSQL database")

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

    def create_table(
        self, table_name: str, query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a table from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)

        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)

        # Extract schema and table name
        if "." in table_name:
            schema_name, _ = table_name.split(".", 1)
            # Create schema if it doesn't exist
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.connection.commit()
                cursor.close()
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")

        # Wrap the query in a CREATE TABLE statement
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} AS {converted_query}"

        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            self.connection.commit()
            cursor.close()
            self.logger.info(f"Created table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def create_view(
        self, view_name: str, query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a view from a qualified SQL query."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)

        # Qualify table references if schema is specified
        if self.config.schema:
            converted_query = self.qualify_table_references(converted_query, self.config.schema)

        # Extract schema and view name
        if "." in view_name:
            schema_name, _ = view_name.split(".", 1)
            # Create schema if it doesn't exist
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.connection.commit()
                cursor.close()
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")

        # Wrap the query in a CREATE VIEW statement
        create_query = f"CREATE OR REPLACE VIEW {view_name} AS {converted_query}"

        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            self.connection.commit()
            cursor.close()
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
        if "." in view_name:
            schema_name, _ = view_name.split(".", 1)
            # Create schema if it doesn't exist
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                self.connection.commit()
                cursor.close()
                self.logger.debug(f"Created schema: {schema_name}")
            except Exception as e:
                self.logger.warning(f"Could not create schema {schema_name}: {e}")

        # PostgreSQL materialized view syntax
        create_query = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS {converted_query}"

        try:
            cursor = self.connection.cursor()
            cursor.execute(create_query)
            self.connection.commit()
            cursor.close()
            self.logger.info(f"Created materialized view: {view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create materialized view {view_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                [table_name],
            )
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else False
        except Exception:
            return False

    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()
            cursor.close()
            self.logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            raise

    def get_table_info(self, table_name: str) -> dict[str, Any]:
        """Get information about a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            cursor = self.connection.cursor()

            # Get table schema
            cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                [table_name],
            )
            schema_result = cursor.fetchall()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cursor.fetchone()

            cursor.close()

            return {
                "schema": [{"column": row[0], "type": row[1]} for row in schema_result],
                "row_count": count_result[0] if count_result else 0,
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise


# Register the adapter
register_adapter("postgresql", PostgreSQLAdapter)
