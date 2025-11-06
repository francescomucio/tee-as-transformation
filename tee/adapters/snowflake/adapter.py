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
    REQUIRED_FIELDS = ["type", "user", "password", "database"]

    def __init__(self, config_dict: Dict[str, Any]):
        if snowflake is None:
            raise ImportError(
                "Snowflake connector is not installed. Install it with: uv add snowflake-connector-python"
            )

        super().__init__(config_dict)

    def _validate_field_values(self, config_dict: Dict[str, Any]) -> None:
        """Validate Snowflake-specific field values."""
        super()._validate_field_values(config_dict)

        # Validate account format
        if "account" in config_dict:
            account = config_dict["account"]
            if not isinstance(account, str) or not account.strip():
                raise ValueError("Snowflake account must be a non-empty string")

        # Validate warehouse if provided
        if "warehouse" in config_dict and config_dict["warehouse"]:
            if (
                not isinstance(config_dict["warehouse"], str)
                or not config_dict["warehouse"].strip()
            ):
                raise ValueError("Snowflake warehouse must be a non-empty string")

        # Validate role if provided
        if "role" in config_dict and config_dict["role"]:
            if not isinstance(config_dict["role"], str) or not config_dict["role"].strip():
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
            type=config_dict["type"],
            host=config_dict.get("host"),
            port=config_dict.get("port"),
            database=config_dict.get("database"),
            user=config_dict.get("user"),
            password=config_dict.get("password"),
            path=config_dict.get("path"),
            source_dialect=config_dict.get("source_dialect"),
            target_dialect=config_dict.get("target_dialect"),
            connection_timeout=config_dict.get("connection_timeout", 30),
            query_timeout=config_dict.get("query_timeout", 300),
            schema=config_dict.get("schema"),
            warehouse=config_dict.get("warehouse"),
            role=config_dict.get("role"),
            project=config_dict.get("project"),
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
            MaterializationType.INCREMENTAL,
        ]

    def connect(self) -> None:
        """Establish connection to Snowflake database."""
        if not all(
            [self.config.host, self.config.user, self.config.password, self.config.database]
        ):
            raise ValueError("Snowflake connection requires host, user, password, and database")

        # Get account from extra fields or extract from host
        account = self.config.host
        if self.config.extra and "account" in self.config.extra:
            account = self.config.extra["account"]
        else:
            # Extract account from host (e.g., "IZOMIWY-AM07852.snowflakecomputing.com" -> "IZOMIWY-AM07852")
            if self.config.host and ".snowflakecomputing.com" in self.config.host:
                account = self.config.host.replace(".snowflakecomputing.com", "")

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

    def create_table(
        self, table_name: str, query: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a table from a qualified SQL query with optional column metadata."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        # Create schema if needed (schema tags will be handled by execution engine)
        self._create_schema_if_needed(table_name)

        # Use 3-part naming for the table (DATABASE.SCHEMA.TABLE)
        qualified_table_name = self._qualify_object_name(table_name)
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
                    table_description = metadata.get("description")
                    if table_description:
                        self._add_table_comment(qualified_table_name, table_description)

                    # Add column comments
                    column_descriptions = self._validate_column_metadata(metadata)
                    if column_descriptions:
                        self._add_column_comments(qualified_table_name, column_descriptions)

                    # Add tags (dbt-style, list of strings) if present
                    tags = metadata.get("tags", [])
                    if tags:
                        self.attach_tags("TABLE", qualified_table_name, tags)

                    # Add object_tags (database-style, key-value pairs) if present
                    object_tags = metadata.get("object_tags", {})
                    if object_tags and isinstance(object_tags, dict):
                        self.attach_object_tags("TABLE", qualified_table_name, object_tags)
                except ValueError as e:
                    self.logger.error(f"Invalid metadata for table {table_name}: {e}")
                    raise
                except Exception as e:
                    self.logger.warning(f"Could not add comments/tags for table {table_name}: {e}")
                    # Don't raise here - table creation succeeded, comments/tags are optional

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

        # Use 3-part naming for the view (DATABASE.SCHEMA.VIEW)
        qualified_view_name = self._qualify_object_name(view_name)

        # Build the CREATE VIEW statement with inline comments if metadata is provided
        if metadata and (
            ("schema" in metadata and metadata["schema"])
            or ("description" in metadata and metadata["description"])
        ):
            create_query = self._build_view_with_column_comments(
                qualified_view_name, query, metadata
            )
        else:
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

            # Add tags if metadata is provided
            if metadata:
                # Add tags (dbt-style, list of strings) if present
                tags = metadata.get("tags", [])
                if tags:
                    try:
                        self.attach_tags("VIEW", qualified_view_name, tags)
                    except Exception as e:
                        self.logger.warning(f"Could not add tags for view {view_name}: {e}")
                        # Don't raise here - view creation succeeded, tags are optional

                # Add object_tags (database-style, key-value pairs) if present
                object_tags = metadata.get("object_tags", {})
                if object_tags and isinstance(object_tags, dict):
                    try:
                        self.attach_object_tags("VIEW", qualified_view_name, object_tags)
                    except Exception as e:
                        self.logger.warning(f"Could not add object_tags for view {view_name}: {e}")
                        # Don't raise here - view creation succeeded, tags are optional

            # Note: View and column comments are now included inline during creation

        except Exception as e:
            self.logger.error(f"Failed to create view {view_name}: {e}")
            raise

    def _build_view_with_column_comments(
        self, qualified_view_name: str, query: str, metadata: Dict[str, Any]
    ) -> str:
        """
        Build a CREATE VIEW statement with inline column comments and view comment for Snowflake.

        Snowflake supports both view comments and column comments inline during view creation:
        CREATE OR REPLACE VIEW schema.view_name COMMENT='view comment' (
            column1 COMMENT 'comment1',
            column2 COMMENT 'comment2'
        ) AS SELECT ...

        Args:
            qualified_view_name: Fully qualified view name (DATABASE.SCHEMA.VIEW)
            query: The SELECT query for the view
            metadata: Metadata containing schema information

        Returns:
            Complete CREATE VIEW statement with inline column comments and view comment
        """
        try:
            # Extract view description from metadata
            view_description = metadata.get("description", "")
            escaped_view_description = (
                view_description.replace("'", "''") if view_description else ""
            )

            # Extract column descriptions from metadata
            column_descriptions = self._validate_column_metadata(metadata)

            if not column_descriptions and not escaped_view_description:
                # No descriptions available, use simple CREATE VIEW
                return f"CREATE OR REPLACE VIEW {qualified_view_name} AS {query}"

            # Build the view comment part
            view_comment_part = (
                f" COMMENT='{escaped_view_description}'" if escaped_view_description else ""
            )

            if not column_descriptions:
                # Only view comment, no column comments
                create_query = (
                    f"CREATE OR REPLACE VIEW {qualified_view_name}{view_comment_part} AS {query}"
                )
            else:
                # Both view comment and column comments
                column_list = []
                for col_name, description in column_descriptions.items():
                    # Escape single quotes in description
                    escaped_description = description.replace("'", "''")
                    column_list.append(f"{col_name} COMMENT '{escaped_description}'")

                # Create the view with both view comment and inline column comments
                column_spec = ",\n    ".join(column_list)
                create_query = f"""CREATE OR REPLACE VIEW {qualified_view_name}{view_comment_part} (
    {column_spec}
) AS {query}"""

            self.logger.debug(f"Built view with inline comments: {create_query}")
            return create_query

        except Exception as e:
            self.logger.warning(f"Failed to build view with comments: {e}")
            # Fall back to simple CREATE VIEW without comments
            return f"CREATE OR REPLACE VIEW {qualified_view_name} AS {query}"

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            # Snowflake connector uses %s parameter style
            result = self._execute_with_cursor(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                (table_name,),
            )
            return result[0][0] > 0
        except Exception:
            return False

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            # Use 3-part naming for Snowflake (DATABASE.SCHEMA.TABLE)
            qualified_table_name = self._qualify_object_name(table_name)

            # Get table schema - extract just the table name for the query
            if "." in table_name:
                _, table_name_only = table_name.split(".", 1)
            else:
                table_name_only = table_name

            schema_result = self._execute_with_cursor(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name_only,),
            )

            # Get row count
            count_result = self._execute_with_cursor(f"SELECT COUNT(*) FROM {qualified_table_name}")

            return {
                "schema": [{"column": row[0], "type": row[1]} for row in schema_result],
                "row_count": count_result[0][0] if count_result else 0,
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise

    def get_table_columns(self, table_name: str) -> List[str]:
        """Return ordered column names for a table."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        try:
            # Extract schema and table name for information_schema lookup
            if "." in table_name:
                parts = table_name.split(".", 1)
                if len(parts) == 2:
                    schema_name, table_name_only = parts
                else:
                    schema_name = self.config.schema or "PUBLIC"
                    table_name_only = parts[0]
            else:
                schema_name = self.config.schema or "PUBLIC"
                table_name_only = table_name
            
            rows = self._execute_with_cursor(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = UPPER(%s) AND table_name = UPPER(%s)
                ORDER BY ordinal_position
                """,
                (schema_name, table_name_only),
            )
            return [r[0] for r in rows]
        except Exception as e:
            self.logger.error(f"Error getting columns for table {table_name}: {e}")
            # Fallback to empty list; callers should handle
            return []

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
                if not table.name.startswith(schema + "."):
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
                # Get Snowflake version
                version_result = self._execute_with_cursor("SELECT CURRENT_VERSION()")
                base_info["snowflake_version"] = (
                    version_result[0][0] if version_result else "unknown"
                )

                # Get current warehouse
                warehouse_result = self._execute_with_cursor("SELECT CURRENT_WAREHOUSE()")
                base_info["current_warehouse"] = (
                    warehouse_result[0][0] if warehouse_result else "unknown"
                )

                # Get current role
                role_result = self._execute_with_cursor("SELECT CURRENT_ROLE()")
                base_info["current_role"] = role_result[0][0] if role_result else "unknown"

            except Exception as e:
                self.logger.warning(f"Could not get Snowflake-specific info: {e}")

        return base_info

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

    def _qualify_object_name(self, object_name: str) -> str:
        """Return DATABASE-prefixed name if not already fully qualified."""
        database_name = self.config.database
        if "." in object_name:
            return f"{database_name}.{object_name}"
        return f"{database_name}.{object_name}"

    def _create_schema_if_needed(
        self, object_name: str, schema_metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Create schema if needed for the given object name and attach tags if provided.

        Args:
            object_name: Object name (e.g., "schema.table")
            schema_metadata: Optional metadata containing tags and object_tags for the schema
        """
        if "." in object_name:
            schema_name, _ = object_name.split(".", 1)
            database_name = self.config.database
            qualified_schema_name = f"{database_name}.{schema_name}"
            
            try:
                cursor = self.connection.cursor()
                # Check if schema exists
                cursor.execute(
                    f"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '{schema_name}'"
                )
                schema_exists = cursor.fetchone()[0] > 0
                
                if not schema_exists:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name}")
                    self.logger.debug(f"Created schema: {qualified_schema_name}")
                else:
                    self.logger.debug(f"Schema already exists: {qualified_schema_name}")
                
                # Attach tags if schema was just created or if metadata is provided
                if schema_metadata and (not schema_exists or schema_metadata.get("force_tag_update", False)):
                    try:
                        # Add tags (dbt-style, list of strings) if present
                        tags = schema_metadata.get("tags", [])
                        if tags:
                            self.attach_tags("SCHEMA", qualified_schema_name, tags)
                        
                        # Add object_tags (database-style, key-value pairs) if present
                        object_tags = schema_metadata.get("object_tags", {})
                        if object_tags and isinstance(object_tags, dict):
                            self.attach_object_tags("SCHEMA", qualified_schema_name, object_tags)
                    except Exception as e:
                        self.logger.warning(
                            f"Could not add tags to schema {qualified_schema_name}: {e}"
                        )
                        # Don't raise - schema creation succeeded, tags are optional
                
                cursor.close()
            except Exception as e:
                self.logger.warning(f"Could not create schema {qualified_schema_name}: {e}")

    def _execute_with_cursor(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query with proper cursor management."""
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    def execute_incremental_append(self, table_name: str, sql_query: str) -> None:
        """Execute incremental append into an existing table, or create if missing."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        cursor = self.connection.cursor()
        try:
            qualified_table = self._qualify_object_name(table_name)
            if not self.table_exists(table_name):
                # First run: create table from filtered select
                create_sql = f"CREATE OR REPLACE TABLE {qualified_table} AS {sql_query}"
                cursor.execute(create_sql)
                self.logger.info(f"Created table (append first run): {table_name}")
                return
            # Subsequent runs: insert aligned columns
            columns = self.get_table_columns(table_name)
            if columns:
                column_list = ", ".join(columns)
                insert_sql = f"INSERT INTO {qualified_table} ({column_list}) SELECT {column_list} FROM ({sql_query})"
            else:
                # Fallback without explicit columns
                insert_sql = f"INSERT INTO {qualified_table} {sql_query}"
            cursor.execute(insert_sql)
            self.logger.info(f"Executed incremental append for table: {table_name}")
        except Exception as e:
            self.logger.error(f"Error executing incremental append for {table_name}: {e}")
            raise
        finally:
            cursor.close()

    def _generate_merge_sql(
        self,
        table_name: str,
        source_sql: str,
        unique_key: List[str],
        all_columns: List[str],
        time_column: Optional[str],
    ) -> str:
        """Generate Snowflake MERGE SQL with tuple ON and optional dedup by latest time_column."""
        qualified_table = self._qualify_object_name(table_name)
        # Deduplicate by unique key picking latest by time_column if provided
        if time_column:
            partition_keys = ", ".join(unique_key)
            dedup_cte = (
                "WITH src AS (" + source_sql + "), dedup AS ("
                f"SELECT * FROM src QUALIFY ROW_NUMBER() OVER (PARTITION BY ({partition_keys}) ORDER BY {time_column} DESC) = 1)"
            )
            using_alias = "dedup"
        else:
            dedup_cte = "WITH dedup AS (" + source_sql + ")"
            using_alias = "dedup"
        tuple_left = ", ".join([f"t.{k}" for k in unique_key])
        tuple_right = ", ".join([f"s.{k}" for k in unique_key])
        # Update set excludes keys
        update_cols = [c for c in all_columns if c not in unique_key]
        update_set = (
            ", ".join([f"{c} = s.{c}" for c in update_cols])
            if update_cols
            else f"{unique_key[0]} = s.{unique_key[0]}"
        )
        insert_cols = ", ".join(all_columns)
        insert_vals = ", ".join([f"s.{c}" for c in all_columns])
        merge_sql = (
            f"{dedup_cte} \n"
            f"MERGE INTO {qualified_table} t USING {using_alias} s ON ({tuple_left}) = ({tuple_right}) \n"
            f"WHEN MATCHED THEN UPDATE SET {update_set} \n"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        return merge_sql

    def execute_incremental_merge(
        self, table_name: str, source_sql: str, config: Dict[str, Any]
    ) -> None:
        """Execute incremental merge (upsert) with dedup and tuple ON for composite keys."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        unique_key = config.get("unique_key")
        if not unique_key:
            raise ValueError("unique_key is required for incremental merge")
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        time_column = config.get("time_column")
        columns = self.get_table_columns(table_name)
        if not columns:
            # As a fallback, attempt executing using select * mapping
            self.logger.warning(
                "Could not resolve table columns; proceeding with '*' mapping may fail if order mismatches"
            )
            # We still build merge but it may not be correct; raise to be explicit
            raise ValueError("Cannot resolve table columns for merge")
        merge_sql = self._generate_merge_sql(
            table_name, source_sql, unique_key, columns, time_column
        )
        cursor = self.connection.cursor()
        try:
            cursor.execute(merge_sql)
            self.logger.info("Executed incremental merge")
        except Exception as e:
            self.logger.error(f"Error executing incremental merge for {table_name}: {e}")
            raise
        finally:
            cursor.close()

    def execute_incremental_delete_insert(
        self, table_name: str, delete_sql: str, insert_sql: str
    ) -> None:
        """Execute delete+insert atomically in a transaction, aligning columns."""
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        cursor = self.connection.cursor()
        try:
            qualified_table = self._qualify_object_name(table_name)
            columns = self.get_table_columns(table_name)
            if columns:
                column_list = ", ".join(columns)
                aligned_insert = f"INSERT INTO {qualified_table} ({column_list}) SELECT {column_list} FROM ({insert_sql})"
            else:
                aligned_insert = f"INSERT INTO {qualified_table} {insert_sql}"
            # Begin transaction
            cursor.execute("BEGIN")
            cursor.execute(delete_sql)
            cursor.execute(aligned_insert)
            cursor.execute("COMMIT")
            self.logger.info(f"Executed incremental delete+insert for table: {table_name}")
        except Exception as e:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            self.logger.error(f"Error executing incremental delete+insert for {table_name}: {e}")
            raise
        finally:
            cursor.close()

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
                    self.logger.warning(
                        f"Could not add comment to column {table_name}.{col_name}: {e}"
                    )
                    # Continue with other columns even if one fails
        finally:
            cursor.close()

    def attach_tags(
        self, object_type: str, object_name: str, tags: List[str]
    ) -> None:
        """
        Attach tags to a Snowflake database object.

        Snowflake supports tags on tables, views, and other objects using:
        ALTER TABLE/VIEW object_name SET TAG tag_name = 'tag_value'

        For simple string tags, we create/use a generic 'tag' tag and set values.

        Args:
            object_type: Type of object ('TABLE', 'VIEW', etc.)
            object_name: Fully qualified object name (DATABASE.SCHEMA.OBJECT)
            tags: List of tag strings to attach
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not tags:
            return

        cursor = self.connection.cursor()
        try:
            # For each tag, we'll use Snowflake's tag system
            # Snowflake tags work as key-value pairs, so we'll create a generic 'tag' tag
            # and set multiple values, or create individual tags for each value
            
            # Strategy: Create/use a generic 'tee_tags' tag and set comma-separated values
            # OR create individual tags for each tag value
            # We'll use the simpler approach: create a 'tee_tag' tag for each unique tag value
            
            for tag_value in tags:
                if not tag_value or not isinstance(tag_value, str) or not tag_value.strip():
                    continue
                
                # Sanitize tag name (Snowflake tag names must be valid identifiers)
                # Use a prefix to avoid conflicts
                sanitized_tag = f"tee_tag_{tag_value.replace(' ', '_').replace('-', '_').lower()}"
                # Truncate if too long (Snowflake has limits)
                if len(sanitized_tag) > 128:
                    sanitized_tag = sanitized_tag[:128]
                
                try:
                    # Create tag if it doesn't exist (ignore error if it exists)
                    try:
                        create_tag_sql = f"CREATE TAG IF NOT EXISTS {sanitized_tag}"
                        cursor.execute(create_tag_sql)
                        self.logger.debug(f"Created tag: {sanitized_tag}")
                    except Exception:
                        # Tag might already exist, continue
                        pass
                    
                    # Attach tag to object
                    # Snowflake syntax: ALTER TABLE/VIEW object SET TAG tag_name = 'value'
                    alter_sql = f"ALTER {object_type} {object_name} SET TAG {sanitized_tag} = '{tag_value.replace("'", "''")}'"
                    cursor.execute(alter_sql)
                    self.logger.debug(f"Attached tag {sanitized_tag}='{tag_value}' to {object_type} {object_name}")
                    
                except Exception as e:
                    self.logger.warning(
                        f"Could not attach tag '{tag_value}' to {object_type} {object_name}: {e}"
                    )
                    # Continue with other tags even if one fails
                    continue

            self.logger.info(f"Attached {len(tags)} tag(s) to {object_type} {object_name}")

        except Exception as e:
            self.logger.warning(f"Error attaching tags to {object_type} {object_name}: {e}")
            # Don't raise - tag attachment is optional
        finally:
            cursor.close()

    def attach_object_tags(
        self, object_type: str, object_name: str, object_tags: Dict[str, str]
    ) -> None:
        """
        Attach object tags (key-value pairs) to a Snowflake database object.

        This method handles database-style tags where each tag is a key-value pair,
        like {"sensitivity_tag": "pii", "classification": "public"}.

        Snowflake syntax: ALTER TABLE/VIEW object_name SET TAG tag_name = 'tag_value'

        Args:
            object_type: Type of object ('TABLE', 'VIEW', etc.)
            object_name: Fully qualified object name (DATABASE.SCHEMA.OBJECT)
            object_tags: Dictionary of tag key-value pairs
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not object_tags or not isinstance(object_tags, dict):
            return

        cursor = self.connection.cursor()
        try:
            for tag_key, tag_value in object_tags.items():
                if not tag_key or not isinstance(tag_key, str):
                    continue
                if tag_value is None:
                    continue

                # Sanitize tag key (Snowflake tag names must be valid identifiers)
                sanitized_tag_key = tag_key.replace(" ", "_").replace("-", "_")
                # Truncate if too long (Snowflake has limits)
                if len(sanitized_tag_key) > 128:
                    sanitized_tag_key = sanitized_tag_key[:128]

                # Convert tag value to string
                tag_value_str = str(tag_value)

                try:
                    # Create tag if it doesn't exist (ignore error if it exists)
                    try:
                        create_tag_sql = f"CREATE TAG IF NOT EXISTS {sanitized_tag_key}"
                        cursor.execute(create_tag_sql)
                        self.logger.debug(f"Created tag: {sanitized_tag_key}")
                    except Exception:
                        # Tag might already exist, continue
                        pass

                    # Attach tag to object
                    # Snowflake syntax: ALTER TABLE/VIEW object SET TAG tag_name = 'value'
                    escaped_value = tag_value_str.replace("'", "''")
                    alter_sql = f"ALTER {object_type} {object_name} SET TAG {sanitized_tag_key} = '{escaped_value}'"
                    cursor.execute(alter_sql)
                    self.logger.debug(
                        f"Attached object tag {sanitized_tag_key}='{tag_value_str}' to {object_type} {object_name}"
                    )

                except Exception as e:
                    self.logger.warning(
                        f"Could not attach object tag '{tag_key}'='{tag_value}' to {object_type} {object_name}: {e}"
                    )
                    # Continue with other tags even if one fails
                    continue

            self.logger.info(
                f"Attached {len(object_tags)} object tag(s) to {object_type} {object_name}"
            )

        except Exception as e:
            self.logger.warning(
                f"Error attaching object tags to {object_type} {object_name}: {e}"
            )
            # Don't raise - tag attachment is optional
        finally:
            cursor.close()

    def generate_no_duplicates_test_query(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> str:
        """
        Generate SQL query for no_duplicates test (Snowflake-specific).

        Snowflake does NOT support GROUP BY *, so we must use explicit column names.
        If columns are not provided, we fetch them from the table.
        """
        # If columns provided, use them
        if columns and len(columns) > 0:
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

        # Snowflake doesn't support GROUP BY *, so we must get columns
        # Try to get columns from the table
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        table_columns = self.get_table_columns(table_name)
        if not table_columns or len(table_columns) == 0:
            # If we can't get columns, raise an error with helpful message
            raise ValueError(
                f"Cannot generate no_duplicates test query for {table_name}: "
                "Unable to retrieve column names. Snowflake requires explicit column names for GROUP BY."
            )
        
        # Use the retrieved columns
        column_list = ", ".join(table_columns)
        return f"""
            SELECT COUNT(*) 
            FROM (
                SELECT {column_list}, COUNT(*) as row_count
                FROM {table_name}
                GROUP BY {column_list}
                HAVING COUNT(*) > 1
            ) AS duplicate_groups
        """


# Register the adapter
register_adapter("snowflake", SnowflakeAdapter)
