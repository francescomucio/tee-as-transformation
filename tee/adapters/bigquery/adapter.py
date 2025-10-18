"""
BigQuery adapter implementation.

This module provides BigQuery-specific functionality including:
- SQL dialect conversion
- BigQuery-specific optimizations
- Connection management with service account authentication
- Materialization support including external tables
"""

from typing import Any
from ..base import DatabaseAdapter, AdapterConfig, MaterializationType
from ..registry import register_adapter


class BigQueryAdapter(DatabaseAdapter):
    """BigQuery database adapter with SQLglot integration."""
    
    def __init__(self, config: AdapterConfig):
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError("google-cloud-bigquery is not installed. Install it with: uv add google-cloud-bigquery")
        
        super().__init__(config)
    
    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for BigQuery."""
        return "bigquery"
    
    def get_supported_materializations(self) -> list[MaterializationType]:
        """Get list of supported materialization types for BigQuery."""
        return [
            MaterializationType.TABLE,
            MaterializationType.VIEW,
            MaterializationType.MATERIALIZED_VIEW,
            MaterializationType.EXTERNAL_TABLE,
        ]
    
    def connect(self) -> None:
        """Establish connection to BigQuery."""
        if not all([self.config.project, self.config.database]):
            raise ValueError("BigQuery connection requires project and database")
        
        try:
            from google.cloud import bigquery
            
            # Initialize BigQuery client
            if self.config.extra and "service_account_path" in self.config.extra:
                # Use service account file
                self.client = bigquery.Client.from_service_account_json(
                    self.config.extra["service_account_path"],
                    project=self.config.project
                )
            else:
                # Use default credentials
                self.client = bigquery.Client(project=self.config.project)
            
            self.connection = self.client  # For compatibility
            self.logger.info(f"Connected to BigQuery: {self.config.project}/{self.config.database}")
        except Exception as e:
            self.logger.error(f"Failed to connect to BigQuery: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close the BigQuery connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.connection = None
            self.logger.info("Disconnected from BigQuery")
    
    def execute_query(self, query: str) -> Any:
        """Execute a SQL query and return results."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            # Convert SQL if needed
            converted_query = self.convert_sql_dialect(query)
            
            # Qualify table references if dataset is specified
            if self.config.database:
                converted_query = self.qualify_table_references(converted_query, self.config.database)
            
            # Execute query
            query_job = self.client.query(converted_query)
            result = query_job.result()
            
            # Convert to list of tuples for compatibility
            rows = []
            for row in result:
                rows.append(tuple(row.values()))
            
            self.logger.debug(f"Executed query: {converted_query[:100]}...")
            return rows
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def create_table(self, table_name: str, query: str) -> None:
        """Create a table from a qualified SQL query."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if dataset is specified
        if self.config.database:
            converted_query = self.qualify_table_references(converted_query, self.config.database)
        
        # Create fully qualified table name
        if '.' not in table_name and self.config.database:
            full_table_name = f"{self.config.project}.{self.config.database}.{table_name}"
        else:
            full_table_name = table_name
        
        # Wrap the query in a CREATE TABLE statement
        create_query = f"CREATE OR REPLACE TABLE `{full_table_name}` AS {converted_query}"
        
        try:
            query_job = self.client.query(create_query)
            query_job.result()  # Wait for completion
            self.logger.info(f"Created table: {full_table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create table {full_table_name}: {e}")
            raise
    
    def create_view(self, view_name: str, query: str) -> None:
        """Create a view from a qualified SQL query."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if dataset is specified
        if self.config.database:
            converted_query = self.qualify_table_references(converted_query, self.config.database)
        
        # Create fully qualified view name
        if '.' not in view_name and self.config.database:
            full_view_name = f"{self.config.project}.{self.config.database}.{view_name}"
        else:
            full_view_name = view_name
        
        # Wrap the query in a CREATE VIEW statement
        create_query = f"CREATE OR REPLACE VIEW `{full_view_name}` AS {converted_query}"
        
        try:
            query_job = self.client.query(create_query)
            query_job.result()  # Wait for completion
            self.logger.info(f"Created view: {full_view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create view {full_view_name}: {e}")
            raise
    
    def create_materialized_view(self, view_name: str, query: str) -> None:
        """Create a materialized view from a qualified SQL query."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if dataset is specified
        if self.config.database:
            converted_query = self.qualify_table_references(converted_query, self.config.database)
        
        # Create fully qualified view name
        if '.' not in view_name and self.config.database:
            full_view_name = f"{self.config.project}.{self.config.database}.{view_name}"
        else:
            full_view_name = view_name
        
        # BigQuery materialized view syntax
        create_query = f"CREATE MATERIALIZED VIEW `{full_view_name}` AS {converted_query}"
        
        try:
            query_job = self.client.query(create_query)
            query_job.result()  # Wait for completion
            self.logger.info(f"Created materialized view: {full_view_name}")
        except Exception as e:
            self.logger.error(f"Failed to create materialized view {full_view_name}: {e}")
            raise
    
    def create_external_table(self, table_name: str, query: str, external_location: str) -> None:
        """Create an external table from a qualified SQL query."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        # Convert SQL if needed
        converted_query = self.convert_sql_dialect(query)
        
        # Qualify table references if dataset is specified
        if self.config.database:
            converted_query = self.qualify_table_references(converted_query, self.config.database)
        
        # Create fully qualified table name
        if '.' not in table_name and self.config.database:
            full_table_name = f"{self.config.project}.{self.config.database}.{table_name}"
        else:
            full_table_name = table_name
        
        # BigQuery external table syntax (simplified - would need proper column definitions)
        create_query = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{full_table_name}`
        OPTIONS (
            format = 'PARQUET',
            uris = ['{external_location}']
        )
        """
        
        try:
            query_job = self.client.query(create_query)
            query_job.result()  # Wait for completion
            self.logger.info(f"Created external table: {full_table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create external table {full_table_name}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            # Create fully qualified table name
            if '.' not in table_name and self.config.database:
                full_table_name = f"{self.config.project}.{self.config.database}.{table_name}"
            else:
                full_table_name = table_name
            
            table_ref = self.client.get_table(full_table_name)
            return table_ref is not None
        except Exception:
            return False
    
    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            # Create fully qualified table name
            if '.' not in table_name and self.config.database:
                full_table_name = f"{self.config.project}.{self.config.database}.{table_name}"
            else:
                full_table_name = table_name
            
            self.client.delete_table(full_table_name)
            self.logger.info(f"Dropped table: {full_table_name}")
        except Exception as e:
            self.logger.error(f"Error dropping table {table_name}: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> dict[str, Any]:
        """Get information about a table."""
        if not self.client:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        try:
            # Create fully qualified table name
            if '.' not in table_name and self.config.database:
                full_table_name = f"{self.config.project}.{self.config.database}.{table_name}"
            else:
                full_table_name = table_name
            
            table = self.client.get_table(full_table_name)
            
            # Get schema information
            schema = []
            for field in table.schema:
                schema.append({
                    "column": field.name,
                    "type": field.field_type
                })
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM `{full_table_name}`"
            query_job = self.client.query(count_query)
            result = query_job.result()
            row_count = next(result)[0]
            
            return {
                "schema": schema,
                "row_count": row_count
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    def validate_connection_string(self, connection_string: str) -> bool:
        """Validate BigQuery connection string format."""
        if not connection_string or not connection_string.strip():
            return False
        
        # BigQuery connection strings should be in format:
        # bigquery://project/dataset
        return connection_string.startswith("bigquery://")


# Register the adapter
register_adapter("bigquery", BigQueryAdapter)