"""
Base classes for database adapters.

This module defines the abstract base class and configuration structures
for database adapters that handle SQL dialect conversion and database-specific features.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Type
from dataclasses import dataclass
from enum import Enum
import logging
import sqlglot
from sqlglot.dialects import Dialect


class MaterializationType(Enum):
    """Supported materialization types across databases."""
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"


@dataclass
class AdapterConfig:
    """Configuration for database adapters."""
    
    # Database connection
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    path: Optional[str] = None  # For file-based databases like DuckDB
    
    # SQL dialect settings
    source_dialect: Optional[str] = None  # Dialect to convert FROM
    target_dialect: Optional[str] = None  # Dialect to convert TO (usually auto-detected)
    
    # Connection settings
    connection_timeout: int = 30
    query_timeout: int = 300
    
    # Database-specific settings
    schema: Optional[str] = None
    warehouse: Optional[str] = None  # For Snowflake
    role: Optional[str] = None  # For Snowflake
    project: Optional[str] = None  # For BigQuery
    
    # Additional custom settings
    extra: Optional[Dict[str, Any]] = None
    
    def to_connection_string(self) -> str:
        """Convert configuration to connection string format."""
        if self.type.lower() == "duckdb":
            return self.path or ":memory:"
        elif self.type.lower() == "snowflake":
            return f"snowflake://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?warehouse={self.warehouse}&role={self.role}"
        elif self.type.lower() == "postgresql":
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.type.lower() == "bigquery":
            return f"bigquery://{self.project}/{self.database}"
        else:
            # Generic connection string
            return f"{self.type}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseAdapter(ABC):
    """
    Abstract base class for database adapters.
    
    This class defines the interface that all database adapters must implement,
    including SQL dialect conversion, connection management, and database-specific features.
    """
    
    def __init__(self, config: AdapterConfig):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # SQLglot dialect objects
        self.source_dialect = self._get_dialect(config.source_dialect)
        self.target_dialect = self._get_dialect(config.target_dialect or self.get_default_dialect())
        
        # Validate configuration
        self.validate_config()
    
    @abstractmethod
    def get_default_dialect(self) -> str:
        """Get the default SQL dialect for this database."""
        pass
    
    @abstractmethod
    def get_supported_materializations(self) -> List[MaterializationType]:
        """Get list of supported materialization types for this database."""
        pass
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Execute a SQL query and return results."""
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, query: str) -> None:
        """Create a table from a qualified SQL query."""
        pass
    
    @abstractmethod
    def create_view(self, view_name: str, query: str) -> None:
        """Create a view from a qualified SQL query."""
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        pass
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        pass
    
    def validate_connection_string(self, connection_string: str) -> bool:
        """
        Validate a connection string format.
        
        Args:
            connection_string: The connection string to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Default implementation - can be overridden by specific adapters
        return bool(connection_string and connection_string.strip())
    
    def convert_sql_dialect(self, sql: str, source_dialect: Optional[str] = None) -> str:
        """
        Convert SQL from source dialect to target dialect.
        
        Args:
            sql: SQL query to convert
            source_dialect: Source dialect (uses config default if None)
            
        Returns:
            Converted SQL query
        """
        if not sql or not sql.strip():
            return sql
        
        # Use provided source dialect or fall back to config
        source = source_dialect or self.config.source_dialect
        if not source:
            # No conversion needed
            return sql
        
        try:
            # Parse with source dialect
            parsed = sqlglot.parse_one(sql, read=self._get_dialect(source))
            
            # Convert to target dialect
            converted = parsed.sql(dialect=self.target_dialect)
            
            # Log warning if dialects are different
            if source != self.get_default_dialect():
                self.logger.warning(
                    f"Converted SQL from {source} to {self.get_default_dialect()}. "
                    f"Please review the converted query for correctness."
                )
            
            return converted
            
        except Exception as e:
            self.logger.error(f"Failed to convert SQL from {source} to {self.get_default_dialect()}: {e}")
            raise ValueError(f"SQL dialect conversion failed: {e}")
    
    def qualify_table_references(self, sql: str, schema: Optional[str] = None) -> str:
        """
        Qualify table references with schema names.
        
        Args:
            sql: SQL query to qualify
            schema: Schema name to use for qualification
            
        Returns:
            SQL with qualified table references
        """
        if not sql or not sql.strip():
            return sql
        
        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, read=self.target_dialect)
            
            # Use sqlglot's qualify optimizer
            from sqlglot.optimizer.qualify import qualify
            qualified = qualify(parsed, db=schema)
            
            return qualified.sql(dialect=self.target_dialect)
            
        except Exception as e:
            self.logger.warning(f"Failed to qualify table references: {e}")
            return sql
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get information about the current database connection."""
        return {
            "adapter_type": self.__class__.__name__,
            "database_type": self.config.type,
            "source_dialect": self.config.source_dialect,
            "target_dialect": self.get_default_dialect(),
            "is_connected": self.connection is not None,
            "supported_materializations": [m.value for m in self.get_supported_materializations()],
        }
    
    def validate_config(self) -> None:
        """Validate the adapter configuration."""
        if not self.config.type:
            raise ValueError("Database type is required")
        
        # Validate connection string if provided
        if hasattr(self.config, 'connection_string') and self.config.connection_string:
            if not self.validate_connection_string(self.config.connection_string):
                raise ValueError("Invalid connection string format")
    
    def _get_dialect(self, dialect_name: Optional[str]) -> Optional[Dialect]:
        """Get SQLglot dialect object from name."""
        if not dialect_name:
            return None
        
        try:
            return sqlglot.dialects.Dialect.get(dialect_name)
        except Exception:
            self.logger.warning(f"Unknown dialect: {dialect_name}")
            return None
    
    def _log_sql_conversion(self, original_sql: str, converted_sql: str, source_dialect: str) -> None:
        """Log SQL conversion details for debugging."""
        if original_sql != converted_sql:
            self.logger.debug(f"SQL converted from {source_dialect}:")
            self.logger.debug(f"Original: {original_sql[:200]}...")
            self.logger.debug(f"Converted: {converted_sql[:200]}...")
