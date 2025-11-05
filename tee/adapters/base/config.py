"""
Configuration types and structures for database adapters.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional


class MaterializationType(Enum):
    """Supported materialization types across databases."""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    EXTERNAL_TABLE = "external_table"
    INCREMENTAL = "incremental"


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
