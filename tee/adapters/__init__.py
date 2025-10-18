"""
Database adapters for the TEE execution engine.

This package provides pluggable database adapters that handle:
- Database-specific connection management
- SQL dialect conversion using SQLglot
- Database-specific optimizations and features
- Connection string validation

Each adapter is organized in its own subpackage for better maintainability.
"""

from .base import DatabaseAdapter, AdapterConfig, MaterializationType
from .registry import AdapterRegistry, get_adapter, register_adapter, list_available_adapters, is_adapter_supported
from .testing import AdapterTester, test_adapter, benchmark_adapter

# Import adapters to register them
from .duckdb import DuckDBAdapter
from .snowflake import SnowflakeAdapter
from .postgresql import PostgreSQLAdapter
from .bigquery import BigQueryAdapter

__all__ = [
    # Base classes and configuration
    "DatabaseAdapter",
    "AdapterConfig", 
    "MaterializationType",
    
    # Registry and factory functions
    "AdapterRegistry",
    "get_adapter",
    "register_adapter",
    "list_available_adapters",
    "is_adapter_supported",
    
    # Testing utilities
    "AdapterTester",
    "test_adapter",
    "benchmark_adapter",
    
    # Available adapters
    "DuckDBAdapter",
    "SnowflakeAdapter",
    "PostgreSQLAdapter",
    "BigQueryAdapter",
]
