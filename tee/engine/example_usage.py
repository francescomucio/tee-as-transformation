#!/usr/bin/env python3
"""
Example usage of the enhanced database adapter system.

This script demonstrates how to use the new adapter system with different databases
and SQL dialect conversion.
"""

import logging
import tempfile
from pathlib import Path

from tee.engine import ModelExecutor, load_database_config
from tee.adapters import AdapterConfig, get_adapter
from tee.adapters.testing import test_adapter


def example_basic_usage():
    """Example of basic usage with DuckDB."""
    print("=== Basic Usage Example ===")
    
    # Create a temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        # Configure DuckDB adapter
        config = AdapterConfig(
            type="duckdb",
            path=db_path,
            source_dialect="postgresql"  # Write in PostgreSQL, convert to DuckDB
        )
        
        # Create executor
        executor = ModelExecutor("/path/to/project", config)
        
        # Test connection
        if executor.test_connection():
            print("✅ Database connection successful")
        else:
            print("❌ Database connection failed")
            return
        
        # Get database info
        db_info = executor.get_database_info()
        print(f"Database: {db_info['database_type']}")
        print(f"Adapter: {db_info['adapter_type']}")
        print(f"Dialect conversion: {db_info.get('source_dialect')} -> {db_info.get('target_dialect')}")
        
        # List supported materializations
        materializations = executor.list_supported_materializations()
        print(f"Supported materializations: {materializations}")
        
    finally:
        # Cleanup
        Path(db_path).unlink(missing_ok=True)


def example_sql_conversion():
    """Example of SQL dialect conversion."""
    print("\n=== SQL Dialect Conversion Example ===")
    
    # PostgreSQL SQL
    postgresql_sql = """
    SELECT 
        u.id,
        u.name,
        COUNT(o.id) as order_count,
        EXTRACT(YEAR FROM u.created_at) as signup_year
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.created_at > '2023-01-01'
    GROUP BY u.id, u.name, EXTRACT(YEAR FROM u.created_at)
    ORDER BY order_count DESC
    """
    
    # Create DuckDB adapter
    config = AdapterConfig(
        type="duckdb",
        path=":memory:",
        source_dialect="postgresql"
    )
    
    adapter = get_adapter(config)
    
    try:
        adapter.connect()
        
        # Convert SQL
        converted_sql = adapter.convert_sql_dialect(postgresql_sql)
        
        print("Original PostgreSQL SQL:")
        print(postgresql_sql.strip())
        print("\nConverted DuckDB SQL:")
        print(converted_sql.strip())
        
        # Test the converted SQL
        adapter.execute_query("CREATE TABLE users (id INT, name VARCHAR, created_at TIMESTAMP)")
        adapter.execute_query("CREATE TABLE orders (id INT, user_id INT)")
        adapter.execute_query("INSERT INTO users VALUES (1, 'Alice', '2023-06-01'), (2, 'Bob', '2022-12-01')")
        adapter.execute_query("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)")
        
        result = adapter.execute_query(converted_sql)
        print(f"\nQuery result: {result}")
        
    finally:
        adapter.disconnect()


def example_adapter_testing():
    """Example of testing database adapters."""
    print("\n=== Adapter Testing Example ===")
    
    # Test DuckDB adapter
    config = AdapterConfig(
        type="duckdb",
        path=":memory:",
        source_dialect="postgresql"
    )
    
    adapter = get_adapter(config)
    results = test_adapter(adapter)
    
    print("Test Results:")
    for test_name, result in results.items():
        status = "✅ PASSED" if result.get('success', False) else "❌ FAILED"
        print(f"  {test_name}: {status}")
        if 'message' in result:
            print(f"    {result['message']}")


def example_configuration_loading():
    """Example of loading configuration from different sources."""
    print("\n=== Configuration Loading Example ===")
    
    # Example 1: Direct configuration
    config1 = AdapterConfig(
        type="duckdb",
        path=":memory:",
        source_dialect="postgresql"
    )
    print(f"Direct config: {config1.type} with {config1.source_dialect} -> {config1.target_dialect}")
    
    # Example 2: Loading from pyproject.toml (if it exists)
    try:
        config2 = load_database_config("default")
        print(f"Config from pyproject.toml: {config2.type}")
    except Exception as e:
        print(f"Could not load from pyproject.toml: {e}")
    
    # Example 3: Environment variables
    import os
    os.environ["TEE_DB_TYPE"] = "duckdb"
    os.environ["TEE_DB_PATH"] = ":memory:"
    os.environ["TEE_DB_SOURCE_DIALECT"] = "mysql"
    
    try:
        config3 = load_database_config("default")
        print(f"Config from env vars: {config3.type} with {config3.source_dialect}")
    except Exception as e:
        print(f"Could not load from env vars: {e}")


def main():
    """Run all examples."""
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("Database Adapter System Examples")
    print("=" * 40)
    
    try:
        example_basic_usage()
        example_sql_conversion()
        example_adapter_testing()
        example_configuration_loading()
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
