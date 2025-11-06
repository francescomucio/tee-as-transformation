# Database Adapters

TEE provides a powerful pluggable database adapter system that allows you to write SQL models in one dialect and run them on different databases with automatic SQL dialect conversion.

## Overview

The adapter system allows you to:
- Write SQL models in one dialect (e.g., PostgreSQL) and run them on different databases (e.g., Snowflake, BigQuery)
- Use database-specific features and optimizations
- Manage multiple database configurations
- Automatically convert SQL using SQLglot

## Quick Start

### 1. Basic Usage

```python
from tee.engine import ModelExecutor, load_database_config

# Load configuration from pyproject.toml or environment variables
config = load_database_config("default")

# Create executor
executor = ModelExecutor("/path/to/project", config)

# Execute models
results = executor.execute_models(parser)
```

### 2. Configuration

Add to your `pyproject.toml`:

```toml
[tool.tee.database]
type = "duckdb"
path = "data/my_project.db"
source_dialect = "postgresql"  # Write models in PostgreSQL, convert to DuckDB
```

Or use environment variables:

```bash
export TEE_DB_TYPE=duckdb
export TEE_DB_PATH=data/my_project.db
export TEE_DB_SOURCE_DIALECT=postgresql
```

## Supported Databases

### DuckDB
- **Dialect**: `duckdb`
- **Features**: Tables, Views, Materialized Views (as tables)
- **Configuration**:
  ```toml
  type = "duckdb"
  path = "database.db"  # or ":memory:"
  ```

### Snowflake
- **Dialect**: `snowflake`
- **Features**: Tables, Views, Materialized Views, External Tables, **Tag Support**
- **Configuration**:
  ```toml
  type = "snowflake"
  host = "account.snowflakecomputing.com"
  user = "username"
  password = "password"
  database = "database"
  warehouse = "warehouse"
  role = "role"
  ```
- **Tag Support**: Full support for both dbt-style tags and database object tags on tables, views, and schemas. See [Tags and Metadata](tags-and-metadata.md) for details.

### PostgreSQL
- **Dialect**: `postgresql`
- **Features**: Tables, Views, Materialized Views
- **Configuration**:
  ```toml
  type = "postgresql"
  host = "localhost"
  port = 5432
  database = "database"
  user = "user"
  password = "password"
  ```

## SQL Dialect Conversion

The system automatically converts SQL between dialects using SQLglot:

```python
# Write models in PostgreSQL dialect
sql = """
SELECT 
    u.id,
    u.name,
    COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2023-01-01'
GROUP BY u.id, u.name
"""

# Automatically converted to DuckDB/Snowflake/etc. when executed
```

### Supported Source Dialects
- PostgreSQL
- MySQL
- SQLite
- DuckDB
- Snowflake
- BigQuery
- And many more via SQLglot

## Materialization Types

Different databases support different materialization types:

| Type | DuckDB | Snowflake | PostgreSQL |
|------|--------|-----------|------------|
| Table | ✅ | ✅ | ✅ |
| View | ✅ | ✅ | ✅ |
| Materialized View | ✅ (as table) | ✅ | ✅ |
| External Table | ❌ | ✅ | ❌ |

## Advanced Usage

### Custom Adapter Configuration

```python
from tee.engine.adapters import AdapterConfig

config = AdapterConfig(
    type="snowflake",
    host="account.snowflakecomputing.com",
    user="user",
    password="password",
    database="database",
    warehouse="warehouse",
    source_dialect="postgresql",
    extra={"external_location": "s3://bucket/path"}
)

executor = ModelExecutor("/path/to/project", config)
```

### Multiple Database Configurations

```toml
[tool.tee.databases]

[tool.tee.databases.dev]
type = "duckdb"
path = "dev.db"
source_dialect = "postgresql"

[tool.tee.databases.prod]
type = "snowflake"
host = "prod.snowflakecomputing.com"
# ... other config
```

```python
# Use specific configuration
config = load_database_config("prod")
executor = ModelExecutor("/path/to/project", config)
```

### Testing Adapters

```python
from tee.engine.adapters.testing import test_adapter
from tee.engine.adapters import get_adapter

# Test adapter
adapter = get_adapter(config)
results = test_adapter(adapter)

print(f"Connection test: {results['connection']['success']}")
print(f"Dialect conversion: {results['dialect_conversion']['success']}")
```

## Creating Custom Adapters

To create a custom adapter:

1. Inherit from `DatabaseAdapter`:

```python
from tee.engine.adapters.base import DatabaseAdapter, MaterializationType

class MyDatabaseAdapter(DatabaseAdapter):
    def get_default_dialect(self):
        return "mydb"
    
    def get_supported_materializations(self):
        return [MaterializationType.TABLE, MaterializationType.VIEW]
    
    def connect(self):
        # Implementation
        pass
    
    # ... implement other required methods
```

2. Register the adapter:

```python
from tee.engine.adapters.registry import register_adapter

register_adapter("mydb", MyDatabaseAdapter)
```

## Migration from Legacy System

The new system is backward compatible. To migrate:

1. **Gradual Migration**: Use `ModelExecutor` alongside `ModelExecutor`
2. **Configuration**: Move database config to `pyproject.toml`
3. **Features**: Take advantage of dialect conversion and new features

```python
# Old way
from tee.engine import ModelExecutor
executor = ModelExecutor("/path/to/project", {"type": "duckdb"})

# New way
from tee.engine import ModelExecutor, load_database_config
config = load_database_config()
executor = ModelExecutor("/path/to/project", config)
```

## Troubleshooting

### Common Issues

1. **SQL Conversion Errors**: Check if the source dialect is supported
2. **Connection Failures**: Verify configuration and credentials
3. **Materialization Not Supported**: Check adapter capabilities

### Debugging

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Testing Connection

```python
executor = ModelExecutor("/path/to/project", config)
success = executor.test_connection()
print(f"Connection test: {'PASSED' if success else 'FAILED'}")
```

## Performance Considerations

- **Dialect Conversion**: Adds overhead but enables cross-database compatibility
- **Connection Pooling**: Not yet implemented but planned for future versions
- **Query Optimization**: Database-specific optimizations are applied automatically

## Future Enhancements

- Connection pooling
- Query result caching
- Advanced materialization strategies
- Real-time schema validation
- Performance monitoring and metrics
