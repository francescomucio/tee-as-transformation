# Configuration

TEE supports flexible configuration through `pyproject.toml` files and environment variables. This guide covers all configuration options.

## Configuration Methods

### 1. pyproject.toml (Recommended)

Add configuration to your project's `pyproject.toml`:

```toml
[tool.tee.database]
type = "duckdb"
path = "data/my_project.db"
source_dialect = "postgresql"
```

### 2. Environment Variables

Set environment variables for configuration:

```bash
export TEE_DB_TYPE=duckdb
export TEE_DB_PATH=data/my_project.db
export TEE_DB_SOURCE_DIALECT=postgresql
```

### 3. Programmatic Configuration

Create configuration objects in code:

```python
from tee.adapters import AdapterConfig

config = AdapterConfig(
    type="duckdb",
    path="data/my_project.db",
    source_dialect="postgresql"
)
```

## Database Configurations

### DuckDB

```toml
[tool.tee.database]
type = "duckdb"
path = "data/my_project.db"  # or ":memory:" for in-memory
source_dialect = "postgresql"
```

### Snowflake

```toml
[tool.tee.database]
type = "snowflake"
host = "account.snowflakecomputing.com"
user = "username"
password = "password"
database = "database"
warehouse = "warehouse"
role = "role"
source_dialect = "postgresql"
```

### PostgreSQL

```toml
[tool.tee.database]
type = "postgresql"
host = "localhost"
port = 5432
database = "mydb"
user = "username"
password = "password"
source_dialect = "postgresql"
```

## Multiple Database Configurations

Configure multiple databases for different environments:

```toml
[tool.tee.databases]

[tool.tee.databases.dev]
type = "duckdb"
path = "dev.db"
source_dialect = "postgresql"

[tool.tee.databases.staging]
type = "snowflake"
host = "staging.snowflakecomputing.com"
# ... other config

[tool.tee.databases.prod]
type = "snowflake"
host = "prod.snowflakecomputing.com"
# ... other config
```

Use specific configurations:

```python
# Use development database
config = load_database_config("dev")

# Use production database
config = load_database_config("prod")
```

## SQL Dialect Configuration

Configure the source dialect for SQL conversion:

```toml
[tool.tee.database]
source_dialect = "postgresql"  # Write models in PostgreSQL
# Automatically converts to target database dialect
```

Supported source dialects:
- `postgresql`
- `mysql`
- `sqlite`
- `duckdb`
- `snowflake`
- `bigquery`
- And many more via SQLglot

## Advanced Configuration

### Custom Adapter Configuration

```python
from tee.adapters import AdapterConfig

config = AdapterConfig(
    type="snowflake",
    host="account.snowflakecomputing.com",
    user="user",
    password="password",
    database="database",
    warehouse="warehouse",
    source_dialect="postgresql",
    extra={
        "external_location": "s3://bucket/path",
        "file_format": "PARQUET"
    }
)
```

### Environment Variable Overrides

Environment variables can override `pyproject.toml` settings:

```bash
# Override database type
export TEE_DB_TYPE=snowflake

# Override specific Snowflake settings
export TEE_DB_HOST=prod.snowflakecomputing.com
export TEE_DB_WAREHOUSE=PROD_WAREHOUSE
```

## Configuration Validation

TEE validates configuration at startup:

```python
from tee.engine import load_database_config

try:
    config = load_database_config()
    print("Configuration valid!")
except Exception as e:
    print(f"Configuration error: {e}")
```

## Best Practices

1. **Use `pyproject.toml`** for project-specific configuration
2. **Use environment variables** for sensitive data (passwords, keys)
3. **Use multiple configurations** for different environments
4. **Validate configuration** before running models
5. **Document configuration** for team members

## Troubleshooting

### Common Issues

1. **Missing Dependencies**: Install the appropriate database driver
2. **Invalid Configuration**: Check configuration syntax and required fields
3. **Connection Failures**: Verify database credentials and network access
4. **SQL Conversion Errors**: Ensure source dialect is supported

### Debug Configuration

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# This will show detailed configuration loading
config = load_database_config()
```

## Next Steps

- [Database Adapters](user-guide/database-adapters.md) - Learn about adapter capabilities
- [SQL Dialect Conversion](user-guide/sql-dialect-conversion.md) - Understand SQL conversion
- [Examples](user-guide/examples/) - See configuration in action
