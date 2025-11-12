# Model API Reference

This document provides API reference for working with models in t4t.

## Model Decorator

### `@model`

Decorator for marking Python functions as SQL models.

**Usage:**

```python
from tee.parser.processing.model import model

@model(
    table_name="users",
    description="User dimension table",
    tags=["analytics", "production"],
    variables=["env", "debug"]
)
def users_model():
    return "SELECT * FROM source.users"
```

**Parameters:**
- `table_name` (str, optional): Custom table name. If not provided, uses function name.
- `description` (str, optional): Model description
- `variables` (list[str], optional): List of variable names to inject into the function's namespace
- `tags` (list[str], optional): dbt-style tags for filtering and organization
- `object_tags` (dict, optional): Database object tags (key-value pairs)
- `**metadata` (Any): Additional metadata to store with the model

**Returns:**
- The decorated function with model metadata stored as `_model_metadata` attribute

**Example with variables:**

```python
@model(
    table_name="users",
    variables=["env", "debug"]
)
def users_model():
    # Variables are injected into the function's namespace
    return f"SELECT * FROM source.users WHERE environment = '{env}'"
```

## Model Factory

### `create_model()`

Dynamically create a model without using a decorator. Useful for creating multiple similar models programmatically.

**Usage:**

```python
from tee.parser.processing.model import create_model

# Simple example
create_model(
    table_name="users",
    sql="SELECT * FROM staging.users",
    description="Select from staging.users"
)

# In a loop for multiple models
STAGING_TABLES = ["users", "orders", "products"]
STAGING_SCHEMA = "staging"

for table_name in STAGING_TABLES:
    create_model(
        table_name=table_name,
        sql=f"SELECT * FROM {STAGING_SCHEMA}.{table_name}",
        description=f"Select from {STAGING_SCHEMA}.{table_name}",
        tags=["staging", "raw"]
    )
```

**Parameters:**
- `table_name` (str, required): Name of the table/model to create
- `sql` (str, required): SQL query string
- `description` (str, optional): Model description
- `variables` (list[str], optional): List of variable names (extracted by AST parser)
- `tags` (list[str], optional): dbt-style tags
- `object_tags` (dict, optional): Database object tags
- `**metadata` (Any): Additional metadata

**Returns:**
- `None` (models are registered via AST parsing)

**Note:**
The parser extracts model information via AST analysis, so this function primarily serves as a validation and documentation marker. The parser reads the source code directly, not the runtime values.

**Example with metadata:**

```python
create_model(
    table_name="daily_sales",
    sql="""
    SELECT 
        *,
        CURRENT_TIMESTAMP() as updated_at
    FROM staging.sales
    """,
    description="Daily sales mart table",
    tags=["daily", "sales", "mart"],
    object_tags={
        "source_table": "staging.sales",
        "refresh_frequency": "daily",
        "data_owner": "analytics-team"
    }
)
```

## When to Use Each

### Use `@model` decorator when:
- Each model has unique logic or complex transformations
- You need fine-grained control over individual models
- Models have different patterns or structures

### Use `create_model()` when:
- You have many similar models that follow a pattern
- You want to reduce code repetition
- Models can be generated from a list or configuration
- You're creating staging or intermediate tables with similar structures

## Model Metadata

Both decorators create models with the following metadata structure:

```python
{
    "table_name": str,           # Model/table name
    "function_name": str,        # Function name (for @model)
    "description": str | None,   # Model description
    "variables": list[str],       # Variables to inject
    "tags": list[str],            # dbt-style tags
    "object_tags": dict,          # Database object tags
    "metadata": dict,             # Additional metadata
    "needs_evaluation": bool,     # Whether SQL needs evaluation
    "sql": str | None            # SQL string (for create_model)
}
```

## Related Documentation

- [Tags and Metadata](../../user-guide/tags-and-metadata.md) - Comprehensive guide to tags and metadata
- [Execution Engine](../../user-guide/execution-engine.md) - Running models
- [CLI Reference](../../user-guide/cli-reference.md) - Command-line usage

