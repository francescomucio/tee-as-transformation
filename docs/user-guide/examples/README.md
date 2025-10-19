# Examples

This section contains practical examples demonstrating TEE's capabilities.

## Available Examples

- [Basic Usage](basic-usage.md) - Common usage patterns and workflows
- [Incremental Example](incremental-example.md) - Complete incremental materialization example

## Quick Start Examples

### Basic SQL Model

```python
# models/my_table.py
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "description": "My first table",
    "materialization": "table"
}
```

```sql
-- models/my_table.sql
SELECT 
    id,
    name,
    created_at
FROM source_table
WHERE status = 'active'
```

### Incremental Model

```python
# models/incremental_table.py
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "description": "Incremental table",
    "materialization": "incremental",
    "incremental": {
        "strategy": "merge",
        "merge": {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "1 hour"
        }
    }
}
```

```sql
-- models/incremental_table.sql
SELECT 
    id,
    name,
    updated_at,
    status
FROM source_table
WHERE status = 'active'
```

## Running Examples

```bash
# Run all models
uv run python -m tee.cli.main run t_project

# Run with variables
uv run python -m tee.cli.main run t_project --vars 'start_date=2024-01-01'

# Run specific model
uv run python -m tee.cli.main run t_project --models my_schema.my_table
```

## Example Projects

### Complete Project Structure

```
t_project/
├── models/
│   └── my_schema/
│       ├── my_table.sql
│       ├── my_table.py
│       ├── incremental_table.sql
│       └── incremental_table.py
├── data/
│   ├── t_project.duckdb
│   └── tee_state.db
└── project.toml
```

### Configuration

```toml
# project.toml
project_folder = "t_project"

[connection]
type = "duckdb"
path = "t_project/data/t_project.duckdb"
schema = "my_schema"

[flags]
materialization_change_behavior = "warn"
```

## Next Steps

- Explore the [Basic Usage Examples](basic-usage.md) for common patterns
- Try the [Incremental Example](incremental-example.md) for advanced data processing
- Check the [Incremental Materialization Guide](../incremental-materialization.md) for detailed configuration options
