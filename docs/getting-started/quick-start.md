# Quick Start

Get up and running with TEE in minutes. This guide will walk you through creating your first SQL model and executing it.

## 1. Create a Project

```bash
# Create a new project directory
mkdir my_tee_project
cd my_tee_project

# Initialize with uv
uv init
```

## 2. Install TEE

```bash
# Add TEE and DuckDB
uv add tee duckdb
```

## 3. Create Your First Model

Create a `models/` directory and add your first SQL model:

```sql
-- models/users.sql
SELECT 
    id,
    name,
    email,
    created_at
FROM source_users
WHERE active = true
```

## 4. Configure Database

Add database configuration to `pyproject.toml`:

```toml
[tool.tee.database]
type = "duckdb"
path = "data/my_project.db"
source_dialect = "postgresql"  # Write in PostgreSQL, convert to DuckDB
```

## 5. Execute Your Model

Create a simple script to run your model:

```python
# run_models.py
from tee.engine import ModelExecutor, load_database_config
from tee.parser import ProjectParser

# Load configuration
config = load_database_config()

# Parse models
parser = ProjectParser(".", config)
parsed_models = parser.collect_models()

# Execute models
executor = ModelExecutor(".", config)
results = executor.execute_models(parsed_models)

print(f"Executed {len(results['executed_tables'])} models successfully!")
```

## 6. Run Your Project

```bash
# Execute the script
uv run python run_models.py
```

## What Happened?

1. **Parsing**: TEE analyzed your SQL model and identified dependencies
2. **Configuration**: Database settings were loaded from `pyproject.toml`
3. **Execution**: Your model was executed in the correct order
4. **Results**: The model was materialized as a table in DuckDB

## Next Steps

- [Configuration](configuration.md) - Learn about advanced configuration options
- [Database Adapters](user-guide/database-adapters.md) - Explore multi-database support
- [Examples](user-guide/examples/) - See more complex examples
