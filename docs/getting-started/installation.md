# Installation

TEE is a Python package that can be installed using `uv`, pip, or other Python package managers.

## Prerequisites

- Python 3.14+
- uv (recommended) or pip

## Installation Methods

### Using uv (Recommended)

```bash
# Install TEE
uv add tee

# Or add to an existing project
uv add tee
```

### Using pip

```bash
# Install TEE
pip install tee

# Or add to requirements.txt
echo "tee" >> requirements.txt
pip install -r requirements.txt
```

### Development Installation

```bash
# Clone the repository
git clone https://github.com/your-username/tee.git
cd tee

# Install in development mode
uv sync
uv pip install -e .
```

## Database Dependencies

TEE supports multiple databases. Install the appropriate driver for your database:

### DuckDB (Default)
```bash
uv add duckdb
```

### Snowflake
```bash
uv add snowflake-connector-python
```

### PostgreSQL
```bash
uv add psycopg2-binary
```

### BigQuery
```bash
uv add google-cloud-bigquery
```

## Verify Installation

```python
# Test the installation
import tee
print(f"TEE version: {tee.__version__}")

# Test database adapters
from tee.adapters import get_adapter
print("Installation successful!")
```

## Next Steps

- [Quick Start](quick-start.md) - Get up and running quickly
- [Configuration](configuration.md) - Configure your database connections
