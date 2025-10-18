# TEE - Transform, Extract, Execute

A powerful Python framework for managing SQL data transformations with support for multiple database backends and automatic SQL dialect conversion.

## 🚀 Quick Start

```bash
# Install TEE
uv add tee duckdb

# Create your first model
mkdir models
echo "SELECT * FROM source_table" > models/my_model.sql

# Run with TEE
uv run python -c "
from tee.engine import ModelExecutor, load_database_config
config = load_database_config()
executor = ModelExecutor('.', config)
# Execute your models...
"
```

## ✨ Key Features

- **Multi-Database Support**: DuckDB, Snowflake, PostgreSQL, and more
- **SQL Dialect Conversion**: Write in PostgreSQL, run on any database
- **Dependency-Aware Execution**: Automatic model dependency resolution
- **Pluggable Architecture**: Easy to add new database adapters
- **Configuration Management**: Flexible configuration via `pyproject.toml`

## 📚 Documentation

**📖 [Full Documentation](docs/README.md)** - Complete guides, API reference, and examples

### Quick Links
- [Installation](docs/getting-started/installation.md)
- [Quick Start Guide](docs/getting-started/quick-start.md)
- [Configuration](docs/getting-started/configuration.md)
- [Database Adapters](docs/user-guide/database-adapters.md)
- [Examples](docs/user-guide/examples/)

## 🛠️ Development

### Building Documentation

```bash
# Install documentation dependencies
uv add --dev mkdocs mkdocs-material

# Build documentation
uv run python docs/build_docs.py build

# Serve documentation locally
uv run python docs/build_docs.py serve
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest tests/ -v
```

## 📦 Installation

```bash
# Using uv (recommended)
uv add tee

# Using pip
pip install tee
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](docs/development/contributing.md) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [Documentation](docs/README.md)
- [GitHub Repository](https://github.com/your-username/tee)
- [PyPI Package](https://pypi.org/project/tee)
