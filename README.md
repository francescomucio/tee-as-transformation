# Tee - Transform, Extract, Execute

<img src="https://upload.wikimedia.org/wikipedia/commons/f/f7/Rpb_clothing_icon.svg" alt="Tee Logo" width="80" height="80" align="left" style="margin-right: 15px; margin-bottom: 10px;">

**This is a "what if" project**: What if dbt supported functions? What if dbt allowed for richer metadata? What if data modeling was a priority? After a chat with a friend, I decided to see how far I could go...

A Python framework for managing SQL data transformations with support for multiple database backends, automatic SQL dialect conversion, and rich metadata-driven modeling. Named "Tee" for Transform, but also because everyone loves a good t-shirt! 👕

## 🚀 Quick Start

Since this is an initial release, there's no pip package available yet. The only way to use Tee is by cloning this repository:

```bash
# Clone the repository
git clone https://github.com/francescomucio/tee-as-transformation
cd tee-as-transformation

# Install dependencies
uv sync

# Create your first project
mkdir my_project
cd my_project

# Initialize project configuration
cat > project.toml << EOF
project_folder = "my_project"

[connection]
type = "duckdb"
path = "data/my_project.db"
schema = "public"

[flags]
source_dialect = "postgresql"
EOF

# Create your first model
mkdir models
echo "SELECT 1 as id, 'hello' as message" > models/my_first_model.sql

# Run your models
uv run tcli run .
```

## ✨ Key Features

- **Multi-Database Support**: DuckDB, Snowflake, PostgreSQL, and more
- **SQL Dialect Conversion**: Write in PostgreSQL, run on any database
- **Dependency-Aware Execution**: Automatic model dependency resolution
- **Incremental Materialization**: Efficient data processing with append, merge, and delete+insert strategies
- **Rich Metadata Support**: Python-based metadata configuration with full type safety
- **Pluggable Architecture**: Easy to add new database adapters
- **Configuration Management**: Flexible configuration via `project.toml`

## 📦 Installation

**Note**: This is an initial release with no pip package available. You must clone this repository to use Tee.

```bash
# Clone the repository
git clone https://github.com/francescomucio/tee-as-transformation
cd tee-as-transformation

# Install dependencies using uv
uv sync

# Install the package in development mode to make tcli available
uv pip install -e .

# Or install specific dependencies
uv add duckdb  # For DuckDB support
uv add snowflake-connector-python  # For Snowflake support
uv add psycopg2-binary  # For PostgreSQL support
```

## 🛠️ CLI Commands

Tee provides a comprehensive command-line interface through `tcli`. All commands can be run using `uv run tcli`:

### Run Models
Execute SQL models in your project:

```bash
# Run all models in a project
uv run tcli run ./my_project

# Run with variables (JSON format)
uv run tcli run ./my_project --vars '{"env": "prod", "start_date": "2024-01-01"}'

# Run with variables (key=value format)
uv run tcli run ./my_project --vars 'env=prod,start_date=2024-01-01'
```

### Parse Models
Parse and analyze SQL models without execution:

```bash
# Parse models and show dependency analysis
uv run tcli parse ./my_project

# Parse with variables
uv run tcli parse ./my_project --vars 'env=dev'
```

### Debug Connection
Test database connectivity and configuration:

```bash
# Test database connection
uv run tcli debug ./my_project
```

### Help
Show help information:

```bash
# Show general help
uv run tcli help

# Show help for specific command
uv run tcli run --help
uv run tcli parse --help
uv run tcli debug --help
```

## 📚 Documentation

**📖 [Full Documentation](docs/README.md)** - Complete guides, API reference, and examples

### Quick Links
- [Installation](docs/getting-started/installation.md)
- [Quick Start Guide](docs/getting-started/quick-start.md)
- [Configuration](docs/getting-started/configuration.md)
- [Incremental Materialization](docs/user-guide/incremental-materialization.md)
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

# Run specific test patterns
uv run pytest tests/ -k test_name
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](docs/development/contributing.md) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [Documentation](docs/README.md)
- [GitHub Repository](https://github.com/francescomucio/tee-as-transformation)
- [Examples](docs/user-guide/examples/)