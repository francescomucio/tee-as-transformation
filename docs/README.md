# TEE Documentation

Welcome to the TEE (Transform, Extract, Execute) documentation. TEE is a powerful Python framework for managing SQL data transformations with support for multiple database backends and automatic SQL dialect conversion.

## Quick Navigation

### 🚀 Getting Started
- [Installation](getting-started/installation.md) - Install TEE and its dependencies
- [Quick Start](getting-started/quick-start.md) - Get up and running in minutes
- [Configuration](getting-started/configuration.md) - Configure databases and settings

### 📖 User Guide
- [Overview](user-guide/overview.md) - Core concepts and architecture
- [Execution Engine](user-guide/execution-engine.md) - Running SQL models
- [Data Quality Tests](user-guide/data-quality-tests.md) - Automated data validation
- [Incremental Materialization](user-guide/incremental-materialization.md) - Efficient data processing
- [Database Adapters](user-guide/database-adapters.md) - Multi-database support
- [SQL Dialect Conversion](user-guide/sql-dialect-conversion.md) - Write once, run anywhere
- [Examples](user-guide/examples/) - Practical usage examples and tutorials

### 🔧 API Reference
- [Engine API](api-reference/engine/) - Execution engine classes and methods
- [Parser API](api-reference/parser/) - SQL parsing and analysis
- [Adapters API](api-reference/adapters/) - Database adapter implementations

### 🛠️ Development
- [Architecture](development/architecture.md) - System design and components
- [Contributing](development/contributing.md) - How to contribute to TEE
- [Migration Guides](development/migration-guides/) - Upgrading between versions

## Key Features

- **Multi-Database Support**: DuckDB, Snowflake, PostgreSQL, and more
- **SQL Dialect Conversion**: Write in PostgreSQL, run on any database
- **Dependency-Aware Execution**: Automatic model dependency resolution
- **Data Quality Tests**: Automated data validation with 6 standard tests and custom SQL tests (dbt-style)
- **Incremental Materialization**: Efficient data processing with append, merge, and delete+insert strategies
- **Pluggable Architecture**: Easy to add new database adapters
- **Configuration Management**: Flexible configuration via `pyproject.toml`

## Quick Example

```python
from tee.engine import ModelExecutor, load_database_config

# Load configuration
config = load_database_config()

# Create executor
executor = ModelExecutor("/path/to/project", config)

# Execute models
results = executor.execute_models(parser)
```

## Building Documentation

### Prerequisites

```bash
# Install documentation dependencies
uv add --dev mkdocs mkdocs-material
```

### Build Commands

```bash
# Build documentation
uv run python docs/build_docs.py build

# Serve documentation locally
uv run python docs/build_docs.py serve

# Clean build directory
uv run python docs/build_docs.py clean
```

### Direct MkDocs Commands

```bash
# Build documentation
uv run mkdocs build

# Serve documentation locally
uv run mkdocs serve

# Deploy to GitHub Pages
uv run mkdocs gh-deploy
```

## Documentation Structure

```
docs/
├── README.md                    # Main documentation index
├── getting-started/             # Getting started guides
│   ├── installation.md
│   ├── quick-start.md
│   └── configuration.md
├── user-guide/                  # User documentation
│   ├── overview.md
│   ├── execution-engine.md
│   ├── database-adapters.md
│   ├── sql-dialect-conversion.md
│   └── examples/
│       ├── basic-usage.md
│       ├── multi-database.md
│       └── custom-adapters.md
├── api-reference/               # API documentation
│   ├── engine/
│   ├── parser/
│   └── adapters/
├── development/                 # Development documentation
│   ├── architecture.md
│   ├── contributing.md
│   └── migration-guides/
├── assets/                      # Static assets
│   ├── images/
│   └── diagrams/
├── mkdocs.yml                   # MkDocs configuration
└── build_docs.py               # Build script
```

## Publishing Documentation

### GitHub Pages

1. **Enable GitHub Pages** in your repository settings
2. **Deploy documentation**:
   ```bash
   uv run mkdocs gh-deploy
   ```

### Other Platforms

- **Netlify**: Connect your repository and set build command to `uv run mkdocs build`
- **Vercel**: Similar to Netlify
- **Custom hosting**: Upload the `site/` directory after building

## Contributing to Documentation

1. **Edit documentation** in the `docs/` directory
2. **Test locally**:
   ```bash
   uv run python docs/build_docs.py serve
   ```
3. **Build and verify**:
   ```bash
   uv run python docs/build_docs.py build
   ```

## Need Help?

- Check the [FAQ](user-guide/faq.md) for common questions
- Browse the [examples](user-guide/examples/) for practical usage
- Join our community discussions
- Report issues on GitHub