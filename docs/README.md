# t4t Documentation

Welcome to the Tee for Transform (t4t) documentation. t4t is a powerful Python framework for managing SQL data transformations with support for multiple database backends and automatic SQL dialect conversion.

## Quick Navigation

### 🚀 Getting Started
- [Installation](getting-started/installation.md) - Install t4t and its dependencies
- [Quick Start](getting-started/quick-start.md) - Get up and running in minutes
- [Configuration](getting-started/configuration.md) - Configure databases and settings

### 📖 User Guide
- [Overview](user-guide/overview.md) - Core concepts and architecture
- [CLI Reference](user-guide/cli-reference.md) - Complete CLI commands and options reference
- [Execution Engine](user-guide/execution-engine.md) - Running SQL models
- [Functions](user-guide/functions.md) - User-Defined Functions (UDFs)
- [Seeds](user-guide/seeds.md) - Loading static data files (CSV, JSON, TSV)
- [Data Quality Tests](user-guide/data-quality-tests.md) - Automated data validation
- [Incremental Materialization](user-guide/incremental-materialization.md) - Efficient data processing
- [Database Adapters](user-guide/database-adapters.md) - Multi-database support
- [Tags and Metadata](user-guide/tags-and-metadata.md) - Tagging tables, views, and schemas
- [SQL Dialect Conversion](user-guide/sql-dialect-conversion.md) - Write once, run anywhere
- [dbt Import Guide](user-guide/dbt-import.md) - Importing dbt projects into t4t
- [dbt Import Limitations](user-guide/dbt-import-limitations.md) - Known limitations and unsupported features
- [Examples](user-guide/examples/) - Practical usage examples and tutorials

### 🔧 API Reference
- [API Reference Overview](api-reference/README.md) - API documentation status and overview
- [Functions API](api-reference/functions.md) - Function decorators and API reference
- [Models API](api-reference/models.md) - Model decorators and dynamic model creation
- [Engine API](api-reference/engine/) - Execution engine classes and methods (coming soon)
- [Parser API](api-reference/parser/) - SQL parsing and analysis (coming soon)
- [Adapters API](api-reference/adapters/) - Database adapter implementations (coming soon)

### 🛠️ Development
- [Architecture](development/architecture.md) - System design and components
- [Contributing](development/contributing.md) - How to contribute to t4t
- [Migration Guides](development/migration-guides/) - Upgrading between versions

## Key Features

- **Multi-Database Support**: DuckDB, Snowflake, PostgreSQL, and more
- **User-Defined Functions (UDFs)**: Create reusable SQL and Python functions with automatic dependency resolution
- **SQL Dialect Conversion**: Write in PostgreSQL, run on any database
- **Dependency-Aware Execution**: Automatic model and function dependency resolution
- **Open Transformation Specification (OTS)**: Full OTS support for interoperability with other tools
  - Compile projects to OTS modules and test libraries
  - Import and merge OTS modules from other tools
  - Export in JSON or YAML format
  - Validate OTS modules for compliance
- **Seeds**: Load static data files (CSV, JSON, TSV) into database tables
- **Data Quality Tests**: Automated data validation with 6 standard tests and custom SQL tests (dbt-style), including function tests
- **Incremental Materialization**: Efficient data processing with append, merge, and delete+insert strategies
- **Comprehensive Tagging**: dbt-style tags and database object tags for tables, views, schemas, and functions
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
│   ├── functions.md
│   ├── seeds.md
│   ├── data-quality-tests.md
│   ├── incremental-materialization.md
│   ├── database-adapters.md
│   ├── tags-and-metadata.md
│   ├── sql-dialect-conversion.md
│   └── examples/
│       ├── basic-usage.md
│       ├── functions.md
│       ├── incremental-example.md
│       └── testing-example.md
├── api-reference/               # API documentation
│   ├── functions.md
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

- Browse the [examples](user-guide/examples/) for practical usage
- Check the [User Guide](user-guide/) for comprehensive documentation
- Join our community discussions
- Report issues on GitHub