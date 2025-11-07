# CLI Reference

Complete reference for all t4t command-line interface commands and options.

## Overview

t4t provides a comprehensive command-line interface through `t4t`. All commands can be run using `uv run t4t` or directly as `t4t` if installed:

```bash
# Using uv (recommended for development)
uv run t4t <command> [options]

# Direct execution (if installed)
t4t <command> [options]
```

## Common Options

Most commands support these common options:

- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Pass variables to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)

### Variables Format

Variables are passed as JSON strings:

```bash
--vars '{"env": "prod", "start_date": "2024-01-01"}'
```

### Selection Patterns

Selection patterns support:
- Model names: `--select my_model`
- Wildcards: `--select my_*` or `--select *users*`
- Tags: `--select tag:nightly` or `--select tag:production`
- Multiple patterns: `--select my_model --select tag:analytics`

## Commands

### `init` - Initialize a New Project

Create a new t4t project with the proper structure.

**Usage:**
```bash
t4t init <project_name> [--database-type <type>]
```

**Arguments:**
- `project_name` (required) - Name of the project (will create a folder with this name)

**Options:**
- `-d, --database-type <type>` - Database type (default: `duckdb`)
  - Supported: `duckdb`, `snowflake`, `postgresql`, `bigquery`

**Examples:**
```bash
# Initialize with DuckDB (default)
t4t init my_project

# Initialize with Snowflake
t4t init my_project -d snowflake

# Initialize with PostgreSQL
t4t init my_project -d postgresql

# Initialize with BigQuery
t4t init my_project -d bigquery
```

**What it creates:**
- Project directory with the specified name
- `project.toml` configuration file with database connection template
- Default directories: `models/`, `tests/`, `seeds/`
- `data/` directory (for DuckDB projects only)

**Generated `project.toml` structure:**
```toml
project_folder = "my_project"

[connection]
# Database-specific connection settings
type = "duckdb"
path = "data/my_project.duckdb"

[flags]
materialization_change_behavior = "warn"  # Options: "warn", "error", "ignore"
```

---

### `run` - Execute SQL Models

Parse and execute SQL models in dependency order.

**Usage:**
```bash
t4t run <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)

**Examples:**
```bash
# Run all models
t4t run ./my_project

# Run with variables
t4t run ./my_project --vars '{"env": "prod", "start_date": "2024-01-01"}'

# Run specific models
t4t run ./my_project --select my_model

# Run models with specific tag
t4t run ./my_project --select tag:nightly

# Exclude test models
t4t run ./my_project --exclude tag:test

# Combine selection and exclusion
t4t run ./my_project --select my_model --exclude tag:deprecated
```

**What it does:**
1. Parses all SQL models in the `models/` directory
2. Resolves dependencies automatically
3. Executes models in the correct order
4. Materializes results as tables/views in the database
5. Automatically runs tests after model execution

---

### `parse` - Parse Models Without Execution

Parse SQL models and analyze dependencies without executing them.

**Usage:**
```bash
t4t parse <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output (shows full analysis)
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)

**Examples:**
```bash
# Parse all models
t4t parse ./my_project

# Parse with variables
t4t parse ./my_project --vars '{"env": "dev"}'

# Parse specific models
t4t parse ./my_project --select my_schema.*

# Verbose output
t4t parse ./my_project -v
```

**Output:**
- Total number of models found
- Execution order
- Dependency graph (if verbose)
- Model metadata

---

### `build` - Build Models with Tests

Build models with interleaved test execution, stopping on the first ERROR severity test failure.

**Usage:**
```bash
t4t build <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)

**Examples:**
```bash
# Build all models with tests
t4t build ./my_project

# Build with variables
t4t build ./my_project --vars '{"env": "prod"}'

# Build specific models
t4t build ./my_project --select my_model
```

**What it does:**
1. Executes models in dependency order
2. Runs tests after each model execution
3. Stops on first ERROR severity test failure
4. Continues on WARNING severity test failures
5. Automatically loads seeds before execution

**Exit codes:**
- `0` - All models and tests passed
- `1` - Model execution failed or ERROR severity test failed

---

### `test` - Run Data Quality Tests

Execute data quality tests on models independently.

**Usage:**
```bash
t4t test <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output (shows detailed test results)
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)
- `--severity <override>` - Override test severity (format: `test_name=error|warning` or `table.column.test_name=error|warning`). Can be used multiple times.

**Examples:**
```bash
# Run all tests
t4t test ./my_project

# Run tests with variables
t4t test ./my_project --vars '{"env": "prod"}'

# Override test severity
t4t test ./my_project --severity not_null=warning

# Override severity for specific table/column/test
t4t test ./my_project --severity my_table.id.unique=warning

# Multiple severity overrides
t4t test ./my_project --severity not_null=warning --severity unique=error

# Test specific models
t4t test ./my_project --select my_schema.*

# Verbose output
t4t test ./my_project -v
```

**Severity Override Format:**
- `test_name=severity` - Override for all instances of a test
- `table.test_name=severity` - Override for a specific table
- `table.column.test_name=severity` - Override for a specific column test

**Severity values:**
- `error` - Test failure stops execution (default for most tests)
- `warning` - Test failure is reported but doesn't stop execution

**Exit codes:**
- `0` - All tests passed
- `1` - One or more ERROR severity tests failed

---

### `seed` - Load Seed Files

Load seed files (CSV, JSON, TSV) into database tables.

**Usage:**
```bash
t4t seed <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)

**Examples:**
```bash
# Load all seeds
t4t seed ./my_project

# Load seeds with verbose output
t4t seed ./my_project -v
```

**What it does:**
1. Discovers seed files in the `seeds/` directory
2. Supports CSV, TSV, and JSON formats
3. Organizes seeds by schema using subdirectories
4. Loads seeds into database tables
5. Reports loading results and row counts

**Seed file organization:**
```
seeds/
├── users.csv                    # → users table
├── orders.csv                   # → orders table
└── my_schema/
    ├── products.json            # → my_schema.products table
    └── customers.tsv            # → my_schema.customers table
```

**Note:** The `build` command automatically loads seeds before execution. Use `seed` when you want to load seeds independently.

---

### `debug` - Test Database Connectivity

Test database connectivity and configuration.

**Usage:**
```bash
t4t debug <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)

**Examples:**
```bash
# Test database connection
t4t debug ./my_project

# Test with verbose output
t4t debug ./my_project -v
```

**What it does:**
1. Tests database connection
2. Displays database information (type, version, host, etc.)
3. Lists supported materializations
4. Validates configuration

**Output includes:**
- Connection status
- Database type and version
- Connection details (host, database, warehouse, role, etc.)
- Supported materialization strategies

---

### `help` - Show Help Information

Display help information for the CLI.

**Usage:**
```bash
t4t help
```

**Alternative:**
```bash
t4t --help              # Show general help
t4t <command> --help    # Show command-specific help
```

**Examples:**
```bash
# Show general help
t4t help
t4t --help

# Show help for specific command
t4t init --help
t4t run --help
t4t test --help
```

---

## Command Comparison

| Command | Executes Models | Runs Tests | Loads Seeds | Stops on Test Failure |
|---------|----------------|------------|-------------|----------------------|
| `run` | ✅ | ✅ (after models) | ❌ | ❌ |
| `build` | ✅ | ✅ (interleaved) | ✅ | ✅ |
| `test` | ❌ | ✅ | ❌ | ✅ (ERROR only) |
| `parse` | ❌ | ❌ | ❌ | N/A |
| `seed` | ❌ | ❌ | ✅ | N/A |
| `debug` | ❌ | ❌ | ❌ | N/A |

## Exit Codes

- `0` - Success
- `1` - Error (command failed, test failure, etc.)
- `130` - Interrupted by user (Ctrl+C)

## Getting Help

For more information:

- Run `t4t --help` for general help
- Run `t4t <command> --help` for command-specific help
- See [Quick Start Guide](getting-started/quick-start.md) for examples
- See [Examples](examples/) for practical usage patterns

