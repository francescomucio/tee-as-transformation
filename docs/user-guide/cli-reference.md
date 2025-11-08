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
1. Compiles project to OTS modules (parses SQL/Python models, loads imported OTS modules)
2. Loads compiled OTS modules from `output/ots_modules/`
3. Resolves dependencies automatically
4. Executes models in the correct order
5. Materializes results as tables/views in the database

**Note:** The `run` command does NOT execute tests. Use `t4t test` to run tests separately, or `t4t build` to execute models with interleaved test execution.

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
1. Compiles project to OTS modules (parses SQL/Python models, loads imported OTS modules)
2. Loads compiled OTS modules from `output/ots_modules/`
3. Executes models in dependency order
4. Automatically loads seeds before execution

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

**Examples:**
```bash
# Run all tests
t4t test ./my_project

# Run tests with variables
t4t test ./my_project --vars '{"env": "prod"}'

# Test specific models
t4t test ./my_project --select my_schema.*

# Verbose output
t4t test ./my_project -v
```

**What it does:**
1. Compiles project to OTS modules (parses SQL/Python models, loads imported OTS modules)
2. Loads compiled OTS modules from `output/ots_modules/`
3. Builds dependency graph
4. Runs all data quality tests

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

### `compile` - Compile Project to OTS Modules

Compile t4t project to Open Transformation Specification (OTS) modules and test libraries.

**Usage:**
```bash
t4t compile <project_folder> [options]
```

**Arguments:**
- `project_folder` (required) - Path to the project folder containing `project.toml`

**Options:**
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-f, --format <format>` - Output format: `json` or `yaml` (default: `json`)

**Examples:**
```bash
# Compile to JSON (default)
t4t compile ./my_project

# Compile to YAML
t4t compile ./my_project --format yaml

# Compile with variables
t4t compile ./my_project --vars '{"env": "prod"}'
```

**What it does:**
1. Parses all SQL/Python models in the `models/` directory
2. Discovers and validates imported OTS modules in `models/`
3. Detects conflicts (duplicate `transformation_id`)
4. Merges all models (SQL, Python, and imported OTS)
5. Converts to OTS format
6. Validates compiled modules
7. Exports OTS modules to `output/ots_modules/`
8. Exports merged test library to `output/ots_modules/`

**Output:**
- OTS modules: `{database}__{schema}.ots.json` (or `.ots.yaml`)
- Test library: `{project_name}_test_library.ots.json` (or `.ots.yaml`)

**Note:** The `run`, `build`, and `test` commands automatically compile before execution. Use `compile` when you want to generate OTS modules without executing. Use `parse` for quick analysis without compilation.

---

### `ots` - OTS Module Commands

Commands for working with Open Transformation Specification (OTS) modules.

#### `ots run` - Execute OTS Modules

Execute OTS modules directly without compilation.

**Usage:**
```bash
t4t ots run <ots_path> [options]
```

**Arguments:**
- `ots_path` (required) - Path to OTS module file (`.ots.json`, `.ots.yaml`, `.ots.yml`) or directory containing OTS modules

**Options:**
- `--project-folder <path>` - Optional project folder for connection config and merging with existing models
- `-v, --verbose` - Enable verbose output
- `--vars <JSON>` - Variables to pass to models (JSON format)
- `-s, --select <pattern>` - Select models by pattern (can be used multiple times)
- `-e, --exclude <pattern>` - Exclude models by pattern (can be used multiple times)

**Examples:**
```bash
# Execute a single OTS module
t4t ots run ./output/ots_modules/my_db__schema1.ots.json

# Execute all OTS modules in a directory
t4t ots run ./output/ots_modules/

# Execute with project folder (merges with existing models)
t4t ots run ./external_modules/ --project-folder ./my_project

# Execute with variables
t4t ots run ./modules/ --vars '{"env": "prod"}'
```

**What it does:**
1. Loads OTS modules from the specified path
2. Optionally merges with existing models from project folder
3. Builds dependency graph
4. Executes transformations in dependency order

---

#### `ots validate` - Validate OTS Modules

Validate OTS module files for compliance with the OTS specification.

**Usage:**
```bash
t4t ots validate <ots_path> [options]
```

**Arguments:**
- `ots_path` (required) - Path to OTS module file (`.ots.json`, `.ots.yaml`, `.ots.yml`) or directory containing OTS modules

**Options:**
- `-v, --verbose` - Enable verbose output

**Examples:**
```bash
# Validate a single OTS module
t4t ots validate ./output/ots_modules/my_db__schema1.ots.json

# Validate all OTS modules in a directory
t4t ots validate ./output/ots_modules/

# Validate with verbose output
t4t ots validate ./module.ots.yaml -v
```

**What it validates:**
- OTS version compatibility (supports 0.1.0 and below)
- Schema location matches file path
- Required fields and structure
- Module format (JSON/YAML)

---

### `help` - Show Help Information

Display help information for the CLI. This command shows the same output as `t4t --help`.

**Usage:**
```bash
t4t help              # Equivalent to 't4t --help'
t4t --help            # Show general help (same as 't4t help')
t4t <command> --help  # Show command-specific help
```

**Examples:**
```bash
# Show general help (both commands are equivalent)
t4t help
t4t --help

# Show help for specific command
t4t init --help
t4t run --help
t4t compile --help
t4t ots --help
t4t ots run --help
```

---

## Command Comparison

| Command | Compiles | Executes Models | Runs Tests | Loads Seeds | Stops on Test Failure |
|---------|----------|----------------|------------|-------------|----------------------|
| `compile` | ✅ | ❌ | ❌ | ❌ | N/A |
| `run` | ✅ | ✅ | ❌ | ❌ | N/A |
| `build` | ✅ | ✅ | ✅ (interleaved) | ✅ | ✅ |
| `test` | ✅ | ❌ | ✅ | ❌ | ✅ (ERROR only) |
| `parse` | ❌ | ❌ | ❌ | ❌ | N/A |
| `seed` | ❌ | ❌ | ❌ | ✅ | N/A |
| `debug` | ❌ | ❌ | ❌ | ❌ | N/A |
| `ots run` | ❌ | ✅ | ❌ | ❌ | ❌ |
| `ots validate` | ❌ | ❌ | ❌ | ❌ | N/A |

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

