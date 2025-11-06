# Data Quality Tests

TEE provides a comprehensive data quality testing framework that allows you to automatically validate your data transformations. Tests are defined in model metadata and automatically executed after model creation.

## Overview

The testing framework supports:
- **Standard Tests**: Pre-built tests for common data quality checks
- **Custom SQL Tests**: User-defined SQL tests in `tests/` folder (dbt-style)
- **Parameterized Tests**: Tests that accept configuration parameters
- **Test Severity Levels**: Control whether tests fail builds (ERROR) or just warn (WARNING)
- **Automatic Execution**: Tests run automatically after models are executed
- **Standalone Execution**: Run tests independently with `tcli test`

## Quick Start

### Basic Test Definition

Tests are defined in your model metadata files:

```python
# models/my_table.py
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "description": "My first table",
    "materialization": "table",
    "schema": [
        {
            "name": "id",
            "datatype": "integer",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "name",
            "datatype": "string",
            "tests": ["not_null"]
        }
    ],
    "tests": ["row_count_gt_0"]
}
```

### Running Tests

Tests are automatically executed after model runs:

```bash
# Run models (tests execute automatically)
tcli run examples/t_project

# Run tests independently
tcli test examples/t_project
```

## Available Tests

### Column-Level Tests

#### `not_null`
Verifies a column contains no NULL values.

```python
{
    "name": "id",
    "datatype": "integer",
    "tests": ["not_null"]
}
```

**Parameters:** None

**Severity:** ERROR (default)

---

#### `unique`
Verifies values in a column are unique. Also supports composite uniqueness at the table level.

**Single column:**
```python
{
    "name": "id",
    "datatype": "integer",
    "tests": ["unique"]
}
```

**Composite key (table level):**
```python
{
    "description": "My table",
    "tests": [
        {
            "name": "unique",
            "params": {"columns": ["col1", "col2"]}
        }
    ]
}
```

**Parameters:**
- `columns` (optional, for table-level): List of columns for composite uniqueness

**Severity:** ERROR (default)

---

#### `accepted_values`
Validates that all values in a column are within a specified list of allowed values.

```python
{
    "name": "status",
    "datatype": "string",
    "tests": [
        {
            "name": "accepted_values",
            "params": {
                "values": ["active", "inactive", "pending"]
            }
        }
    ]
}
```

**Parameters:**
- `values` (required): List of accepted values

**Severity:** ERROR (default)

**Supported Types:** Strings, numbers, and other types (with proper SQL escaping)

---

#### `relationships`
Validates referential integrity by checking that values in a source column exist in a target table's column. Supports both single-column and composite key relationships.

**Single column:**
```python
{
    "name": "user_id",
    "datatype": "integer",
    "tests": [
        {
            "name": "relationships",
            "params": {
                "to": "my_schema.users",
                "field": "id"
            }
        }
    ]
}
```

**Composite key:**
```python
{
    "name": "order_id",
    "datatype": "integer",
    "tests": [
        {
            "name": "relationships",
            "params": {
                "to": "my_schema.products",
                "fields": ["order_id", "product_id"],
                "source_fields": ["order_id", "product_id"]
            }
        }
    ]
}
```

**Parameters:**
- `to` (required): Target table name (fully qualified)
- `field` (required for single column): Target column name
- `fields` (required for composite key): List of target column names
- `source_fields` (optional): List of source column names (defaults to column_name for single column)

**Severity:** ERROR (default)

---

### Model-Level Tests

#### `row_count_gt_0`
Verifies a table has at least one row.

```python
{
    "description": "My table",
    "tests": ["row_count_gt_0"]
}
```

**Parameters:** None

**Severity:** ERROR (default)

**Note:** This test has inverted logic - passes if row_count > 0, fails if row_count == 0

---

#### `no_duplicates`
Verifies no duplicate rows exist in a table. This checks if entire rows are duplicates, not just specific columns.

```python
{
    "description": "My table",
    "tests": ["no_duplicates"]
}
```

**Parameters:** None

**Severity:** ERROR (default)

**Note:** Different from `unique` test which checks specific columns. This test checks if entire rows are duplicates.

---

## Test Definition Formats

### Simple String Format
For tests without parameters:

```python
"tests": ["not_null", "unique"]
```

### Dictionary Format
For parameterized tests:

```python
"tests": [
    {
        "name": "accepted_values",
        "params": {
            "values": ["active", "inactive"]
        }
    }
]
```

### With Severity Override
Override test severity:

```python
"tests": [
    {
        "name": "not_null",
        "severity": "warning"
    }
]
```

---

## Test Severity Levels

### ERROR (Default)
- Test failures cause the build to fail
- Exit code: 1
- Blocks execution

### WARNING
- Test failures are logged but don't block execution
- Exit code: 0 (with warnings)
- Useful for non-critical validations

### Setting Severity

**In metadata:**
```python
"tests": [
    {
        "name": "not_null",
        "severity": "warning"
    }
]
```

**Via CLI:**
```bash
tcli test examples/t_project --severity not_null=warning
tcli test examples/t_project --severity my_table.name.not_null=error
```

---

## Running Tests

### Automatic Execution

Tests are automatically executed after models are created:

```bash
tcli run examples/t_project
```

Output includes test results:
```
==================================================
EXECUTING TESTS
==================================================

Test Results:
  Total tests: 7
  ✅ Passed: 7
  ❌ Failed: 0

✅ All tests passed!
```

### Standalone Test Execution

Run tests independently without re-running models:

```bash
# Run all tests
tcli test examples/t_project

# Run with verbose output
tcli test examples/t_project --verbose

# Override test severity
tcli test examples/t_project --severity not_null=warning

# Run with variables (JSON format)
tcli test ./examples/t_project --vars '{"start_date": "2024-01-01"}'
```

### Test Execution Order

Tests are executed in dependency order, ensuring that:
1. Source tables exist before relationship tests run
2. Models are fully materialized before tests execute
3. Tests run in the same order as model execution

---

## Test Results

### Result Structure

Each test returns a `TestResult` object with:
- `test_name`: Name of the test
- `table_name`: Fully qualified table name
- `column_name`: Column name (if applicable)
- `passed`: Boolean indicating pass/fail
- `message`: Human-readable message
- `severity`: ERROR or WARNING
- `rows_returned`: Number of violating rows (for failed tests)

### Result Categories

Tests are categorized into:
- **Passed**: All tests that passed
- **Failed**: Tests that failed with ERROR severity
- **Warnings**: Tests that failed with WARNING severity or unimplemented tests

### Exit Codes

- **0**: All tests passed (or only warnings)
- **1**: One or more tests failed with ERROR severity

---

## Examples

### Complete Example

```python
# models/orders.py
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "description": "Orders table",
    "materialization": "table",
    "schema": [
        {
            "name": "order_id",
            "datatype": "integer",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "user_id",
            "datatype": "integer",
            "tests": [
                "not_null",
                {
                    "name": "relationships",
                    "params": {
                        "to": "my_schema.users",
                        "field": "id"
                    }
                }
            ]
        },
        {
            "name": "status",
            "datatype": "string",
            "tests": [
                {
                    "name": "accepted_values",
                    "params": {
                        "values": ["pending", "completed", "cancelled"]
                    }
                }
            ]
        },
        {
            "name": "amount",
            "datatype": "decimal",
            "tests": ["not_null"]
        }
    ],
    "tests": [
        "row_count_gt_0",
        "no_duplicates"
    ]
}
```

### Running with Severity Overrides

```bash
# Make all not_null tests warnings
tcli test examples/t_project --severity not_null=warning

# Make specific test a warning
tcli test examples/t_project --severity my_schema.orders.status.accepted_values=warning
```

---

## Best Practices

1. **Start with Critical Tests**: Use `not_null` and `unique` for primary keys
2. **Use Relationships**: Validate foreign key integrity with `relationships` tests
3. **Test Data Quality**: Use `accepted_values` for enumerated fields
4. **Monitor Table Health**: Use `row_count_gt_0` to ensure tables aren't empty
5. **Check for Duplicates**: Use `no_duplicates` for fact tables
6. **Set Appropriate Severity**: Use WARNING for non-critical checks
7. **Test in CI/CD**: Include `tcli test` in your CI/CD pipeline

---

## Troubleshooting

### Unimplemented Tests

If a test is not implemented, it will show as a warning:

```
⚠️  Test 'unknown_test' not implemented yet. Available tests: ['not_null', 'unique', ...]
```

### Test Failures

When a test fails, check:
1. The test message for specific violation details
2. The `rows_returned` count to see how many rows violate the test
3. The SQL query generated (with `--verbose` flag)

### Performance

For large tables, tests use `COUNT(*)` queries for better performance:
- `not_null`: Uses `COUNT(*) WHERE column IS NULL`
- `unique`: Uses `COUNT(*)` on duplicate groups
- `relationships`: Uses `COUNT(*)` with LEFT JOIN

---

## Custom SQL Tests

You can create custom SQL tests by placing `.sql` files in a `tests/` folder in your project (alongside the `models/` folder).

### Project Structure

```
examples/t_project/
├── models/
│   └── my_schema/
│       └── my_table.sql
├── tests/
│   ├── my_custom_test.sql
│   └── check_minimum_rows.sql
└── project.toml
```

### SQL Test File Format

SQL tests follow the **dbt pattern**:
- Query returns **rows when test fails**
- **0 rows returned = test passes**
- **1+ rows returned = test fails**

### Available Variables

SQL tests automatically have access to these variables:

- `{{ table_name }}` or `@table_name` - The fully qualified table name (e.g., `my_schema.my_table`)
- `{{ column_name }}` or `@column_name` - Column name (if test is applied to a column)
- Custom parameters from metadata (see Parameterized Tests below)

**Note:** `table_name` and `column_name` are substituted as **identifiers** (unquoted), while other parameters are substituted as **SQL values** (quoted if strings).

### Example: Model-Level Test

```sql
-- tests/check_minimum_rows.sql
-- Check that a table has at least a minimum number of rows

SELECT 1 as violation
FROM {{ table_name }}
GROUP BY 1
HAVING COUNT(*) < 5
```

**Usage in model metadata:**
```python
metadata: ModelMetadataDict = {
    "description": "My table",
    "tests": [
        "check_minimum_rows"  # Uses default min_rows=10
    ]
}
```

### Example: Parameterized Test

```sql
-- tests/check_minimum_rows.sql
-- Accepts min_rows parameter (defaults to 10)

SELECT 1 as violation
FROM {{ table_name }}
GROUP BY 1
HAVING COUNT(*) < {{ min_rows | default(10) }}
```

**Usage with parameters:**
```python
metadata: ModelMetadataDict = {
    "description": "My table",
    "tests": [
        {
            "name": "check_minimum_rows",
            "params": {"min_rows": 5}
        }
    ]
}
```

### Example: Column-Level Test

```sql
-- tests/column_not_negative.sql
-- Check that a numeric column has no negative values

SELECT {{ column_name }}
FROM {{ table_name }}
WHERE {{ column_name }} < 0
```

**Usage in column metadata:**
```python
metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "amount",
            "datatype": "number",
            "tests": ["column_not_negative"]
        }
    ]
}
```

### Variable Substitution

SQL tests support multiple variable syntaxes:

**Jinja-style (recommended):**
```sql
FROM {{ table_name }}
WHERE {{ column_name }} = {{ status | default('active') }}
```

**At-sign syntax:**
```sql
FROM @table_name
WHERE @column_name = @status
```

**Default values:**
```sql
-- Default value syntax
{{ min_rows | default(10) }}
{{ status | default('active') }}
```

### Test Discovery

SQL tests are automatically discovered from the `tests/` folder when:
- Running `tcli test`
- Running `tcli run` (tests execute automatically)

Test names are derived from file names (without `.sql` extension):
- `tests/my_test.sql` → test name `"my_test"`
- `tests/check_minimum_rows.sql` → test name `"check_minimum_rows"`

### Best Practices

1. **Use COUNT(*) for performance** when possible:
   ```sql
   SELECT COUNT(*) as violation_count
   FROM {{ table_name }}
   WHERE condition
   HAVING COUNT(*) > 0
   ```

2. **Return rows directly** for simple checks:
   ```sql
   SELECT * FROM {{ table_name }} WHERE violation_condition
   ```

3. **Document parameters** in SQL comments:
   ```sql
   -- Accepts: min_rows (default: 10), max_rows (optional)
   ```

4. **Use meaningful test names** that describe what they check

---

## Extending Tests

### Adding Custom Python Tests

To add a custom Python test class, create a class inheriting from `StandardTest`:

```python
from tee.testing.base import StandardTest, TestSeverity
from tee.testing import TestRegistry

class MyCustomTest(StandardTest):
    def __init__(self):
        super().__init__("my_custom_test", severity=TestSeverity.ERROR)
    
    def validate_params(self, params, column_name):
        # Validate parameters
        pass
    
    def get_test_query(self, adapter, table_name, column_name, params):
        # Generate SQL query
        return f"SELECT COUNT(*) FROM {table_name} WHERE ..."
    
    def check_passed(self, count):
        # Custom logic for determining pass/fail
        return count == 0

# Register the test
MY_CUSTOM_TEST = MyCustomTest()
TestRegistry.register(MY_CUSTOM_TEST)
```

### Database-Specific Optimizations

Adapters can override test query generation for database-specific optimizations:

```python
class MyAdapter(DatabaseAdapter):
    def generate_not_null_test_query(self, table_name, column_name):
        # Database-specific SQL generation
        return f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
```

---

## Related Documentation

- [Execution Engine](execution-engine.md) - Learn about model execution
- [Database Adapters](database-adapters.md) - Database-specific features
- [Examples](../user-guide/examples/) - Practical usage examples

