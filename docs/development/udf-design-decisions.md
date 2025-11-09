# UDF Design Decisions - Final Summary

This document captures all final design decisions for UDF implementation in t4t.

## Core Design Principles

1. **Consistency with Models:** Functions follow the same patterns as models where applicable
2. **No Name Matching:** Functions and tests are not matched by name (consistent with models)
3. **Always Overwrite:** Functions use `CREATE OR REPLACE` (always overwritten)
4. **Error on Failure:** Raise error if function creation fails (no rollback)
5. **Tagging Support:** Functions support both `tags` (dbt-style) and `object_tags` (database-style)

---

## File Structure

### Function Files
```
project/
├── functions/
│   ├── my_schema/
│   │   ├── calculate_metric.sql          # Single SQL function
│   │   ├── calculate_metric.py           # Metadata for calculate_metric
│   │   ├── calculate_metric.postgresql.sql  # PostgreSQL override
│   │   ├── helper_functions.py          # Multiple functions in one file
│   │   └── complex_function/            # Folder-based (optional)
│   │       ├── complex_function.sql     # or complex_function.py
│   │       └── complex_function.snowflake.js  # Database-specific override
│   └── another_schema/
│       └── utility_func.sql
```

### Discovery Rules
- Flat structure: `functions/{schema}/{function_name}.sql` or `.py`
- Folder structure: `functions/{schema}/{function_name}/{function_name}.sql` or `.py`
- Database overrides: `{function_name}.{database}.sql` (e.g., `calculate_metric.postgresql.sql`)
- Multiple functions: Any `.py` file with multiple `@functions.*()` decorators
- Function name: Auto-detect from filename, override via decorator/metadata

---

## Function Definition Formats

### SQL Functions (`function.sql`)
```sql
CREATE OR REPLACE FUNCTION my_schema.calculate_metric(
    input_value FLOAT,
    multiplier FLOAT DEFAULT 1.0
) RETURNS FLOAT
LANGUAGE SQL
AS $$
    SELECT input_value * multiplier + 10
$$;
```

### Python Functions (`function.py`)

**Option A: Metadata-only**
```python
from tee.typing.metadata import FunctionMetadataDict

metadata: FunctionMetadataDict = {
    "function_name": "calculate_metric",
    "description": "Calculates a custom metric",
    "parameters": [
        {"name": "input_value", "type": "float"},
        {"name": "multiplier", "type": "float", "default": "1.0"}
    ],
    "return_type": "float",
    "tags": ["analytics", "production"],
    "object_tags": {"sensitivity_tag": "public"}
}
```

**Option B: SQL-generating function**
```python
@functions.sql(
    function_name="calculate_metric",
    return_type="float",
    tags=["analytics"]
)
def generate_calc_sql(adapter_type: str) -> str:
    if adapter_type == "snowflake":
        return "CREATE FUNCTION ..."
    return "CREATE FUNCTION ..."
```

**Option C: Python UDF**
```python
@functions.python(
    function_name="python_calculator",
    return_type="float"
)
def python_calculator(x: float) -> float:
    return x * 2.5 + 10
```

---

## Decorators

### `@functions.sql()`
- Generates SQL function definitions
- Returns SQL string or dict mapping adapter types to SQL
- Supports: `function_name`, `description`, `parameters`, `return_type`, `tags`, `object_tags`, `database_name`

### `@functions.python()`
- Defines actual Python UDFs (executed in database)
- Supports: `function_name`, `description`, `parameters`, `return_type`, `language`, `tags`, `object_tags`, `database_name`

---

## Metadata & Tags

### Function Metadata Structure
```python
FunctionMetadataDict = {
    "function_name": str,
    "description": Optional[str],
    "function_type": Optional[FunctionType],  # "scalar", "aggregate", "table"
    "language": Optional[str],  # "sql", "python", "javascript"
    "parameters": Optional[List[FunctionParameter]],
    "return_type": str,  # For scalar/aggregate
    "return_table_schema": Optional[List[ColumnDefinition]],  # For table functions
    "schema": Optional[str],
    "tags": Optional[List[str]],  # dbt-style tags
    "object_tags": Optional[Dict[str, str]],  # database-style tags
    "tests": Optional[List[TestDefinition]],  # Test references
    "deterministic": Optional[bool]
}
```

### Tag Support
- **dbt-style tags** (`tags`): List of strings for filtering/selection
- **Database object tags** (`object_tags`): Key-value pairs attached to database objects
- **Tag merging**: Schema-level, module-level, function-level (like models)
- **Database support**: Snowflake (full), others (log debug)

---

## Dependency Graph

### Function Dependencies
- **Function → Function**: Functions can depend on other functions
- **Function → Table**: Functions can depend on tables
- **Function → Seed**: Functions can depend on seed data
- **Model → Function**: Models can call functions

### Execution Order
1. Seeds (loaded first)
2. Functions (created in dependency order)
3. Models (can use functions)

### Dependency Extraction
- **SQL functions**: Parse function body with SQLglot to find function calls and table references
- **Python functions**: AST analysis to find function calls and dependencies
- Filter out built-in functions (keep only project UDFs)

---

## OTS 0.2.0 Support

### OTS Module Structure
```yaml
ots_version: "0.2.0"
module_name: "my_project"
target: {...}
transformations: [...]  # Existing models
functions:  # NEW
  - function_id: "my_schema.calculate_metric"
    description: "..."
    function_type: "scalar"
    language: "sql"
    parameters: [...]
    return_type: "float"
    code:
      generic_sql: "..."
      database_specific:
        postgresql: "..."
    dependencies:
      functions: []
      tables: []
    metadata:
      tags: [...]
      object_tags: {...}
```

---

## Adapter Interface

### Base Methods
```python
@abstractmethod
def create_function(
    self, 
    function_name: str, 
    function_definition: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    """Create a user-defined function in the database."""

@abstractmethod
def function_exists(self, function_name: str) -> bool:
    """Check if a function exists."""

@abstractmethod
def drop_function(self, function_name: str) -> None:
    """Drop a function."""

def attach_tags(
    self, object_type: str, object_name: str, tags: List[str]
) -> None:
    """Attach tags to function (override in adapters that support it)."""
```

### Implementation Notes
- **DuckDB**: SQL functions, Python UDFs, tags (if supported)
- **PostgreSQL**: SQL functions, PL/pgSQL, function overloading by signature
- **Snowflake**: SQL functions, JavaScript UDFs, Python UDFs, full tag support
- **BigQuery**: SQL functions, JavaScript UDFs (if applicable)

---

## Function Testing

### Test Location
- Tests in `tests/functions/` folder
- Schema structure optional (for organization only)
- No name matching (consistent with models)

### Test Types

#### 1. Generic Tests (with `@function_name` placeholder)
```sql
-- tests/functions/test_positive_result.sql
SELECT @function_name(10.0) > 0 AS test_passed
```
- Must be referenced in function metadata: `tests: ["test_positive_result"]`
- `@function_name` is substituted with actual function name

#### 2. Singular Tests (hardcoded function name)
```sql
-- tests/functions/test_calc_metric.sql
SELECT my_schema.calculate_metric(10.0) = 10.0 AS test_passed
```
- Always executed (when function exists)
- Parse SQL to find function calls → match to functions

### Test Execution Patterns

#### Pattern 1: Assertion-based (default)
```sql
SELECT my_schema.calculate_metric(10.0) = 10.0 AS test_passed
```
- Returns boolean: `TRUE` = pass, `FALSE` = fail
- Handle different boolean representations across databases

#### Pattern 2: Expected value in metadata
```python
metadata = {
    "tests": [
        {
            "name": "test_calc_metric",
            "expected": 10.0,
            "params": {"input": 10.0}
        }
    ]
}
```
```sql
-- tests/functions/test_calc_metric.sql
SELECT my_schema.calculate_metric({{ input }}) AS result
```
- Compare result to `expected` value from metadata
- Match = pass, mismatch = fail

### Test Execution Logic
1. Discover all tests in `tests/functions/`
2. For each function:
   - Run tests referenced in metadata (generic tests)
   - Run singular tests that call this function (parsed from SQL)
3. Execute test query
4. Check result:
   - If boolean assertion → `TRUE`/truthy = pass
   - If metadata has `expected` → compare result to expected
   - Default to assertion pattern

### TestExecutor Integration
- Extend `execute_all_tests()` to handle both models and functions
- Add `execute_tests_for_function()` method
- Parse singular test SQL to extract function dependencies
- Support function parameter placeholders (`@param1`, `@param2`)

---

## Execution Flow

### Build Command Flow
1. **Compile** → Parse models and functions, build dependency graph
2. **Load Seeds** → Load seed data into database
3. **Create Functions** → Execute functions in dependency order
4. **Execute Models** → Run models (can use functions)
5. **Run Tests** → Execute tests for models and functions

### Test Command Flow
1. **Compile** → Parse models and functions
2. **Connect** → Establish database connection
3. **Run Tests** → Execute all tests (models + functions)

---

## Function Overloading

### Approach
- Use separate t4t function names for different signatures
- Optional `database_name` parameter to map to same database function name
- Example:
  ```python
  @functions.sql(function_name="calculate_v1", database_name="calculate", ...)
  @functions.sql(function_name="calculate_v2", database_name="calculate", ...)
  ```

---

## Key Decisions Summary

✅ **No variables support** (for now)  
✅ **Always overwrite** (CREATE OR REPLACE)  
✅ **Error on failure** (raise error, no rollback)  
✅ **Tags like models** (both `tags` and `object_tags`)  
✅ **No name matching** (consistent with models)  
✅ **Hybrid test approach** (assertion-based + expected value)  
✅ **Generic + singular tests** (like models)  
✅ **Tests in `tests/functions/`** (integrated with existing framework)  
✅ **OTS 0.2.0** (functions support)  
✅ **Execution order**: Seeds → Functions → Models  

---

## Ready for Implementation

All design decisions are finalized. Implementation can begin with Phase 0 (Foundation & Type System).

