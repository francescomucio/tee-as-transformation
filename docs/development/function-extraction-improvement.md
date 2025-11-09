# Function Call Extraction During Model Parsing

## Problem

Currently, function dependencies from model SQL are extracted **during graph building** by re-parsing the SQL:

```python
# In DependencyGraphBuilder.build_graph()
model_function_deps = self._extract_function_dependencies_from_sql(
    code_data["sql"].get("resolved_sql", ""),
    parsed_functions,
    table_resolver,
)
```

This is inefficient because:
1. We already parsed the SQL during model parsing
2. We extract tables during parsing, but not functions
3. We re-parse SQL just to find function calls
4. Creates duplication with function body parsing logic

## Solution

Extract function calls **during model parsing** (like we do for tables), then read them during graph building.

### Current Flow (Inefficient)
```
Model SQL → Parse → Extract tables only
                ↓
         Graph Building → Re-parse SQL → Extract functions
```

### Improved Flow (Efficient)
```
Model SQL → Parse → Extract tables AND functions
                ↓
         Graph Building → Read pre-extracted data
```

## Implementation

### Step 1: Update `SQLParser` to Extract Function Calls

Modify `SQLParser._parse_sqlglot_expression()` to extract function calls alongside tables:

```python
# Extract table references
source_tables = []
for table in expr.find_all(sqlglot.exp.Table):
    source_tables.append(table.name)

# Extract function calls (NEW)
source_functions = []
for func_node in expr.find_all(sqlglot.exp.Function):
    func_name = func_node.name if hasattr(func_node, "name") else str(func_node)
    if func_name and func_name.lower() not in SQL_BUILT_IN_FUNCTIONS:
        source_functions.append(func_name)

# Store in code_data
code_data = {
    "sql": {
        "original_sql": sql_content.strip(),
        "resolved_sql": resolved_sql,
        "operation_type": expr.key if hasattr(expr, "key") else "unknown",
        "source_tables": source_tables,
        "source_functions": source_functions,  # NEW
    }
}
```

### Step 2: Update `DependencyGraphBuilder` to Read Pre-extracted Functions

Replace `_extract_function_dependencies_from_sql()` with simple data reading:

```python
# Get dependencies from the parsed tables
code_data = model_info.get("code", {})
if code_data and "sql" in code_data:
    source_tables = code_data["sql"].get("source_tables", [])
    for referenced_table in source_tables:
        # ... resolve table references
    
    # Read pre-extracted function calls (NEW - no re-parsing!)
    source_functions = code_data["sql"].get("source_functions", [])
    for func_ref in source_functions:
        full_func_name = table_resolver.resolve_function_reference(
            func_ref, parsed_functions
        )
        if full_func_name:
            table_deps.add(full_func_name)
```

### Step 3: Remove `_extract_function_dependencies_from_sql()` Method

Delete the entire method (~70 lines) from `DependencyGraphBuilder`.

## Benefits

1. **Consistency**: Function extraction works like table extraction
2. **Performance**: Parse once, use many times
3. **Simplicity**: Graph builder just reads data, no parsing logic
4. **Maintainability**: One place to extract functions (during parsing)
5. **Code Reduction**: Remove ~70 lines from dependency_graph.py

## Shared Utility

The function extraction logic can be shared:

- `tee/parser/shared/function_extraction.py`:
  - `extract_function_calls_from_sqlglot(expr)` - Extract from SQLglot AST
  - `extract_function_calls_from_regex(sql_content)` - Fallback regex extraction
  - Used by both `SQLParser` and `FunctionSQLParser`

## Updated File Sizes

After this change:
- `dependency_graph.py`: **523 → ~450 lines** (-73 lines)
- `sql_parser.py`: **~245 → ~260 lines** (+15 lines)
- New shared utility: **~60 lines**

**Net reduction**: ~13 lines, but much cleaner architecture!

