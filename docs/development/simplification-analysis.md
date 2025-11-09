# Simplification Analysis: After Adding source_functions to OTS

## Key Question: Can We Simplify the Two Big Modules?

**Answer: Partially YES - we can simplify `dependency_graph.py`, but `function_sql_parser.py` still needs dependency extraction (different use case).**

## Two Different Use Cases

### 1. Extracting Dependencies FROM Function Bodies
**Location**: `FunctionSQLParser._extract_dependencies()`
**Purpose**: Extract what a **function depends on** (tables and other functions)
**Used when**: Parsing function definitions
**Example**: 
```sql
CREATE FUNCTION my_func() AS $$
  SELECT * FROM users WHERE helper_func(status) = 'active'
$$;
```
This extracts: `tables: ["users"]`, `functions: ["helper_func"]`

**Status**: ✅ **STILL NEEDED** - This is different from model SQL extraction

### 2. Extracting Function Calls FROM Model SQL
**Location**: `DependencyGraphBuilder._extract_function_dependencies_from_sql()`
**Purpose**: Extract what **models depend on** (which functions models call)
**Used when**: Building dependency graph for models
**Example**:
```sql
SELECT id, analytics.calculate_percentage(amount, total) FROM orders;
```
This extracts: `functions: ["analytics.calculate_percentage"]`

**Status**: ❌ **CAN BE REMOVED** - Will be extracted during model parsing instead

## Simplification Opportunities

### ✅ Can Simplify: `dependency_graph.py`

**Current** (lines 97-103):
```python
# Extract function dependencies from model SQL
model_function_deps = self._extract_function_dependencies_from_sql(
    code_data["sql"].get("resolved_sql", ""),
    parsed_functions,
    table_resolver,
)
table_deps.update(model_function_deps)
```

**After** (simplified):
```python
# Read pre-extracted function dependencies (like source_tables)
source_functions = code_data["sql"].get("source_functions", [])
for func_ref in source_functions:
    full_func_name = table_resolver.resolve_function_reference(
        func_ref, parsed_functions
    )
    if full_func_name:
        table_deps.add(full_func_name)
```

**Benefits**:
- ✅ Remove `_extract_function_dependencies_from_sql()` method (~70 lines)
- ✅ No SQL re-parsing needed
- ✅ Consistent with how `source_tables` is handled
- ✅ Simpler, cleaner code

**File size reduction**: `dependency_graph.py`: 523 → ~450 lines (-73 lines)

### ❌ Cannot Simplify: `function_sql_parser.py`

**Why**: `_extract_dependencies()` serves a different purpose:
- It extracts dependencies **FROM function bodies** (what functions depend on)
- This is needed to build the dependency graph for functions
- Different from extracting function calls from model SQL

**However**, we can still improve it:
- Share function extraction logic with `SQLParser` (reduce duplication)
- Extract to shared utility module

## Code Sharing Opportunity

Both `SQLParser` and `FunctionSQLParser` need to extract function calls from SQL. We can:

1. **Create shared utility**: `tee/parser/shared/function_extraction.py`
   - `extract_function_calls_from_sqlglot(expr)` - Extract from SQLglot AST
   - `extract_function_calls_from_regex(sql_content)` - Fallback regex extraction
   - Used by both parsers

2. **Benefits**:
   - Remove duplication between parsers
   - Single place to maintain function extraction logic
   - Consistent behavior across parsers

## Summary

| Module | Method | Can Remove? | Why |
|--------|--------|-------------|-----|
| `dependency_graph.py` | `_extract_function_dependencies_from_sql()` | ✅ **YES** ✅ **DONE** | Function calls extracted during model parsing |
| `function_sql_parser.py` | `_extract_dependencies()` | ❌ **NO** | Extracts dependencies FROM function bodies (different use case) |

## Implementation Plan

1. **Update `SQLParser`** to extract function calls during parsing
   - Add `source_functions` extraction alongside `source_tables`
   - Store in `code_data["sql"]["source_functions"]`

2. **Simplify `DependencyGraphBuilder`**
   - Remove `_extract_function_dependencies_from_sql()` method
   - Read `source_functions` from pre-extracted data (like `source_tables`)

3. **Extract shared utility** (optional but recommended)
   - Create `function_extraction.py` with shared logic
   - Use by both `SQLParser` and `FunctionSQLParser`

4. **Result**:
   - `dependency_graph.py`: 523 → ~450 lines (-73 lines) ✅
   - `function_sql_parser.py`: 618 → ~600 lines (can extract shared utility)
   - Cleaner, more maintainable code

