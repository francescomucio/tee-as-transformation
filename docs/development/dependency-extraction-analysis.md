# Dependency Extraction Analysis

## Key Finding: They Are NOT the Same!

The two methods serve **different purposes**:

### 1. `FunctionSQLParser._extract_dependencies()` 
**Location**: `function_sql_parser.py:544`
**Purpose**: Extract dependencies **FROM function bodies** (what functions depend on)
**Used when**: Parsing a function definition
**Extracts**: 
- Tables that the function references (FROM/JOIN clauses)
- Functions that the function calls
**Stored in**: Function metadata during parsing

### 2. `DependencyGraphBuilder._extract_function_dependencies_from_sql()`
**Location**: `dependency_graph.py:419`
**Purpose**: Extract function dependencies **FROM model SQL** (what models depend on functions)
**Used when**: Building dependency graph for models
**Extracts**: 
- Functions that models call in their SQL
**Stored in**: Model dependencies in the graph

## Comparison with Models

### How Model Dependencies Work:

1. **During Parsing** (`SQLParser._parse_sqlglot_expression()`):
   ```python
   # Extract table references during parsing
   source_tables = []
   for table in expr.find_all(sqlglot.exp.Table):
       source_tables.append(table.name)
   ```
   - Extracted **once** during model parsing
   - Stored in `code_data["sql"]["source_tables"]`

2. **During Graph Building** (`DependencyGraphBuilder.build_graph()`):
   ```python
   # Just read the pre-extracted data
   source_tables = code_data["sql"].get("source_tables", [])
   ```
   - **No re-parsing** - just reads pre-extracted data
   - **No duplication**

### How Function Dependencies Work:

1. **During Function Parsing** (`FunctionSQLParser._extract_dependencies()`):
   ```python
   # Extract dependencies FROM function body
   dependencies = {"tables": [], "functions": []}
   # ... extract tables and functions from function_body
   ```
   - Extracted **once** during function parsing
   - Stored in function metadata

2. **During Graph Building** (`DependencyGraphBuilder.build_graph()`):
   ```python
   # Read pre-extracted function dependencies
   deps_dict = function_metadata.get("dependencies", {})
   table_refs = deps_dict.get("tables", [])
   function_refs = deps_dict.get("functions", [])
   ```
   - **No re-parsing** for function dependencies
   - Uses pre-extracted data

3. **For Model → Function Dependencies** (`DependencyGraphBuilder._extract_function_dependencies_from_sql()`):
   ```python
   # Extract function calls FROM model SQL
   model_function_deps = self._extract_function_dependencies_from_sql(
       code_data["sql"].get("resolved_sql", ""),
       parsed_functions,
       table_resolver,
   )
   ```
   - **Re-parses model SQL** to find function calls
   - This is **different** from function body parsing

## The Issue: Duplication in Function Extraction Logic

While the **use cases are different**, there IS code duplication in the **function extraction logic**:

### Duplicated Code:

1. **Regex pattern for function calls**:
   - `function_sql_parser.py:569`: `r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*\("`
   - `dependency_graph.py:474`: `r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*\("`

2. **Built-in function filtering**:
   - Both use `SQL_BUILT_IN_FUNCTIONS` (now shared constant ✅)

3. **Function name resolution**:
   - Both try to resolve function references
   - Both handle qualified names (schema.function_name)

## Why This Happened

1. **Function dependencies weren't extracted during model parsing** (unlike table dependencies)
2. **Model SQL parsing doesn't extract function calls** - only tables
3. **We need to extract function calls from model SQL** during graph building
4. **This creates duplication** with function body parsing logic

## Solution: Extract Function Calls During Model Parsing

### Current Flow (with duplication):
```
Model SQL → Parse → Extract tables only
                ↓
         Graph Building → Re-parse SQL → Extract functions
```

### Better Flow (no duplication):
```
Model SQL → Parse → Extract tables AND functions
                ↓
         Graph Building → Use pre-extracted data
```

## Recommendation

**Extract function calls during model parsing** (like we do for tables):

1. **Modify `SQLParser._parse_sqlglot_expression()`**:
   - Add function call extraction alongside table extraction
   - Store in `code_data["sql"]["source_functions"]`

2. **Update `DependencyGraphBuilder.build_graph()`**:
   - Read `source_functions` from model data (like `source_tables`)
   - Remove `_extract_function_dependencies_from_sql()` method

3. **Benefits**:
   - ✅ No duplication
   - ✅ Consistent with table dependency extraction
   - ✅ Faster (no re-parsing)
   - ✅ Simpler code

## Updated Refactoring Proposal

Since function extraction should happen during parsing (not graph building), the refactoring should be:

1. **Extract shared function extraction utility**:
   - `tee/parser/shared/function_extraction.py`
   - Contains regex patterns and filtering logic
   - Used by both `SQLParser` and `FunctionSQLParser`

2. **Update `SQLParser`** to extract functions during parsing

3. **Remove `_extract_function_dependencies_from_sql()`** from `DependencyGraphBuilder`

4. **Update graph builder** to read pre-extracted function dependencies

