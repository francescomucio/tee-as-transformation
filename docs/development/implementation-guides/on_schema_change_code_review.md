# Code Review: on_schema_change Implementation Plan

## Review Date
Current codebase state review after recent changes

## Summary

The implementation plan for `on_schema_change` is **still valid and makes sense**, but there are some adjustments needed based on existing infrastructure. The codebase has good foundations that can be leveraged.

## Current State Analysis

### ✅ What Already Exists

1. **Type System** (`tee/typing/metadata.py`)
   - `IncrementalConfig` TypedDict exists
   - Strategy configs (`IncrementalAppendConfig`, `IncrementalMergeConfig`, etc.) are well-defined
   - **Missing**: `on_schema_change` field in `IncrementalConfig`

2. **Incremental Executor** (`tee/engine/materialization/incremental_executor.py`)
   - Three strategy methods exist: `execute_append_strategy`, `execute_merge_strategy`, `execute_delete_insert_strategy`
   - Good structure for adding schema change handling
   - **Missing**: `on_schema_change` parameter in strategy methods

3. **Materialization Handler** (`tee/engine/materialization/materialization_handler.py`)
   - Extracts incremental config from metadata
   - Calls executor methods with proper parameters
   - **Missing**: Extraction and passing of `on_schema_change` value

4. **Database Adapters**
   - ✅ `get_table_info(table_name)` - Returns `{"schema": [...], "row_count": ...}`
     - Available in: Snowflake, DuckDB, PostgreSQL, BigQuery
   - ✅ `get_table_columns(table_name)` - Returns `list[str]` of column names
     - Available in: Snowflake, DuckDB
   - ✅ `table_exists(table_name)` - Checks if table exists
   - ❌ `add_column()` - **Does not exist** (needs to be added)
   - ❌ `drop_column()` - **Does not exist** (needs to be added)

5. **Schema Inference** (`tee/parser/output/ots/inferencers/schema_inferencer.py`)
   - `SchemaInferencer` class exists
   - Uses sqlglot to infer schema from SQL
   - Currently used for OTS output generation
   - **Can be adapted/reused** for runtime schema inference

6. **dbt Importer** (`tee/importer/dbt/converters/metadata_converter.py`)
   - `_convert_incremental_config()` method exists
   - Currently extracts `on_schema_change` but only warns about it
   - **Needs update**: Convert instead of warn

## Implementation Plan Validation

### ✅ Step 1: Update Type Definitions
**Status**: Still needed, straightforward

**Current state**: `IncrementalConfig` doesn't have `on_schema_change` field

**Action needed**:
```python
# Add to tee/typing/metadata.py
OnSchemaChange = Literal["ignore", "append_new_columns", "sync_all_columns", "fail"]

class IncrementalConfig(TypedDict):
    strategy: IncrementalStrategy
    on_schema_change: NotRequired[OnSchemaChange]  # NEW
    append: NotRequired[IncrementalAppendConfig | None]
    # ... rest
```

### ✅ Step 2: Create Schema Comparison Module
**Status**: Needed, can leverage existing infrastructure

**Existing infrastructure**:
- `adapter.get_table_info()` returns schema info
- `adapter.get_table_columns()` returns column names (some adapters)
- `SchemaInferencer` can infer schema from SQL

**Implementation approach**:
1. **For query schema inference**: 
   - Option A: Execute `SELECT * FROM (query) LIMIT 0` and use result metadata
   - Option B: Adapt `SchemaInferencer` for runtime use
   - Option C: Use database-specific `DESCRIBE` or `EXPLAIN` commands
   
2. **For table schema**: 
   - Use `adapter.get_table_info()` which returns `{"schema": [{"column": "...", "type": "..."}], ...}`
   - May need to normalize format across adapters

**Recommendation**: Start with Option A (LIMIT 0 query) as it's most reliable and database-agnostic.

### ✅ Step 3: Create Schema Change Handler
**Status**: Needed, straightforward

**Note**: Will need to add `add_column()` and `drop_column()` methods to adapters first, or generate DDL directly.

### ✅ Step 4: Integrate into Incremental Executor
**Status**: Still needed, good integration points exist

**Current method signatures**:
```python
def execute_append_strategy(
    self,
    model_name: str,
    sql_query: str,
    config: IncrementalAppendConfig,
    adapter: DatabaseAdapter,
    table_name: str,
    variables: dict[str, Any] | None = None,
) -> None:
```

**Needs**: Add `on_schema_change: OnSchemaChange | None = None` parameter

**Integration point**: Before executing the strategy (after filtering SQL, before adapter call)

### ✅ Step 5: Update Materialization Handler
**Status**: Still needed, clear integration point

**Current code** (line 91):
```python
incremental_config = metadata.get("incremental") if metadata else None
```

**Needs**: Extract `on_schema_change` and pass to executor methods

### ✅ Step 6: Update dbt Importer
**Status**: Still needed, simple change

**Current code** (lines 302-308):
```python
# Warn about unsupported config options
if on_schema_change:
    warnings.append(
        f"on_schema_change config ({on_schema_change}) is not supported in t4t. "
        "Schema changes will need to be handled manually. "
        "See https://github.com/francescomucio/open-transformation-specification/issues/2"
    )
```

**Needs**: Convert instead of warn:
```python
if on_schema_change:
    result["on_schema_change"] = on_schema_change
```

## Key Findings & Recommendations

### 1. Schema Inference Strategy

**Finding**: Multiple options exist for inferring query schema.

**Recommendation**: 
- **Primary**: Execute `SELECT * FROM (query) LIMIT 0` and use cursor/result metadata
  - Most reliable across databases
  - Works with any SQL query
  - Gets actual database types
  
- **Fallback**: Use `SchemaInferencer` for static analysis
  - Good for development/debugging
  - May not handle complex queries well

### 2. Adapter Methods

**Finding**: `get_table_info()` exists but returns different formats per adapter:
- Snowflake: `{"schema": [{"column": "...", "type": "..."}], "row_count": ...}`
- DuckDB: `{"schema": [{"column": "...", "type": "..."}], "row_count": ...}`
- PostgreSQL: `{"schema": [{"column": "...", "type": "..."}], "row_count": ...}`
- BigQuery: `{"schema": [{"column": "...", "type": "..."}], "row_count": ...}`

**Good news**: Format is consistent! Schema comparison can use `get_table_info()["schema"]`.

**Missing**: `add_column()` and `drop_column()` methods don't exist.

**Options**:
1. Add abstract methods to `DatabaseAdapter` base class
2. Generate DDL directly in `SchemaChangeHandler` (database-specific)
3. Hybrid: Add methods but with default DDL generation

**Recommendation**: Option 3 (Hybrid)
- Add abstract methods to base class
- Implement in each adapter (Snowflake, DuckDB, etc.)
- Provide default implementation that generates DDL if adapter doesn't override

### 3. Schema Comparison Timing

**Finding**: Schema comparison should happen:
- **After** filtering SQL (for incremental strategies)
- **Before** executing the incremental operation
- **Only if** table exists

**Current flow in executor**:
1. Get state
2. Generate time filter
3. Apply filter to SQL
4. Execute strategy

**New flow should be**:
1. Get state
2. Generate time filter
3. Apply filter to SQL
4. **NEW: Check schema changes (if table exists and on_schema_change is set)**
5. Execute strategy

**Integration point**: Right before calling `adapter.execute_incremental_*` methods.

### 4. Error Handling

**Finding**: Current code has good error handling patterns.

**Recommendation**: 
- Schema comparison failures should log warning and continue with `ignore` behavior
- DDL execution failures should raise clear errors
- `fail` behavior should raise `ValueError` with detailed message

### 5. Performance Considerations

**Finding**: Schema comparison adds overhead:
- Query execution for schema inference (LIMIT 0 is fast)
- Table schema query (already fast via `get_table_info()`)
- Comparison logic (negligible)

**Recommendation**:
- Only run schema comparison if:
  - Table exists
  - `on_schema_change` is set and not `"ignore"`
- Cache schema comparison results if possible (within single execution)

## Updated Implementation Checklist

### Phase 1: Foundation
- [ ] Add `OnSchemaChange` type to `metadata.py`
- [ ] Add `on_schema_change` field to `IncrementalConfig`
- [ ] Update dbt importer to convert `on_schema_change` (remove warning)

### Phase 2: Schema Infrastructure
- [ ] Create `schema_comparator.py` module
  - [ ] Implement `infer_query_schema()` using LIMIT 0 approach
  - [ ] Implement `get_table_schema()` using `adapter.get_table_info()`
  - [ ] Implement `compare_schemas()` logic
- [ ] Create `schema_change_handler.py` module
  - [ ] Implement `handle_schema_changes()` with all 4 behaviors
  - [ ] Implement DDL generation for `add_column` and `drop_column`

### Phase 3: Adapter Methods
- [ ] Add `add_column()` abstract method to `DatabaseAdapter`
- [ ] Add `drop_column()` abstract method to `DatabaseAdapter`
- [ ] Implement in Snowflake adapter
- [ ] Implement in DuckDB adapter
- [ ] Implement in PostgreSQL adapter
- [ ] Implement in BigQuery adapter
- [ ] Add default DDL generation fallback

### Phase 4: Integration
- [ ] Update `IncrementalExecutor.execute_append_strategy()` to accept `on_schema_change`
- [ ] Update `IncrementalExecutor.execute_merge_strategy()` to accept `on_schema_change`
- [ ] Update `IncrementalExecutor.execute_delete_insert_strategy()` to accept `on_schema_change`
- [ ] Add schema change handling before strategy execution
- [ ] Update `MaterializationHandler._execute_incremental_materialization()` to extract and pass `on_schema_change`

### Phase 5: Testing
- [ ] Unit tests for `SchemaComparator`
- [ ] Unit tests for `SchemaChangeHandler`
- [ ] Integration tests for each `on_schema_change` behavior
- [ ] Database-specific tests for DDL generation
- [ ] Test with dbt importer conversion

## Conclusion

**The implementation plan is still valid and well-structured.** The codebase has good foundations:

✅ Existing adapter methods (`get_table_info`, `get_table_columns`)  
✅ Schema inference infrastructure (`SchemaInferencer`)  
✅ Clean separation of concerns (executor, handler, adapters)  
✅ Good error handling patterns  

**Main adjustments needed:**
1. Leverage existing `get_table_info()` instead of creating new methods
2. Add `add_column()` and `drop_column()` to adapters
3. Use LIMIT 0 query approach for schema inference (most reliable)
4. Integrate schema checking at the right point in execution flow

The plan can proceed as outlined with these minor adjustments.


