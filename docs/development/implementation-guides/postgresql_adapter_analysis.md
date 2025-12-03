# PostgreSQL Adapter Analysis and Comparison

## Folder Structure Comparison

### DuckDB Adapter Structure
```
tee/adapters/duckdb/
├── __init__.py
├── adapter.py
├── functions/
│   ├── __init__.py
│   └── function_manager.py
├── materialization/
│   ├── __init__.py
│   ├── incremental_handler.py
│   ├── table_handler.py
│   └── view_handler.py
└── utils/
    ├── __init__.py
    └── helpers.py
```

### Snowflake Adapter Structure
```
tee/adapters/snowflake/
├── __init__.py
├── adapter.py
├── functions/
│   ├── __init__.py
│   └── function_manager.py
├── materialization/
│   ├── __init__.py
│   ├── incremental_handler.py
│   ├── table_handler.py
│   └── view_handler.py
├── tags/
│   ├── __init__.py
│   └── tag_manager.py
└── utils/
    ├── __init__.py
    └── helpers.py
```

### PostgreSQL Adapter Structure (Current)
```
tee/adapters/postgresql/
├── __init__.py
└── adapter.py
```

**Missing in PostgreSQL:**
- ❌ `materialization/` directory
- ❌ `utils/` directory
- ❌ `functions/` directory (optional, but DuckDB/Snowflake have it)

## Component Manager Comparison

### DuckDB Adapter
```python
def __init__(self, config: AdapterConfig) -> None:
    super().__init__(config)
    
    # Component managers
    self.function_manager = FunctionManager(self)
    self.table_handler = TableHandler(self)
    self.view_handler = ViewHandler(self)
    self.incremental_handler = IncrementalHandler(self)
    self.utils = DuckDBUtils(self)
```

### Snowflake Adapter
```python
def __init__(self, config_dict: dict[str, Any]) -> None:
    super().__init__(config_dict)
    
    # Component managers
    self.tag_manager = TagManager(self)
    self.function_manager = FunctionManager(self)
    self.table_handler = TableHandler(self)
    self.view_handler = ViewHandler(self)
    self.incremental_handler = IncrementalHandler(self)
    self.utils = SnowflakeUtils(self)
```

### PostgreSQL Adapter (Current)
```python
def __init__(self, config: AdapterConfig) -> None:
    super().__init__(config)
    # ❌ No component managers initialized
```

**Missing in PostgreSQL:**
- ❌ `incremental_handler` - No incremental materialization support
- ❌ `table_handler` - Table creation is inline in adapter
- ❌ `view_handler` - View creation is inline in adapter
- ❌ `utils` - No utility helpers
- ❌ `function_manager` - Function management is inline

## Method Comparison

### Schema Change Methods (on_schema_change support)

| Method | DuckDB | Snowflake | PostgreSQL |
|--------|--------|-----------|------------|
| `describe_query_schema()` | ✅ Implemented | ✅ Implemented | ❌ NotImplementedError |
| `add_column()` | ✅ Implemented | ✅ Implemented | ❌ NotImplementedError |
| `drop_column()` | ✅ Implemented | ✅ Implemented | ❌ NotImplementedError |

### Incremental Materialization Methods

| Method | DuckDB | Snowflake | PostgreSQL |
|--------|--------|-----------|------------|
| `execute_incremental_append()` | ✅ Via IncrementalHandler | ✅ Via IncrementalHandler | ❌ Missing |
| `execute_incremental_merge()` | ✅ Via IncrementalHandler | ✅ Via IncrementalHandler | ❌ Missing |
| `execute_incremental_delete_insert()` | ✅ Via IncrementalHandler | ✅ Via IncrementalHandler | ❌ Missing |

### Table/View Management

| Method | DuckDB | Snowflake | PostgreSQL |
|--------|--------|-----------|------------|
| `create_table()` | ✅ Via TableHandler | ✅ Via TableHandler | ✅ Inline implementation |
| `create_view()` | ✅ Via ViewHandler | ✅ Via ViewHandler | ✅ Inline implementation |
| `table_exists()` | ✅ Implemented | ✅ Implemented (tables + views) | ✅ Implemented (tables only) |
| `get_table_info()` | ✅ Implemented | ✅ Implemented | ✅ Implemented |

### Other Methods

| Method | DuckDB | Snowflake | PostgreSQL |
|--------|--------|-----------|------------|
| `create_function()` | ✅ Via FunctionManager | ✅ Via FunctionManager | ✅ Inline implementation |
| `function_exists()` | ✅ Implemented | ✅ Implemented | ✅ Implemented |
| `drop_function()` | ✅ Implemented | ✅ Implemented | ✅ Implemented |

## Key Differences

### 1. **Architecture Pattern**

**DuckDB/Snowflake**: Modular architecture with separate handlers
- Separation of concerns
- Easier to test and maintain
- Reusable components

**PostgreSQL**: Monolithic architecture
- All logic in `adapter.py`
- Harder to maintain as it grows
- Less modular

### 2. **Incremental Materialization Support**

**DuckDB/Snowflake**: ✅ Full support
- `IncrementalHandler` class
- Methods: `execute_append()`, `execute_merge()`, `execute_delete_insert()`
- Adapter methods delegate to handlers

**PostgreSQL**: ❌ No support
- No `IncrementalHandler`
- No incremental methods
- Cannot use incremental materialization

### 3. **Schema Change Support**

**DuckDB/Snowflake**: ✅ Full support
- `describe_query_schema()` - Uses DESCRIBE or temporary views
- `add_column()` - ALTER TABLE ADD COLUMN
- `drop_column()` - ALTER TABLE DROP COLUMN

**PostgreSQL**: ❌ No support
- All three methods raise `NotImplementedError`
- Cannot use `on_schema_change` feature

### 4. **Table Existence Check**

**DuckDB**: Checks `information_schema.tables` only

**Snowflake**: Checks both `information_schema.tables` AND `information_schema.views`
- Updated to support views

**PostgreSQL**: Checks `information_schema.tables` only
- Should be updated to also check views (like Snowflake)

### 5. **Schema Filtering in Queries**

**DuckDB**: Simple table name check

**Snowflake**: Filters by both `table_schema` and `table_name`
- Properly handles schema-qualified table names

**PostgreSQL**: Only filters by `table_name` (missing schema filter)
- `get_table_info()` doesn't filter by schema
- `table_exists()` doesn't filter by schema
- Could cause issues with multiple schemas

## What Needs to be Implemented for PostgreSQL

### Priority 1: Schema Change Methods (Required for on_schema_change)
1. ✅ `describe_query_schema()` - Use PostgreSQL's approach (LIMIT 0 or EXPLAIN)
2. ✅ `add_column()` - `ALTER TABLE ... ADD COLUMN`
3. ✅ `drop_column()` - `ALTER TABLE ... DROP COLUMN`

### Priority 2: Incremental Materialization (Optional but Recommended)
1. Create `materialization/incremental_handler.py`
2. Implement `execute_incremental_append()`
3. Implement `execute_incremental_merge()`
4. Implement `execute_incremental_delete_insert()`
5. Add adapter methods that delegate to handler

### Priority 3: Refactoring (Optional but Recommended)
1. Create `materialization/table_handler.py` - Extract table creation logic
2. Create `materialization/view_handler.py` - Extract view creation logic
3. Create `utils/helpers.py` - Extract utility methods
4. Update `table_exists()` to check views (like Snowflake)
5. Fix `get_table_info()` to filter by schema (like Snowflake)

### Priority 4: Bug Fixes
1. Fix `get_table_info()` - Add schema filtering
2. Fix `table_exists()` - Add schema filtering and view support

## Implementation Approach for PostgreSQL

### Option A: Minimal (Just Schema Change Methods)
- Implement only the 3 required methods inline in `adapter.py`
- Quick to implement
- Keeps current architecture

### Option B: Full Refactoring (Recommended)
- Create modular structure like DuckDB/Snowflake
- Extract handlers and utilities
- Implement incremental materialization
- Better long-term maintainability

## PostgreSQL-Specific Considerations

1. **Schema Inference**: PostgreSQL can use:
   - `EXPLAIN (FORMAT JSON) SELECT * FROM (query) LIMIT 0` - Gets query plan with schema
   - `CREATE TEMP TABLE ... AS SELECT * FROM (query) LIMIT 0; DESCRIBE temp_table` - Similar to Snowflake
   - Direct query with LIMIT 0 and cursor description

2. **Column Operations**: PostgreSQL supports:
   - `ALTER TABLE ... ADD COLUMN` - Standard SQL
   - `ALTER TABLE ... DROP COLUMN` - Standard SQL (with CASCADE if needed)

3. **Schema Filtering**: PostgreSQL `information_schema` requires:
   - `table_schema` filter (defaults to 'public' if not specified)
   - Case-sensitive matching (unlike Snowflake which is uppercase)

4. **Transaction Management**: PostgreSQL requires explicit commits:
   - All DDL operations need `connection.commit()`
   - Already handled in current implementation

## Recommendations

1. **Start with Priority 1** (schema change methods) - Required for `on_schema_change`
2. **Fix schema filtering bugs** in `get_table_info()` and `table_exists()`
3. **Consider Option B** (full refactoring) for better maintainability
4. **Follow DuckDB pattern** as it's simpler than Snowflake (no tags)


