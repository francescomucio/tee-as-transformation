# Snowflake Adapter Refactoring - 3 Approaches

## Current State Analysis

**File:** `tee/adapters/snowflake/adapter.py`
- **Lines:** 1,124
- **Methods:** 32
- **Longest methods:**
  - `attach_object_tags`: 88 lines
  - `attach_tags`: 85 lines
  - `_build_view_with_column_comments`: 65 lines
  - `create_function`: 60 lines
  - `function_exists`: 60 lines

**Key Responsibilities:**
1. Connection management (connect, disconnect, execute_query)
2. Table operations (create_table, table_exists, drop_table, get_table_info, get_table_columns)
3. View operations (create_view, _build_view_with_column_comments)
4. Function operations (create_function, function_exists, drop_function)
5. Tag operations (attach_tags, attach_object_tags) - **Very long methods**
6. Incremental operations (execute_incremental_append, execute_incremental_merge, execute_incremental_delete_insert, _generate_merge_sql)
7. Schema operations (_create_schema_if_needed)
8. Utility methods (_qualify_object_name, qualify_table_references, _execute_with_cursor, _add_table_comment, _add_column_comments)
9. Test generation (generate_no_duplicates_test_query)

**Issues:**
- Single class with too many responsibilities
- Tag management methods are very long (85-88 lines each)
- View creation logic is complex and could be isolated
- Function management mixed with other operations
- Incremental operations could be grouped together

---

## Approach 1: Feature-Based Split (Recommended ⭐)

**Philosophy:** Split by feature/concern - each module handles one feature area.

### Structure

```
tee/adapters/snowflake/
├── adapter.py                    # Main orchestrator (~400 lines)
├── connection.py                 # Connection management (~100 lines)
├── materialization/
│   ├── __init__.py
│   ├── table_handler.py          # Table operations (~150 lines)
│   ├── view_handler.py           # View operations (~150 lines)
│   └── incremental_handler.py    # Incremental operations (~200 lines)
├── functions/
│   ├── __init__.py
│   └── function_manager.py       # Function operations (~150 lines)
├── tags/
│   ├── __init__.py
│   └── tag_manager.py            # Tag operations (~200 lines)
└── utils/
    ├── __init__.py
    └── helpers.py                # Utility methods (~100 lines)
```

### Details

**1. `adapter.py` (Main Orchestrator)**
```python
class SnowflakeAdapter(DatabaseAdapter):
    """Snowflake database adapter with SQLglot integration."""
    
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__(config_dict)
        self.connection_manager = SnowflakeConnectionManager(self.config)
        self.table_handler = TableHandler(self)
        self.view_handler = ViewHandler(self)
        self.incremental_handler = IncrementalHandler(self)
        self.function_manager = FunctionManager(self)
        self.tag_manager = TagManager(self)
        self.utils = SnowflakeUtils(self)
    
    def create_table(self, table_name, sql_query, metadata):
        return self.table_handler.create(table_name, sql_query, metadata)
    
    def create_view(self, view_name, query, metadata):
        return self.view_handler.create(view_name, query, metadata)
    
    def attach_tags(self, object_type, object_name, tags):
        return self.tag_manager.attach_tags(object_type, object_name, tags)
    
    def attach_object_tags(self, object_type, object_name, object_tags):
        return self.tag_manager.attach_object_tags(object_type, object_name, object_tags)
    
    def create_function(self, function_name, function_sql, metadata):
        return self.function_manager.create(function_name, function_sql, metadata)
    
    # ... delegate other methods
```

**2. `connection.py`**
```python
class SnowflakeConnectionManager:
    """Manages Snowflake connection lifecycle."""
    
    def connect(self, config):
        # Connection logic
    
    def disconnect(self, connection):
        # Disconnect logic
    
    def execute_query(self, connection, query):
        # Query execution logic
```

**3. `materialization/table_handler.py`**
```python
class TableHandler:
    """Handles table creation and management."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def create(self, table_name, sql_query, metadata):
        # Table creation logic
        # Tag attachment via adapter.tag_manager
```

**4. `materialization/view_handler.py`**
```python
class ViewHandler:
    """Handles view creation with column comments."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def create(self, view_name, query, metadata):
        # View creation logic
        # Uses _build_view_with_column_comments
```

**5. `materialization/incremental_handler.py`**
```python
class IncrementalHandler:
    """Handles incremental materialization strategies."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def execute_append(self, table_name, sql_query):
        # Append strategy
    
    def execute_merge(self, table_name, sql_query, config):
        # Merge strategy
    
    def execute_delete_insert(self, table_name, sql_query, config):
        # Delete-insert strategy
    
    def _generate_merge_sql(self, target_table, source_query, unique_keys):
        # Merge SQL generation
```

**6. `functions/function_manager.py`**
```python
class FunctionManager:
    """Manages function creation and existence checking."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def create(self, function_name, function_sql, metadata):
        # Function creation logic
    
    def exists(self, function_name, signature):
        # Function existence checking
    
    def drop(self, function_name):
        # Function dropping
```

**7. `tags/tag_manager.py`**
```python
class TagManager:
    """Manages tag attachment for Snowflake objects."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def attach_tags(self, object_type, object_name, tags):
        # Tag attachment logic (85 lines)
    
    def attach_object_tags(self, object_type, object_name, object_tags):
        # Object tag attachment logic (88 lines)
    
    def _sanitize_tag_name(self, tag_name):
        # Tag name sanitization
    
    def _create_tag_if_needed(self, tag_name):
        # Tag creation logic
```

**8. `utils/helpers.py`**
```python
class SnowflakeUtils:
    """Utility methods for Snowflake operations."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def qualify_object_name(self, object_name):
        # Object name qualification
    
    def qualify_table_references(self, sql, schema):
        # Table reference qualification
    
    def create_schema_if_needed(self, object_name, schema_metadata):
        # Schema creation
    
    def add_table_comment(self, table_name, description):
        # Table comment addition
    
    def add_column_comments(self, table_name, column_descriptions):
        # Column comment addition
```

### Pros
- ✅ Clear separation by feature (tables, views, functions, tags, incremental)
- ✅ Easy to find code (tag logic in tag_manager, etc.)
- ✅ Reusable components (tag_manager can be used by all handlers)
- ✅ Testable: Each handler can be tested independently
- ✅ Scalable: Easy to add new features (e.g., materialized views)

### Cons
- ⚠️ More files to navigate
- ⚠️ Need to manage dependencies between handlers

### File Sizes After Refactoring
- `adapter.py`: ~400 lines
- `connection.py`: ~100 lines
- `materialization/table_handler.py`: ~150 lines
- `materialization/view_handler.py`: ~150 lines
- `materialization/incremental_handler.py`: ~200 lines
- `functions/function_manager.py`: ~150 lines
- `tags/tag_manager.py`: ~200 lines
- `utils/helpers.py`: ~100 lines

**Total:** ~1,450 lines (slight increase due to imports/docstrings, but much better organized)

---

## Approach 2: Component-Based Split

**Philosophy:** Split by component type - handlers, managers, utilities.

### Structure

```
tee/adapters/snowflake/
├── adapter.py                    # Main orchestrator (~300 lines)
├── handlers/
│   ├── __init__.py
│   ├── base.py                  # Base handler class (~50 lines)
│   ├── table_handler.py         # Table operations (~200 lines)
│   ├── view_handler.py          # View operations (~200 lines)
│   └── incremental_handler.py   # Incremental operations (~250 lines)
├── managers/
│   ├── __init__.py
│   ├── connection_manager.py    # Connection management (~150 lines)
│   ├── function_manager.py      # Function management (~200 lines)
│   └── tag_manager.py           # Tag management (~250 lines)
└── utils/
    ├── __init__.py
    └── helpers.py               # Utility methods (~150 lines)
```

### Details

**1. `adapter.py` (Main Orchestrator)**
```python
class SnowflakeAdapter(DatabaseAdapter):
    """Main Snowflake adapter orchestrator."""
    
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__(config_dict)
        self.connection_manager = ConnectionManager(self.config)
        self.table_handler = TableHandler(self)
        self.view_handler = ViewHandler(self)
        self.incremental_handler = IncrementalHandler(self)
        self.function_manager = FunctionManager(self)
        self.tag_manager = TagManager(self)
        self.utils = Helpers(self)
    
    # Delegate all methods to appropriate handlers/managers
```

**2. `handlers/base.py`**
```python
class BaseHandler(ABC):
    """Base class for operation handlers."""
    
    def __init__(self, adapter):
        self.adapter = adapter
        self.connection = adapter.connection
        self.config = adapter.config
        self.logger = adapter.logger
    
    @abstractmethod
    def create(self, name, sql, metadata):
        pass
```

**3. `handlers/table_handler.py`**
```python
class TableHandler(BaseHandler):
    """Handles table operations."""
    
    def create(self, table_name, sql_query, metadata):
        # Table creation logic
    
    def exists(self, table_name):
        # Table existence check
    
    def drop(self, table_name):
        # Table dropping
    
    def get_info(self, table_name):
        # Table info retrieval
```

**4. `managers/tag_manager.py`**
```python
class TagManager:
    """Manages tag operations for Snowflake."""
    
    def __init__(self, adapter):
        self.adapter = adapter
    
    def attach_tags(self, object_type, object_name, tags):
        # Tag attachment (85 lines)
    
    def attach_object_tags(self, object_type, object_name, object_tags):
        # Object tag attachment (88 lines)
```

### Pros
- ✅ Clear component boundaries (handlers, managers, utils)
- ✅ Easy to understand component roles
- ✅ Good for team organization
- ✅ Logical grouping (all handlers together, all managers together)

### Cons
- ⚠️ More nested structure (subdirectories)
- ⚠️ Some components might be thin (e.g., utils)
- ⚠️ Need to manage imports across subdirectories

### File Sizes After Refactoring
- `adapter.py`: ~300 lines
- `handlers/base.py`: ~50 lines
- `handlers/table_handler.py`: ~200 lines
- `handlers/view_handler.py`: ~200 lines
- `handlers/incremental_handler.py`: ~250 lines
- `managers/connection_manager.py`: ~150 lines
- `managers/function_manager.py`: ~200 lines
- `managers/tag_manager.py`: ~250 lines
- `utils/helpers.py`: ~150 lines

**Total:** ~1,750 lines

---

## Approach 3: Responsibility-Based Split

**Philosophy:** Split by responsibility - each module handles one type of responsibility.

### Structure

```
tee/adapters/snowflake/
├── adapter.py                    # Main orchestrator (~250 lines)
├── core/
│   ├── __init__.py
│   ├── connection.py             # Connection management (~150 lines)
│   └── execution.py              # Query execution (~100 lines)
├── ddl/
│   ├── __init__.py
│   ├── tables.py                 # Table DDL operations (~200 lines)
│   ├── views.py                  # View DDL operations (~200 lines)
│   └── functions.py              # Function DDL operations (~200 lines)
├── dml/
│   ├── __init__.py
│   └── incremental.py           # Incremental DML operations (~250 lines)
└── metadata/
    ├── __init__.py
    ├── tags.py                   # Tag management (~250 lines)
    └── comments.py               # Comment management (~100 lines)
```

### Details

**1. `adapter.py` (Main Orchestrator)**
```python
class SnowflakeAdapter(DatabaseAdapter):
    """Main Snowflake adapter."""
    
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__(config_dict)
        self.connection = ConnectionManager(self.config)
        self.executor = QueryExecutor(self)
        self.tables = TableDDL(self)
        self.views = ViewDDL(self)
        self.functions = FunctionDDL(self)
        self.incremental = IncrementalDML(self)
        self.tags = TagManager(self)
        self.comments = CommentManager(self)
```

**2. `core/connection.py`**
```python
class ConnectionManager:
    """Manages Snowflake connections."""
    
    def connect(self, config):
        # Connection logic
    
    def disconnect(self, connection):
        # Disconnect logic
```

**3. `ddl/tables.py`**
```python
class TableDDL:
    """Table DDL operations."""
    
    def create(self, table_name, sql_query, metadata):
        # CREATE TABLE logic
    
    def exists(self, table_name):
        # Table existence check
    
    def drop(self, table_name):
        # DROP TABLE logic
```

**4. `ddl/views.py`**
```python
class ViewDDL:
    """View DDL operations."""
    
    def create(self, view_name, query, metadata):
        # CREATE VIEW logic
    
    def _build_with_comments(self, view_name, query, metadata):
        # View with column comments
```

**5. `ddl/functions.py`**
```python
class FunctionDDL:
    """Function DDL operations."""
    
    def create(self, function_name, function_sql, metadata):
        # CREATE FUNCTION logic
    
    def exists(self, function_name, signature):
        # Function existence check
    
    def drop(self, function_name):
        # DROP FUNCTION logic
```

**6. `dml/incremental.py`**
```python
class IncrementalDML:
    """Incremental DML operations."""
    
    def execute_append(self, table_name, sql_query):
        # INSERT INTO logic
    
    def execute_merge(self, table_name, sql_query, config):
        # MERGE logic
    
    def execute_delete_insert(self, table_name, sql_query, config):
        # DELETE + INSERT logic
```

**7. `metadata/tags.py`**
```python
class TagManager:
    """Tag metadata management."""
    
    def attach_tags(self, object_type, object_name, tags):
        # Tag attachment (85 lines)
    
    def attach_object_tags(self, object_type, object_name, object_tags):
        # Object tag attachment (88 lines)
```

**8. `metadata/comments.py`**
```python
class CommentManager:
    """Comment metadata management."""
    
    def add_table_comment(self, table_name, description):
        # Table comment
    
    def add_column_comments(self, table_name, column_descriptions):
        # Column comments
```

### Pros
- ✅ Clear separation by SQL operation type (DDL vs DML vs metadata)
- ✅ Follows database operation categories
- ✅ Easy to understand for database developers
- ✅ Logical grouping (all DDL together, all DML together)

### Cons
- ⚠️ More abstract categorization
- ⚠️ Some operations don't fit cleanly (e.g., table_exists is DDL but also a query)
- ⚠️ More nested structure

### File Sizes After Refactoring
- `adapter.py`: ~250 lines
- `core/connection.py`: ~150 lines
- `core/execution.py`: ~100 lines
- `ddl/tables.py`: ~200 lines
- `ddl/views.py`: ~200 lines
- `ddl/functions.py`: ~200 lines
- `dml/incremental.py`: ~250 lines
- `metadata/tags.py`: ~250 lines
- `metadata/comments.py`: ~100 lines

**Total:** ~1,700 lines

---

## Comparison Table

| Aspect             | Approach 1 (Feature) | Approach 2 (Component) | Approach 3 (Responsibility) |
|--------------------|----------------------|------------------------|-----------------------------|
| **Maintainability**| ⭐⭐⭐⭐⭐             | ⭐⭐⭐⭐                  | ⭐⭐⭐                       |
| **Testability**    | ⭐⭐⭐⭐⭐             | ⭐⭐⭐⭐                  | ⭐⭐⭐                       |
| **Extensibility**  | ⭐⭐⭐⭐              | ⭐⭐⭐⭐⭐                 | ⭐⭐⭐                       |
| **Complexity**     | Low                  | Medium                 | Medium                      |
| **File Count**     | 8 files              | 9 files                | 9 files                     |
| **Max File Size**  | ~400 lines           | ~300 lines             | ~250 lines                  |
| **Code Duplication** | Minimal              | Minimal                | Minimal                     |
| **Best For**       | Most projects        | Large teams            | Database-focused teams      |

---

## Recommendation: Approach 1 (Feature-Based Split) ⭐

**Why Approach 1?**

1. **Clear Feature Boundaries**: Tables, views, functions, tags, and incremental operations are distinct features
2. **Natural Grouping**: Materialization operations (tables, views, incremental) naturally group together
3. **Simpler Structure**: Fewer nested directories, easier to navigate
4. **Better Testability**: Each feature handler can be tested independently
5. **Proven Pattern**: Similar to what we did with execution engine and OTS transformer (which worked well)
6. **Tag Manager Isolation**: The longest methods (tag operations) are isolated in their own module

**Key Benefits:**
- Tag management logic isolated in `tag_manager.py` (~200 lines)
- View creation logic isolated in `view_handler.py` (~150 lines)
- Function management isolated in `function_manager.py` (~150 lines)
- Incremental operations isolated in `incremental_handler.py` (~200 lines)
- Main adapter becomes a thin orchestrator (~400 lines)

**Implementation Order:**
1. Extract tag manager (no dependencies on other handlers)
2. Extract function manager (no dependencies on other handlers)
3. Extract view handler (uses tag manager)
4. Extract table handler (uses tag manager)
5. Extract incremental handler (uses table handler)
6. Extract connection manager (used by all)
7. Extract utils (used by all)
8. Refactor main adapter (becomes orchestrator)

---

## Additional Considerations

### Edge Cases to Handle

1. **Tag Attachment for Functions**
   - Snowflake requires function signature for ALTER FUNCTION
   - Current code skips tag attachment for functions (logs debug)
   - Consider if we want to support this in the future

2. **3-Part Naming**
   - Snowflake uses DATABASE.SCHEMA.OBJECT naming
   - All object names must be qualified
   - Helper methods for qualification needed

3. **Incremental Strategies**
   - Merge requires unique key handling
   - Delete-insert requires where condition handling
   - Append requires column alignment

4. **View Column Comments**
   - Snowflake supports inline column comments in CREATE VIEW
   - Complex logic for building view with comments
   - Fallback to simple CREATE VIEW if comments fail

### Testing Strategy

1. **Unit Tests**
   - Each handler tested independently
   - Tag manager tested separately
   - Function manager tested separately
   - Mock adapter for handler tests

2. **Integration Tests**
   - Full adapter flow with real Snowflake connection
   - Test with different materialization types
   - Test tag attachment for different object types

3. **Mocking Strategy**
   - Mock connection for handler tests
   - Use real Snowflake connection only in integration tests
   - Mock tag creation/attachment for unit tests

### Dependencies Between Components

- **Tag Manager**: Used by Table Handler, View Handler, Function Manager
- **Connection Manager**: Used by all handlers
- **Utils**: Used by all handlers
- **Table Handler**: Used by Incremental Handler
- **View Handler**: Independent (uses Tag Manager)
- **Function Manager**: Independent (uses Tag Manager)

### Backward Compatibility

- All public methods remain on `SnowflakeAdapter`
- Internal implementation changes only
- No breaking changes to adapter interface
- Existing tests should continue to work

