# Database Adapters - Reorganized Structure

## 📁 New Adapter Structure

The adapters have been reorganized into a more modular and maintainable structure:

```
tee/adapters/
├── __init__.py              # Main adapter exports and registration
├── base.py                   # DatabaseAdapter base class and AdapterConfig
├── registry.py               # Adapter registry and factory functions
├── testing.py                # Testing framework for adapters
├── duckdb/
│   ├── __init__.py           # DuckDB adapter exports
│   └── adapter.py            # DuckDBAdapter implementation
├── snowflake/
│   ├── __init__.py           # Snowflake adapter exports
│   └── adapter.py            # SnowflakeAdapter implementation
├── postgresql/
│   ├── __init__.py           # PostgreSQL adapter exports
│   └── adapter.py            # PostgreSQLAdapter implementation
└── bigquery/
    ├── __init__.py           # BigQuery adapter exports
    └── adapter.py            # BigQueryAdapter implementation
```

## 🎯 Benefits of This Structure

### **1. Modularity**
- Each adapter is in its own subpackage
- Easy to add new adapters without affecting existing ones
- Clear separation of concerns

### **2. Maintainability**
- Adapter-specific code is isolated
- Easy to find and modify specific adapter implementations
- Clear import paths and dependencies

### **3. Extensibility**
- Adding new adapters is straightforward:
  1. Create new subpackage (e.g., `mysql/`)
  2. Implement adapter class
  3. Register in `__init__.py`
- No need to modify core files

### **4. Testing**
- Each adapter can have its own test suite
- Isolated testing of adapter-specific functionality
- Easy to mock and test individual adapters

## 🚀 Usage Examples

### **Importing Adapters**
```python
# Import specific adapters
from tee.adapters import DuckDBAdapter, SnowflakeAdapter

# Import adapter factory
from tee.adapters import get_adapter, AdapterConfig

# Import testing utilities
from tee.adapters.testing import test_adapter
```

### **Creating Custom Adapters**
```python
# 1. Create new adapter subpackage
mkdir tee/adapters/mysql/

# 2. Create adapter implementation
# tee/adapters/mysql/adapter.py
from ..base import DatabaseAdapter, AdapterConfig, MaterializationType
from ..registry import register_adapter

class MySQLAdapter(DatabaseAdapter):
    def get_default_dialect(self):
        return "mysql"
    
    # ... implement other methods

# Register the adapter
register_adapter("mysql", MySQLAdapter)

# 3. Create __init__.py
# tee/adapters/mysql/__init__.py
from .adapter import MySQLAdapter
__all__ = ["MySQLAdapter"]

# 4. Update main __init__.py
# tee/adapters/__init__.py
from .mysql import MySQLAdapter
```

## 📊 Available Adapters

| Adapter | Dialect | Materializations | Status |
|---------|---------|------------------|--------|
| DuckDB | `duckdb` | Table, View, Materialized View | ✅ Complete |
| Snowflake | `snowflake` | Table, View, Materialized View, External Table | ✅ Complete |
| PostgreSQL | `postgresql` | Table, View, Materialized View | ✅ Complete |
| BigQuery | `bigquery` | Table, View, Materialized View, External Table | ✅ Complete |

## 🔧 Configuration

The adapter system supports configuration from multiple sources:

### **pyproject.toml**
```toml
[tool.tee.database]
type = "snowflake"
host = "account.snowflakecomputing.com"
source_dialect = "postgresql"
```

### **Environment Variables**
```bash
export TEE_DB_TYPE=snowflake
export TEE_DB_HOST=account.snowflakecomputing.com
export TEE_DB_SOURCE_DIALECT=postgresql
```

### **Direct Configuration**
```python
from tee.adapters import AdapterConfig

config = AdapterConfig(
    type="snowflake",
    host="account.snowflakecomputing.com",
    source_dialect="postgresql"
)
```

## 🧪 Testing

Each adapter can be tested individually:

```python
from tee.adapters import get_adapter, AdapterConfig
from tee.adapters.testing import test_adapter

config = AdapterConfig(type="duckdb", path=":memory:")
adapter = get_adapter(config)
results = test_adapter(adapter)

print(f"All tests passed: {all(r['success'] for r in results.values())}")
```

## 🎉 Migration Complete!

The adapter system has been successfully reorganized with:

- ✅ **Modular Structure**: Each adapter in its own subpackage
- ✅ **Clean Imports**: Clear import paths and dependencies
- ✅ **Easy Extension**: Simple process to add new adapters
- ✅ **Comprehensive Testing**: Testing framework for all adapters
- ✅ **Full Compatibility**: All existing functionality preserved
- ✅ **Documentation**: Complete usage guides and examples

The new structure makes the adapter system more maintainable, extensible, and easier to work with!
