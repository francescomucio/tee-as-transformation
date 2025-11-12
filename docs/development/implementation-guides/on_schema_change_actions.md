# on_schema_change Actions Reference

## Overview

The `on_schema_change` parameter controls how incremental materialization handles schema differences between the transformation output and the existing target table. This document defines all supported actions and their behaviors.

## Supported Actions

### 1. `ignore` (Default)

**Behavior:** Ignore schema differences and proceed with the incremental operation.

**What happens:**
- ✅ Transformation executes normally
- ⚠️ New columns in output are ignored (not added to table)
- ⚠️ Missing columns in output remain in table
- ⚠️ Type mismatches may cause errors during INSERT/MERGE
- ⚠️ Column order differences may cause data misalignment

**Use cases:**
- When schema changes are handled manually
- When you want explicit control over schema evolution
- Backward compatibility (default behavior)

**Example:**
```python
@model(
    table_name="orders",
    materialization="incremental",
    incremental={
        "strategy": "append",
        "on_schema_change": "ignore"  # or omit for default
    }
)
def orders():
    return "SELECT order_id, customer_id, total FROM ..."
```

**Risks:**
- Data may be inserted into wrong columns if order changes
- Type mismatches will cause runtime errors
- New columns won't be available in target table

---

### 2. `fail`

**Behavior:** Fail the transformation if any schema differences are detected.

**What happens:**
- ❌ Transformation stops immediately if schema differences found
- ✅ Raises clear error message describing differences
- ✅ Prevents unexpected data corruption
- ✅ Forces explicit schema change handling

**Use cases:**
- Production environments requiring strict schema control
- When schema changes should be reviewed manually
- When you want to catch schema drift early
- Compliance/audit scenarios

**Example:**
```python
@model(
    table_name="customers",
    materialization="incremental",
    incremental={
        "strategy": "merge",
        "on_schema_change": "fail",
        "merge": {
            "unique_key": ["customer_id"],
            "time_column": "updated_at"
        }
    }
)
def customers():
    return "SELECT customer_id, name, email FROM ..."
```

**Error message format:**
```
Schema changes detected for table 'customers' but on_schema_change='fail'.
New columns: ['phone_number', 'address']
Missing columns: ['old_column']
Type mismatches: {'email': ('VARCHAR(100)', 'TEXT')}
```

**Risks:**
- None - fails safely before data corruption
- Requires manual intervention for any schema change

---

### 3. `append_new_columns`

**Behavior:** Automatically add new columns from transformation output to target table. Existing columns remain unchanged.

**What happens:**
- ✅ New columns in output are automatically added to table
- ✅ Existing columns remain in table (even if missing from output)
- ✅ Missing columns in output are NOT removed from table
- ⚠️ Type mismatches may still cause errors
- ⚠️ Column order differences may cause data misalignment

**Use cases:**
- When you frequently add new columns
- When you want to preserve existing columns
- When column removal should be manual
- Most common production use case

**Example:**
```python
@model(
    table_name="orders",
    materialization="incremental",
    incremental={
        "strategy": "append",
        "on_schema_change": "append_new_columns",
        "append": {
            "time_column": "created_at"
        }
    }
)
def orders():
    # If this query adds 'discount_code' column, it will be auto-added to table
    return "SELECT order_id, customer_id, total, discount_code FROM ..."
```

**DDL generated:**
```sql
ALTER TABLE orders ADD COLUMN discount_code VARCHAR(50);
```

**Risks:**
- Type mismatches on existing columns still cause errors
- Orphaned columns (in table but not in output) accumulate over time
- Column order issues if not handled properly

---

### 4. `sync_all_columns`

**Behavior:** Synchronize target table schema with transformation output by adding new columns and removing missing columns.

**What happens:**
- ✅ New columns in output are automatically added to table
- ✅ Missing columns in output are automatically removed from table
- ⚠️ Type changes may require manual intervention (database-dependent)
- ⚠️ Data loss risk when columns are removed
- ⚠️ Column order differences may cause data misalignment

**Use cases:**
- When transformation output is the source of truth
- When you want exact schema matching
- Development/testing environments
- When column removal is intentional

**Example:**
```python
@model(
    table_name="products",
    materialization="incremental",
    incremental={
        "strategy": "merge",
        "on_schema_change": "sync_all_columns",
        "merge": {
            "unique_key": ["product_id"],
            "time_column": "updated_at"
        }
    }
)
def products():
    # If 'old_column' was removed from query, it will be dropped from table
    return "SELECT product_id, name, price, new_column FROM ..."
```

**DDL generated:**
```sql
ALTER TABLE products ADD COLUMN new_column VARCHAR(100);
ALTER TABLE products DROP COLUMN old_column;  -- With warning
```

**Warnings:**
- Logs warning before dropping columns
- May require database-specific permissions
- Some databases don't support DROP COLUMN

**Risks:**
- **Data loss** when columns are removed
- May break downstream dependencies
- Type changes may fail (database-dependent)

---

## Action Comparison Matrix

| Action | Add New Columns | Remove Missing Columns | Handle Type Changes | Fail on Differences | Data Loss Risk |
|--------|----------------|------------------------|---------------------|---------------------|----------------|
| `ignore` | ❌ No | ❌ No | ❌ No | ❌ No | ⚠️ Medium (misalignment) |
| `fail` | ❌ No | ❌ No | ❌ No | ✅ Yes | ✅ None |
| `append_new_columns` | ✅ Yes | ❌ No | ❌ No | ❌ No | ⚠️ Low |
| `sync_all_columns` | ✅ Yes | ✅ Yes | ⚠️ Partial | ❌ No | ⚠️ High (if columns removed) |

## Schema Change Types

Each action handles these schema change scenarios differently:

### 1. New Columns Added
- **Query has:** `order_id, customer_id, discount_code`
- **Table has:** `order_id, customer_id`
- **Difference:** `discount_code` is new

| Action | Result |
|--------|--------|
| `ignore` | Column ignored, INSERT may fail |
| `fail` | Transformation fails |
| `append_new_columns` | ✅ `discount_code` added to table |
| `sync_all_columns` | ✅ `discount_code` added to table |

### 2. Columns Removed
- **Query has:** `order_id, customer_id`
- **Table has:** `order_id, customer_id, old_column`
- **Difference:** `old_column` is missing from query

| Action | Result |
|--------|--------|
| `ignore` | Column remains, INSERT may fail |
| `fail` | Transformation fails |
| `append_new_columns` | Column remains (not removed) |
| `sync_all_columns` | ⚠️ `old_column` dropped from table (with warning) |

### 3. Type Mismatches
- **Query has:** `email VARCHAR(255)`
- **Table has:** `email TEXT`
- **Difference:** Same column, different type

| Action | Result |
|--------|--------|
| `ignore` | May cause INSERT/MERGE errors |
| `fail` | Transformation fails |
| `append_new_columns` | May cause INSERT/MERGE errors |
| `sync_all_columns` | ⚠️ May attempt type change (database-dependent) |

### 4. Column Order Changes
- **Query has:** `order_id, customer_id, total`
- **Table has:** `order_id, total, customer_id`
- **Difference:** Same columns, different order

| Action | Result |
|--------|--------|
| `ignore` | ⚠️ Data may be inserted into wrong columns |
| `fail` | Transformation fails |
| `append_new_columns` | ⚠️ Data may be inserted into wrong columns |
| `sync_all_columns` | ⚠️ Data may be inserted into wrong columns |

## Recommended Practices

### Production Environments
- **Recommended:** `append_new_columns` or `fail`
- **Avoid:** `sync_all_columns` (data loss risk)
- **Rationale:** Safe schema evolution with explicit control

### Development/Testing
- **Recommended:** `sync_all_columns` or `append_new_columns`
- **Rationale:** Faster iteration, schema matches code

### Critical Data
- **Recommended:** `fail`
- **Rationale:** Explicit review of all schema changes

### Frequent Schema Evolution
- **Recommended:** `append_new_columns`
- **Rationale:** Automatic handling of new columns, preserves existing

## Database Compatibility

| Database | ADD COLUMN | DROP COLUMN | Type Change | Notes |
|----------|------------|-------------|-------------|-------|
| Snowflake | ✅ Yes | ✅ Yes | ✅ Yes | Full support |
| PostgreSQL | ✅ Yes | ✅ Yes | ⚠️ Limited | Some type changes require explicit casting |
| BigQuery | ✅ Yes | ⚠️ Limited | ❌ No | DROP COLUMN has restrictions |
| DuckDB | ✅ Yes | ✅ Yes | ✅ Yes | Full support |
| MySQL | ✅ Yes | ✅ Yes | ⚠️ Limited | Type changes may require ALTER |
| SQL Server | ✅ Yes | ✅ Yes | ⚠️ Limited | Type changes may require explicit conversion |

## Future Enhancements (Not in Initial Implementation)

These actions may be considered for future versions:

1. **`warn`**: Log warnings but proceed (similar to `ignore` but with visibility)
2. **`append_new_columns_safe`**: Add new columns with type checking
3. **`sync_with_backup`**: Sync columns but backup dropped column data first
4. **`migrate_types`**: Explicitly handle type changes with conversion logic
5. **`rename_columns`**: Support column renaming/mapping

## Implementation Notes

- Default value should be `"ignore"` for backward compatibility
- All actions should log what they're doing at INFO level
- `sync_all_columns` should require explicit confirmation or flag for production
- Type changes may need separate configuration or manual handling
- Column order handling may require explicit column mapping in INSERT statements

