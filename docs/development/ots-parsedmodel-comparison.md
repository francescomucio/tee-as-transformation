# OTS Module vs ParsedModel Format Comparison

This document compares the OTS Module format with t4t's internal ParsedModel format to understand the differences and mapping requirements.

## High-Level Structure

### OTS Module (Collection Level)
```json
{
  "ots_version": "0.1.0",
  "module_name": "database.schema",
  "module_description": "...",
  "version": "1.0.0",
  "tags": ["tag1", "tag2"],           // Module-level tags
  "test_library_path": "path/to/test_library.ots.json",
  "target": {
    "database": "db_name",
    "schema": "schema_name",
    "sql_dialect": "postgres",
    "connection_profile": "profile_name"
  },
  "transformations": [                // Array of transformations
    { /* OTSTransformation */ }
  ],
  "generic_tests": { ... },            // Optional: inline generic tests
  "singular_tests": { ... }            // Optional: inline singular tests
}
```

### ParsedModel (Individual Model)
```json
{
  "code": {
    "sql": {
      "original_sql": "SELECT ...",
      "resolved_sql": "SELECT ...",
      "operation_type": "select",
      "source_tables": ["table1", "table2"]
    }
  },
  "model_metadata": {
    "table_name": "schema.table",
    "function_name": null,              // For Python models
    "description": "...",
    "variables": [],
    "file_path": "path/to/file.sql",
    "metadata": {                       // Nested metadata dict
      "schema": [...],
      "materialization": "table",
      "tests": [...],
      "incremental": {...},
      "tags": [...],
      "object_tags": {...}
    }
  },
  "sqlglot_hash": "abc123...",
  "needs_evaluation": false            // For Python models
}
```

## Key Differences

### 1. **Organization Level**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Scope** | Collection of transformations (module-level) | Single transformation (model-level) |
| **Grouping** | Groups by `database.schema` | Individual models, grouped externally |
| **Target Config** | Module-level `target` object | Inferred from project config |
| **Module Metadata** | Module-level tags, description, version | Not present |

### 2. **Transformation Identification**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **ID Field** | `transformation_id` (e.g., "schema.table") | `model_metadata.table_name` |
| **Location** | Top-level in transformation | Nested in `model_metadata` |

### 3. **Code Structure**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Structure** | `code.sql.original_sql`<br>`code.sql.resolved_sql`<br>`code.sql.source_tables` | `code.sql.original_sql`<br>`code.sql.resolved_sql`<br>`code.sql.source_tables`<br>`code.sql.operation_type` |
| **Additional Fields** | None | `operation_type` (t4t-specific) |

**Similarity**: ✅ The code structure is **very similar** - OTS format matches t4t's structure almost exactly!

### 4. **Schema Definition**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Location** | Top-level `schema` object | `model_metadata.metadata.schema` |
| **Structure** | `schema.columns[]`<br>`schema.partitioning[]`<br>`schema.indexes[]` | `metadata.schema[]` (list of ColumnDefinition)<br>`metadata.partitions[]`<br>`metadata.indexes[]` |
| **Column Format** | `{name, datatype, description}` | `{name, datatype, description, tests[]}` |

**Difference**: 
- OTS: Schema is top-level, columns don't include tests
- ParsedModel: Schema is nested, columns can include tests inline

### 5. **Materialization**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Location** | Top-level `materialization` object | `model_metadata.metadata.materialization` |
| **Structure** | `materialization.type`<br>`materialization.incremental_details`<br>`materialization.scd2_details` | `metadata.materialization`<br>`metadata.incremental`<br>`metadata.scd2_details` |
| **Incremental Config** | Flattened in `incremental_details` | Nested in `incremental` with strategy-specific configs |

**Difference**: OTS flattens incremental config, ParsedModel uses nested strategy-specific configs.

### 6. **Tests**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Location** | Top-level `tests` object | `model_metadata.metadata.tests[]` (table)<br>`model_metadata.metadata.schema[].tests[]` (columns) |
| **Structure** | `tests.table[]`<br>`tests.columns.{col_name}[]` | `metadata.tests[]` (table)<br>`metadata.schema[].tests[]` (columns) |
| **Test Library** | Module-level `test_library_path` or inline `generic_tests`/`singular_tests` | Not present (tests are inline or in separate files) |

**Difference**: OTS has explicit test library support, ParsedModel embeds tests in metadata.

### 7. **Metadata**

| Aspect | OTS Module | ParsedModel |
|--------|-----------|-------------|
| **Location** | Top-level `metadata` object | `model_metadata` (top-level) + `model_metadata.metadata` (nested) |
| **Fields** | `metadata.file_path`<br>`metadata.tags[]`<br>`metadata.object_tags{}`<br>`metadata.owner` (optional) | `model_metadata.file_path`<br>`model_metadata.metadata.tags[]`<br>`model_metadata.metadata.object_tags{}`<br>`model_metadata.description`<br>`model_metadata.variables[]` |
| **Tags** | Module-level `tags` + transformation-level `metadata.tags` | Only in `metadata.tags` |
| **Additional** | `transformation_type`, `sql_dialect` at transformation level | `function_name`, `variables` for Python models |

**Difference**: OTS separates module-level and transformation-level metadata more clearly.

### 8. **Module-Level Features (OTS Only)**

| Feature | OTS Module | ParsedModel |
|---------|-----------|-------------|
| **Module Tags** | `tags` at module level | Not present |
| **Module Description** | `module_description` | Not present |
| **Module Version** | `version` | Not present |
| **Test Library Reference** | `test_library_path` | Not present |
| **Target Configuration** | `target` object | Inferred from project config |

### 9. **Additional ParsedModel Fields**

| Field | ParsedModel | OTS Module |
|-------|-------------|------------|
| `sqlglot_hash` | ✅ Present | ❌ Not present |
| `needs_evaluation` | ✅ For Python models | ❌ Not present |
| `function_name` | ✅ For Python models | ❌ Not present |
| `variables` | ✅ List of variable names | ❌ Not present |
| `operation_type` | ✅ In code.sql | ❌ Not present |

## Mapping Complexity

### ✅ Easy Mappings (Direct 1:1)

1. **Code Structure**: Almost identical
   - `code.sql.original_sql` → `code.sql.original_sql`
   - `code.sql.resolved_sql` → `code.sql.resolved_sql`
   - `code.sql.source_tables` → `code.sql.source_tables`

2. **Transformation ID**: 
   - `transformation_id` → `model_metadata.table_name`

3. **Description**:
   - `description` → `model_metadata.description`

### ⚠️ Moderate Mappings (Structure Reorganization)

1. **Schema**: 
   - OTS: `schema.columns[]` (no tests)
   - ParsedModel: `metadata.schema[]` (with tests)
   - **Action**: Extract column tests, move to separate structure

2. **Materialization**:
   - OTS: Flattened `incremental_details`
   - ParsedModel: Nested `incremental` with strategy-specific configs
   - **Action**: Reorganize nested structure

3. **Tests**:
   - OTS: `tests.table[]` and `tests.columns.{col_name}[]`
   - ParsedModel: `metadata.tests[]` and `metadata.schema[].tests[]`
   - **Action**: Reorganize test structure

### 🔴 Complex Mappings (Missing/Extra Data)

1. **Module-Level Metadata**:
   - OTS has module-level tags, description, version
   - ParsedModel doesn't have module concept
   - **Action**: Store in project config or create module metadata structure

2. **Target Configuration**:
   - OTS: `target` object in module
   - ParsedModel: Inferred from project config
   - **Action**: Map to project config or store per-model

3. **Test Library**:
   - OTS: `test_library_path` reference
   - ParsedModel: Tests are inline
   - **Action**: Load test library, merge with inline tests

4. **t4t-Specific Fields**:
   - `sqlglot_hash`, `needs_evaluation`, `function_name`, `variables`, `operation_type`
   - **Action**: Can be lost or stored as additional metadata

## Conversion Strategy

### OTS → ParsedModel

1. **For each transformation in module**:
   - Extract `transformation_id` → `model_metadata.table_name`
   - Copy `code` structure (add `operation_type` if missing)
   - Move `schema` → `model_metadata.metadata.schema`
   - Move `materialization` → `model_metadata.metadata.materialization`
   - Reorganize `tests` → `model_metadata.metadata.tests` and column tests
   - Copy `metadata` fields → `model_metadata` and `model_metadata.metadata`
   - Merge module-level `tags` with transformation `tags`

2. **Module-level handling**:
   - Store `target` config in project config or connection config
   - Store module metadata separately or in project config
   - Load test library if `test_library_path` is present

### ParsedModel → OTS

1. **Group models by schema** (already done in `OTSTransformer`)

2. **For each module**:
   - Extract module-level config from project config
   - Create `target` from connection config
   - Collect module-level tags from project config

3. **For each transformation**:
   - Extract `model_metadata.table_name` → `transformation_id`
   - Copy `code` structure (remove `operation_type`)
   - Move `metadata.schema` → `schema` (extract tests)
   - Reorganize `metadata.materialization` → `materialization`
   - Reorganize tests → `tests` structure
   - Copy metadata fields

## Summary

**Good News**: 
- The core transformation code structure is **nearly identical** ✅
- Most metadata fields have direct mappings ✅
- The main differences are organizational (module vs model, nesting levels) ⚠️

**Challenges**:
- Module-level concepts don't exist in ParsedModel 🔴
- Test library support needs to be added 🔴
- Some t4t-specific fields may be lost in conversion ⚠️

**Overall Assessment**: The formats are **reasonably compatible**. The conversion is mostly about:
1. Reorganizing nesting levels
2. Handling module-level metadata
3. Managing test library references

The conversion should be **straightforward** to implement, with most complexity in handling module-level features and test libraries.

