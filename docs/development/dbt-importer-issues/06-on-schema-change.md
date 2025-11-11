# Add on_schema_change Support to t4t Incremental Materialization

## Description

Add support for `on_schema_change` configuration in t4t incremental materialization to handle schema differences between the target table and the transformation output.

## Motivation

When using incremental materialization, the output schema of a transformation may differ from the existing target table schema. This can happen when:
- New columns are added to the transformation
- Columns are removed from the transformation
- Column types change
- Column order changes

Currently, t4t explicitly warns that `on_schema_change` from dbt imports is not supported, requiring manual intervention. This creates operational overhead and potential for errors in production environments.

## Current State

- t4t incremental materialization supports strategies (`append`, `merge`, `delete_insert`) but has no schema change handling
- dbt importer explicitly warns that `on_schema_change` is not supported
- Schema changes must be handled manually through DDL statements

## Related OTS Issue

This feature is part of the Open Transformation Specification (OTS). See:
- [OTS Issue #2: Add on_schema_change Support to OTS Incremental Materialization](https://github.com/francescomucio/open-transformation-specification/issues/2)

The OTS specification should be updated first, then t4t will implement the feature according to the specification.

## Proposed Solution

Add an `on_schema_change` field to the `incremental_details` section of the materialization configuration in t4t.

### Behavior Options

1. **`ignore`** (default): Ignore schema differences and proceed with the incremental operation. The transformation will use the existing table schema, potentially causing errors if columns don't match.

2. **`append_new_columns`**: Automatically add new columns from the transformation output to the target table. Existing columns remain unchanged. Missing columns in the output are not removed from the table.

3. **`sync_all_columns`**: Synchronize the target table schema with the transformation output:
   - Add new columns that exist in the output but not in the table
   - Remove columns that exist in the table but not in the output (with appropriate warnings)
   - Column type changes may require manual intervention

4. **`fail`**: Fail the transformation if any schema differences are detected. This ensures explicit handling of schema changes and prevents unexpected behavior.

## Examples

### Append New Columns

```python
@model(
    table_name="marts.orders",
    materialization="incremental",
    incremental={
        "strategy": "append",
        "on_schema_change": "append_new_columns",
        "filter_condition": "created_at >= '@start_date'"
    }
)
def orders():
    return exp.parse_one("SELECT ...")
```

### Fail on Schema Change

```python
@model(
    table_name="marts.customers",
    materialization="incremental",
    incremental={
        "strategy": "merge",
        "on_schema_change": "fail",
        "merge_key": ["customer_id"],
        "filter_condition": "updated_at >= '@start_date'"
    }
)
def customers():
    return exp.parse_one("SELECT ...")
```

## Implementation Considerations

### Database Compatibility

Different databases have varying capabilities for schema changes:

- **Snowflake**: Supports `ALTER TABLE ADD COLUMN` and column type changes
- **PostgreSQL**: Supports `ALTER TABLE ADD COLUMN` and some type changes
- **BigQuery**: Supports `ALTER TABLE ADD COLUMN` but has limitations on type changes
- **DuckDB**: Supports schema modifications but may have different syntax

Implementation should:
1. Detect schema differences by comparing transformation output schema with target table schema
2. Generate appropriate DDL statements based on the `on_schema_change` setting
3. Handle database-specific limitations gracefully
4. Provide clear error messages when operations are not supported

### Schema Detection

The implementation needs to:
1. Infer the schema from the transformation output (SELECT query)
2. Query the existing table schema from the database
3. Compare schemas to identify:
   - New columns (in output, not in table)
   - Missing columns (in table, not in output)
   - Type mismatches (same column, different type)

### Execution Order

For incremental materialization with schema changes:
1. Execute transformation query to get output schema
2. Compare with target table schema
3. Apply schema changes based on `on_schema_change` setting
4. Execute incremental materialization strategy

## Related Work

- dbt's `on_schema_change` parameter (reference implementation)
- [OTS Issue #2](https://github.com/francescomucio/open-transformation-specification/issues/2)
- t4t incremental materialization system

## Acceptance Criteria

- [ ] OTS specification updated with `on_schema_change` field
- [ ] t4t metadata schema updated to support `on_schema_change`
- [ ] Schema comparison logic implemented
- [ ] All four behavior options (`ignore`, `append_new_columns`, `sync_all_columns`, `fail`) implemented
- [ ] Database-specific DDL generation implemented
- [ ] dbt importer updated to convert `on_schema_change` config
- [ ] Documentation updated
- [ ] Tests added for all behavior options
- [ ] Tests added for database-specific implementations

## Future Enhancements

- Support for column type changes (may require separate configuration)
- Support for column reordering
- Support for column renaming/mapping
- Schema versioning and migration tracking

## Notes

- This feature is particularly important for dbt migration scenarios where `on_schema_change` is commonly used
- The default behavior (`ignore`) maintains backward compatibility with existing t4t transformations
- Database adapter implementations will need to handle schema comparison and DDL generation
- Implementation should wait for OTS specification update

