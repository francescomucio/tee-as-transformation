# Add Freshness/Recency Tests to t4t

## Description

Add support for freshness/recency tests to t4t's data quality testing framework. These tests validate that data sources are updated within a specified time window (e.g., "source data should be updated within 24 hours").

## Motivation

This feature is needed to support importing dbt projects that use source freshness tests. Currently, dbt source freshness tests are skipped during import with a warning. Once this feature is implemented, the dbt importer can convert these tests to t4t format.

## Use Cases

- Validate that source tables are updated regularly
- Alert when data pipelines are stale
- Ensure data freshness for time-sensitive analytics

## Proposed Implementation

### Test Definition Format

```python
# In model metadata
metadata: ModelMetadataDict = {
    "tests": [
        {
            "name": "freshness",
            "params": {
                "date_column": "updated_at",
                "warn_after": {"hours": 24},
                "error_after": {"hours": 48}
            }
        }
    ]
}
```

### Test Logic

- Check the maximum value of the date column
- Compare against current timestamp
- Fail if data is older than `error_after`
- Warn if data is older than `warn_after`

## Related

- Part of dbt importer feature
- Related to data quality testing framework

## Acceptance Criteria

- [ ] Freshness test can be defined in model metadata
- [ ] Test supports `warn_after` and `error_after` parameters
- [ ] Test supports different time units (hours, days, etc.)
- [ ] Test works with all supported database adapters
- [ ] Test can be run via `t4t test` command
- [ ] Documentation updated
- [ ] dbt importer updated to convert source freshness tests

