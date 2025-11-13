# Incremental Materialization

t4t supports incremental materialization for efficient data processing, allowing you to process only new or changed data instead of reprocessing entire datasets. This is particularly useful for large datasets where full refreshes are expensive.

## Overview

Incremental materialization in t4t supports three main strategies:

- **Append**: Add new records to existing tables
- **Merge**: Update existing records and insert new ones (upsert)
- **Delete+Insert**: Remove old data and insert fresh data for a specific time range

## Configuration

Incremental materialization is configured through model metadata. You can define this in either:

1. **Python metadata files** (`.py`) - Recommended for complex configurations
2. **SQL comment metadata** - For simple configurations

### Python Metadata Configuration

```python
# models/my_schema/incremental_example.py
from tee.typing import ModelMetadata

# Append strategy
metadata_append: ModelMetadata = {
    "description": "Incremental table using append strategy",
    "materialization": "incremental",
    "incremental": {
        "strategy": "append",
        "append": {
            "time_column": "created_at",
            "start_date": "2024-01-01",
            "lookback": "7 days"
        }
    }
}

# Merge strategy
metadata_merge: ModelMetadata = {
    "description": "Incremental table using merge strategy",
    "materialization": "incremental",
    "incremental": {
        "strategy": "merge",
        "merge": {
            "unique_key": ["id"],
            "time_column": "updated_at",
            "start_date": "auto",
            "lookback": "3 hours"
        }
    }
}

# Delete+Insert strategy
metadata_delete_insert: ModelMetadata = {
    "description": "Incremental table using delete+insert strategy",
    "materialization": "incremental",
    "incremental": {
        "strategy": "delete_insert",
        "delete_insert": {
            "where_condition": "updated_at >= @start_date",
            "time_column": "updated_at",
            "start_date": "@start_date"
        }
    }
}

# Choose which strategy to use
metadata = metadata_merge  # Currently using merge strategy
```

### SQL Comment Configuration

```sql
-- models/my_schema/incremental_example.sql
-- metadata: {"materialization": "incremental", "incremental": {"strategy": "append", "append": {"time_column": "created_at", "start_date": "2024-01-01", "lookback": "7 days"}}}

SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active'
```

## Strategies

### 1. Append Strategy

The append strategy adds new records to existing tables without modifying existing data.

**Configuration:**
```python
"incremental": {
    "strategy": "append",
    "append": {
        "time_column": "created_at",      # Column to use for time filtering
        "start_date": "2024-01-01",       # Start date for first run
        "lookback": "7 days"              # Optional: lookback period
    }
}
```

**Behavior:**
- First run: Creates table with all data matching the time filter
- Subsequent runs: Appends new data based on time filtering
- No modification of existing records

**Example SQL Generated:**
```sql
-- First run
CREATE TABLE my_schema.incremental_example AS
SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active' 
AND created_at >= '2024-01-01'

-- Subsequent runs
INSERT INTO my_schema.incremental_example
SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active' 
AND created_at > '2024-01-15'  -- Last processed timestamp
```

### 2. Merge Strategy

The merge strategy performs upsert operations, updating existing records and inserting new ones.

**Configuration:**
```python
"incremental": {
    "strategy": "merge",
    "merge": {
        "unique_key": ["id"],             # Columns that uniquely identify records
        "time_column": "updated_at",      # Column to use for time filtering
        "start_date": "auto",             # Use max(time_column) from target table
        "lookback": "3 hours"             # Optional: lookback period
    }
}
```

**Behavior:**
- First run: Creates table with all data
- Subsequent runs: Updates existing records and inserts new ones
- Uses database-specific MERGE/UPSERT syntax

**Example SQL Generated:**
```sql
-- First run
CREATE TABLE my_schema.incremental_example AS
SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active'

-- Subsequent runs
MERGE INTO my_schema.incremental_example AS target
USING (
    SELECT id, name, created_at, updated_at, status 
    FROM t_project.source_table 
    WHERE status = 'active' 
    AND updated_at > (SELECT COALESCE(MAX(updated_at) - INTERVAL '3 hours', '1900-01-01') FROM my_schema.incremental_example)
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET 
    name = source.name, 
    created_at = source.created_at, 
    updated_at = source.updated_at, 
    status = source.status
WHEN NOT MATCHED THEN INSERT (id, name, created_at, updated_at, status) 
VALUES (source.id, source.name, source.created_at, source.updated_at, source.status)
```

### 3. Delete+Insert Strategy

The delete+insert strategy removes old data and inserts fresh data for a specific time range.

**Configuration:**
```python
"incremental": {
    "strategy": "delete_insert",
    "delete_insert": {
        "where_condition": "updated_at >= @start_date",  # Condition for deletion
        "time_column": "updated_at",                     # Column for time filtering
        "start_date": "@start_date"                      # Variable reference
    }
}
```

**Behavior:**
- First run: Creates table with all data
- Subsequent runs: Deletes data matching the where condition, then inserts fresh data
- Useful for time-partitioned data or when you need to reprocess specific time ranges

**Example SQL Generated:**
```sql
-- First run
CREATE TABLE my_schema.incremental_example AS
SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active'

-- Subsequent runs
DELETE FROM my_schema.incremental_example 
WHERE updated_at >= CAST('2024-01-01' AS DATE)

INSERT INTO my_schema.incremental_example
SELECT id, name, created_at, updated_at, status 
FROM t_project.source_table 
WHERE status = 'active' 
AND updated_at >= '2024-01-01'
```

## Configuration Options

### Time Column

The `time_column` specifies which column to use for time-based filtering:

```python
"time_column": "created_at"    # Use created_at column
"time_column": "updated_at"    # Use updated_at column
```

### Start Date

The `start_date` option controls when to start processing data:

```python
"start_date": "2024-01-01"           # Specific date
"start_date": "auto"                 # Use max(time_column) from target table
"start_date": "CURRENT_DATE"         # Use current date
"start_date": "@start_date"          # Use CLI variable
"start_date": "{{ start_date }}"     # Use CLI variable (alternative syntax)
```

### Lookback

The `lookback` option adds a buffer period to handle late-arriving data:

```python
"lookback": "7 days"          # 7 days
"lookback": "3 hours"         # 3 hours
"lookback": "30 minutes"      # 30 minutes
"lookback": "2 weeks"         # 2 weeks
"lookback": "1 month"         # 1 month (approximate)
```

### CLI Variables

You can pass variables via the command line:

```bash
# Using @variable syntax (JSON format)
uv run t4t run ./examples/t_project --vars '{"start_date": "2024-01-01"}'

# Using {{ variable }} syntax (JSON format)
uv run t4t run ./examples/t_project --vars '{"start_date": "2024-01-01"}'
```

Variables are resolved in the configuration:

```python
"start_date": "@start_date"          # Resolved to 2024-01-01
"where_condition": "created_at >= @start_date"  # Resolved to "created_at >= 2024-01-01"
```

## State Management

t4t automatically tracks the state of incremental models:

- **Execution timestamps**: When the model was last run
- **SQL hashes**: Detects changes in model SQL
- **Config hashes**: Detects changes in model configuration
- **Last processed values**: For tracking incremental progress

State is stored in `examples/t_project/data/tee_state.db` by default.

## Database Support

### Currently Supported

- **DuckDB**: Full support for all strategies
- **Snowflake**: Planned support (merge strategy ready)

### Database-Specific Features

#### DuckDB
- Native MERGE syntax for merge strategy
- Automatic column detection for merge operations
- Full SQL interval support for lookback periods

#### Snowflake (Planned)
- MERGE syntax with Snowflake-specific optimizations
- Automatic clustering key detection
- Support for Snowflake-specific data types

## Best Practices

### 1. Choose the Right Strategy

- **Append**: Use for append-only data (logs, events, metrics)
- **Merge**: Use for slowly changing dimensions or fact tables
- **Delete+Insert**: Use for time-partitioned data or when you need to reprocess specific ranges

### 2. Optimize Time Columns

- Use indexed columns for time filtering
- Consider partitioning large tables by time columns
- Use appropriate data types (TIMESTAMP, DATE)

### 3. Handle Late-Arriving Data

- Use lookback periods to catch late-arriving data
- Consider the trade-off between lookback period and performance
- Monitor for data quality issues

### 4. Monitor Performance

- Check execution logs for performance issues
- Consider parallel processing for large datasets
- Monitor database resource usage

### 5. Test Incremental Logic

- Test with different data scenarios
- Verify that incremental runs produce correct results
- Test edge cases (empty tables, missing data, etc.)

## Troubleshooting

### Common Issues

1. **Model always runs full load**
   - Check that the model has been run at least once
   - Verify that the state database exists and is accessible
   - Check for configuration changes that trigger full loads

2. **Variable resolution not working**
   - Ensure variables are passed via `--vars` flag
   - Check variable name spelling and syntax
   - Verify that the variable is used in the correct context

3. **Date casting errors**
   - Ensure date values are in correct format (YYYY-MM-DD)
   - Check that time columns have appropriate data types
   - Verify that date comparisons are valid

4. **Merge strategy not working**
   - Check that unique_key columns exist in both source and target
   - Verify that the target table has the expected structure
   - Ensure that the database supports MERGE syntax

### Debug Logging

Enable debug logging to see detailed information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This will show:
- Generated SQL queries
- Variable resolution
- State management operations
- Database-specific operations

## Examples

### Complete Example

```python
# models/my_schema/user_events.py
from tee.typing import ModelMetadata

metadata: ModelMetadata = {
    "description": "Incremental user events table",
    "materialization": "incremental",
    "incremental": {
        "strategy": "merge",
        "merge": {
            "unique_key": ["user_id", "event_id"],
            "time_column": "event_timestamp",
            "start_date": "auto",
            "lookback": "1 hour"
        }
    }
}
```

```sql
-- models/my_schema/user_events.sql
SELECT 
    user_id,
    event_id,
    event_type,
    event_timestamp,
    properties
FROM raw_events
WHERE event_timestamp >= '2024-01-01'
```

Run with:
```bash
uv run t4t run ./examples/t_project
```

This will create an incremental table that:
- Uses merge strategy for upsert operations
- Automatically determines start date from existing data
- Includes 1-hour lookback for late-arriving events
- Updates existing records and inserts new ones
