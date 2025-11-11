# Add Spark Engine Support with Dedicated Incremental Strategies

## Description

Add support for Apache Spark as a database engine in t4t, including Spark-specific incremental materialization strategies such as `insert_overwrite`.

## Motivation

During dbt import, we encounter Spark-specific incremental strategies (e.g., `insert_overwrite`) that are not supported in t4t. Spark is commonly used in big data environments and requires specific handling for incremental materialization.

## Current State

- t4t supports: DuckDB, Snowflake, PostgreSQL, BigQuery
- Spark is not supported
- Spark-specific incremental strategies are not implemented
- dbt importer warns about unsupported Spark strategies

## Spark-Specific Incremental Strategies

### `insert_overwrite`

The `insert_overwrite` strategy is Spark-specific and works differently from standard SQL strategies:

- Overwrites data in target partitions (if partitioned)
- More efficient than delete+insert for partitioned tables
- Requires partition column specification

**Example:**
```yaml
materialization:
  type: "incremental"
  incremental_details:
    strategy: "insert_overwrite"
    partition_by: ["date"]
    filter_condition: "date >= '@start_date'"
```

## Proposed Solution

### 1. Add Spark Adapter

Create a new Spark adapter that:
- Supports Spark SQL dialect
- Handles Spark-specific connection (SparkSession)
- Implements Spark-specific materialization strategies

### 2. Implement Spark Incremental Strategies

Implement `insert_overwrite` strategy:
- Detect partition columns
- Generate Spark-specific SQL for partition overwrite
- Handle both partitioned and non-partitioned tables

### 3. Update dbt Importer

- Map `insert_overwrite` strategy from dbt to t4t
- Support Spark as target database type
- Document Spark-specific configurations

## Implementation Considerations

### Spark Connection

Spark requires:
- SparkSession configuration
- Cluster connection details
- Catalog/database configuration
- Partition management

### SQL Dialect

Spark SQL has specific syntax:
- Partition management: `INSERT OVERWRITE TABLE ... PARTITION (...)`
- Catalog references: `catalog.database.table`
- Delta Lake support (if using Delta)

### Partition Handling

`insert_overwrite` requires:
- Partition column specification
- Partition value extraction from data
- Dynamic partition mode support

## Examples

### Basic insert_overwrite

```sql
-- Spark SQL
INSERT OVERWRITE TABLE target_table
PARTITION (date)
SELECT 
    col1,
    col2,
    date
FROM source_table
WHERE date >= '2024-01-01'
```

### With Dynamic Partitions

```sql
-- Spark SQL with dynamic partitions
SET spark.sql.sources.partitionOverwriteMode = 'dynamic';
INSERT OVERWRITE TABLE target_table
PARTITION (date)
SELECT 
    col1,
    col2,
    date
FROM source_table
WHERE date >= '2024-01-01'
```

## Related Work

- dbt Spark adapter (reference implementation)
- Spark SQL documentation
- Delta Lake documentation (if supporting Delta tables)

## Acceptance Criteria

- [ ] Spark adapter implemented
- [ ] `insert_overwrite` strategy implemented
- [ ] Spark connection configuration supported
- [ ] dbt importer maps Spark strategies correctly
- [ ] Documentation updated
- [ ] Tests added for Spark adapter
- [ ] Tests added for insert_overwrite strategy

## Future Enhancements

- Delta Lake support
- Iceberg table support
- Spark-specific optimizations
- Partition management utilities

