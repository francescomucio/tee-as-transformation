# SQL Model Metadata Example

This example demonstrates how to add metadata to SQL models using companion Python files.

## How it works

For each SQL model file (e.g., `my_table.sql`), you can create a companion Python file with the same name (e.g., `my_table.py`) that contains a `metadata` object with additional information about the model.

## Two Approaches

### Approach 1: Simple Dictionary (Backward Compatible)
```python
# my_table.py
metadata = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Unique identifier for the record",
            "tests": ["not_null", "unique"]
        }
    ],
    "partitions": ["created_at"],
    "materialization": "table",
    "tests": ["row_count_gt_0"]
}
```

### Approach 2: Typed Dictionary (Recommended)
```python
# my_table.py
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Unique identifier for the record",
            "tests": ["not_null", "unique"]
        }
    ],
    "partitions": ["created_at"],
    "materialization": "table",
    "tests": ["row_count_gt_0"]
}
```

## Benefits of Typed Approach

- **IDE Support**: Full autocomplete and type hints
- **Type Checking**: Static type checkers catch errors at write time
- **Self-Documenting**: Types serve as documentation
- **Refactoring Safety**: Type-aware refactoring tools work better

## Example

### SQL File: `my_table.sql`
```sql
SELECT 
    id,
    name,
    created_at
FROM source_table
WHERE status = 'active'
```

### Typed Metadata File: `my_table.py`
```python
# Metadata for my_table.sql
from tee.typing.metadata import ModelMetadataDict

metadata: ModelMetadataDict = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Unique identifier for the record",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "name",
            "datatype": "string",
            "description": "Name of the record",
            "tests": ["not_null"]
        },
        {
            "name": "created_at",
            "datatype": "timestamp",
            "description": "When the record was created",
            "tests": ["not_null"]
        }
    ],
    "partitions": ["created_at"],
    "materialization": "table",
    "tests": ["row_count_gt_0", "no_duplicates"]
}
```

## Metadata Schema

### Schema (array of column definitions)
Each column definition must include:
- `name` (required): Column name
- `datatype` (required): Data type (e.g., "number", "string", "timestamp")
- `description` (optional): Human-readable description
- `tests` (optional): Array of test names to run on this column

### Model-level properties
- `partitions` (optional): Array of column names to partition by
- `materialization` (optional): How to materialize the model ("table", "view", "incremental")
- `tests` (optional): Array of model-level test names

## Supported Materialization Types
- `table`: Materialize as a table
- `view`: Materialize as a view
- `incremental`: Materialize as an incremental table

## Example Test Names
- Column tests: `not_null`, `unique`, `accepted_values`, `relationships`
- Model tests: `row_count_gt_0`, `no_duplicates`, `freshness`

## Output

When you run the parser, the metadata will be included in the `model_metadata.metadata` field of each parsed model, making it available for downstream processing, documentation generation, and validation.
