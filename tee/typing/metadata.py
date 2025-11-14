"""
Type definitions for SQL model metadata.
"""

from typing import Any, Literal, NotRequired, TypedDict

# Data type definitions
DataType = Literal[
    "string",
    "number",
    "integer",
    "float",
    "boolean",
    "timestamp",
    "date",
    "time",
    "json",
    "array",
    "object",
]

# Materialization types
MaterializationType = Literal["table", "view", "incremental", "scd2"]

# Incremental strategy types
IncrementalStrategy = Literal["append", "merge", "delete_insert"]

# on_schema_change options (OTS 0.2.1)
OnSchemaChange = Literal[
    "fail",                    # Default - fail on schema changes
    "ignore",                  # Ignore schema differences, proceed anyway
    "append_new_columns",      # Add new columns only
    "sync_all_columns",         # Add new, remove missing columns
    "full_refresh",            # Drop and recreate with full query
    "full_incremental_refresh", # Drop, recreate, then run incremental in chunks
    "recreate_empty"           # Drop and recreate as empty table
]

# Function types
FunctionType = Literal["scalar", "aggregate", "table"]

# Test names for columns
ColumnTestName = Literal[
    "not_null", "unique", "accepted_values", "relationships", "expression", "custom"
]

# Test names for models
ModelTestName = Literal["row_count_gt_0", "unique", "freshness", "custom"]


# Test definition can be a simple string name or a dict with name/params/severity
TestDefinition = (
    str  # Simple test name like "not_null"
    | dict[str, Any]  # Dict with "name"/"test", optional "params", and optional "severity"
)


class ColumnDefinition(TypedDict):
    """Type definition for a column in the schema."""

    name: str
    datatype: DataType
    description: NotRequired[str | None]
    # Tests can be simple strings or dicts with parameters and severity
    tests: NotRequired[list[ColumnTestName | dict[str, Any]]]


class IncrementalAppendConfig(TypedDict):
    """Configuration for append-only incremental strategy."""

    time_column: str
    start_date: NotRequired[str | None]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[str | None]  # e.g., "7 days", "1 week"


class IncrementalMergeConfig(TypedDict):
    """Configuration for merge incremental strategy."""

    unique_key: list[str]
    time_column: str
    start_date: NotRequired[str | None]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[str | None]  # e.g., "7 days", "1 week"


class IncrementalDeleteInsertConfig(TypedDict):
    """Configuration for delete+insert incremental strategy."""

    where_condition: str  # SQL WHERE clause to identify records to delete
    time_column: str
    start_date: NotRequired[str | None]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[str | None]  # e.g., "7 days", "1 week"


class FullIncrementalRefreshParameter(TypedDict):
    """Parameter configuration for full_incremental_refresh chunking (OTS 0.2.1)."""

    name: str  # Parameter name (matches placeholder in query, e.g., "@start_date", "@end_date")
    start_value: str  # Initial value for the parameter
    end_value: str  # End condition: hardcoded value (e.g., "2025-12-31") or expression evaluated against source table (e.g., "max(event_date)")
    step: str  # Increment step: SQL interval (e.g., "INTERVAL 1 DAY") or numeric value


class FullIncrementalRefreshConfig(TypedDict):
    """Configuration for full_incremental_refresh on_schema_change behavior (OTS 0.2.1)."""

    parameters: list[FullIncrementalRefreshParameter]


class IncrementalConfig(TypedDict):
    """Configuration for incremental materialization strategies."""

    strategy: IncrementalStrategy
    on_schema_change: NotRequired[OnSchemaChange]  # Default: "fail" (OTS 0.2.1)
    append: NotRequired[IncrementalAppendConfig | None]
    merge: NotRequired[IncrementalMergeConfig | None]
    delete_insert: NotRequired[IncrementalDeleteInsertConfig | None]


class ModelMetadata(TypedDict):
    """
    Unified type definition for model metadata.

    Works for both user input (from Python files) and parsed/validated metadata.
    """

    description: NotRequired[str | None]
    schema: NotRequired[list[ColumnDefinition] | None]
    partitions: NotRequired[list[str] | None]
    materialization: NotRequired[MaterializationType | None]
    # Tests can be simple strings or dicts with parameters and severity
    tests: NotRequired[list[ModelTestName | dict[str, Any]] | None]
    incremental: NotRequired[IncrementalConfig | None]
    scd2_details: NotRequired[dict[str, Any] | None]  # For SCD2 materialization
    indexes: NotRequired[list[dict[str, Any]] | None]  # Explicit index definitions
    full_incremental_refresh: NotRequired[FullIncrementalRefreshConfig | None]  # For full_incremental_refresh on_schema_change (OTS 0.2.1)


# Function-specific types


class FunctionParameter(TypedDict):
    """Type definition for a function parameter."""

    name: str
    type: str  # SQL type string (e.g., "FLOAT", "VARCHAR(255)", "INTEGER")
    description: NotRequired[str | None]
    default: NotRequired[str | None]  # Default value as string
    mode: NotRequired[Literal["IN", "OUT", "INOUT"]]  # Parameter mode (for some databases)


class FunctionMetadata(TypedDict):
    """
    Unified type definition for function metadata.

    Works for both user input (from Python files) and parsed/validated metadata.
    """

    function_name: str
    description: NotRequired[str | None]
    function_type: NotRequired[FunctionType]  # "scalar", "aggregate", "table" (default: "scalar")
    language: NotRequired[str | None]  # "sql", "python", "javascript", etc.
    parameters: NotRequired[list[FunctionParameter] | None]
    return_type: NotRequired[str | None]  # SQL type string for scalar/aggregate functions
    return_table_schema: NotRequired[list[ColumnDefinition] | None]  # For table-valued functions
    schema: NotRequired[str | None]  # Schema name
    deterministic: NotRequired[bool | None]  # Whether function is deterministic
    # Tests can be simple strings or dicts with parameters and severity
    tests: NotRequired[list[str | dict[str, Any]] | None]
    # Tags (dbt-style, list of strings)
    tags: NotRequired[list[str] | None]
    # Object tags (database-style, key-value pairs)
    object_tags: NotRequired[dict[str, str] | None]
    # Source SQL dialect for conversion (e.g., "postgres", "mysql", "generic")
    # If not specified, uses project config source_sql_dialect or "generic" as default
    source_sql_dialect: NotRequired[str | None]


# OTS-specific types for the Open Transformation Specification


class OTSTarget(TypedDict):
    """Target configuration for an OTS Module."""

    database: str
    schema: str
    sql_dialect: NotRequired[str | None]
    connection_profile: NotRequired[str | None]


class OTSTransformation(TypedDict):
    """Single transformation definition in an OTS Module."""

    transformation_id: str
    description: NotRequired[str | None]
    transformation_type: NotRequired[
        str | None
    ]  # "sql" (default), future: "python", "pyspark", "r"
    sql_dialect: NotRequired[str | None]
    code: dict[
        str, Any
    ]  # Type-based structure: {"sql": {"original_sql": ..., "resolved_sql": ..., "source_tables": [...]}}
    schema: NotRequired[dict[str, Any] | None]
    materialization: NotRequired[dict[str, Any] | None]
    tests: NotRequired[dict[str, Any] | None]
    metadata: dict[str, Any]


class OTSFunction(TypedDict):
    """Single function definition in an OTS Module (OTS 0.2.0+)."""

    function_id: str  # Fully qualified function name (e.g., "schema.function_name")
    description: NotRequired[str | None]
    function_type: FunctionType  # "scalar", "aggregate", "table"
    language: str  # "sql", "python", "javascript", etc.
    parameters: NotRequired[list[FunctionParameter]]
    return_type: NotRequired[str | None]  # For scalar/aggregate functions
    return_table_schema: NotRequired[list[ColumnDefinition] | None]  # For table functions
    deterministic: NotRequired[
        bool | None
    ]  # Whether function is deterministic (same inputs = same outputs)
    code: dict[str, Any]  # Type-based structure with generic_sql and database_specific
    dependencies: NotRequired[dict[str, list[str]]]  # {"functions": [], "tables": []}
    metadata: dict[str, Any]  # Includes tags, object_tags, file_path, etc.


class OTSModule(TypedDict):
    """Complete OTS Module structure."""

    ots_version: str
    module_name: str
    module_description: NotRequired[str | None]
    version: NotRequired[str | None]
    tags: NotRequired[list[str] | None]
    test_library_path: NotRequired[str | None]
    target: OTSTarget
    transformations: list[OTSTransformation]
    functions: NotRequired[list[OTSFunction] | None]  # NEW in OTS 0.2.0
