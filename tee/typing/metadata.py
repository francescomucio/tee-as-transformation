"""
Type definitions for SQL model metadata.
"""

from typing import TypedDict, List, Optional, Literal, Dict, Any, Union
from typing_extensions import NotRequired


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

# Test names for columns
ColumnTestName = Literal[
    "not_null", "unique", "accepted_values", "relationships", "expression", "custom"
]

# Test names for models
ModelTestName = Literal["row_count_gt_0", "no_duplicates", "freshness", "custom"]


# Test definition can be a simple string name or a dict with name/params/severity
TestDefinition = Union[
    str,  # Simple test name like "not_null"
    Dict[str, Any],  # Dict with "name"/"test", optional "params", and optional "severity"
]


class ColumnDefinition(TypedDict):
    """Type definition for a column in the schema."""

    name: str
    datatype: DataType
    description: NotRequired[Optional[str]]
    # Tests can be simple strings or dicts with parameters and severity
    tests: NotRequired[List[Union[ColumnTestName, Dict[str, Any]]]]


class IncrementalAppendConfig(TypedDict):
    """Configuration for append-only incremental strategy."""

    time_column: str
    start_date: NotRequired[Optional[str]]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[Optional[str]]  # e.g., "7 days", "1 week"


class IncrementalMergeConfig(TypedDict):
    """Configuration for merge incremental strategy."""

    unique_key: List[str]
    time_column: str
    start_date: NotRequired[Optional[str]]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[Optional[str]]  # e.g., "7 days", "1 week"


class IncrementalDeleteInsertConfig(TypedDict):
    """Configuration for delete+insert incremental strategy."""

    where_condition: str  # SQL WHERE clause to identify records to delete
    time_column: str
    start_date: NotRequired[Optional[str]]  # "auto" for max(time_column) pattern, or specific date
    lookback: NotRequired[Optional[str]]  # e.g., "7 days", "1 week"


class IncrementalConfig(TypedDict):
    """Configuration for incremental materialization strategies."""

    strategy: IncrementalStrategy
    append: NotRequired[Optional[IncrementalAppendConfig]]
    merge: NotRequired[Optional[IncrementalMergeConfig]]
    delete_insert: NotRequired[Optional[IncrementalDeleteInsertConfig]]


class ModelMetadataDict(TypedDict):
    """Type definition for the raw metadata dictionary from Python files."""

    description: NotRequired[Optional[str]]
    schema: NotRequired[Optional[List[ColumnDefinition]]]
    partitions: NotRequired[Optional[List[str]]]
    materialization: NotRequired[Optional[MaterializationType]]
    # Tests can be simple strings or dicts with parameters and severity
    tests: NotRequired[Optional[List[Union[ModelTestName, Dict[str, Any]]]]]
    incremental: NotRequired[Optional[IncrementalConfig]]


class ParsedModelMetadata(TypedDict):
    """Type definition for parsed and validated metadata."""

    description: NotRequired[Optional[str]]
    schema: NotRequired[Optional[List[ColumnDefinition]]]
    partitions: NotRequired[List[str]]
    materialization: NotRequired[Optional[MaterializationType]]
    tests: NotRequired[List[ModelTestName]]
    incremental: NotRequired[Optional[IncrementalConfig]]
    scd2_details: NotRequired[Optional[Dict[str, Any]]]  # For SCD2 materialization
    indexes: NotRequired[Optional[List[Dict[str, Any]]]]  # Explicit index definitions


# OTS-specific types for the Open Transformation Specification


class OTSTarget(TypedDict):
    """Target configuration for an OTS Module."""

    database: str
    schema: str
    sql_dialect: NotRequired[Optional[str]]
    connection_profile: NotRequired[Optional[str]]


class OTSTransformation(TypedDict):
    """Single transformation definition in an OTS Module."""

    transformation_id: str
    description: NotRequired[Optional[str]]
    sql_dialect: NotRequired[Optional[str]]
    sqlglot: Dict[str, Any]
    schema: NotRequired[Optional[Dict[str, Any]]]
    materialization: NotRequired[Optional[Dict[str, Any]]]
    tests: NotRequired[Optional[Dict[str, Any]]]
    metadata: Dict[str, Any]


class OTSModule(TypedDict):
    """Complete OTS Module structure."""

    ots_version: str
    module_name: str
    module_description: NotRequired[Optional[str]]
    version: NotRequired[Optional[str]]
    tags: NotRequired[Optional[List[str]]]
    target: OTSTarget
    transformations: List[OTSTransformation]
