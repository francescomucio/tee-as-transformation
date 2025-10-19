"""
Type definitions for SQL model metadata.
"""

from typing import TypedDict, List, Optional, Literal
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
    "object"
]

# Materialization types
MaterializationType = Literal["table", "view", "incremental"]

# Incremental strategy types
IncrementalStrategy = Literal["append", "merge", "delete_insert"]

# Test names for columns
ColumnTestName = Literal[
    "not_null",
    "unique", 
    "accepted_values",
    "relationships",
    "expression",
    "custom"
]

# Test names for models
ModelTestName = Literal[
    "row_count_gt_0",
    "no_duplicates",
    "freshness",
    "custom"
]


class ColumnDefinition(TypedDict):
    """Type definition for a column in the schema."""
    name: str
    datatype: DataType
    description: NotRequired[Optional[str]]
    tests: NotRequired[List[ColumnTestName]]


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
    tests: NotRequired[Optional[List[ModelTestName]]]
    incremental: NotRequired[Optional[IncrementalConfig]]


class ParsedModelMetadata(TypedDict):
    """Type definition for parsed and validated metadata."""
    description: NotRequired[Optional[str]]
    schema: NotRequired[Optional[List[ColumnDefinition]]]
    partitions: NotRequired[List[str]]
    materialization: NotRequired[Optional[MaterializationType]]
    tests: NotRequired[List[ModelTestName]]
    incremental: NotRequired[Optional[IncrementalConfig]]


