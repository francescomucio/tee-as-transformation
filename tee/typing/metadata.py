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


class ModelMetadataDict(TypedDict):
    """Type definition for the raw metadata dictionary from Python files."""
    schema: NotRequired[Optional[List[ColumnDefinition]]]
    partitions: NotRequired[Optional[List[str]]]
    materialization: NotRequired[Optional[MaterializationType]]
    tests: NotRequired[Optional[List[ModelTestName]]]


class ParsedModelMetadata(TypedDict):
    """Type definition for parsed and validated metadata."""
    schema: NotRequired[Optional[List[ColumnDefinition]]]
    partitions: NotRequired[List[str]]
    materialization: NotRequired[Optional[MaterializationType]]
    tests: NotRequired[List[ModelTestName]]


# Type aliases for backward compatibility and convenience
ColumnTest = ColumnTestName
ModelTest = ModelTestName
