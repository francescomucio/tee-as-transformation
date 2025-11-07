"""
Metadata schema definitions and validation for SQL models.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import logging
import ast
import os

from tee.typing.metadata import (
    ColumnDefinition,
    ModelMetadataDict,
    ParsedModelMetadata,
    MaterializationType,
    DataType,
    ColumnTestName,
    ModelTestName,
    IncrementalStrategy,
    IncrementalConfig,
    IncrementalAppendConfig,
    IncrementalMergeConfig,
    IncrementalDeleteInsertConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class ColumnSchema:
    """Schema definition for a table column."""

    name: str
    datatype: DataType
    description: Optional[str] = None
    tests: Optional[List[ColumnTestName]] = None

    def __post_init__(self):
        """Validate required fields after initialization."""
        if not self.name:
            raise ValueError("Column name is required")
        if not self.datatype:
            raise ValueError("Column datatype is required")
        if self.tests is None:
            self.tests = []


@dataclass
class ModelMetadata:
    """Metadata definition for a SQL model."""

    description: Optional[str] = None
    schema: Optional[List[ColumnSchema]] = None
    partitions: Optional[List[str]] = None
    materialization: Optional[MaterializationType] = None
    tests: Optional[List[ModelTestName]] = None
    incremental: Optional[IncrementalConfig] = None

    def __post_init__(self):
        """Validate metadata after initialization."""
        if self.materialization and self.materialization not in ["table", "view", "incremental"]:
            raise ValueError(
                f"Invalid materialization type: {self.materialization}. Must be one of: table, view, incremental"
            )
        if self.tests is None:
            self.tests = []
        if self.partitions is None:
            self.partitions = []

        # Validate incremental configuration if present
        if self.incremental:
            self._validate_incremental_config()

    def _validate_incremental_config(self):
        """Validate incremental configuration."""
        if not self.incremental:
            return

        strategy = self.incremental.get("strategy")
        if not strategy:
            raise ValueError("Incremental strategy is required when incremental config is provided")

        if strategy not in ["append", "merge", "delete_insert"]:
            raise ValueError(
                f"Invalid incremental strategy: {strategy}. Must be one of: append, merge, delete_insert"
            )

        # Validate strategy-specific configuration
        if strategy == "append":
            if "append" not in self.incremental or not self.incremental["append"]:
                raise ValueError("Append strategy requires 'append' configuration")
            append_config = self.incremental["append"]
            if "time_column" not in append_config:
                raise ValueError("Append strategy requires 'time_column' in append configuration")

        elif strategy == "merge":
            if "merge" not in self.incremental or not self.incremental["merge"]:
                raise ValueError("Merge strategy requires 'merge' configuration")
            merge_config = self.incremental["merge"]
            if "unique_key" not in merge_config or not merge_config["unique_key"]:
                raise ValueError("Merge strategy requires 'unique_key' in merge configuration")
            if "time_column" not in merge_config:
                raise ValueError("Merge strategy requires 'time_column' in merge configuration")

        elif strategy == "delete_insert":
            if "delete_insert" not in self.incremental or not self.incremental["delete_insert"]:
                raise ValueError("Delete+insert strategy requires 'delete_insert' configuration")
            delete_insert_config = self.incremental["delete_insert"]
            if "where_condition" not in delete_insert_config:
                raise ValueError(
                    "Delete+insert strategy requires 'where_condition' in delete_insert configuration"
                )
            if "time_column" not in delete_insert_config:
                raise ValueError(
                    "Delete+insert strategy requires 'time_column' in delete_insert configuration"
                )


def validate_metadata_dict(metadata_dict: ModelMetadataDict) -> ModelMetadata:
    """
    Validate and convert a metadata dictionary to ModelMetadata object.

    Args:
        metadata_dict: Dictionary containing metadata

    Returns:
        Validated ModelMetadata object

    Raises:
        ValueError: If metadata is invalid
    """
    try:
        # Validate schema if present
        schema = None
        if "schema" in metadata_dict and metadata_dict["schema"]:
            if not isinstance(metadata_dict["schema"], list):
                raise ValueError("Schema must be a list of column definitions")

            schema = []
            for col_dict in metadata_dict["schema"]:
                if not isinstance(col_dict, dict):
                    raise ValueError("Each column in schema must be a dictionary")

                # Validate required fields
                if "name" not in col_dict:
                    raise ValueError("Column name is required")
                if "datatype" not in col_dict:
                    raise ValueError("Column datatype is required")

                schema.append(
                    ColumnSchema(
                        name=col_dict["name"],
                        datatype=col_dict["datatype"],
                        description=col_dict.get("description"),
                        tests=col_dict.get("tests", []),
                    )
                )

        # Validate other fields
        partitions = metadata_dict.get("partitions")
        if partitions is not None and not isinstance(partitions, list):
            raise ValueError("Partitions must be a list")

        materialization = metadata_dict.get("materialization")
        if materialization is not None and not isinstance(materialization, str):
            raise ValueError("Materialization must be a string")

        tests = metadata_dict.get("tests")
        if tests is not None and not isinstance(tests, list):
            raise ValueError("Tests must be a list")

        # Validate incremental configuration if present
        incremental = metadata_dict.get("incremental")
        if incremental is not None and not isinstance(incremental, dict):
            raise ValueError("Incremental configuration must be a dictionary")

        return ModelMetadata(
            description=metadata_dict.get("description"),
            schema=schema,
            partitions=partitions,
            materialization=materialization,
            tests=tests,
            incremental=incremental,
        )

    except Exception as e:
        raise ValueError(f"Invalid metadata format: {str(e)}")


def parse_metadata_from_python_file(file_path: str) -> Optional[ModelMetadataDict]:
    """
    Parse metadata from a Python file containing a metadata object.
    Supports both typed imports and AST parsing approaches.

    Args:
        file_path: Path to the Python file

    Returns:
        Dictionary containing metadata or None if not found

    Raises:
        ValueError: If file cannot be parsed or metadata is invalid
    """
    try:
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # First, try to execute the file to get typed metadata
        # This will work if the file imports the typing classes
        try:
            # Create a safe namespace for execution
            namespace = {}
            # Add the typing classes to the namespace
            from tee.typing.metadata import (
                ModelMetadataDict,
                ColumnDefinition,
                ParsedModelMetadata,
                DataType,
                MaterializationType,
                ColumnTestName,
                ModelTestName,
                IncrementalStrategy,
                IncrementalConfig,
                IncrementalAppendConfig,
                IncrementalMergeConfig,
                IncrementalDeleteInsertConfig,
            )

            namespace.update(
                {
                    "ModelMetadataDict": ModelMetadataDict,
                    "ColumnDefinition": ColumnDefinition,
                    "ParsedModelMetadata": ParsedModelMetadata,
                    "DataType": DataType,
                    "MaterializationType": MaterializationType,
                    "ColumnTestName": ColumnTestName,
                    "ModelTestName": ModelTestName,
                    "IncrementalStrategy": IncrementalStrategy,
                    "IncrementalConfig": IncrementalConfig,
                    "IncrementalAppendConfig": IncrementalAppendConfig,
                    "IncrementalMergeConfig": IncrementalMergeConfig,
                    "IncrementalDeleteInsertConfig": IncrementalDeleteInsertConfig,
                }
            )

            # Execute the file
            exec(content, namespace)

            # Look for metadata in the namespace
            if "metadata" in namespace:
                metadata = namespace["metadata"]
                if isinstance(metadata, dict):
                    return metadata
                elif hasattr(metadata, "__dict__"):
                    # If it's a dataclass or similar, convert to dict
                    return metadata.__dict__

        except Exception as exec_error:
            logger.debug(
                f"Typed execution failed for {file_path}, falling back to AST parsing: {exec_error}"
            )

        # Fall back to AST parsing if typed execution failed
        tree = ast.parse(content)

        # Look for metadata variable assignment
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "metadata":
                        # Evaluate the metadata value
                        if isinstance(node.value, ast.Dict):
                            # Convert AST dict to Python dict
                            metadata_dict = {}
                            for key, value in zip(node.value.keys, node.value.values):
                                if isinstance(key, ast.Constant):
                                    key_name = key.value
                                else:
                                    continue

                                metadata_dict[key_name] = _ast_to_python_value(value)

                            return metadata_dict
                        elif isinstance(node.value, ast.Constant):
                            # Direct constant assignment
                            return node.value.value

        return None

    except Exception as e:
        logger.warning(f"Failed to parse metadata from {file_path}: {str(e)}")
        return None


def _ast_to_python_value(node: ast.AST) -> Any:
    """
    Convert an AST node to its Python value.

    Args:
        node: AST node to convert

    Returns:
        Python value
    """
    if isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.List):
        return [_ast_to_python_value(item) for item in node.elts]
    elif isinstance(node, ast.Dict):
        result = {}
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant):
                key_name = key.value
            else:
                continue
            result[key_name] = _ast_to_python_value(value)
        return result
    else:
        # For complex expressions, return a string representation
        return str(node)
