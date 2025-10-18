"""
Metadata schema definitions and validation for SQL models.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import logging
import ast
import os

from ...typing.metadata import (
    ColumnDefinition,
    ModelMetadataDict,
    ParsedModelMetadata,
    MaterializationType,
    DataType,
    ColumnTestName,
    ModelTestName
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
    schema: Optional[List[ColumnSchema]] = None
    partitions: Optional[List[str]] = None
    materialization: Optional[MaterializationType] = None
    tests: Optional[List[ModelTestName]] = None
    
    def __post_init__(self):
        """Validate metadata after initialization."""
        if self.materialization and self.materialization not in ['table', 'view', 'incremental']:
            raise ValueError(f"Invalid materialization type: {self.materialization}. Must be one of: table, view, incremental")
        if self.tests is None:
            self.tests = []
        if self.partitions is None:
            self.partitions = []


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
        if 'schema' in metadata_dict and metadata_dict['schema']:
            if not isinstance(metadata_dict['schema'], list):
                raise ValueError("Schema must be a list of column definitions")
            
            schema = []
            for col_dict in metadata_dict['schema']:
                if not isinstance(col_dict, dict):
                    raise ValueError("Each column in schema must be a dictionary")
                
                # Validate required fields
                if 'name' not in col_dict:
                    raise ValueError("Column name is required")
                if 'datatype' not in col_dict:
                    raise ValueError("Column datatype is required")
                
                schema.append(ColumnSchema(
                    name=col_dict['name'],
                    datatype=col_dict['datatype'],
                    description=col_dict.get('description'),
                    tests=col_dict.get('tests', [])
                ))
        
        # Validate other fields
        partitions = metadata_dict.get('partitions')
        if partitions is not None and not isinstance(partitions, list):
            raise ValueError("Partitions must be a list")
        
        materialization = metadata_dict.get('materialization')
        if materialization is not None and not isinstance(materialization, str):
            raise ValueError("Materialization must be a string")
        
        tests = metadata_dict.get('tests')
        if tests is not None and not isinstance(tests, list):
            raise ValueError("Tests must be a list")
        
        return ModelMetadata(
            schema=schema,
            partitions=partitions,
            materialization=materialization,
            tests=tests
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
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # First, try to execute the file to get typed metadata
        # This will work if the file imports the typing classes
        try:
            # Create a safe namespace for execution
            namespace = {}
            # Add the typing classes to the namespace
            from ...typing.metadata import (
                ModelMetadataDict, ColumnDefinition, ParsedModelMetadata,
                DataType, MaterializationType, ColumnTestName, ModelTestName
            )
            namespace.update({
                'ModelMetadataDict': ModelMetadataDict,
                'ColumnDefinition': ColumnDefinition,
                'ParsedModelMetadata': ParsedModelMetadata,
                'DataType': DataType,
                'MaterializationType': MaterializationType,
                'ColumnTestName': ColumnTestName,
                'ModelTestName': ModelTestName,
            })
            
            # Execute the file
            exec(content, namespace)
            
            # Look for metadata in the namespace
            if 'metadata' in namespace:
                metadata = namespace['metadata']
                if isinstance(metadata, dict):
                    return metadata
                elif hasattr(metadata, '__dict__'):
                    # If it's a dataclass or similar, convert to dict
                    return metadata.__dict__
                    
        except Exception as exec_error:
            logger.debug(f"Typed execution failed for {file_path}, falling back to AST parsing: {exec_error}")
        
        # Fall back to AST parsing if typed execution failed
        tree = ast.parse(content)
        
        # Look for metadata variable assignment
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'metadata':
                        # Evaluate the metadata value
                        if isinstance(node.value, ast.Dict):
                            # Convert AST dict to Python dict
                            metadata_dict = {}
                            for key, value in zip(node.value.keys, node.value.values):
                                if isinstance(key, ast.Constant):
                                    key_name = key.value
                                elif hasattr(ast, 'Str') and isinstance(key, ast.Str):  # Python < 3.8 compatibility
                                    key_name = key.s
                                else:
                                    continue
                                
                                metadata_dict[key_name] = _ast_to_python_value(value)
                            
                            return metadata_dict
                        elif isinstance(node.value, ast.Constant):
                            # Direct constant assignment
                            return node.value.value
                        elif hasattr(ast, 'Str') and isinstance(node.value, ast.Str):  # Python < 3.8 compatibility
                            return node.value.s
        
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
    elif hasattr(ast, 'Str') and isinstance(node, ast.Str):  # Python < 3.8 compatibility
        return node.s
    elif hasattr(ast, 'Num') and isinstance(node, ast.Num):  # Python < 3.8 compatibility
        return node.n
    elif hasattr(ast, 'NameConstant') and isinstance(node, ast.NameConstant):  # Python < 3.8 compatibility
        return node.value
    elif isinstance(node, ast.List):
        return [_ast_to_python_value(item) for item in node.elts]
    elif isinstance(node, ast.Dict):
        result = {}
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant):
                key_name = key.value
            elif hasattr(ast, 'Str') and isinstance(key, ast.Str):  # Python < 3.8 compatibility
                key_name = key.s
            else:
                continue
            result[key_name] = _ast_to_python_value(value)
        return result
    else:
        # For complex expressions, return a string representation
        return str(node)
