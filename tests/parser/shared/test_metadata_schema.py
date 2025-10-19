"""
Tests for metadata schema functionality.
"""

import pytest
import tempfile
import os
from pathlib import Path

from tee.parser.shared.metadata_schema import (
    ColumnSchema,
    ModelMetadata,
    validate_metadata_dict,
    parse_metadata_from_python_file
)
from tee.typing.metadata import (
    ColumnDefinition,
    ModelMetadataDict,
    ParsedModelMetadata,
    DataType,
    MaterializationType,
    ColumnTestName,
    ModelTestName
)


class TestColumnSchema:
    """Test ColumnSchema dataclass."""
    
    def test_column_schema_required_fields(self):
        """Test that required fields are validated."""
        # Valid column
        col = ColumnSchema(name="id", datatype="number")
        assert col.name == "id"
        assert col.datatype == "number"
        assert col.description is None
        assert col.tests == []
    
    def test_column_schema_optional_fields(self):
        """Test optional fields."""
        col = ColumnSchema(
            name="name",
            datatype="string",
            description="User name",
            tests=["not_null", "unique"]
        )
        assert col.name == "name"
        assert col.datatype == "string"
        assert col.description == "User name"
        assert col.tests == ["not_null", "unique"]
    
    def test_column_schema_validation(self):
        """Test validation of required fields."""
        with pytest.raises(ValueError, match="Column name is required"):
            ColumnSchema(name="", datatype="number")
        
        with pytest.raises(ValueError, match="Column datatype is required"):
            ColumnSchema(name="id", datatype="")


class TestModelMetadata:
    """Test ModelMetadata dataclass."""
    
    def test_model_metadata_defaults(self):
        """Test default values."""
        metadata = ModelMetadata()
        assert metadata.schema is None
        assert metadata.partitions == []
        assert metadata.materialization is None
        assert metadata.tests == []
    
    def test_model_metadata_validation(self):
        """Test validation of materialization type."""
        with pytest.raises(ValueError, match="Invalid materialization type"):
            ModelMetadata(materialization="invalid_type")


class TestValidateMetadataDict:
    """Test validate_metadata_dict function."""
    
    def test_valid_metadata(self):
        """Test valid metadata dictionary."""
        metadata_dict = {
            "schema": [
                {
                    "name": "id",
                    "datatype": "number",
                    "description": "ID field",
                    "tests": ["not_null"]
                }
            ],
            "partitions": ["id"],
            "materialization": "table",
            "tests": ["row_count_gt_0"]
        }
        
        result = validate_metadata_dict(metadata_dict)
        assert isinstance(result, ModelMetadata)
        assert len(result.schema) == 1
        assert result.schema[0].name == "id"
        assert result.partitions == ["id"]
        assert result.materialization == "table"
        assert result.tests == ["row_count_gt_0"]
    
    def test_minimal_metadata(self):
        """Test minimal metadata dictionary."""
        metadata_dict = {}
        result = validate_metadata_dict(metadata_dict)
        assert isinstance(result, ModelMetadata)
        assert result.schema is None
        assert result.partitions == []
        assert result.materialization is None
        assert result.tests == []
    
    def test_invalid_schema(self):
        """Test invalid schema format."""
        metadata_dict = {
            "schema": "not_a_list"
        }
        
        with pytest.raises(ValueError, match="Schema must be a list"):
            validate_metadata_dict(metadata_dict)
    
    def test_missing_required_column_fields(self):
        """Test missing required column fields."""
        metadata_dict = {
            "schema": [
                {
                    "name": "id"
                    # Missing datatype
                }
            ]
        }
        
        with pytest.raises(ValueError, match="Column datatype is required"):
            validate_metadata_dict(metadata_dict)


class TestParseMetadataFromPythonFile:
    """Test parse_metadata_from_python_file function."""
    
    def test_parse_metadata_file(self):
        """Test parsing metadata from a Python file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
# Metadata for test table
metadata = {
    "schema": [
        {
            "name": "id",
            "datatype": "number",
            "description": "Primary key",
            "tests": ["not_null", "unique"]
        },
        {
            "name": "name",
            "datatype": "string",
            "description": "Name field",
            "tests": ["not_null"]
        }
    ],
    "partitions": ["id"],
    "materialization": "table",
    "tests": ["row_count_gt_0"]
}
""")
            temp_file = f.name
        
        try:
            result = parse_metadata_from_python_file(temp_file)
            assert result is not None
            assert "schema" in result
            assert "partitions" in result
            assert "materialization" in result
            assert "tests" in result
            assert len(result["schema"]) == 2
            assert result["schema"][0]["name"] == "id"
            assert result["materialization"] == "table"
        finally:
            os.unlink(temp_file)
    
    def test_parse_nonexistent_file(self):
        """Test parsing non-existent file."""
        result = parse_metadata_from_python_file("nonexistent_file.py")
        assert result is None
    
    def test_parse_file_without_metadata(self):
        """Test parsing file without metadata variable."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
# This file has no metadata
def some_function():
    return "hello"
""")
            temp_file = f.name
        
        try:
            result = parse_metadata_from_python_file(temp_file)
            assert result is None
        finally:
            os.unlink(temp_file)
    
    def test_parse_invalid_python_file(self):
        """Test parsing invalid Python file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("invalid python syntax {")
            temp_file = f.name
        
        try:
            result = parse_metadata_from_python_file(temp_file)
            assert result is None
        finally:
            os.unlink(temp_file)


class TestTyping:
    """Test typing functionality."""
    
    def test_column_definition_typing(self):
        """Test that ColumnDefinition typing works correctly."""
        col: ColumnDefinition = {
            "name": "id",
            "datatype": "number",
            "description": "Primary key",
            "tests": ["not_null", "unique"]
        }
        assert col["name"] == "id"
        assert col["datatype"] == "number"
        assert col["description"] == "Primary key"
        assert col["tests"] == ["not_null", "unique"]
    
    def test_model_metadata_dict_typing(self):
        """Test that ModelMetadataDict typing works correctly."""
        metadata: ModelMetadataDict = {
            "schema": [
                {
                    "name": "id",
                    "datatype": "number",
                    "description": "Primary key",
                    "tests": ["not_null"]
                }
            ],
            "partitions": ["id"],
            "materialization": "table",
            "tests": ["row_count_gt_0"]
        }
        assert metadata["materialization"] == "table"
        assert len(metadata["schema"]) == 1
        assert metadata["partitions"] == ["id"]
    
    def test_parsed_model_metadata_typing(self):
        """Test that ParsedModelMetadata typing works correctly."""
        metadata: ParsedModelMetadata = {
            "schema": [
                {
                    "name": "id", 
                    "datatype": "number",
                    "description": "Primary key",
                    "tests": ["not_null"]
                }
            ],
            "partitions": ["id"],
            "materialization": "table",
            "tests": ["row_count_gt_0"]
        }
        assert metadata["materialization"] == "table"
        assert len(metadata["schema"]) == 1
        assert metadata["partitions"] == ["id"]
        assert metadata["tests"] == ["row_count_gt_0"]
