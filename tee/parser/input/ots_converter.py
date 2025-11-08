"""
OTS to ParsedModel Converter - Converts OTS modules to t4t's internal ParsedModel format.

This module handles the transformation from OTS (Open Transformation Specification) format
to t4t's internal ParsedModel format for execution.
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path

from tee.parser.shared.types import ParsedModel
from tee.typing.metadata import OTSModule, OTSTransformation, OTSTarget
from tee.parser.shared.model_utils import create_model_metadata

logger = logging.getLogger(__name__)


class OTSConverterError(Exception):
    """Exception raised when converting OTS modules fails."""
    pass


class OTSConverter:
    """Converts OTS modules to ParsedModel format."""

    def __init__(self, module_path: Optional[Path] = None):
        """
        Initialize the OTS converter.

        Args:
            module_path: Optional path to the OTS module file (for resolving relative paths)
        """
        self.module_path = module_path

    def convert_module(self, module: OTSModule) -> Dict[str, ParsedModel]:
        """
        Convert an OTS module to a dictionary of ParsedModels.

        Args:
            module: OTS module dictionary

        Returns:
            Dictionary mapping transformation_id to ParsedModel

        Raises:
            OTSConverterError: If conversion fails
        """
        try:
            transformations = module.get("transformations", [])
            parsed_models = {}

            for transformation in transformations:
                transformation_id = transformation.get("transformation_id")
                if not transformation_id:
                    logger.warning("Skipping transformation without transformation_id")
                    continue

                try:
                    parsed_model = self._convert_transformation(transformation, module)
                    parsed_models[transformation_id] = parsed_model
                except Exception as e:
                    logger.error(f"Failed to convert transformation {transformation_id}: {e}")
                    raise OTSConverterError(f"Failed to convert transformation {transformation_id}: {e}")

            logger.info(f"Converted {len(parsed_models)} transformations from OTS module")
            return parsed_models

        except Exception as e:
            raise OTSConverterError(f"Failed to convert OTS module: {e}")

    def _convert_transformation(
        self, transformation: OTSTransformation, module: OTSModule
    ) -> ParsedModel:
        """
        Convert a single OTS transformation to ParsedModel format.

        Args:
            transformation: OTS transformation dictionary
            module: Parent OTS module (for module-level metadata)

        Returns:
            ParsedModel dictionary
        """
        transformation_id = transformation.get("transformation_id", "")
        transformation_type = transformation.get("transformation_type", "sql")

        # Extract code structure
        code_data = self._convert_code(transformation.get("code", {}), transformation_type)

        # Extract and merge metadata
        model_metadata = self._convert_metadata(transformation, module)

        # Build ParsedModel structure
        parsed_model: ParsedModel = {
            "code": code_data,
            "model_metadata": model_metadata,
            "sqlglot_hash": "",  # Will be computed if needed
        }

        return parsed_model

    def _convert_code(self, code: Dict[str, Any], transformation_type: str) -> Dict[str, Any]:
        """
        Convert OTS code structure to ParsedModel code structure.

        Args:
            code: OTS code dictionary
            transformation_type: Type of transformation (e.g., "sql")

        Returns:
            ParsedModel code dictionary
        """
        if transformation_type == "sql":
            sql_code = code.get("sql", {})
            if not sql_code:
                raise OTSConverterError("SQL transformation missing 'code.sql' field")

            # OTS format matches ParsedModel format closely
            # Use resolved_sql if available, fallback to original_sql
            resolved_sql = sql_code.get("resolved_sql") or sql_code.get("original_sql", "")
            original_sql = sql_code.get("original_sql", resolved_sql)
            source_tables = sql_code.get("source_tables", [])

            return {
                "sql": {
                    "original_sql": original_sql,
                    "resolved_sql": resolved_sql,
                    "operation_type": "select",  # Default, could be inferred from SQL
                    "source_tables": source_tables,
                }
            }
        else:
            # For non-SQL transformations, preserve as-is for now
            logger.warning(f"Non-SQL transformation type '{transformation_type}' not fully supported yet")
            return code

    def _convert_metadata(
        self, transformation: OTSTransformation, module: OTSModule
    ) -> Dict[str, Any]:
        """
        Convert OTS metadata to ParsedModel metadata structure.

        Args:
            transformation: OTS transformation dictionary
            module: Parent OTS module (for module-level metadata)

        Returns:
            ParsedModel model_metadata dictionary
        """
        transformation_id = transformation.get("transformation_id", "")
        
        # Extract schema.table from transformation_id
        if "." in transformation_id:
            table_name = transformation_id
            schema = transformation_id.split(".")[0]
        else:
            table_name = transformation_id
            schema = module.get("target", {}).get("schema", "default")

        # Extract description
        description = transformation.get("description")

        # Extract file_path from metadata
        metadata = transformation.get("metadata", {})
        file_path = metadata.get("file_path", "")

        # Convert schema first (needed for attaching column tests)
        schema_data = self._convert_schema(transformation.get("schema"))

        # Convert tests (attaches column tests to schema_data)
        tests_data = self._convert_tests(transformation.get("tests"), schema_data)

        # Convert materialization
        materialization_data = self._convert_materialization(transformation.get("materialization"))

        # Merge module-level and transformation-level tags
        module_tags = module.get("tags", [])
        transformation_tags = metadata.get("tags", [])
        all_tags = list(set(module_tags + transformation_tags))  # Deduplicate

        # Build nested metadata structure
        nested_metadata: Dict[str, Any] = {}

        if schema_data:
            nested_metadata["schema"] = schema_data
            # Add partitions if present
            if "partitioning" in transformation.get("schema", {}):
                nested_metadata["partitions"] = transformation["schema"]["partitioning"]
            # Add indexes if present
            if "indexes" in transformation.get("schema", {}):
                nested_metadata["indexes"] = transformation["schema"]["indexes"]

        if materialization_data:
            nested_metadata["materialization"] = materialization_data.get("type", "table")
            if materialization_data.get("type") == "incremental":
                nested_metadata["incremental"] = self._convert_incremental_details(
                    materialization_data.get("incremental_details", {})
                )
            elif materialization_data.get("type") == "scd2":
                nested_metadata["scd2_details"] = materialization_data.get("scd2_details", {})

        if tests_data:
            # Add table tests
            if "table" in tests_data:
                nested_metadata["tests"] = tests_data["table"]
            # Column tests are already in schema_data

        if all_tags:
            nested_metadata["tags"] = all_tags

        # Add object_tags if present
        object_tags = metadata.get("object_tags")
        if object_tags:
            nested_metadata["object_tags"] = object_tags

        # Create model_metadata structure
        model_metadata = create_model_metadata(
            table_name=table_name,
            file_path=file_path,
            description=description,
            metadata=nested_metadata,
        )

        return model_metadata

    def _convert_schema(self, schema: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Convert OTS schema to ParsedModel schema format.

        Args:
            schema: OTS schema dictionary

        Returns:
            List of column definitions (ParsedModel format)
        """
        if not schema:
            return None

        columns = schema.get("columns", [])
        if not columns:
            return None

        # Convert columns - OTS format is already compatible
        # But we need to handle tests separately
        column_definitions = []
        for col in columns:
            col_def = {
                "name": col.get("name"),
                "datatype": col.get("datatype", "string"),
                "description": col.get("description"),
            }
            # Tests will be added separately in _convert_tests
            column_definitions.append(col_def)

        return column_definitions

    def _convert_materialization(
        self, materialization: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Convert OTS materialization to ParsedModel format.

        Args:
            materialization: OTS materialization dictionary

        Returns:
            ParsedModel materialization dictionary
        """
        if not materialization:
            return {"type": "table"}

        mat_type = materialization.get("type", "table")
        result = {"type": mat_type}

        if mat_type == "incremental":
            # Store incremental_details for later conversion
            result["incremental_details"] = materialization.get("incremental_details", {})
        elif mat_type == "scd2":
            result["scd2_details"] = materialization.get("scd2_details", {})

        return result

    def _convert_incremental_details(
        self, incremental_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert OTS incremental_details to ParsedModel incremental config.

        Args:
            incremental_details: OTS incremental_details dictionary

        Returns:
            ParsedModel incremental config dictionary
        """
        if not incremental_details:
            return {}

        strategy = incremental_details.get("strategy", "append")

        result: Dict[str, Any] = {"strategy": strategy}

        if strategy == "delete_insert":
            delete_condition = incremental_details.get("delete_condition", "")
            result["delete_insert"] = {
                "where_condition": delete_condition,
                "time_column": "",  # OTS doesn't specify this separately
                "start_date": "auto",
            }
        elif strategy == "append":
            filter_condition = incremental_details.get("filter_condition", "")
            # Try to extract time_column from filter_condition
            # This is a heuristic - OTS format doesn't explicitly separate these
            result["append"] = {
                "time_column": "",  # Would need to parse from filter_condition
                "start_date": "auto",
            }
        elif strategy == "merge":
            merge_key = incremental_details.get("merge_key", [])
            update_columns = incremental_details.get("update_columns")
            result["merge"] = {
                "unique_key": merge_key,
                "time_column": "",  # OTS doesn't specify this separately
                "start_date": "auto",
            }
            if update_columns:
                result["merge"]["update_columns"] = update_columns

        return result

    def _convert_tests(
        self, tests: Optional[Dict[str, Any]], schema_data: Optional[List[Dict[str, Any]]]
    ) -> Optional[Dict[str, Any]]:
        """
        Convert OTS tests to ParsedModel format.

        Args:
            tests: OTS tests dictionary
            schema_data: Schema data (to attach column tests)

        Returns:
            Tests dictionary with table and column tests
        """
        if not tests:
            return None

        result: Dict[str, Any] = {}

        # Convert table tests
        table_tests = tests.get("table", [])
        if table_tests:
            result["table"] = table_tests

        # Convert column tests - attach to schema columns
        column_tests = tests.get("columns", {})
        if column_tests and schema_data:
            # Create a mapping of column name to tests
            col_test_map = {}
            for col_name, col_test_list in column_tests.items():
                col_test_map[col_name] = col_test_list

            # Attach tests to columns in schema
            for col_def in schema_data:
                col_name = col_def.get("name")
                if col_name in col_test_map:
                    col_def["tests"] = col_test_map[col_name]

        return result if result else None

