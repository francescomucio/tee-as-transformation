"""Model transformation to OTS format."""

import logging
from typing import Dict, Any, Optional

from tee.typing.metadata import OTSTransformation
from tee.parser.shared.types import ParsedModel
from .base import BaseTransformer
from ..inferencers.schema_inferencer import SchemaInferencer

logger = logging.getLogger(__name__)


class ModelTransformer(BaseTransformer):
    """Transforms parsed models to OTS transformations."""

    def __init__(self, project_config: Dict[str, Any], sql_dialect: str):
        """
        Initialize the model transformer.

        Args:
            project_config: Project configuration dictionary
            sql_dialect: SQL dialect string
        """
        super().__init__(project_config)
        self.sql_dialect = sql_dialect
        self.schema_inferencer = SchemaInferencer()

    def transform(
        self, model_id: str, model_data: ParsedModel, schema: str
    ) -> OTSTransformation:
        """
        Transform a single parsed model to OTS transformation format.

        Args:
            model_id: Model identifier (e.g., "my_schema.table")
            model_data: Parsed model data
            schema: Schema name

        Returns:
            Transformed model as OTSTransformation
        """
        model_metadata = model_data.get("model_metadata", {})

        # Extract description
        description = model_metadata.get("description")

        # Get code structure
        code_data = model_data.get("code", {})

        # Transform structure
        transformation: OTSTransformation = {
            "transformation_id": model_id,
            "description": description,
            "transformation_type": "sql",  # Default to SQL for now
            "sql_dialect": self.sql_dialect,
            "code": code_data,
            "schema": self._transform_schema(model_data),
            "materialization": self._transform_materialization(model_data),
            "metadata": {"file_path": model_metadata.get("file_path", "")},
        }

        # Add tests if present
        tests = self._transform_tests(model_data)
        if tests:
            transformation["tests"] = tests

        # Add tags (dbt-style, list of strings) if present
        metadata = model_data.get("model_metadata", {}).get("metadata", {})
        transformation_tags = metadata.get("tags", [])
        tags = self.tag_manager.merge_tags(transformation_tags)
        if tags:
            transformation["metadata"]["tags"] = tags

        # Add object_tags (database-style, key-value pairs) if present
        object_tags = self.tag_manager.extract_object_tags(metadata)
        if object_tags:
            transformation["metadata"]["object_tags"] = object_tags

        return transformation

    def _transform_schema(self, model_data: ParsedModel) -> Optional[Dict[str, Any]]:
        """
        Transform schema structure from metadata.

        If schema is not in metadata, attempts to infer from SQL query using sqlglot.

        Args:
            model_data: Parsed model data

        Returns:
            Schema structure with columns, partitioning, and indexes
        """
        metadata = model_data.get("model_metadata", {}).get("metadata", {})

        # First try to get schema from metadata
        if "schema" in metadata and metadata["schema"]:
            schema_data: Dict[str, Any] = {
                "columns": [],
                "partitioning": metadata.get("partitions", []),
            }

            # Extract columns without tests
            for col in metadata["schema"]:
                col_def = {
                    "name": col["name"],
                    "datatype": col["datatype"],
                    "description": col.get("description"),
                }
                schema_data["columns"].append(col_def)

            # Extract indexes if explicitly defined
            if "indexes" in metadata and metadata["indexes"]:
                schema_data["indexes"] = metadata["indexes"]

            return schema_data

        # If no explicit schema in metadata, try to infer from SQL
        return self.schema_inferencer.infer_from_sql(model_data)

    def _transform_materialization(self, model_data: ParsedModel) -> Dict[str, Any]:
        """
        Transform materialization configuration.

        Args:
            model_data: Parsed model data

        Returns:
            Materialization structure
        """
        metadata = model_data.get("model_metadata", {}).get("metadata", {})
        mat_type = metadata.get("materialization", "table")

        if mat_type == "incremental":
            inc_config = metadata.get("incremental", {})
            strategy = inc_config.get("strategy")

            return {
                "type": "incremental",
                "incremental_details": self._transform_incremental_details(inc_config, strategy),
            }
        elif mat_type == "scd2":
            scd2_config = metadata.get("scd2_details", {})
            return {"type": "scd2", "scd2_details": scd2_config}
        else:
            return {"type": mat_type}

    def _transform_incremental_details(
        self, inc_config: Dict[str, Any], strategy: str
    ) -> Dict[str, Any]:
        """
        Transform incremental strategy details to OTS format.

        Args:
            inc_config: Incremental configuration
            strategy: Strategy name (append, merge, delete_insert)

        Returns:
            Transformed incremental details
        """
        details = {"strategy": strategy}

        if strategy == "delete_insert":
            di_config = inc_config.get("delete_insert", {})
            where_condition = di_config.get("where_condition", "")
            details.update(
                {
                    "delete_condition": where_condition,
                    "filter_condition": where_condition,  # Same as delete_condition
                }
            )
        elif strategy == "append":
            append_config = inc_config.get("append", {})
            time_col = append_config.get("time_column", "")
            start_date = append_config.get("start_date", "")
            details["filter_condition"] = f"{time_col} >= {start_date}"
        elif strategy == "merge":
            merge_config = inc_config.get("merge", {})
            unique_key = merge_config.get("unique_key", [])
            details["merge_key"] = unique_key
            # Add update_columns if specified
            if "update_columns" in merge_config:
                details["update_columns"] = merge_config["update_columns"]

        return details

    def _transform_tests(self, model_data: ParsedModel) -> Optional[Dict[str, Any]]:
        """
        Extract and structure tests from model data.

        Args:
            model_data: Parsed model data

        Returns:
            Test structure with columns and table tests, or None
        """
        metadata = model_data.get("model_metadata", {}).get("metadata", {})

        tests = {}

        # Extract column tests from schema
        if "schema" in metadata and metadata["schema"]:
            col_tests = {}
            for col in metadata["schema"]:
                col_name = col["name"]
                col_test_list = col.get("tests", [])
                if col_test_list:
                    col_tests[col_name] = col_test_list
            if col_tests:
                tests["columns"] = col_tests

        # Extract table tests
        table_tests = metadata.get("tests", [])
        if table_tests:
            # Replace no_duplicates with unique (per OTS spec)
            normalized_table_tests = []
            for test_def in table_tests:
                if isinstance(test_def, str):
                    # Replace "no_duplicates" with "unique"
                    if test_def == "no_duplicates":
                        normalized_table_tests.append("unique")
                    else:
                        normalized_table_tests.append(test_def)
                elif isinstance(test_def, dict):
                    # Handle dict format: {"name": "no_duplicates", ...}
                    test_name = test_def.get("name") or test_def.get("test")
                    if test_name == "no_duplicates":
                        # Replace with unique
                        new_test = test_def.copy()
                        new_test["name"] = "unique"
                        normalized_table_tests.append(new_test)
                    else:
                        normalized_table_tests.append(test_def)
                else:
                    normalized_table_tests.append(test_def)

            tests["table"] = normalized_table_tests

        return tests if tests else None

