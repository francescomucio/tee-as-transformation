"""
OTS Transformer - Transforms parsed models to OTS Module format.

This module implements the transformation from tee's internal parsed model format
to the Open Transformation Specification (OTS) Module format.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple

from ...typing.metadata import OTSModule, OTSTarget, OTSTransformation
from ..shared.types import ParsedModel

# Configure logging
logger = logging.getLogger(__name__)


class OTSTransformer:
    """Transforms parsed models to OTS Module format."""

    def __init__(self, project_config: Dict[str, Any]):
        """
        Initialize the OTS transformer.

        Args:
            project_config: Project configuration from project.toml
        """
        self.project_config = project_config
        self.database = project_config.get("project_folder", "unknown")
        self.sql_dialect = self._infer_sql_dialect()
        logger.debug(
            f"Initialized OTS transformer: database={self.database}, dialect={self.sql_dialect}"
        )

    def _infer_sql_dialect(self) -> str:
        """Infer SQL dialect from connection type."""
        conn_type = self.project_config.get("connection", {}).get("type", "duckdb")
        dialect_map = {
            "duckdb": "duckdb",
            "postgresql": "postgres",
            "postgres": "postgres",
            "snowflake": "snowflake",
            "mysql": "mysql",
            "bigquery": "bigquery",
            "spark": "spark",
        }
        return dialect_map.get(conn_type, "duckdb")

    def transform_to_ots_modules(
        self, parsed_models: Dict[str, ParsedModel]
    ) -> Dict[str, OTSModule]:
        """
        Transform parsed models into OTS Module(s).

        Groups models by schema and creates one module per schema.

        Args:
            parsed_models: Dictionary of parsed models

        Returns:
            Dictionary mapping module_name to OTS Module
        """
        # Group models by schema
        models_by_schema = self._group_by_schema(parsed_models)

        modules = {}
        for schema, models in models_by_schema.items():
            module_name = f"{self.database}.{schema}"
            logger.info(f"Creating OTS module: {module_name} with {len(models)} transformations")
            module = self._create_ots_module(module_name, schema, models)
            modules[module_name] = module

        return modules

    def _group_by_schema(
        self, parsed_models: Dict[str, ParsedModel]
    ) -> Dict[str, List[Tuple[str, ParsedModel]]]:
        """
        Group models by their schema (first part of transformation_id).

        Args:
            parsed_models: Dictionary of parsed models

        Returns:
            Dictionary mapping schema to list of (model_id, model_data) tuples
        """
        grouped = {}
        for model_id, model_data in parsed_models.items():
            # Extract schema from model_id (e.g., "my_schema.table_name" → "my_schema")
            if "." in model_id:
                schema = model_id.split(".")[0]
            else:
                schema = "default"

            if schema not in grouped:
                grouped[schema] = []
            grouped[schema].append((model_id, model_data))

        logger.debug(f"Grouped models into {len(grouped)} schemas: {list(grouped.keys())}")
        return grouped

    def _create_ots_module(
        self, module_name: str, schema: str, models: List[Tuple[str, ParsedModel]]
    ) -> OTSModule:
        """
        Create a single OTS Module for a schema.

        Args:
            module_name: Full module name (database.schema)
            schema: Schema name
            models: List of (model_id, model_data) tuples for this schema

        Returns:
            Complete OTS Module structure
        """
        # Extract transformations
        transformations = []
        for model_id, model_data in models:
            transformation = self._transform_model(model_id, model_data, schema)
            transformations.append(transformation)

        # Build target configuration
        target: OTSTarget = {
            "database": self.database,
            "schema": schema,
            "sql_dialect": self.sql_dialect,
        }

        # Extract module-level tags from project config
        module_tags = []
        if "module" in self.project_config:
            module_config = self.project_config.get("module", {})
            if isinstance(module_config, dict):
                module_tags = module_config.get("tags", [])
        elif "tags" in self.project_config:
            root_tags = self.project_config.get("tags", [])
            if isinstance(root_tags, list):
                module_tags = root_tags

        # Ensure module_tags is a list
        if not isinstance(module_tags, list):
            module_tags = []

        # Build module
        module: OTSModule = {
            "ots_version": "0.1.0",
            "module_name": module_name,
            "module_description": f"Transformations for {schema} schema",
            "target": target,
            "transformations": transformations,
        }

        # Add module-level tags if present
        if module_tags:
            module["tags"] = module_tags

        return module

    def _transform_model(
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
        tags = self._merge_tags(model_data)
        if tags:
            transformation["metadata"]["tags"] = tags

        # Add object_tags (database-style, key-value pairs) if present
        object_tags = self._extract_object_tags(model_data)
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
        return self._infer_schema_from_sql(model_data)

    def _infer_schema_from_sql(self, model_data: ParsedModel) -> Optional[Dict[str, Any]]:
        """
        Infer schema from SQL query using sqlglot.

        Args:
            model_data: Parsed model data

        Returns:
            Schema structure with inferred columns, or None if inference fails
        """
        try:
            import sqlglot

            # Get SQL from code structure
            code_data = model_data.get("code", {})
            if not code_data or "sql" not in code_data:
                return None
            
            sql_content = code_data["sql"].get("original_sql")
            if not sql_content:
                return None

            # Parse the SQL to extract column definitions
            expr = sqlglot.parse_one(sql_content)

            # Extract SELECT columns
            columns = []
            if hasattr(expr, "expressions"):
                for col_expr in expr.expressions:
                    if hasattr(col_expr, "alias"):
                        col_name = col_expr.alias
                    elif hasattr(col_expr, "this"):
                        col_name = (
                            col_expr.this.name
                            if hasattr(col_expr.this, "name")
                            else str(col_expr.this)
                        )
                    else:
                        col_name = str(col_expr)

                    # Try to infer datatype from column expression
                    datatype = self._infer_datatype(col_expr)

                    if col_name and col_name != "*":
                        columns.append(
                            {"name": col_name, "datatype": datatype, "description": None}
                        )

            if columns:
                return {"columns": columns, "partitioning": []}
        except Exception as e:
            logger.debug(f"Failed to infer schema from SQL: {e}")

        return None

    def _infer_datatype(self, col_expr) -> str:
        """
        Infer OTS datatype from SQL column expression.

        Args:
            col_expr: SQLGlot column expression

        Returns:
            OTS datatype string
        """
        # Check for obvious type hints in the expression
        if hasattr(col_expr, "this"):
            sql_type = str(col_expr.this)

            # Simple heuristic based on SQL type
            if any(word in sql_type.upper() for word in ["TEXT", "VARCHAR", "CHAR", "STRING"]):
                return "string"
            elif any(word in sql_type.upper() for word in ["INT", "BIGINT", "SMALLINT", "INTEGER"]):
                return "number"
            elif any(
                word in sql_type.upper() for word in ["FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]
            ):
                return "number"
            elif any(word in sql_type.upper() for word in ["DATE", "TIMESTAMP", "TIME"]):
                return "date"
            elif any(word in sql_type.upper() for word in ["BOOLEAN", "BOOL"]):
                return "boolean"

        # Default to string if can't infer
        return "string"

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
            tests["table"] = table_tests

        return tests if tests else None

    def _merge_tags(self, model_data: ParsedModel) -> List[str]:
        """
        Merge module tags with transformation-specific tags.

        Args:
            model_data: Parsed model data

        Returns:
            Merged list of tags
        """
        # Extract module tags from project config
        module_tags = []
        if "module" in self.project_config:
            module_config = self.project_config.get("module", {})
            if isinstance(module_config, dict):
                module_tags = module_config.get("tags", [])
            elif isinstance(module_config, list):
                # Handle case where module is a list (unlikely but handle gracefully)
                module_tags = []
        elif "tags" in self.project_config:
            # Also support tags at root level of project config
            root_tags = self.project_config.get("tags", [])
            if isinstance(root_tags, list):
                module_tags = root_tags

        # Ensure module_tags is a list
        if not isinstance(module_tags, list):
            module_tags = []

        # Transformation-specific tags
        metadata = model_data.get("model_metadata", {}).get("metadata", {})
        transformation_tags = metadata.get("tags", [])
        if not isinstance(transformation_tags, list):
            transformation_tags = []

        # Merge and deduplicate while preserving order
        all_tags = module_tags + transformation_tags
        seen = set()
        merged_tags = []
        for tag in all_tags:
            tag_str = str(tag).lower() if tag else ""
            if tag_str and tag_str not in seen:
                seen.add(tag_str)
                merged_tags.append(tag)

        return merged_tags

    def _extract_object_tags(self, model_data: ParsedModel) -> Dict[str, str]:
        """
        Extract object_tags (database-style key-value pairs) from model data.

        Object tags are key-value pairs that are attached directly to database objects,
        like {"sensitivity_tag": "pii", "classification": "public"}.

        Args:
            model_data: Parsed model data

        Returns:
            Dictionary of object tags (key-value pairs)
        """
        metadata = model_data.get("model_metadata", {}).get("metadata", {})

        # Extract object_tags from metadata
        object_tags = metadata.get("object_tags", {})
        if not isinstance(object_tags, dict):
            return {}

        # Validate that all values are strings (or convert them)
        validated_tags = {}
        for key, value in object_tags.items():
            if key and isinstance(key, str):
                # Convert value to string if it's not already
                if value is not None:
                    validated_tags[key] = str(value)

        return validated_tags
