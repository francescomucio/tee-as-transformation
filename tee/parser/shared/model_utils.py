"""
Utility functions for model metadata standardization.
"""

import hashlib
from typing import Dict, Any, Optional
from pathlib import Path

from tee.typing.metadata import ParsedModelMetadata


def create_model_metadata(
    table_name: str,
    function_name: Optional[str] = None,
    file_path: Optional[str] = None,
    description: Optional[str] = None,
    variables: Optional[list] = None,
    metadata: Optional[ParsedModelMetadata] = None,
) -> Dict[str, Any]:
    """
    Create standardized model metadata for both SQL and Python models.

    Args:
        table_name: Name of the table/model
        function_name: Name of the Python function (for Python models)
        file_path: Path to the source file
        description: Description of the model
        variables: List of variables used in the model
        metadata: Additional metadata dictionary

    Returns:
        Standardized model metadata dictionary
    """
    # Prioritize file metadata description over parameter description
    final_description = description
    if metadata and hasattr(metadata, "description") and metadata.description:
        final_description = metadata.description

    result = {
        "table_name": table_name,
        "function_name": function_name,
        "description": final_description,
        "variables": variables or [],
        "metadata": metadata or {},
    }

    # Add file_path if provided
    if file_path is not None:
        result["file_path"] = file_path

    return result


def compute_sqlglot_hash(sql_data: Dict[str, Any]) -> str:
    """
    Compute SHA256 hash of the resolved_sql from code data.

    Args:
        sql_data: Dictionary containing resolved_sql

    Returns:
        SHA256 hash of the resolved_sql as hexadecimal string
    """
    resolved_sql = sql_data.get("resolved_sql")
    if not resolved_sql:
        return ""

    # Compute SHA256 hash of the resolved SQL
    return hashlib.sha256(resolved_sql.encode("utf-8")).hexdigest()


def standardize_parsed_model(
    model_data: Dict[str, Any],
    table_name: str,
    file_path: Optional[str] = None,
    is_python_model: bool = False,
) -> Dict[str, Any]:
    """
    Standardize a parsed model to have both code and model_metadata keys.

    Args:
        model_data: Current model data
        table_name: Name of the table/model
        file_path: Path to the source file
        is_python_model: Whether this is a Python model

    Returns:
        Standardized model data with code and model_metadata keys
    """
    # Extract existing metadata
    if is_python_model:
        # For Python models, we need to get function_name from the original model_metadata
        # since it's not in the top-level model_data anymore
        original_metadata = model_data.get("model_metadata", {})
        function_name = original_metadata.get("function_name")
        description = original_metadata.get("description")
        variables = original_metadata.get("variables", [])
        metadata = original_metadata.get("metadata", {})

        # Create standardized model_metadata
        model_metadata = create_model_metadata(
            table_name=table_name,
            function_name=function_name,
            file_path=file_path,
            description=description,
            variables=variables,
            metadata=metadata,
        )

        # Keep code data (may be None for unexecuted Python models)
        code_data = model_data.get("code")

    else:
        # For SQL models, create model_metadata from table_name
        model_metadata = create_model_metadata(
            table_name=table_name, file_path=file_path, description=f"SQL model for {table_name}"
        )

        # Keep existing code data
        code_data = model_data.get("code", {})

    # Compute hash of the resolved SQL for change detection
    sqlglot_hash = ""
    if code_data and "sql" in code_data:
        sqlglot_hash = compute_sqlglot_hash(code_data["sql"])

    # Return standardized structure
    result = {
        "code": code_data,
        "model_metadata": model_metadata,
        "sqlglot_hash": sqlglot_hash,
    }

    # Preserve other important flags (but not duplicated metadata)
    if is_python_model:
        result["needs_evaluation"] = model_data.get("needs_evaluation", True)

    return result
