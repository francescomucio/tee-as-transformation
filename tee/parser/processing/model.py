"""
Model creation utilities for Python-based SQL models.

This module provides both decorator and factory patterns for creating models:
- @model decorator: For decorating functions as models
- create_model(): For programmatically creating models

Inspired by dlt's resource decorator pattern.
"""

import logging
from collections.abc import Callable
from typing import Any

from tee.parser.shared.exceptions import ParserError

# Configure logging
logger = logging.getLogger(__name__)


class ModelError(ParserError):
    """Base exception for model-related errors."""

    pass


class ModelDecoratorError(ModelError):
    """Raised when model decorator validation fails."""

    pass


class ModelFactoryError(ModelError):
    """Raised when model factory operations fail."""

    pass


def create_model_metadata(
    table_name: str,
    function_name: str | None = None,
    description: str | None = None,
    variables: list[str] | None = None,
    needs_evaluation: bool = True,
    sql: str | None = None,
    **metadata: Any,
) -> dict[str, Any]:
    """
    Create standardized model metadata dictionary.

    This is a shared utility used by both the decorator and factory functions.

    Args:
        table_name: Name of the table/model
        function_name: Optional function name (defaults to table_name)
        description: Optional description
        variables: Optional list of variable names
        needs_evaluation: Whether the model needs evaluation
        sql: Optional SQL string (for pre-computed models)
        **metadata: Additional metadata

    Returns:
        Dictionary with model metadata
    """
    return {
        "table_name": table_name,
        "function_name": function_name or table_name,
        "description": description,
        "variables": variables or [],
        "metadata": metadata,
        "needs_evaluation": needs_evaluation,
        "sql": sql,  # Store SQL if provided
    }


def model(
    table_name: str | None = None,
    description: str | None = None,
    variables: list[str] | None = None,
    **metadata: Any,
) -> Callable:
    """
    Decorator for marking Python functions as SQL models.

    This decorator follows the dlt pattern - it stores metadata on the function
    without executing it. The function is only executed when actually needed.

    Args:
        table_name: Optional custom table name. If not provided, uses function name.
        description: Optional description of the model.
        variables: Optional list of variable names to inject into the function's namespace.
        **metadata: Additional metadata to store with the model.

    Returns:
        Decorated function with model metadata stored as _model_metadata attribute.

    Example:
        @model(table_name="custom_table", description="My custom table", variables=["env", "debug"])
        def my_function():
            return f"SELECT * FROM other_table WHERE environment = '{env}'"

        # If using sqlglot, convert to string:
        # from sqlglot import exp
        # return str(exp.select("*").from_("other_table").where(exp.column("environment") == env))
    """

    def decorator(func: Callable) -> Callable:
        try:
            # Validate function name
            if not func.__name__ or not func.__name__.replace("_", "").isalnum():
                raise ModelDecoratorError(f"Invalid function name: {func.__name__}")

            # Validate table name
            final_table_name = table_name or func.__name__
            if final_table_name and not final_table_name.replace(".", "").replace("_", "").isalnum():
                raise ModelDecoratorError(f"Invalid table name: {final_table_name}")

            # Create metadata using shared utility
            func._model_metadata = create_model_metadata(
                table_name=final_table_name,
                function_name=func.__name__,
                description=description,
                variables=variables,
                needs_evaluation=True,  # Decorated functions need evaluation
                **metadata,
            )

            logger.debug(
                f"Registered function {func.__name__} as model with table name: {func._model_metadata['table_name']}"
            )

            return func

        except Exception as e:
            if isinstance(e, ModelDecoratorError):
                raise
            raise ModelDecoratorError(f"Failed to create model decorator: {str(e)}") from e

    return decorator


def create_model(
    table_name: str,
    sql: str | None = None,
    description: str | None = None,  # noqa: ARG001
    variables: list[str] | None = None,  # noqa: ARG001
    **metadata: Any,  # noqa: ARG001
) -> None:
    """
    Dynamically create a model without using a decorator.

    This function is detected by the AST parser, which extracts the model
    metadata directly from the source code. At runtime, this function primarily
    validates inputs and serves as documentation.

    Note: The parser extracts model information via AST analysis, so the
    function created here is not actually used. This is intentional - the
    parser is the source of truth.

    Args:
        table_name: Name of the table/model to create
        sql: SQL query string (required)
        description: Optional description of the model (extracted by AST parser)
        variables: Optional list of variable names to inject (extracted by AST parser)
        **metadata: Additional metadata to store with the model (extracted by AST parser)

    Note:
        All parameters are validated here but extracted by the AST parser.
        The parser reads the source code directly, not the runtime values.

    Example:
        from tee.parser.model import create_model

        for table in ["users", "orders"]:
            create_model(
                table_name=table,
                sql=f"SELECT * FROM staging.{table}",
                description=f"Select from staging.{table}"
            )

    Raises:
        ModelFactoryError: If validation fails or sql is missing
    """
    # Validate inputs (helps catch errors early during development)
    if not sql:
        raise ModelFactoryError("sql parameter is required for create_model()")

    # Validate table name
    if not table_name or not table_name.replace(".", "").replace("_", "").isalnum():
        raise ModelFactoryError(f"Invalid table name: {table_name}")

    # Note: We don't create a function or register anything here.
    # The AST parser extracts this information directly from the source code.
    # This function exists primarily for:
    # 1. Validation (catches errors at import time)
    # 2. Documentation (makes the intent clear)
    # 3. IDE support (type checking, autocomplete)

    logger.debug(
        f"create_model() called for table: {table_name} "
        f"(parser will extract this via AST analysis)"
    )

