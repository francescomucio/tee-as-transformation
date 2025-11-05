"""
Model decorator for Python-based SQL models.
Inspired by dlt's resource decorator pattern.
"""

import logging
from typing import Dict, Any, Optional, Callable, List

from ..shared.exceptions import ParserError

# Configure logging
logger = logging.getLogger(__name__)


class ModelDecoratorError(ParserError):
    """Raised when model decorator validation fails."""

    pass


def model(
    table_name: Optional[str] = None,
    description: Optional[str] = None,
    variables: Optional[List[str]] = None,
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
            return exp.select("*").from_("other_table").where(exp.column("environment") == env)
    """

    def decorator(func: Callable) -> Callable:
        try:
            # Validate function name
            if not func.__name__ or not func.__name__.replace("_", "").isalnum():
                raise ModelDecoratorError(f"Invalid function name: {func.__name__}")

            # Validate table name
            if table_name and not table_name.replace(".", "").replace("_", "").isalnum():
                raise ModelDecoratorError(f"Invalid table name: {table_name}")

            # Store model metadata directly on the function (no wrapper needed)
            func._model_metadata = {
                "table_name": table_name or func.__name__,
                "function_name": func.__name__,
                "description": description,
                "variables": variables or [],
                "metadata": metadata,
                "needs_evaluation": True,  # Flag indicating this function needs to be evaluated
            }

            logger.debug(
                f"Registered function {func.__name__} as model with table name: {func._model_metadata['table_name']}"
            )

            return func

        except Exception as e:
            if isinstance(e, ModelDecoratorError):
                raise
            raise ModelDecoratorError(f"Failed to create model decorator: {str(e)}")

    return decorator
