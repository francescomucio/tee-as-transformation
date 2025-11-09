"""
Function decorators for Python-based UDF definitions.
Inspired by the model decorator pattern.
"""

import logging
from typing import Dict, Any, Optional, Callable, List, Union

from tee.parser.shared.exceptions import ParserError
from tee.typing.metadata import FunctionParameter, FunctionType

# Configure logging
logger = logging.getLogger(__name__)


class FunctionDecoratorError(ParserError):
    """Raised when function decorator validation fails."""

    pass


def sql(
    function_name: Optional[str] = None,
    description: Optional[str] = None,
    function_type: Optional[FunctionType] = None,
    parameters: Optional[List[FunctionParameter]] = None,
    return_type: Optional[str] = None,
    return_table_schema: Optional[List[Dict[str, Any]]] = None,
    schema: Optional[str] = None,
    deterministic: Optional[bool] = None,
    database_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    object_tags: Optional[Dict[str, str]] = None,
    **metadata: Any,
) -> Callable:
    """
    Decorator for marking Python functions as SQL-generating UDFs.

    This decorator stores metadata on the function. The function should return
    SQL code (as a string) or a dict mapping adapter types to SQL code.

    Args:
        function_name: Optional custom function name. If not provided, uses function name.
        description: Optional description of the function.
        function_type: Function type ("scalar", "aggregate", "table"). Default: "scalar".
        parameters: Optional list of function parameters.
        return_type: Optional return type (for scalar/aggregate functions).
        return_table_schema: Optional table schema (for table functions).
        schema: Optional schema name.
        deterministic: Whether the function is deterministic.
        database_name: Optional database-specific name (for overloading).
        tags: Optional list of tags (dbt-style).
        object_tags: Optional dict of object tags (database-style).
        **metadata: Additional metadata to store with the function.

    Returns:
        Decorated function with function metadata stored as _function_metadata attribute.

    Example:
        @functions.sql(
            function_name="calculate_metric",
            return_type="FLOAT",
            tags=["analytics"]
        )
        def generate_calc_sql(adapter_type: str) -> str:
            if adapter_type == "snowflake":
                return "CREATE FUNCTION ..."
            return "CREATE FUNCTION ..."
    """

    def decorator(func: Callable) -> Callable:
        try:
            # Validate function name
            if not func.__name__ or not func.__name__.replace("_", "").isalnum():
                raise FunctionDecoratorError(f"Invalid function name: {func.__name__}")

            # Validate function name parameter
            final_function_name = function_name or func.__name__
            if not final_function_name.replace(".", "").replace("_", "").isalnum():
                raise FunctionDecoratorError(f"Invalid function name: {final_function_name}")

            # Store function metadata directly on the function
            func._function_metadata = {
                "function_name": final_function_name,
                "description": description,
                "function_type": function_type or "scalar",
                "language": "sql",
                "parameters": parameters or [],
                "return_type": return_type,
                "return_table_schema": return_table_schema,
                "schema": schema,
                "deterministic": deterministic,
                "database_name": database_name,
                "tags": tags or [],
                "object_tags": object_tags or {},
                "metadata": metadata,
                "needs_evaluation": True,  # Flag indicating this function needs to be evaluated
            }

            logger.debug(
                f"Registered function {func.__name__} as SQL-generating UDF: {func._function_metadata['function_name']}"
            )

            return func

        except Exception as e:
            if isinstance(e, FunctionDecoratorError):
                raise
            raise FunctionDecoratorError(f"Failed to create SQL function decorator: {str(e)}")

    return decorator


def python(
    function_name: Optional[str] = None,
    description: Optional[str] = None,
    function_type: Optional[FunctionType] = None,
    parameters: Optional[List[FunctionParameter]] = None,
    return_type: Optional[str] = None,
    return_table_schema: Optional[List[Dict[str, Any]]] = None,
    schema: Optional[str] = None,
    deterministic: Optional[bool] = None,
    database_name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    object_tags: Optional[Dict[str, str]] = None,
    **metadata: Any,
) -> Callable:
    """
    Decorator for marking Python functions as Python UDFs.

    This decorator stores metadata on the function. The function will be executed
    as a Python UDF in the database (e.g., DuckDB Python UDFs).

    Args:
        function_name: Optional custom function name. If not provided, uses function name.
        description: Optional description of the function.
        function_type: Function type ("scalar", "aggregate", "table"). Default: "scalar".
        parameters: Optional list of function parameters.
        return_type: Optional return type (for scalar/aggregate functions).
        return_table_schema: Optional table schema (for table functions).
        schema: Optional schema name.
        deterministic: Whether the function is deterministic.
        database_name: Optional database-specific name (for overloading).
        tags: Optional list of tags (dbt-style).
        object_tags: Optional dict of object tags (database-style).
        **metadata: Additional metadata to store with the function.

    Returns:
        Decorated function with function metadata stored as _function_metadata attribute.

    Example:
        @functions.python(
            function_name="python_calculator",
            return_type="FLOAT",
            tags=["math"]
        )
        def python_calculator(x: float) -> float:
            return x * 2.5 + 10
    """

    def decorator(func: Callable) -> Callable:
        try:
            # Validate function name
            if not func.__name__ or not func.__name__.replace("_", "").isalnum():
                raise FunctionDecoratorError(f"Invalid function name: {func.__name__}")

            # Validate function name parameter
            final_function_name = function_name or func.__name__
            if not final_function_name.replace(".", "").replace("_", "").isalnum():
                raise FunctionDecoratorError(f"Invalid function name: {final_function_name}")

            # Store function metadata directly on the function
            func._function_metadata = {
                "function_name": final_function_name,
                "description": description,
                "function_type": function_type or "scalar",
                "language": "python",
                "parameters": parameters or [],
                "return_type": return_type,
                "return_table_schema": return_table_schema,
                "schema": schema,
                "deterministic": deterministic,
                "database_name": database_name,
                "tags": tags or [],
                "object_tags": object_tags or {},
                "metadata": metadata,
                "needs_evaluation": False,  # Python UDFs don't need evaluation (they are the code)
            }

            logger.debug(
                f"Registered function {func.__name__} as Python UDF: {func._function_metadata['function_name']}"
            )

            return func

        except Exception as e:
            if isinstance(e, FunctionDecoratorError):
                raise
            raise FunctionDecoratorError(f"Failed to create Python function decorator: {str(e)}")

    return decorator


# Create a namespace class for easier imports
class functions:
    """Namespace for function decorators."""

    sql = sql
    python = python

