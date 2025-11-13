"""
Registry classes for models and functions.

These registries allow Python files to auto-register models and functions
when executed, similar to how tests work with TestRegistry.
"""

from typing import Any


class ModelRegistry:
    """Registry for models discovered from Python files."""

    _models: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(cls, model_data: dict[str, Any]) -> None:
        """
        Register a model.

        Args:
            model_data: Model data dictionary (with model_metadata, code, etc.)
        """
        table_name = model_data["model_metadata"]["table_name"]
        cls._models[table_name] = model_data

    @classmethod
    def get(cls, table_name: str) -> dict[str, Any] | None:
        """
        Get a registered model by table name.

        Args:
            table_name: Table name

        Returns:
            Model data dictionary or None if not found
        """
        return cls._models.get(table_name)

    @classmethod
    def list_all(cls) -> list[str]:
        """
        List all registered model table names.

        Returns:
            List of table names
        """
        return list(cls._models.keys())

    @classmethod
    def get_all(cls) -> dict[str, dict[str, Any]]:
        """
        Get all registered models.

        Returns:
            Dictionary mapping table_name to model data
        """
        return cls._models.copy()

    @classmethod
    def clear(cls) -> None:
        """
        Clear all registered models (mainly for testing).
        """
        cls._models.clear()


class FunctionRegistry:
    """Registry for functions discovered from Python files."""

    _functions: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(cls, function_data: dict[str, Any]) -> None:
        """
        Register a function.

        Args:
            function_data: Function data dictionary (with function_metadata, code, etc.)
        """
        function_name = function_data["function_metadata"]["function_name"]
        cls._functions[function_name] = function_data

    @classmethod
    def get(cls, function_name: str) -> dict[str, Any] | None:
        """
        Get a registered function by function name.

        Args:
            function_name: Function name

        Returns:
            Function data dictionary or None if not found
        """
        return cls._functions.get(function_name)

    @classmethod
    def list_all(cls) -> list[str]:
        """
        List all registered function names.

        Returns:
            List of function names
        """
        return list(cls._functions.keys())

    @classmethod
    def get_all(cls) -> dict[str, dict[str, Any]]:
        """
        Get all registered functions.

        Returns:
            Dictionary mapping function_name to function data
        """
        return cls._functions.copy()

    @classmethod
    def clear(cls) -> None:
        """
        Clear all registered functions (mainly for testing).
        """
        cls._functions.clear()

