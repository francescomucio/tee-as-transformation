"""
Python function file parsing functionality for UDFs.
"""

import ast
import importlib.util
import logging
from pathlib import Path
from typing import Any

from tee.parser.shared.exceptions import FunctionParsingError
from tee.parser.shared.types import FilePath
from tee.typing.metadata import FunctionMetadataDict, FunctionType, ParsedFunctionMetadata

from .base import BaseParser

# Configure logging
logger = logging.getLogger(__name__)


class FunctionPythonParsingError(FunctionParsingError):
    """Raised when Python function parsing fails."""

    pass


class FunctionPythonParser(BaseParser):
    """Handles Python file parsing and function extraction using static analysis."""

    def __init__(self):
        """Initialize the Python function parser."""
        super().__init__()
        # Cache for evaluated functions (lazy evaluation)
        self._evaluation_cache: dict[str, Any] = {}

    def clear_cache(self) -> None:
        """Clear all caches."""
        super().clear_cache()
        self._evaluation_cache.clear()

    def parse(self, content: str, file_path: FilePath = None) -> dict[str, dict[str, Any]]:
        """
        Parse Python content and extract UDF functions using static analysis.

        Args:
            content: The Python content to parse
            file_path: Optional file path for context

        Returns:
            Dict mapping function_name to function registration data

        Raises:
            FunctionPythonParsingError: If parsing fails
        """
        if file_path is None:
            raise FunctionPythonParsingError("file_path is required for Python function parsing")

        file_path = Path(file_path)
        file_path_str = str(file_path)

        # Check cache first
        if file_path_str in self._cache:
            return self._cache[file_path_str]

        try:
            logger.info(f"Parsing Python function file: {file_path}")

            # First, try to parse as metadata-only file
            metadata_dict = self._try_parse_metadata_only(content, file_path_str)
            if metadata_dict:
                # This is a metadata-only file
                functions = self._parse_metadata_dict(metadata_dict, file_path_str)
                self._set_cache(file_path_str, functions)
                logger.info(
                    f"Successfully parsed {len(functions)} function(s) from metadata in {file_path}"
                )
                return functions

            # Otherwise, parse AST to find functions with decorators
            tree = ast.parse(content, filename=file_path_str)

            # Extract functions and their metadata
            functions = {}

            # Walk through function definitions
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Check if function has function metadata (from @functions.sql() or @functions.python() decorator)
                    function_metadata = self._extract_function_metadata(node)

                    if function_metadata:
                        function_name = function_metadata["function_name"]
                        logger.debug(f"Found function: {node.name} -> {function_name}")

                        # Extract docstring for description if not provided
                        if not function_metadata.get("description"):
                            docstring = ast.get_docstring(node)
                            if docstring:
                                function_metadata["description"] = docstring.strip()

                        # Extract function signature for parameters if not provided
                        if not function_metadata.get("parameters"):
                            function_metadata["parameters"] = self._extract_function_signature(node)

                        # Create standardized function structure
                        function_data = {
                            "function_metadata": function_metadata,
                            "needs_evaluation": function_metadata.get("needs_evaluation", False),
                            "code": None,  # Will be populated when evaluated (for SQL-generating functions)
                        }

                        functions[function_name] = function_data
                        logger.debug(f"Registered function: {function_name}")

            # Cache the result
            self._set_cache(file_path_str, functions)
            logger.info(f"Successfully registered {len(functions)} function(s) from {file_path}")
            return functions

        except Exception as e:
            if isinstance(e, FunctionPythonParsingError):
                raise
            raise FunctionPythonParsingError(
                f"Error parsing Python function file {file_path}: {str(e)}"
            ) from e

    def _try_parse_metadata_only(
        self, content: str, file_path_str: str
    ) -> FunctionMetadataDict | None:
        """
        Try to parse the file as a metadata-only file (with a `metadata` dict).

        Args:
            content: File content
            file_path_str: File path for error messages

        Returns:
            Metadata dict if found, None otherwise
        """
        try:
            # Create a temporary module to execute the file
            spec = importlib.util.spec_from_loader("temp_module", loader=None)
            if spec is None:
                return None

            module = importlib.util.module_from_spec(spec)
            # Execute the file content
            exec(content, module.__dict__)

            # Check if there's a metadata variable
            if hasattr(module, "metadata"):
                metadata = module.metadata
                if isinstance(metadata, dict):
                    # Validate it has function_name (required)
                    if "function_name" in metadata:
                        logger.debug(f"Found metadata-only function definition in {file_path_str}")
                        return metadata

            # Also check for multiple functions (list of metadata dicts)
            if hasattr(module, "functions"):
                functions_list = module.functions
                if isinstance(functions_list, list):
                    logger.debug(f"Found multiple function metadata definitions in {file_path_str}")
                    # For now, we'll handle the first one (multiple functions per file will be handled later)
                    if functions_list and isinstance(functions_list[0], dict):
                        return functions_list[0]

        except Exception as e:
            logger.debug(f"Failed to parse as metadata-only file: {e}")
            # Not a metadata-only file, continue with AST parsing

        return None

    def _parse_metadata_dict(
        self, metadata_dict: FunctionMetadataDict, file_path_str: str
    ) -> dict[str, dict[str, Any]]:
        """
        Parse a metadata dictionary into a function structure.

        Args:
            metadata_dict: Function metadata dictionary
            file_path_str: File path

        Returns:
            Dict mapping function_name to function data
        """
        function_name = metadata_dict["function_name"]
        function_type: FunctionType = metadata_dict.get("function_type", "scalar")
        language = metadata_dict.get("language", "sql")

        # Create standardized function metadata
        function_metadata: ParsedFunctionMetadata = {
            "function_name": function_name,
            "description": metadata_dict.get("description"),
            "function_type": function_type,
            "language": language,
            "parameters": metadata_dict.get("parameters", []),
            "return_type": metadata_dict.get("return_type"),
            "return_table_schema": metadata_dict.get("return_table_schema"),
            "schema": metadata_dict.get("schema"),
            "deterministic": metadata_dict.get("deterministic", False),
            "tests": metadata_dict.get("tests", []),
            "tags": metadata_dict.get("tags", []),
            "object_tags": metadata_dict.get("object_tags", {}),
        }

        function_data = {
            "function_metadata": function_metadata,
            "needs_evaluation": language == "sql",  # SQL-generating functions need evaluation
            "code": None,
            "file_path": file_path_str,
        }

        return {function_name: function_data}

    def _extract_function_metadata(self, node: ast.FunctionDef) -> dict[str, Any] | None:
        """
        Extract function metadata from a function node by looking for @functions.sql() or @functions.python() decorators.

        Args:
            node: AST function definition node

        Returns:
            Dict with function metadata or None if not a function
        """
        # Look for @functions.sql() or @functions.python() decorators
        for decorator in node.decorator_list:
            metadata = None

            # Handle @functions.sql() or @functions.python()
            if isinstance(decorator, ast.Call):
                # Check for functions.sql() or functions.python()
                if isinstance(decorator.func, ast.Attribute):
                    if (
                        isinstance(decorator.func.value, ast.Name)
                        and decorator.func.value.id == "functions"
                        and decorator.func.attr in ("sql", "python")
                    ):
                        # Extract decorator arguments
                        metadata = self._extract_decorator_arguments(decorator, decorator.func.attr)
                        if metadata:
                            # Add function name if not provided
                            if "function_name" not in metadata or not metadata["function_name"]:
                                metadata["function_name"] = node.name
                            metadata["language"] = decorator.func.attr
                            return metadata

                # Also check for direct function calls (e.g., @sql() if imported as from functions import sql)
                elif isinstance(decorator.func, ast.Name):
                    # This would require checking imports, which is complex
                    # For now, we'll only support functions.sql() and functions.python()
                    pass

        return None

    def _extract_decorator_arguments(
        self, decorator: ast.Call, language: str
    ) -> dict[str, Any] | None:
        """
        Extract arguments from a decorator call.

        Args:
            decorator: AST call node for the decorator
            language: Language type ("sql" or "python")

        Returns:
            Dict with extracted arguments or None
        """
        metadata = {}

        # Extract keyword arguments
        for keyword in decorator.keywords:
            if keyword.arg:
                value = self._extract_ast_value(keyword.value)
                if value is not None:
                    metadata[keyword.arg] = value

        # Set defaults
        metadata.setdefault("function_type", "scalar")
        metadata.setdefault("needs_evaluation", language == "sql")

        return metadata

    def _extract_ast_value(self, node: ast.AST) -> Any:
        """
        Extract a Python value from an AST node.

        Args:
            node: AST node

        Returns:
            Extracted value or None if cannot be extracted
        """
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.List):
            return [self._extract_ast_value(item) for item in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                self._extract_ast_value(k): self._extract_ast_value(v)
                for k, v in zip(node.keys, node.values)
            }
        elif isinstance(node, ast.Tuple):
            return tuple(self._extract_ast_value(item) for item in node.elts)

        # For complex expressions, return None (caller should handle)
        return None

    def _extract_function_signature(self, node: ast.FunctionDef) -> list[dict[str, Any]]:
        """
        Extract function parameters from AST node signature.

        Args:
            node: AST function definition node

        Returns:
            List of parameter dictionaries
        """
        parameters = []

        for arg in node.args.args:
            param: dict[str, Any] = {
                "name": arg.arg,
            }

            # Extract type hint if available
            if arg.annotation:
                type_str = self._extract_type_hint(arg.annotation)
                if type_str:
                    param["type"] = type_str

            # Check for default value
            # Note: defaults are in node.args.defaults, aligned with args from the end
            arg_index = node.args.args.index(arg)
            defaults_start = len(node.args.args) - len(node.args.defaults)
            if arg_index >= defaults_start:
                default_node = node.args.defaults[arg_index - defaults_start]
                default_value = self._extract_ast_value(default_node)
                if default_value is not None:
                    param["default"] = str(default_value)

            parameters.append(param)

        return parameters

    def _extract_type_hint(self, annotation: ast.AST) -> str | None:
        """
        Extract type hint as string from AST annotation node.

        Args:
            annotation: AST annotation node

        Returns:
            Type string or None
        """
        if isinstance(annotation, ast.Name):
            # Map Python types to SQL types
            type_mapping = {
                "int": "INTEGER",
                "float": "FLOAT",
                "str": "VARCHAR",
                "bool": "BOOLEAN",
            }
            return type_mapping.get(annotation.id, annotation.id.upper())
        elif isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
            return annotation.value

        # For complex types, return None (caller should handle)
        return None
