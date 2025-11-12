"""
Python file parsing functionality for SQL models.
"""

import ast
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

import sqlglot

from tee.parser.shared.exceptions import PythonParsingError
from tee.parser.shared.model_utils import standardize_parsed_model
from tee.parser.shared.types import FilePath, ParsedModel, Variables

from .base import BaseParser
from .sql_parser import SQLParser

# Configure logging
logger = logging.getLogger(__name__)


class PythonModelError(PythonParsingError):
    """Raised when Python model execution fails."""

    pass


class PythonParser(BaseParser):
    """Handles Python file parsing and model extraction using static analysis."""

    def __init__(self):
        """Initialize the Python parser."""
        super().__init__()
        # Cache for evaluated functions (lazy evaluation)
        self._evaluation_cache: dict[str, Any] = {}
        # SQL parser for parsing SQLGlot expressions
        self._sql_parser = SQLParser()

    def clear_cache(self) -> None:
        """Clear all caches."""
        super().clear_cache()
        self._evaluation_cache.clear()
        logger.debug("Python parser caches cleared")

    def parse(self, content: str, file_path: FilePath = None) -> ParsedModel:
        """
        Parse Python content and extract SQL models using static analysis.

        Args:
            content: The Python content to parse
            file_path: Optional file path for context

        Returns:
            Dict mapping table_name to model registration data

        Raises:
            PythonParsingError: If parsing fails
        """
        if file_path is None:
            raise PythonParsingError("file_path is required for Python parsing")

        file_path = Path(file_path)
        file_path_str = str(file_path)

        # Check cache first
        if file_path_str in self._cache:
            logger.debug(f"Using cached result for {file_path}")
            return self._cache[file_path_str]

        try:
            logger.info(f"Parsing Python file: {file_path}")

            # Parse the AST to find functions
            tree = ast.parse(content, filename=file_path_str)

            # Extract functions and their metadata
            models = {}

            # Walk through function definitions
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Check if function has model metadata (from @model decorator)
                    model_metadata = self._extract_model_metadata(node)

                    if model_metadata:
                        table_name = model_metadata["table_name"]
                        logger.debug(f"Found model function: {node.name} -> {table_name}")

                        # Create standardized model structure
                        model_data = {
                            "model_metadata": model_metadata,  # Include the original metadata
                            "needs_evaluation": True,
                            "code": None,  # Will be populated when evaluated
                        }

                        # Standardize the model structure
                        models[table_name] = standardize_parsed_model(
                            model_data=model_data,
                            table_name=table_name,
                            file_path=str(file_path),
                            is_python_model=True,
                        )
                        logger.debug(f"Registered model: {table_name}")
                
                # Also check for create_model() calls (for dynamic model creation)
                elif isinstance(node, ast.Call):
                    model_metadata = self._extract_create_model_call(node)
                    if model_metadata:
                        table_name = model_metadata["table_name"]
                        logger.debug(f"Found create_model() call: {table_name}")

                        # Create standardized model structure
                        model_data = {
                            "model_metadata": model_metadata,
                            "needs_evaluation": False,  # SQL is already provided
                            "code": model_metadata.get("sql"),  # Use the SQL directly
                        }

                        # Standardize the model structure
                        models[table_name] = standardize_parsed_model(
                            model_data=model_data,
                            table_name=table_name,
                            file_path=str(file_path),
                            is_python_model=True,
                        )
                        logger.debug(f"Registered model from create_model(): {table_name}")

            # Cache the result
            self._set_cache(file_path_str, models)
            logger.info(f"Successfully registered {len(models)} models from {file_path}")
            return models

        except Exception as e:
            if isinstance(e, PythonParsingError):
                raise
            raise PythonParsingError(f"Error parsing Python file {file_path}: {str(e)}") from e

    def evaluate_model_function(
        self,
        model_data: dict[str, Any],
        full_table_name: str = None,
        variables: Variables | None = None,
    ) -> dict[str, Any]:
        """
        Evaluate a model function and return the SQL string.

        This method provides lazy evaluation of model functions when they are actually needed.

        Args:
            model_data: Model registration data from parse
            full_table_name: Full table name with schema (e.g., my_schema.users_summary)
            variables: Optional dictionary of variables to inject into the function's namespace

        Returns:
            Updated model data with parsed SQL
        """
        file_path = model_data["model_metadata"]["file_path"]
        function_name = model_data["model_metadata"]["function_name"]
        cache_key = f"{file_path}:{function_name}"

        # Check evaluation cache first
        if cache_key in self._evaluation_cache:
            logger.debug(f"Using cached evaluation result for {function_name}")
            return self._evaluation_cache[cache_key]

        try:
            logger.debug(f"Evaluating function {function_name} from {file_path}")

            # Evaluate the function to get the SQL string
            file_path_obj = Path(file_path)  # Convert string back to Path
            sql_string = self._execute_function(file_path_obj, function_name, variables)

            # Validate SQL syntax before proceeding
            try:
                parsed = sqlglot.parse_one(sql_string)
                if parsed is None:
                    raise PythonModelError(
                        f"Function {function_name} returned invalid SQL (parse returned None)"
                    )
            except Exception as e:
                raise PythonModelError(
                    f"Function {function_name} returned invalid SQL: {str(e)}"
                ) from e

            # Get table name for qualified SQL generation
            # Use full_table_name if provided (with schema), otherwise fall back to metadata table_name
            table_name = (
                full_table_name
                if full_table_name
                else model_data["model_metadata"]["table_name"]
            )

            # Parse the SQL string using the SQL parser
            parsed_data = self._sql_parser.parse(sql_string, file_path=file_path, table_name=table_name)

            # Update model data with code data while maintaining standardized structure
            updated_model_data = model_data.copy()
            updated_model_data["code"] = parsed_data["code"]
            updated_model_data["sqlglot_hash"] = parsed_data.get("sqlglot_hash", "")
            updated_model_data["needs_evaluation"] = False

            # Ensure the structure remains standardized
            updated_model_data = standardize_parsed_model(
                model_data=updated_model_data,
                table_name=full_table_name
                if full_table_name
                else model_data["model_metadata"]["table_name"],
                file_path=file_path,
                is_python_model=True,
            )

            # Cache the result
            self._evaluation_cache[cache_key] = updated_model_data
            logger.debug(
                f"Successfully evaluated model: {model_data['model_metadata']['table_name']}"
            )

            return updated_model_data

        except Exception as e:
            if isinstance(e, PythonModelError):
                raise
            raise PythonModelError(
                f"Error evaluating function {function_name} in {file_path}: {str(e)}"
            ) from e

    def evaluate_all_models(
        self, parsed_models: dict[str, Any], variables: Variables | None = None
    ) -> dict[str, Any]:
        """
        Evaluate all Python models that need evaluation.

        This method is called by the orchestrator to evaluate Python models before SQL execution.

        Args:
            parsed_models: Parsed models from parse
            variables: Optional dictionary of variables to inject into model functions

        Returns:
            Updated parsed models with parsed SQL
        """
        updated_models = parsed_models.copy()

        for table_name, model_data in parsed_models.items():
            if model_data.get("needs_evaluation", False):
                try:
                    # Pass the full table name (with schema) for qualified SQL generation
                    updated_models[table_name] = self.evaluate_model_function(
                        model_data, table_name, variables
                    )
                except Exception as e:
                    logger.error(f"Failed to evaluate model {table_name}: {e}")
                    # Keep the original model data but mark as failed
                    updated_models[table_name]["evaluation_error"] = str(e)
                    updated_models[table_name]["needs_evaluation"] = False

        return updated_models

    def update_models_with_resolved_sql(self, updated_models: dict[str, Any]) -> None:
        """
        Update the parser's cached models with resolved SQL from execution.

        This method should be called by the executor after execution to ensure
        the parser's cached models are updated with the resolved SQL.

        Args:
            updated_models: Models with resolved SQL from execution
        """
        for table_name, model_data in updated_models.items():
            # Skip None or invalid model data
            if not model_data or not isinstance(model_data, dict):
                continue

            # Only update Python models (those with function_name and file_path)
            code_data = model_data.get("code", {})
            if (
                "function_name" in model_data
                and "file_path" in model_data
                and code_data
                and "sql" in code_data
                and "resolved_sql" in code_data["sql"]
            ):
                # Update the execution cache with resolved SQL
                cache_key = f"{model_data['file_path']}:{model_data['function_name']}"
                if cache_key in self._execution_cache:
                    self._execution_cache[cache_key] = model_data
                    logger.debug(f"Updated execution cache with resolved SQL for {table_name}")

                # Also update the file cache if it exists
                file_path = model_data["file_path"]
                if file_path in self._cache and table_name in self._cache[file_path]:
                    self._cache[file_path][table_name] = model_data
                    logger.debug(f"Updated file cache with qualified SQL for {table_name}")

    def _extract_model_metadata(self, node: ast.FunctionDef) -> dict[str, Any] | None:
        """
        Extract model metadata from a function node by looking for @model decorator.

        Args:
            node: AST function definition node

        Returns:
            Dict with model metadata or None if not a model
        """
        # Look for @model decorator
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
                if decorator.func.id == "model":
                    # Extract decorator arguments
                    table_name = None
                    description = None
                    metadata = {}

                    # Handle positional arguments
                    if decorator.args:
                        table_name = self._extract_string_literal(decorator.args[0])

                    # Handle keyword arguments
                    variables = None
                    for keyword in decorator.keywords:
                        if keyword.arg == "table_name":
                            table_name = self._extract_string_literal(keyword.value)
                        elif keyword.arg == "description":
                            description = self._extract_string_literal(keyword.value)
                        elif keyword.arg == "variables":
                            variables = self._extract_literal(keyword.value)
                        else:
                            metadata[keyword.arg] = self._extract_literal(keyword.value)

                    return {
                        "table_name": table_name or node.name,
                        "function_name": node.name,
                        "description": description,
                        "variables": variables or [],
                        "metadata": metadata,
                    }

        # Only return metadata if function has @model decorator
        return None

    def _extract_create_model_call(self, node: ast.Call) -> dict[str, Any] | None:
        """
        Extract model metadata from a create_model() function call.

        Args:
            node: AST call node

        Returns:
            Dict with model metadata or None if not a create_model call
        """
        # Check if this is a call to create_model
        # Handle both: create_model() and module.create_model()
        is_create_model = False
        if isinstance(node.func, ast.Name) and node.func.id == "create_model":
            is_create_model = True
        elif isinstance(node.func, ast.Attribute) and node.func.attr == "create_model":
            is_create_model = True
        
        if is_create_model:
            # Extract keyword arguments
            table_name = None
            sql = None
            description = None
            variables = None
            metadata = {}

            for keyword in node.keywords:
                if keyword.arg == "table_name":
                    table_name = self._extract_string_literal(keyword.value)
                elif keyword.arg == "sql":
                    sql = self._extract_string_literal(keyword.value)
                elif keyword.arg == "description":
                    description = self._extract_string_literal(keyword.value)
                elif keyword.arg == "variables":
                    variables = self._extract_literal(keyword.value)
                else:
                    metadata[keyword.arg] = self._extract_literal(keyword.value)

            if table_name and sql:
                return {
                    "table_name": table_name,
                    "function_name": f"create_{table_name}".replace(".", "_"),
                    "description": description,
                    "variables": variables or [],
                    "metadata": metadata,
                    "sql": sql,  # Store SQL directly
                }

        return None

    def _extract_string_literal(self, node: ast.AST) -> str:
        """Extract string literal from AST node."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        # Handle f-strings (JoinedStr nodes)
        elif isinstance(node, ast.JoinedStr):
            # Try to reconstruct the f-string
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                elif isinstance(value, ast.FormattedValue):
                    # For f-strings with variables, we can't statically determine the value
                    # So we'll return a placeholder or try to extract the variable name
                    if isinstance(value.value, ast.Name):
                        parts.append(f"{{{value.value.id}}}")
                    else:
                        parts.append("{...}")
            return "".join(parts)
        return str(node)

    def _extract_literal(self, node: ast.AST) -> Any:
        """Extract literal value from AST node."""
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.List):
            return [self._extract_literal(item) for item in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                self._extract_literal(k): self._extract_literal(v)
                for k, v in zip(node.keys, node.values)
            }
        return str(node)

    def _execute_function(
        self, file_path: Path, function_name: str, variables: Variables | None = None
    ) -> str:
        """
        Execute a function from a Python file and return its result.

        Args:
            file_path: Path to the Python file
            function_name: Name of the function to execute
            variables: Optional dictionary of variables to inject into the function's namespace

        Returns:
            SQL string returned by the function

        Raises:
            PythonModelError: If function execution fails or returns invalid type
        """
        module_name = f"temp_module_{hash(file_path)}_{function_name}"

        try:
            # Create a module spec
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                raise PythonModelError(f"Could not load module from {file_path}")

            # Create and load the module
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            # Inject the model decorator before loading the module
            from ..processing.model import model

            module.model = model

            # Also inject it into the tee.parser module structure
            if "tee" not in sys.modules:
                import types

                tee_module = types.ModuleType("tee")
                sys.modules["tee"] = tee_module
            if "tee.parser" not in sys.modules:
                import types

                parser_module = types.ModuleType("parser")
                sys.modules["tee.parser"] = parser_module
            if "tee.parser.model" not in sys.modules:
                import types

                model_module = types.ModuleType("model")
                model_module.model = model
                sys.modules["tee.parser.model"] = model_module

            spec.loader.exec_module(module)

            # Inject variables into the module's namespace if provided
            if variables:
                for var_name, var_value in variables.items():
                    setattr(module, var_name, var_value)
                logger.debug(
                    f"Injected variables {list(variables.keys())} into module {module_name}"
                )

            # Inject the model decorator to make it available
            from ..processing.model import model

            module.model = model
            logger.debug(f"Injected model decorator into module {module_name}")

            # Get the function
            if not hasattr(module, function_name):
                raise PythonModelError(f"Function {function_name} not found in {file_path}")

            func = getattr(module, function_name)

            # Execute the function
            logger.debug(f"Executing function {function_name} from {file_path}")
            result = func()

            # Validate result is a string
            if not isinstance(result, str):
                raise PythonModelError(
                    f"Function {function_name} must return a SQL string, got {type(result)}. "
                    f"If using sqlglot, convert to string: return str(exp.select(...))"
                )

            # Validate SQL string is not empty
            if not result or not result.strip():
                raise PythonModelError(
                    f"Function {function_name} returned an empty SQL string"
                )

            return result.strip()

        except Exception as e:
            if isinstance(e, PythonModelError):
                raise
            raise PythonModelError(
                f"Error executing function {function_name} in {file_path}: {str(e)}"
            ) from e

        finally:
            # Clean up
            if module_name in sys.modules:
                del sys.modules[module_name]
