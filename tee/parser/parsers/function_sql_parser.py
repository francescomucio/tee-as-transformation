"""
SQL function parsing functionality for UDFs.
"""

import re
import logging
import sqlglot
from sqlglot import exp
from typing import Dict, Any, Optional, List
from pathlib import Path

from .base import BaseParser
from tee.parser.shared.types import FilePath
from tee.parser.shared.exceptions import SQLParsingError, FunctionParsingError, FunctionMetadataError
from tee.typing.metadata import ParsedFunctionMetadata, FunctionParameter, FunctionType
from tee.parser.shared.metadata_schema import parse_metadata_from_python_file
from tee.parser.shared.function_utils import validate_function_metadata_consistency
from tee.parser.shared.constants import SQL_BUILT_IN_FUNCTIONS

# Configure logging
logger = logging.getLogger(__name__)


class FunctionSQLParsingError(FunctionParsingError):
    """Raised when SQL function parsing fails."""

    pass


class FunctionSQLParser(BaseParser):
    """Handles SQL function parsing from CREATE FUNCTION statements."""

    def __init__(self, connection: Optional[Dict[str, Any]] = None, project_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the SQL function parser.

        Args:
            connection: Optional connection configuration dict (for dialect inference)
            project_config: Optional project configuration dict (for dialect inference)
        """
        super().__init__()
        self.connection = connection or {}
        self.project_config = project_config or {}

    def _infer_dialect_from_connection(self) -> str:
        """
        Infer SQL dialect from connection type.

        Returns:
            SQL dialect string (e.g., 'postgres', 'snowflake', 'duckdb')
        """
        conn_type = self.connection.get("type", "duckdb")
        dialect_map = {
            "duckdb": "duckdb",
            "postgresql": "postgres",
            "postgres": "postgres",
            "snowflake": "snowflake",
            "mysql": "mysql",
            "bigquery": "bigquery",
            "spark": "spark",
        }
        return dialect_map.get(conn_type.lower(), "postgres")  # Default to postgres for CREATE FUNCTION

    def _infer_dialect_from_filename(self, file_path: Path) -> Optional[str]:
        """
        Infer dialect from database-specific override filename.

        Args:
            file_path: Path to the SQL file

        Returns:
            Dialect string if detected from filename, None otherwise
        """
        stem = file_path.stem  # filename without extension
        if "." in stem:
            parts = stem.split(".")
            if len(parts) >= 2:
                # Check if last part before extension is a known database name
                potential_db = parts[-1].lower()
                from tee.parser.shared.constants import KNOWN_DATABASE_NAMES
                
                if potential_db in KNOWN_DATABASE_NAMES:
                    # Map database name to dialect
                    dialect_map = {
                        "duckdb": "duckdb",
                        "postgresql": "postgres",
                        "snowflake": "snowflake",
                        "bigquery": "bigquery",
                    }
                    return dialect_map.get(potential_db, "postgres")
        
        return None

    def _find_metadata_file(self, sql_file_path: str) -> Optional[str]:
        """
        Find companion Python metadata file for a SQL function file.

        Args:
            sql_file_path: Path to the SQL file

        Returns:
            Path to the Python metadata file if found, None otherwise
        """
        if not sql_file_path:
            return None

        sql_path = Path(sql_file_path)
        if not sql_path.exists():
            return None

        # Look for Python file with same name in same directory
        python_file = sql_path.with_suffix(".py")
        if python_file.exists():
            return str(python_file)

        return None

    def parse(
        self, content: str, file_path: FilePath = None, dialect: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse SQL function content and extract function metadata.

        Args:
            content: The SQL content to parse
            file_path: Optional file path for context
            dialect: Optional SQL dialect override (if not provided, inferred from connection/project_config/filename)

        Returns:
            Dict mapping function_name to function registration data

        Raises:
            FunctionSQLParsingError: If parsing fails
        """
        if file_path is None:
            raise FunctionSQLParsingError("file_path is required for SQL function parsing")

        file_path = Path(file_path)
        file_path_str = str(file_path)

        # Check cache first
        cache_key = self._get_cache_key(content, file_path)
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result

        try:
            logger.info(f"Parsing SQL function file: {file_path}")

            # Try to find and parse companion metadata file first (to check for dialect override)
            metadata_file = self._find_metadata_file(file_path_str)
            metadata_dialect = None
            raw_metadata = None
            
            if metadata_file:
                try:
                    raw_metadata = parse_metadata_from_python_file(metadata_file)
                    if raw_metadata and isinstance(raw_metadata, dict):
                        # Check for dialect in metadata
                        if "dialect" in raw_metadata:
                            metadata_dialect = raw_metadata["dialect"]
                            logger.debug(f"Found dialect override in metadata: {metadata_dialect}")
                except Exception as e:
                    logger.warning(f"Failed to parse metadata from {metadata_file}: {str(e)}")

            # Determine dialect in priority order:
            # 1. Explicit dialect parameter
            # 2. Dialect from metadata
            # 3. Dialect from filename (database override)
            # 4. Dialect from connection/project config
            if not dialect:
                if metadata_dialect:
                    dialect = metadata_dialect
                else:
                    # Try to infer from filename (database override)
                    filename_dialect = self._infer_dialect_from_filename(file_path)
                    if filename_dialect:
                        dialect = filename_dialect
                        logger.debug(f"Inferred dialect from filename: {dialect}")
                    else:
                        # Infer from connection/project config
                        dialect = self._infer_dialect_from_connection()
                        logger.debug(f"Inferred dialect from connection: {dialect}")

            # Try to parse with SQLglot first
            function_data = self._parse_create_function_sqlglot(content, file_path_str, dialect)

            # Fall back to regex parsing if SQLglot fails
            if not function_data:
                logger.debug(f"SQLglot parsing failed, falling back to regex for {file_path_str}")
                function_data = self._parse_create_function_regex(content, file_path_str)

            if not function_data:
                raise FunctionSQLParsingError(f"No CREATE FUNCTION statement found in {file_path}")

            function_name = function_data["function_name"]

            # Merge metadata from Python file if available
            if raw_metadata and isinstance(raw_metadata, dict):
                try:
                    # Merge metadata from Python file
                    function_data = self._merge_metadata(function_data, raw_metadata)
                except Exception as e:
                    logger.warning(f"Failed to merge metadata from {metadata_file}: {str(e)}")

            # Extract dependencies from function_data (temporarily stored as _dependencies)
            dependencies = function_data.pop("_dependencies", {"tables": [], "functions": []})
            source_tables = dependencies.get("tables", [])
            source_functions = dependencies.get("functions", [])

            # Create standardized function structure
            result = {
                function_name: {
                    "function_metadata": function_data,
                    "needs_evaluation": False,  # SQL functions don't need evaluation
                    "code": {
                        "sql": {
                            "original_sql": content.strip(),
                            "function_body": function_data.get("function_body", ""),
                            "dialect": dialect,  # Store the dialect used for parsing
                            "source_tables": source_tables,  # Store dependencies like models
                            "source_functions": source_functions,  # Store dependencies like models
                        }
                    },
                    "file_path": file_path_str,
                }
            }

            # Cache the result
            self._set_cache(cache_key, result)
            logger.info(f"Successfully parsed function {function_name} from {file_path} (dialect: {dialect})")
            return result

        except Exception as e:
            if isinstance(e, FunctionSQLParsingError):
                raise
            raise FunctionSQLParsingError(f"Error parsing SQL function file {file_path}: {str(e)}")

    def _parse_create_function_sqlglot(
        self, content: str, file_path_str: str, dialect: str = "postgres"
    ) -> Optional[Dict[str, Any]]:
        """
        Parse CREATE FUNCTION using SQLglot.

        Args:
            content: SQL content
            file_path_str: File path for error messages
            dialect: SQL dialect to use for parsing

        Returns:
            Dict with function metadata or None if parsing fails
        """
        try:
            # Try parsing with the specified dialect
            parsed = sqlglot.parse_one(content, dialect=dialect)

            if not parsed or not isinstance(parsed, exp.Create):
                return None

            # Check if it's a UserDefinedFunction
            if not isinstance(parsed.this, exp.UserDefinedFunction):
                return None

            udf = parsed.this

            # Extract function name
            function_name_full = ""
            if hasattr(udf, "this") and udf.this:
                if isinstance(udf.this, exp.Table):
                    function_name_full = udf.this.name if hasattr(udf.this, "name") else str(udf.this)
                elif isinstance(udf.this, exp.Identifier):
                    function_name_full = udf.this.name if hasattr(udf.this, "name") else str(udf.this)
                else:
                    function_name_full = str(udf.this)

            if not function_name_full:
                return None

            # Extract schema and function name
            if "." in function_name_full:
                parts = function_name_full.split(".")
                schema = parts[0]
                function_name = parts[1]
            else:
                schema = None
                function_name = function_name_full

            # Extract parameters
            parameters = []
            if hasattr(udf, "expressions") and udf.expressions:
                for expr in udf.expressions:
                    if isinstance(expr, exp.ColumnDef):
                        param: FunctionParameter = {
                            "name": "",
                            "type": "",
                        }

                        # Extract parameter name
                        if hasattr(expr, "this") and expr.this:
                            if isinstance(expr.this, exp.Identifier):
                                param["name"] = expr.this.name if hasattr(expr.this, "name") else str(expr.this)
                            else:
                                param["name"] = str(expr.this)

                        # Extract parameter type
                        if hasattr(expr, "kind") and expr.kind:
                            if hasattr(expr.kind, "this"):
                                type_val = expr.kind.this
                                # Handle Type enum
                                if hasattr(type_val, "name"):
                                    param["type"] = type_val.name.upper()
                                elif hasattr(type_val, "value"):
                                    param["type"] = str(type_val.value).upper()
                                else:
                                    param["type"] = str(type_val).upper()
                            else:
                                param["type"] = str(expr.kind).upper()

                        if param["name"]:
                            parameters.append(param)

            # Extract return type from SQL (SQLglot doesn't expose RETURNS directly)
            return_type = None
            # Match RETURNS followed by type, stopping before AS or LANGUAGE
            returns_match = re.search(r"RETURNS\s+(\w+(?:\s+\w+)?)\s*(?=AS|LANGUAGE|;|$)", content, re.IGNORECASE)
            if returns_match:
                return_type = returns_match.group(1).strip().upper()

            # Extract language
            language = "sql"
            language_match = re.search(r"LANGUAGE\s+(\w+)", content, re.IGNORECASE)
            if language_match:
                language = language_match.group(1).lower()

            # Extract function body (between AS and LANGUAGE or end)
            function_body = ""
            as_match = re.search(r"AS\s+(?:\$\$|['\"`])(.*?)(?:\$\$|['\"`])", content, re.IGNORECASE | re.DOTALL)
            if as_match:
                function_body = as_match.group(1).strip()
            else:
                # Try without delimiters
                as_match = re.search(r"AS\s+(.*?)(?:LANGUAGE|;|$)", content, re.IGNORECASE | re.DOTALL)
                if as_match:
                    function_body = as_match.group(1).strip()

            # Determine function type
            function_type: FunctionType = "scalar"
            if "RETURNS TABLE" in content.upper() or (return_type and "TABLE" in return_type.upper()):
                function_type = "table"

            # Extract dependencies (will be stored in code["sql"] later, not in function_metadata)
            dependencies = self._extract_dependencies(function_body)

            return {
                "function_name": function_name,
                "description": None,
                "function_type": function_type,
                "language": language,
                "parameters": parameters,
                "return_type": return_type,
                "return_table_schema": None,
                "schema": schema,
                "deterministic": None,
                "tests": [],
                "tags": [],
                "object_tags": {},
                "function_body": function_body,
                # Store dependencies temporarily - will be moved to code["sql"] in the result structure
                "_dependencies": dependencies,
            }

        except Exception as e:
            logger.debug(f"SQLglot parsing failed for {file_path_str}: {e}")
            return None

    def _parse_create_function_regex(self, content: str, file_path_str: str) -> Optional[Dict[str, Any]]:
        """
        Parse a CREATE FUNCTION statement and extract function metadata.

        Args:
            content: SQL content
            file_path_str: File path for error messages

        Returns:
            Dict with function metadata or None if not found
        """
        # Normalize whitespace
        content = re.sub(r"\s+", " ", content.strip())

        # Pattern to match CREATE [OR REPLACE] FUNCTION or CREATE [OR REPLACE] MACRO (DuckDB)
        # This is a simplified pattern - we'll handle common variations
        pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|MACRO)\s+([^\s(]+)\s*\(([^)]*)\)\s*(?:RETURNS\s+([^\s{]+)\s*)?(?:AS\s+)?(?:['\"`]?(\$\$|['\"`])?)?\s*(.*?)(?:\1|;)?$"

        # Try to match the pattern
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if not match:
            # Try a more flexible pattern (for MACRO without RETURNS)
            pattern2 = r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|MACRO)\s+([^\s(]+)\s*\(([^)]*)\)"
            match = re.search(pattern2, content, re.IGNORECASE)
            if not match:
                logger.warning(f"Could not parse CREATE FUNCTION/MACRO statement in {file_path_str}")
                return None

        function_name_full = match.group(1).strip()
        # Extract schema and function name
        if "." in function_name_full:
            parts = function_name_full.split(".")
            schema = parts[0]
            function_name = parts[1]
        else:
            schema = None
            function_name = function_name_full

        # Parse parameters
        params_str = match.group(2).strip() if len(match.groups()) > 1 else ""
        parameters = self._parse_parameters(params_str)

        # Parse return type
        return_type = None
        if len(match.groups()) > 2 and match.group(3):
            return_type = match.group(3).strip()

        # Extract function body (everything after RETURNS ... AS or AS for MACRO)
        function_body = ""
        if len(match.groups()) > 4 and match.group(5):
            function_body = match.group(5).strip()
            # Remove delimiter markers if present
            if function_body.startswith("$$"):
                function_body = function_body[2:]
            if function_body.endswith("$$"):
                function_body = function_body[:-2]
            # For MACRO, remove parentheses if present
            if function_body.startswith("(") and function_body.endswith(")"):
                function_body = function_body[1:-1].strip()
            function_body = function_body.strip()

        # Try to extract language
        language = "sql"
        language_match = re.search(r"LANGUAGE\s+(\w+)", content, re.IGNORECASE)
        if language_match:
            language = language_match.group(1).lower()

        # Determine function type (default to scalar)
        function_type: FunctionType = "scalar"
        if "RETURNS TABLE" in content.upper() or return_type and "TABLE" in return_type.upper():
            function_type = "table"

        # Extract dependencies (will be stored in code["sql"] later, not in function_metadata)
        dependencies = self._extract_dependencies(function_body)

        return {
            "function_name": function_name,
            "description": None,  # Will be filled from metadata file if available
            "function_type": function_type,
            "language": language,
            "parameters": parameters,
            "return_type": return_type,
            "return_table_schema": None,  # Will be filled from metadata if table function
            "schema": schema,
            "deterministic": None,  # Will be filled from metadata if available
            "tests": [],
            "tags": [],
            "object_tags": {},
            "function_body": function_body,
            # Store dependencies temporarily - will be moved to code["sql"] in the result structure
            "_dependencies": dependencies,
        }

    def _parse_parameters(self, params_str: str) -> List[FunctionParameter]:
        """
        Parse function parameters from parameter string.

        Args:
            params_str: Parameter string (e.g., "x FLOAT, y INTEGER DEFAULT 0")

        Returns:
            List of parameter dictionaries
        """
        if not params_str or not params_str.strip():
            return []

        parameters = []
        # Split by comma, but be careful with nested parentheses
        param_parts = self._split_parameters(params_str)

        for param_str in param_parts:
            param_str = param_str.strip()
            if not param_str:
                continue

            # Pattern: name type [DEFAULT value] [MODE]
            # Examples:
            #   "x FLOAT"
            #   "y INTEGER DEFAULT 0"
            #   "z VARCHAR(255) DEFAULT 'test'"
            #   "w INOUT INTEGER"

            param: FunctionParameter = {
                "name": "",
                "type": "",
            }

            # Check for mode (IN, OUT, INOUT)
            mode_match = re.match(r"^(IN|OUT|INOUT)\s+", param_str, re.IGNORECASE)
            if mode_match:
                param["mode"] = mode_match.group(1).upper()
                param_str = param_str[len(mode_match.group(0)) :].strip()

            # Split by spaces to get name and type
            parts = param_str.split(None, 2)
            if len(parts) >= 2:
                param["name"] = parts[0]
                param["type"] = parts[1]

                # Check for DEFAULT
                if len(parts) > 2:
                    default_match = re.search(r"DEFAULT\s+(.+)", parts[2], re.IGNORECASE)
                    if default_match:
                        param["default"] = default_match.group(1).strip()

            if param["name"]:
                parameters.append(param)

        return parameters

    def _split_parameters(self, params_str: str) -> List[str]:
        """
        Split parameter string by commas, handling nested parentheses.

        Args:
            params_str: Parameter string

        Returns:
            List of parameter strings
        """
        parts = []
        current = ""
        depth = 0

        for char in params_str:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(current)
                current = ""
                continue
            current += char

        if current:
            parts.append(current)

        return parts

    def _extract_dependencies(self, function_body: str) -> Dict[str, List[str]]:
        """
        Extract dependencies from function body (table references and function calls).

        Args:
            function_body: Function body SQL

        Returns:
            Dict with 'tables' and 'functions' keys
        """
        dependencies = {"tables": [], "functions": []}

        if not function_body:
            return dependencies

        # Extract table references (FROM, JOIN clauses)
        # This is a simplified extraction - full parsing would use SQLglot
        table_pattern = r"(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)"
        table_matches = re.findall(table_pattern, function_body, re.IGNORECASE)
        dependencies["tables"] = list(set(table_matches))

        # Extract function calls (simplified - look for identifier followed by parenthesis)
        # Filter out common built-in functions
        # Pattern: identifier( or schema.identifier( - function call
        # Support both unqualified and qualified function names
        func_pattern = r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*\("
        func_matches = re.findall(func_pattern, function_body, re.IGNORECASE)
        functions = [f.strip() for f in func_matches if f.strip().lower() not in SQL_BUILT_IN_FUNCTIONS]
        dependencies["functions"] = list(set(functions))

        return dependencies

    def _merge_metadata(
        self, sql_metadata: Dict[str, Any], python_metadata: Dict[str, Any]
    ) -> ParsedFunctionMetadata:
        """
        Merge SQL-extracted metadata with Python metadata file.

        Args:
            sql_metadata: Metadata extracted from SQL
            python_metadata: Metadata from Python file

        Returns:
            Merged function metadata
        """
        # Validate consistency first
        try:
            validate_function_metadata_consistency(sql_metadata, python_metadata)
        except FunctionMetadataError as e:
            raise FunctionMetadataError(
                f"Metadata validation failed for function '{sql_metadata.get('function_name', 'unknown')}': {e}"
            )

        # Start with SQL metadata
        merged = sql_metadata.copy()

        # Merge other fields
        for key in ["description", "function_type", "parameters", "return_type", "return_table_schema", "schema", "deterministic", "tests", "tags", "object_tags"]:
            if key in python_metadata and python_metadata[key] is not None:
                # For lists/dicts, prefer Python metadata if provided
                if key in ["parameters", "tests", "tags", "object_tags"]:
                    if python_metadata[key]:
                        merged[key] = python_metadata[key]
                else:
                    merged[key] = python_metadata[key]

        # Ensure required fields
        merged.setdefault("function_type", "scalar")
        merged.setdefault("tags", [])
        merged.setdefault("object_tags", {})
        merged.setdefault("tests", [])

        return merged

