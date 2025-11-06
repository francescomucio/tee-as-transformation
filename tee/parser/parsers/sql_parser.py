"""
Unified SQL parsing functionality using sqlglot.
"""

import sqlglot
import logging
from typing import Optional
from pathlib import Path

from .base import BaseParser
from ..shared.types import ParsedModel, FilePath
from ..shared.exceptions import SQLParsingError
from ..shared.model_utils import create_model_metadata, compute_sqlglot_hash
from ..shared.metadata_schema import parse_metadata_from_python_file, validate_metadata_dict
from ...typing.metadata import ParsedModelMetadata
from ..analysis.sql_qualifier import generate_resolved_sql, validate_resolved_sql

# Configure logging
logger = logging.getLogger(__name__)


class SQLParser(BaseParser):
    """Handles SQL parsing using sqlglot."""

    def _find_metadata_file(self, sql_file_path: str) -> Optional[str]:
        """
        Find companion Python metadata file for a SQL file.

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

    def _parse_metadata(self, sql_file_path: str) -> Optional[ParsedModelMetadata]:
        """
        Parse metadata from companion Python file or SQL comments.

        Args:
            sql_file_path: Path to the SQL file

        Returns:
            Parsed metadata dictionary or None if not found
        """
        # First try companion Python file
        metadata_file = self._find_metadata_file(sql_file_path)
        if metadata_file:
            try:
                raw_metadata = parse_metadata_from_python_file(metadata_file)
                if raw_metadata:
                    # Validate the metadata
                    validated_metadata = validate_metadata_dict(raw_metadata)
                return ParsedModelMetadata(
                    description=validated_metadata.description,
                    schema=[
                        {
                            "name": col.name,
                            "datatype": col.datatype,
                            "description": col.description,
                            "tests": col.tests,
                        }
                        for col in validated_metadata.schema
                    ]
                    if validated_metadata.schema
                    else None,
                    partitions=validated_metadata.partitions or [],
                    materialization=validated_metadata.materialization,
                    tests=validated_metadata.tests or [],
                    incremental=validated_metadata.incremental,
                )
            except Exception as e:
                logger.warning(f"Failed to parse metadata from {metadata_file}: {str(e)}")

        # If no Python file, try to extract metadata from SQL comments
        try:
            logger.info(f"Trying to extract metadata from SQL comments in {sql_file_path}")
            with open(sql_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Look for metadata comment: -- metadata: {...}
            import re
            import json

            metadata_match = re.search(r"--\s*metadata:\s*(\{.*\})", content, re.DOTALL)
            if metadata_match:
                metadata_json = metadata_match.group(1)
                logger.info(f"Found metadata JSON in SQL comments: {metadata_json}")
                raw_metadata = json.loads(metadata_json)
                if raw_metadata:
                    logger.info(f"Parsed metadata from SQL comments: {raw_metadata}")
                    # Validate the metadata
                    validated_metadata = validate_metadata_dict(raw_metadata)
                    return ParsedModelMetadata(
                        description=validated_metadata.description,
                        schema=[
                            {
                                "name": col.name,
                                "datatype": col.datatype,
                                "description": col.description,
                                "tests": col.tests,
                            }
                            for col in validated_metadata.schema
                        ]
                        if validated_metadata.schema
                        else None,
                        partitions=validated_metadata.partitions or [],
                        materialization=validated_metadata.materialization,
                        tests=validated_metadata.tests or [],
                        incremental=validated_metadata.incremental,
                    )
            else:
                logger.info(f"No metadata found in SQL comments for {sql_file_path}")
        except Exception as e:
            logger.warning(
                f"Failed to parse metadata from SQL comments in {sql_file_path}: {str(e)}"
            )

        return None

    def parse(
        self, content: str, file_path: FilePath = None, table_name: str = None
    ) -> ParsedModel:
        """
        Parse SQL content with sqlglot and extract relevant arguments.

        Args:
            content: The SQL content to parse
            file_path: Optional file path for context
            table_name: Name of the table for qualified SQL generation

        Returns:
            Dict containing parsed SQL arguments

        Raises:
            SQLParsingError: If SQL parsing fails
        """
        try:
            # Check cache first
            cache_key = self._get_cache_key(content, file_path)
            cached_result = self._get_from_cache(cache_key)
            if cached_result:
                return cached_result

            # Parse the SQL
            parsed = sqlglot.parse_one(content)

            if parsed is None:
                raise SQLParsingError("Failed to parse SQL")

            # Extract SQLGlot data
            result = self._parse_sqlglot_expression(
                parsed, table_name, str(file_path) if file_path else None
            )

            # Cache the result
            self._set_cache(cache_key, result)

            return result

        except Exception as e:
            if isinstance(e, SQLParsingError):
                raise
            raise SQLParsingError(f"SQL parsing error: {str(e)}")

    def _parse_sqlglot_expression(
        self, expr, table_name: str = None, file_path: str = None
    ) -> ParsedModel:
        """
        Parse a SQLGlot expression and extract relevant information.

        Args:
            expr: SQLGlot expression (either parsed AST or Expression object)
            table_name: Name of the table for qualified SQL generation

        Returns:
            Dict containing parsed SQL arguments
        """
        try:
            # Convert expression to SQL string
            sql_content = str(expr)

            # Extract table references
            source_tables = []
            for table in expr.find_all(sqlglot.exp.Table):
                source_tables.append(table.name)

            # Generate resolved SQL with table reference resolution if table_name provided
            if table_name:
                resolved_sql = generate_resolved_sql(str(expr), source_tables, table_name)

                # Validate resolved SQL length and log warning if significantly different
                validate_resolved_sql(sql_content.strip(), resolved_sql, table_name)
            else:
                # No table name provided, use original SQL as resolved SQL
                resolved_sql = sql_content.strip()

            # Build code structure with new field names
            code_data = {
                "sql": {
                    "original_sql": sql_content.strip(),
                    "resolved_sql": resolved_sql,
                    "operation_type": expr.key if hasattr(expr, "key") else "unknown",
                    "source_tables": source_tables,
                }
            }

            # Parse additional metadata from companion Python file
            additional_metadata = None
            if file_path:
                additional_metadata = self._parse_metadata(str(file_path))

            # Create model metadata
            model_metadata = create_model_metadata(
                table_name=table_name or "unknown_table",
                file_path=str(file_path) if file_path else None,
                description=f"SQL model for {table_name or 'unknown_table'}",
                metadata=additional_metadata,
            )

            # Compute hash of the resolved SQL for change detection
            sqlglot_hash = compute_sqlglot_hash({"resolved_sql": resolved_sql})

            # Return standardized structure with code and model_metadata
            return {
                "code": code_data,
                "model_metadata": model_metadata,
                "sqlglot_hash": sqlglot_hash,
            }

        except Exception as e:
            raise SQLParsingError(f"SQLGlot expression parsing error: {str(e)}")
