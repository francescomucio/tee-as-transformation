"""
SQL-based custom tests.

Supports custom tests defined in SQL files in the tests/ folder, similar to dbt.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import logging

from .base import StandardTest, TestSeverity, TestResult
from tee.parser.processing.variable_substitution import substitute_sql_variables


class SqlTest(StandardTest):
    """
    Custom test defined in a SQL file.

    SQL tests follow the dbt pattern:
    - Query returns rows when test fails
    - 0 rows returned = test passes
    - Query should use COUNT(*) for performance

    The SQL can reference:
    - Table names directly
    - Variables via {{ variable_name }} or @variable_name
    - Model references (future: {{ ref('table_name') }})
    """

    def __init__(
        self,
        name: str,
        sql_file_path: Path,
        project_folder: Path,
        severity: TestSeverity = TestSeverity.ERROR,
    ):
        """
        Initialize a SQL test.

        Args:
            name: Test name (derived from file name or specified)
            sql_file_path: Path to the SQL test file
            project_folder: Project folder path (for resolving variables)
            severity: Test severity level
        """
        super().__init__(name, severity)
        self.sql_file_path = sql_file_path
        self.project_folder = project_folder
        self.logger = logging.getLogger(self.__class__.__name__)
        self._sql_content: Optional[str] = None

    def _load_sql_content(self) -> str:
        """Load SQL content from file."""
        if self._sql_content is None:
            try:
                with open(self.sql_file_path, "r", encoding="utf-8") as f:
                    self._sql_content = f.read()
            except Exception as e:
                raise ValueError(f"Failed to load SQL test file {self.sql_file_path}: {e}")
        return self._sql_content

    def validate_params(
        self, params: Optional[Dict[str, Any]] = None, column_name: Optional[str] = None
    ) -> None:
        """
        Validate SQL test parameters.

        SQL tests can accept parameters that will be substituted as variables.

        Args:
            params: Optional parameters (will be available as variables in SQL)
            column_name: Column name if this is a column-level test (optional)
        """
        # SQL tests can accept any parameters (they become variables)
        # No specific validation needed - the SQL file itself defines what it needs
        pass

    def get_test_query(
        self,
        adapter,
        table_name: str,
        column_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Generate SQL query by loading from file and applying variable substitution.

        Args:
            adapter: Database adapter instance
            table_name: Fully qualified table name (available as 'table_name' variable)
            column_name: Column name if applicable (available as 'column_name' variable)
            params: Additional parameters (available as variables)

        Returns:
            SQL query string with variables substituted
        """
        # Load SQL content
        sql = self._load_sql_content()

        # Apply special substitutions for table_name and column_name (unquoted identifiers)
        # These should be replaced directly, not as SQL values
        sql = sql.replace("{{ table_name }}", table_name)
        sql = sql.replace("{{table_name}}", table_name)  # No spaces
        sql = sql.replace("@table_name", table_name)

        if column_name:
            sql = sql.replace("{{ column_name }}", column_name)
            sql = sql.replace("{{column_name}}", column_name)  # No spaces
            sql = sql.replace("@column_name", column_name)

        # Build variables dict for other parameter substitution
        variables = {}

        # Add any additional params as variables (these will be quoted as SQL values)
        if params:
            variables.update(params)

        # Apply variable substitution for other variables (these are SQL values, quoted)
        # Support both {{ variable }} and @variable syntax
        if variables:
            try:
                sql = substitute_sql_variables(sql, variables)
            except Exception as e:
                self.logger.warning(f"Variable substitution failed in {self.name}: {e}")
                # Continue with original SQL if substitution fails

        # Note: SQL tests should return COUNT(*) for performance
        # If they return SELECT *, we might want to wrap it, but for now
        # we trust the user to write efficient queries

        return sql

    def execute(
        self,
        adapter,
        table_name: str,
        column_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        severity: Optional[TestSeverity] = None,
    ) -> TestResult:
        """
        Execute the SQL test.

        SQL tests follow dbt pattern: 0 rows = pass, 1+ rows = fail.
        The query can return either:
        - Rows directly (SELECT * FROM ... WHERE violation)
        - COUNT(*) result (SELECT COUNT(*) FROM ... WHERE violation)
        """
        # Validate parameters
        self.validate_params(params, column_name)

        # Use provided severity or test default
        test_severity = severity or self.severity

        try:
            # Get test query (with variable substitution)
            query = self.get_test_query(adapter, table_name, column_name, params)

            # Execute query
            results = adapter.execute_query(query)

            # SQL tests return rows when violations are found
            # Count the number of rows returned (not the COUNT(*) value)
            if isinstance(results, list):
                row_count = len(results)
            else:
                row_count = 0

            # Check if test passed (0 rows = pass for SQL tests)
            passed = row_count == 0

            # Format message
            if passed:
                message = f"Test passed: {self.name}"
            else:
                message = f"Test failed: {self.name} found {row_count} violation(s)"

            return TestResult(
                test_name=self.name,
                table_name=table_name,
                column_name=column_name,
                passed=passed,
                message=message,
                severity=test_severity,
                rows_returned=row_count,
            )

        except Exception as e:
            error_msg = f"Error executing SQL test {self.name}: {str(e)}"
            self.logger.error(error_msg)
            return TestResult(
                test_name=self.name,
                table_name=table_name,
                column_name=column_name,
                passed=False,
                message=error_msg,
                severity=test_severity,
                error=str(e),
            )
