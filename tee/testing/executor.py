"""
Test execution engine that integrates with model execution.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from .base import TestRegistry, TestResult, TestSeverity
from .test_discovery import TestDiscovery
from ..typing.metadata import TestDefinition
from ..adapters.base import DatabaseAdapter


class TestExecutor:
    """Executes tests against models after they are created."""

    __test__ = False  # Tell pytest this is not a test class

    def __init__(self, adapter: DatabaseAdapter, project_folder: Optional[str] = None):
        """
        Initialize test executor.

        Args:
            adapter: Database adapter for executing test queries
            project_folder: Optional project folder path for discovering SQL tests
        """
        self.adapter = adapter
        self.project_folder = Path(project_folder) if project_folder else None
        self.logger = logging.getLogger(self.__class__.__name__)

        # Discover and register SQL tests from tests/ folder
        if self.project_folder:
            test_discovery = TestDiscovery(self.project_folder)
            test_discovery.register_discovered_tests()

    def execute_tests_for_model(
        self,
        table_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        severity_overrides: Optional[Dict[str, TestSeverity]] = None,
    ) -> List[TestResult]:
        """
        Execute all tests for a given model based on its metadata.

        Args:
            table_name: Fully qualified table name
            metadata: Model metadata containing test definitions
            severity_overrides: Optional dict to override test severities

        Returns:
            List of TestResult objects
        """
        results = []

        if not metadata:
            return results

        severity_overrides = severity_overrides or {}

        # Execute column-level tests
        if "schema" in metadata and metadata["schema"]:
            for column_def in metadata["schema"]:
                if "tests" in column_def and column_def["tests"]:
                    column_name = column_def.get("name")
                    if not column_name:
                        self.logger.warning(
                            f"Skipping tests for column without name in {table_name}"
                        )
                        continue

                    for test_def in column_def["tests"]:
                        result = self._execute_test(
                            table_name=table_name,
                            column_name=column_name,
                            test_def=test_def,
                            severity_overrides=severity_overrides,
                        )
                        if result:
                            results.append(result)

        # Execute model-level tests
        if "tests" in metadata and metadata["tests"]:
            for test_def in metadata["tests"]:
                result = self._execute_test(
                    table_name=table_name,
                    column_name=None,
                    test_def=test_def,
                    severity_overrides=severity_overrides,
                )
                if result:
                    results.append(result)

        return results

    def _execute_test(
        self,
        table_name: str,
        column_name: Optional[str],
        test_def: TestDefinition,
        severity_overrides: Dict[str, TestSeverity],
    ) -> Optional[TestResult]:
        """
        Execute a single test definition.

        Args:
            table_name: Fully qualified table name
            column_name: Column name (None for model-level tests)
            test_def: Test definition (string name or dict with name/params/severity)
            severity_overrides: Dict of severity overrides

        Returns:
            TestResult or None if test not found
        """
        # Parse test definition
        if isinstance(test_def, str):
            test_name = test_def
            params = None
            severity_override = None
        elif isinstance(test_def, dict):
            test_name = test_def.get("name") or test_def.get("test")
            if not test_name:
                self.logger.warning(f"Test definition missing name: {test_def}")
                return None

            # Extract params (everything except name/test and severity)
            params = {k: v for k, v in test_def.items() if k not in ["name", "test", "severity"]}
            if not params:
                params = None

            # Get severity override from test definition or overrides dict
            severity_str = test_def.get("severity")
            if severity_str:
                try:
                    severity_override = TestSeverity(severity_str.lower())
                except ValueError:
                    self.logger.warning(f"Invalid severity '{severity_str}', using default")
                    severity_override = None
            else:
                # Check severity_overrides dict (key format: "test_name" or "table.column.test_name")
                override_key = (
                    f"{table_name}.{column_name}.{test_name}"
                    if column_name
                    else f"{table_name}.{test_name}"
                )
                severity_override = severity_overrides.get(override_key) or severity_overrides.get(
                    test_name
                )
        else:
            self.logger.warning(f"Invalid test definition type: {type(test_def)}")
            return None

        # Get test from registry
        test = TestRegistry.get(test_name)
        if not test:
            warning_msg = f"Test '{test_name}' not implemented yet. Available tests: {TestRegistry.list_all()}"
            self.logger.warning(warning_msg)
            # Return a warning result instead of error for unimplemented tests
            return TestResult(
                test_name=test_name,
                table_name=table_name,
                column_name=column_name,
                passed=True,  # Passed by default since it's not implemented yet
                message=warning_msg,
                severity=TestSeverity.WARNING,  # Warning instead of error
                error=None,
            )

        # Execute test
        try:
            result = test.execute(
                adapter=self.adapter,
                table_name=table_name,
                column_name=column_name,
                params=params,
                severity=severity_override,
            )
            return result
        except Exception as e:
            error_msg = f"Error executing test {test_name}: {str(e)}"
            self.logger.error(error_msg)
            return TestResult(
                test_name=test_name,
                table_name=table_name,
                column_name=column_name,
                passed=False,
                message=error_msg,
                severity=severity_override or test.severity,
                error=str(e),
            )

    def execute_all_tests(
        self,
        parsed_models: Dict[str, Any],
        execution_order: List[str],
        severity_overrides: Optional[Dict[str, TestSeverity]] = None,
    ) -> Dict[str, Any]:
        """
        Execute all tests for all models in execution order.

        Tests are executed after their dependent models have been created.

        Args:
            parsed_models: Dictionary of parsed models with metadata
            execution_order: List of table names in execution order
            severity_overrides: Optional dict to override test severities

        Returns:
            Dictionary with test execution results
        """
        all_results: List[TestResult] = []
        errors: List[str] = []
        warnings: List[str] = []

        self.logger.info(f"Executing tests for {len(execution_order)} models")

        for table_name in execution_order:
            if table_name not in parsed_models:
                continue

            model_data = parsed_models[table_name]
            metadata = self._extract_metadata(model_data)

            if not metadata:
                continue

            # Execute tests for this model
            results = self.execute_tests_for_model(
                table_name=table_name, metadata=metadata, severity_overrides=severity_overrides
            )

            all_results.extend(results)

            # Categorize results
            for result in results:
                # Warnings include both failed warnings and unimplemented tests
                if result.severity == TestSeverity.WARNING:
                    warnings.append(
                        f"{result.test_name} on {table_name}"
                        + (f".{result.column_name}" if result.column_name else "")
                        + f": {result.message}"
                    )
                elif not result.passed and result.severity == TestSeverity.ERROR:
                    # Only add actual test failures as errors
                    errors.append(
                        f"{result.test_name} on {table_name}"
                        + (f".{result.column_name}" if result.column_name else "")
                        + f": {result.message}"
                    )

        # Count only actual failures (exclude warnings)
        actual_failures = [
            r for r in all_results if not r.passed and r.severity == TestSeverity.ERROR
        ]

        return {
            "test_results": all_results,
            "passed": len([r for r in all_results if r.passed]),
            "failed": len(actual_failures),
            "errors": errors,
            "warnings": warnings,
            "total": len(all_results),
        }

    def _extract_metadata(self, model_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract metadata from model data (similar to ExecutionEngine._extract_metadata)."""
        try:
            # First, try to get metadata from model_metadata
            model_metadata = model_data.get("model_metadata", {})
            if model_metadata and "metadata" in model_metadata:
                nested_metadata = model_metadata["metadata"]
                if nested_metadata:
                    return nested_metadata

            # Fallback to any other metadata in the model data
            if "metadata" in model_data:
                file_metadata = model_data["metadata"]
                if file_metadata:
                    return file_metadata

            return None
        except Exception as e:
            self.logger.warning(f"Error extracting metadata: {e}")
            return None
