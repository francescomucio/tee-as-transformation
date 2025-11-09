"""
Test execution engine that integrates with model execution.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from .base import TestRegistry, TestResult, TestSeverity
from .test_discovery import TestDiscovery
from tee.typing.metadata import TestDefinition
from tee.adapters.base import DatabaseAdapter


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
        self._used_test_names: set[str] = set()
        self._test_discovery: Optional[TestDiscovery] = None

        # Discover and register SQL tests from tests/ folder
        if self.project_folder:
            self._test_discovery = TestDiscovery(self.project_folder)
            self._test_discovery.register_discovered_tests()

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

    def execute_tests_for_function(
        self,
        function_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        severity_overrides: Optional[Dict[str, TestSeverity]] = None,
    ) -> List[TestResult]:
        """
        Execute all tests for a given function based on its metadata.

        Args:
            function_name: Fully qualified function name
            metadata: Function metadata containing test definitions
            severity_overrides: Optional dict to override test severities

        Returns:
            List of TestResult objects
        """
        results = []

        if not metadata:
            return results

        severity_overrides = severity_overrides or {}

        # Execute function-level tests
        if "tests" in metadata and metadata["tests"]:
            for test_def in metadata["tests"]:
                result = self._execute_function_test(
                    function_name=function_name,
                    test_def=test_def,
                    severity_overrides=severity_overrides,
                )
                if result:
                    results.append(result)

        return results

    def _execute_function_test(
        self,
        function_name: str,
        test_def: TestDefinition,
        severity_overrides: Dict[str, TestSeverity],
    ) -> Optional[TestResult]:
        """
        Execute a single function test definition.

        Args:
            function_name: Fully qualified function name
            test_def: Test definition (string name or dict with name/params/severity/expected)
            severity_overrides: Dict of severity overrides

        Returns:
            TestResult or None if test not found
        """
        # Parse test definition
        if isinstance(test_def, str):
            test_name = test_def
            params = None
            expected = None
            severity_override = None
        elif isinstance(test_def, dict):
            test_name = test_def.get("name") or test_def.get("test")
            if not test_name:
                self.logger.warning(f"Test definition missing name: {test_def}")
                return None

            # Extract params (everything except name/test, severity, and expected)
            params = {
                k: v
                for k, v in test_def.items()
                if k not in ["name", "test", "severity", "expected"]
            }
            if not params:
                params = None

            # Extract expected value (for expected value pattern)
            expected = test_def.get("expected")

            # Get severity override from test definition or overrides dict
            severity_str = test_def.get("severity")
            if severity_str:
                try:
                    severity_override = TestSeverity(severity_str.lower())
                except ValueError:
                    self.logger.warning(f"Invalid severity '{severity_str}', using default")
                    severity_override = None
            else:
                # Check severity_overrides dict (key format: "function_name.test_name")
                override_key = f"{function_name}.{test_name}"
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
                function_name=function_name,
                passed=True,  # Passed by default since it's not implemented yet
                message=warning_msg,
                severity=TestSeverity.WARNING,  # Warning instead of error
                error=None,
            )

        # Track that this test was used
        self._used_test_names.add(test_name)

        # Execute test
        try:
            # For SqlTest, pass function_name and expected
            if hasattr(test, "execute"):
                # Check if test supports function_name parameter
                import inspect
                sig = inspect.signature(test.execute)
                if "function_name" in sig.parameters:
                    result = test.execute(
                        adapter=self.adapter,
                        function_name=function_name,
                        params=params,
                        expected=expected,
                        severity=severity_override,
                    )
                else:
                    # Fallback for tests that don't support function_name yet
                    self.logger.warning(
                        f"Test {test_name} does not support function testing, skipping"
                    )
                    return None
            else:
                # Standard tests don't support function testing
                self.logger.warning(
                    f"Test {test_name} does not support function testing, skipping"
                )
                return None

            return result
        except Exception as e:
            error_msg = f"Error executing test {test_name}: {str(e)}"
            self.logger.error(error_msg)
            return TestResult(
                test_name=test_name,
                function_name=function_name,
                passed=False,
                message=error_msg,
                severity=severity_override or test.severity,
                error=str(e),
            )

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

        # Track that this test was used
        self._used_test_names.add(test_name)

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
        execution_order: Optional[List[str]] = None,
        parsed_functions: Optional[Dict[str, Any]] = None,
        severity_overrides: Optional[Dict[str, TestSeverity]] = None,
    ) -> Dict[str, Any]:
        """
        Execute all tests for all models and functions in execution order.

        Tests are executed after their dependent models/functions have been created.

        Args:
            parsed_models: Dictionary of parsed models with metadata
            execution_order: List of table/function names in execution order (optional, defaults to all models)
            parsed_functions: Dictionary of parsed functions with metadata (optional)
            severity_overrides: Optional dict to override test severities

        Returns:
            Dictionary with test execution results
        """
        all_results: List[TestResult] = []
        errors: List[str] = []
        warnings: List[str] = []

        parsed_functions = parsed_functions or {}
        
        # If execution_order not provided, use all models
        if execution_order is None:
            execution_order = list(parsed_models.keys())

        # Separate functions and models from execution order
        function_names = [name for name in execution_order if name in parsed_functions]
        model_names = [name for name in execution_order if name in parsed_models]

        self.logger.info(
            f"Executing tests for {len(model_names)} models and {len(function_names)} functions"
        )

        # Execute function tests first (functions are created before models)
        for function_name in function_names:
            if function_name not in parsed_functions:
                continue

            function_data = parsed_functions[function_name]
            metadata = self._extract_function_metadata(function_data)

            if not metadata:
                continue

            # Execute tests for this function
            results = self.execute_tests_for_function(
                function_name=function_name,
                metadata=metadata,
                severity_overrides=severity_overrides,
            )

            all_results.extend(results)

            # Categorize results
            for result in results:
                if result.severity == TestSeverity.WARNING:
                    warnings.append(
                        f"{result.test_name} on {function_name}: {result.message}"
                    )
                elif not result.passed and result.severity == TestSeverity.ERROR:
                    errors.append(
                        f"{result.test_name} on {function_name}: {result.message}"
                    )

        # Execute model tests
        for table_name in model_names:
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

        # Check for unused generic SQL tests (both model and function tests)
        unused_warnings = self._check_unused_generic_tests(parsed_models, parsed_functions)
        if unused_warnings:
            warnings.extend(unused_warnings)

        return {
            "test_results": all_results,
            "passed": len([r for r in all_results if r.passed]),
            "failed": len(actual_failures),
            "errors": errors,
            "warnings": warnings,
            "total": len(all_results),
        }

    def _check_unused_generic_tests(
        self, parsed_models: Dict[str, Any], parsed_functions: Dict[str, Any]
    ) -> List[str]:
        """
        Check for unused generic SQL tests and return warning messages.
        
        Generic tests are SQL tests that use placeholders like @table_name, @function_name, etc.
        These tests must be referenced in model/function metadata to be used.
        
        Returns:
            List of warning messages for unused generic tests
        """
        if not self._test_discovery:
            return []
        
        warnings = []
        discovered_tests = self._test_discovery.discover_tests()
        discovered_function_tests = self._test_discovery.discover_function_tests()
        
        # Get all test names referenced in model metadata
        referenced_test_names = set()
        for model_data in parsed_models.values():
            metadata = self._extract_metadata(model_data)
            if not metadata:
                continue
            
            # Check model-level tests
            if "tests" in metadata and metadata["tests"]:
                for test_def in metadata["tests"]:
                    if isinstance(test_def, str):
                        referenced_test_names.add(test_def)
                    elif isinstance(test_def, dict):
                        test_name = test_def.get("name") or test_def.get("test")
                        if test_name:
                            referenced_test_names.add(test_name)
            
            # Check column-level tests
            if "schema" in metadata and metadata["schema"]:
                for column_def in metadata["schema"]:
                    if "tests" in column_def and column_def["tests"]:
                        for test_def in column_def["tests"]:
                            if isinstance(test_def, str):
                                referenced_test_names.add(test_def)
                            elif isinstance(test_def, dict):
                                test_name = test_def.get("name") or test_def.get("test")
                                if test_name:
                                    referenced_test_names.add(test_name)
        
        # Get all test names referenced in function metadata
        for function_data in parsed_functions.values():
            metadata = self._extract_function_metadata(function_data)
            if not metadata:
                continue
            
            # Check function-level tests
            if "tests" in metadata and metadata["tests"]:
                for test_def in metadata["tests"]:
                    if isinstance(test_def, str):
                        referenced_test_names.add(test_def)
                    elif isinstance(test_def, dict):
                        test_name = test_def.get("name") or test_def.get("test")
                        if test_name:
                            referenced_test_names.add(test_name)
        
        # Check each discovered model SQL test
        for test_name, sql_test in discovered_tests.items():
            # Skip if test was used during execution
            if test_name in self._used_test_names:
                continue
            
            # Skip if test is referenced in metadata (might be used in future runs)
            if test_name in referenced_test_names:
                continue
            
            # Check if this is a generic test (has placeholders)
            if self._is_generic_test(sql_test):
                warnings.append(
                    f"Generic SQL test '{test_name}' is never used. "
                    f"Add it to model metadata to apply it to tables. "
                    f"File: {sql_test.sql_file_path.relative_to(self.project_folder) if self.project_folder else sql_test.sql_file_path}"
                )
        
        # Check each discovered function SQL test
        for test_name, sql_test in discovered_function_tests.items():
            # Skip if test was used during execution
            if test_name in self._used_test_names:
                continue
            
            # Skip if test is referenced in metadata (might be used in future runs)
            if test_name in referenced_test_names:
                continue
            
            # Check if this is a generic test (has placeholders)
            if self._is_generic_test(sql_test):
                warnings.append(
                    f"Generic function SQL test '{test_name}' is never used. "
                    f"Add it to function metadata to apply it to functions. "
                    f"File: {sql_test.sql_file_path.relative_to(self.project_folder) if self.project_folder else sql_test.sql_file_path}"
                )
        
        return warnings
    
    def _is_generic_test(self, sql_test) -> bool:
        """
        Check if a SQL test is generic (uses placeholders) vs singular (hardcoded table name).
        
        Generic tests use @table_name, {{ table_name }}, or similar placeholders.
        Singular tests have hardcoded table names.
        """
        try:
            sql_content = sql_test._load_sql_content()
            # Check for common placeholder patterns
            placeholder_patterns = [
                "@table_name",
                "{{ table_name }}",
                "{{table_name}}",
                "@column_name",
                "{{ column_name }}",
                "{{column_name}}",
            ]
            return any(pattern in sql_content for pattern in placeholder_patterns)
        except Exception:
            # If we can't load the SQL, assume it's generic to be safe
            return True

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

    def _extract_function_metadata(self, function_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract metadata from function data (similar to ExecutionEngine._extract_function_metadata)."""
        try:
            # First, try to get metadata from function_metadata
            function_metadata = function_data.get("function_metadata", {})
            if function_metadata and "metadata" in function_metadata:
                nested_metadata = function_metadata["metadata"]
                if nested_metadata:
                    return nested_metadata

            # Fallback to function_metadata directly
            if function_metadata:
                return function_metadata

            # Fallback to any other metadata in the function data
            if "metadata" in function_data:
                file_metadata = function_data["metadata"]
                if file_metadata:
                    return file_metadata

            return None
        except Exception as e:
            self.logger.warning(f"Error extracting function metadata: {e}")
            return None
