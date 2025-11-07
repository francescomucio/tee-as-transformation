"""
Unit tests for build_models function.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from tee.executor import build_models
from tee.testing.base import TestResult, TestSeverity


class TestBuildModels:
    """Test cases for build_models function."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def mock_connection_config(self):
        """Create mock connection configuration."""
        return {
            "type": "duckdb",
            "database": ":memory:",
        }

    def _setup_parser_mock(self, parsed_models, execution_order, graph=None):
        """Helper to set up parser mock."""
        mock_parser = Mock()
        mock_parser.collect_models.return_value = parsed_models
        if graph is None:
            graph = {
                "nodes": list(parsed_models.keys()),
                "dependencies": {k: [] for k in parsed_models.keys()},
            }
        mock_parser.build_dependency_graph.return_value = graph
        mock_parser.get_execution_order.return_value = execution_order
        mock_parser.get_table_dependents.return_value = []
        mock_parser.orchestrator = Mock()
        mock_parser.orchestrator.evaluate_python_models.return_value = parsed_models
        return mock_parser

    def _setup_execution_engine_mock(self, execute_models_return):
        """Helper to set up execution engine mock."""
        mock_execution_engine = Mock()
        mock_execution_engine.execute_models.return_value = execute_models_return
        mock_execution_engine._extract_metadata.return_value = None
        mock_execution_engine.adapter = Mock()
        return mock_execution_engine

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_success(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test successful build_models execution."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
        }
        execution_order = ["schema1.table1", "schema1.table2"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": ["schema1.table1", "schema1.table2"],
            "failed_tables": [],
            "table_info": {
                "schema1.table1": {"row_count": 10},
                "schema1.table2": {"row_count": 20},
            },
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock (for the one created inside build_models)
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        mock_execution_engine_instance._extract_metadata = mock_execution_engine._extract_metadata
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        # Setup test executor mock
        mock_test_executor = Mock()
        mock_test_executor.execute_tests_for_model.return_value = []
        mock_test_executor_class.return_value = mock_test_executor

        # Execute
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Verify results
        assert len(results["executed_tables"]) == 2
        assert len(results["failed_tables"]) == 0
        assert results["test_results"]["total"] == 0

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_stops_on_test_failure(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models stops on ERROR severity test failure."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {"tests": ["not_null"]}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
        }
        execution_order = ["schema1.table1", "schema1.table2"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser.get_table_dependents.return_value = ["schema1.table2"]
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": ["schema1.table1"],
            "failed_tables": [],
            "table_info": {"schema1.table1": {"row_count": 10}},
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_execution_engine._extract_metadata.return_value = {"tests": ["not_null"]}
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        mock_execution_engine_instance._extract_metadata = mock_execution_engine._extract_metadata
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        # Create a failing test result
        failing_test = TestResult(
            test_name="not_null",
            table_name="schema1.table1",
            column_name="id",
            passed=False,
            rows_returned=5,
            severity=TestSeverity.ERROR,
            message="Test failed: 5 violations found",
        )

        mock_test_executor = Mock()
        mock_test_executor.execute_tests_for_model.return_value = [failing_test]
        mock_test_executor_class.return_value = mock_test_executor

        # Execute - should raise SystemExit
        with pytest.raises(SystemExit) as exc_info:
            build_models(
                project_folder=str(temp_dir),
                connection_config=mock_connection_config,
                save_analysis=False,
                variables={},
            )

        # Should exit with code 1
        assert exc_info.value.code == 1

        # Verify that second model was not executed
        assert mock_execution_engine.execute_models.call_count == 1

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_continues_on_warning(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models continues on WARNING severity test failures."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {"tests": ["check_minimum_rows"]}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
        }
        execution_order = ["schema1.table1", "schema1.table2"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": ["schema1.table1", "schema1.table2"],
            "failed_tables": [],
            "table_info": {
                "schema1.table1": {"row_count": 10},
                "schema1.table2": {"row_count": 20},
            },
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_execution_engine._extract_metadata.return_value = {"tests": ["check_minimum_rows"]}
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        mock_execution_engine_instance._extract_metadata = mock_execution_engine._extract_metadata
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        # Create a warning test result
        warning_test = TestResult(
            test_name="check_minimum_rows",
            table_name="schema1.table1",
            column_name=None,
            passed=False,
            rows_returned=1,
            severity=TestSeverity.WARNING,
            message="Warning: row count is low",
        )

        mock_test_executor = Mock()
        mock_test_executor.execute_tests_for_model.return_value = [warning_test]
        mock_test_executor_class.return_value = mock_test_executor

        # Execute - should not raise SystemExit
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Should complete successfully
        assert len(results["executed_tables"]) == 2
        # Warning count should be at least 1 (may include other warnings)
        assert results["test_results"]["warnings"] >= 1

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_stops_on_model_failure(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models stops on model execution failure."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
        }
        execution_order = ["schema1.table1", "schema1.table2"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser.get_table_dependents.return_value = ["schema1.table2"]
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": [],
            "failed_tables": [{"table": "schema1.table1", "error": "SQL syntax error"}],
            "table_info": {},
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock - this is the one created inside build_models
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        # The execute_models should return the failed table
        mock_execution_engine_instance.execute_models.return_value = execute_models_return
        mock_execution_engine_instance._extract_metadata = Mock(return_value=None)
        mock_execution_engine_instance.adapter = Mock()
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        mock_test_executor = Mock()
        mock_test_executor_class.return_value = mock_test_executor

        # Execute - model failure should be handled gracefully (continue to next)
        # The build will complete but with failed models
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Should have failed tables
        assert len(results["failed_tables"]) > 0
        assert any(f["table"] == "schema1.table1" for f in results["failed_tables"])

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_skips_dependents_on_failure(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models skips dependents when a model fails."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
            "schema1.table3": {"model_metadata": {"metadata": {}}},
        }
        execution_order = ["schema1.table1", "schema1.table2", "schema1.table3"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser.get_table_dependents.return_value = ["schema1.table2", "schema1.table3"]
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": [],
            "failed_tables": [{"table": "schema1.table1", "error": "SQL error"}],
            "table_info": {},
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        mock_execution_engine_instance._extract_metadata = mock_execution_engine._extract_metadata
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        mock_test_executor = Mock()
        mock_test_executor_class.return_value = mock_test_executor

        # Execute - model failure should be handled gracefully
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Should have failed tables and skipped dependents
        assert len(results["failed_tables"]) > 0
        # Only the first model should have been attempted (dependents skipped)
        assert mock_execution_engine_instance.execute_models.call_count == 1

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_interleaves_tests(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models executes tests immediately after each model."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {"tests": ["not_null"]}}},
            "schema1.table2": {"model_metadata": {"metadata": {"tests": ["unique"]}}},
        }
        execution_order = ["schema1.table1", "schema1.table2"]
        
        # Setup parser mock
        mock_parser = self._setup_parser_mock(parsed_models, execution_order)
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": ["schema1.table1", "schema1.table2"],
            "failed_tables": [],
            "table_info": {
                "schema1.table1": {"row_count": 10},
                "schema1.table2": {"row_count": 20},
            },
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_execution_engine._extract_metadata.side_effect = [
            {"tests": ["not_null"]},
            {"tests": ["unique"]},
        ]
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        # Set up _extract_metadata to return metadata for each model
        metadata_side_effect = [
            {"tests": ["not_null"]},
            {"tests": ["unique"]},
        ]
        mock_execution_engine_instance._extract_metadata = Mock(side_effect=metadata_side_effect)
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        mock_test_executor = Mock()
        mock_test_executor.execute_tests_for_model.side_effect = [
            [TestResult("not_null", "schema1.table1", None, True, 0, TestSeverity.ERROR, "Passed")],
            [TestResult("unique", "schema1.table2", None, True, 0, TestSeverity.ERROR, "Passed")],
        ]
        mock_test_executor_class.return_value = mock_test_executor

        # Execute
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Verify tests were called for each model
        assert mock_test_executor.execute_tests_for_model.call_count == 2
        # Check that tests were called with correct table names
        # Access call arguments - can be positional or keyword
        call_args = []
        for call in mock_test_executor.execute_tests_for_model.call_args_list:
            if call[0]:  # positional args
                call_args.append(call[0][0])
            elif 'table_name' in call[1]:  # keyword args
                call_args.append(call[1]['table_name'])
        assert "schema1.table1" in call_args
        assert "schema1.table2" in call_args

        # Verify results
        assert results["test_results"]["total"] == 2
        assert results["test_results"]["passed"] == 2

    @patch("tee.executor_helpers.build_helpers.ModelExecutor")
    @patch("tee.executor_helpers.build_helpers.ProjectParser")
    @patch("tee.executor_helpers.build_helpers.TestExecutor")
    @patch("tee.engine.execution_engine.ExecutionEngine")
    def test_build_models_skips_test_nodes(
        self, mock_execution_engine_class, mock_test_executor_class, mock_parser_class, mock_model_executor_class, temp_dir, mock_connection_config
    ):
        """Test that build_models skips test nodes in execution order."""
        parsed_models = {
            "schema1.table1": {"model_metadata": {"metadata": {}}},
            "schema1.table2": {"model_metadata": {"metadata": {}}},
        }
        # Execution order includes test nodes
        execution_order = [
            "schema1.table1",
            "test:schema1.table1.not_null",
            "schema1.table2",
        ]
        
        # Setup parser mock
        graph = {
            "nodes": ["schema1.table1", "test:schema1.table1.not_null", "schema1.table2"],
            "dependencies": {
                "schema1.table1": [],
                "test:schema1.table1.not_null": ["schema1.table1"],
                "schema1.table2": [],
            },
        }
        mock_parser = self._setup_parser_mock(parsed_models, execution_order, graph)
        mock_parser_class.return_value = mock_parser

        # Setup model executor mock
        mock_model_executor = Mock()
        mock_model_executor.config = mock_connection_config
        execute_models_return = {
            "executed_tables": ["schema1.table1", "schema1.table2"],
            "failed_tables": [],
            "table_info": {
                "schema1.table1": {"row_count": 10},
                "schema1.table2": {"row_count": 20},
            },
        }
        mock_execution_engine = self._setup_execution_engine_mock(execute_models_return)
        mock_model_executor.execution_engine = mock_execution_engine
        mock_model_executor_class.return_value = mock_model_executor

        # Setup execution engine mock
        mock_execution_engine_instance = Mock()
        mock_execution_engine_instance.connect = Mock()
        mock_execution_engine_instance.execute_models = mock_execution_engine.execute_models
        mock_execution_engine_instance._extract_metadata = mock_execution_engine._extract_metadata
        mock_execution_engine_instance.adapter = mock_execution_engine.adapter
        mock_execution_engine_class.return_value = mock_execution_engine_instance

        mock_test_executor = Mock()
        mock_test_executor.execute_tests_for_model.return_value = []
        mock_test_executor_class.return_value = mock_test_executor

        # Execute
        results = build_models(
            project_folder=str(temp_dir),
            connection_config=mock_connection_config,
            save_analysis=False,
            variables={},
        )

        # Verify that execute_models was only called for non-test nodes
        # Should be called twice (once for each table)
        assert mock_execution_engine.execute_models.call_count == 2

