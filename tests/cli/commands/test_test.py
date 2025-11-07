"""
Tests for the test CLI command.
"""

import pytest
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
from io import StringIO

from tee.cli.commands.test import cmd_test


class TestTestCommand:
    """Tests for the test CLI command."""

    @pytest.fixture
    def temp_project_dir(self):
        """Create a temporary project directory with project.toml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            # Create project.toml
            project_toml = tmpdir_path / "project.toml"
            project_toml.write_text(
                'project_folder = "test"\n[connection]\ntype = "duckdb"\npath = ":memory:"'
            )
            yield tmpdir_path

    @pytest.fixture
    def mock_args(self, temp_project_dir):
        """Create mock CLI arguments."""
        args = MagicMock()
        args.project_folder = str(temp_project_dir)
        args.vars = None
        args.verbose = False
        args.select = None
        args.exclude = None
        args.severity = None
        return args

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_success(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test successful test command execution."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 2,
            "passed": 2,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass  # May exit with 0

        output = fake_out.getvalue()
        assert "Running tests for project:" in output
        assert "EXECUTING TESTS" in output
        assert "Test Results:" in output
        assert "Total tests: 2" in output
        assert "Passed: 2" in output
        assert "Failed: 0" in output
        assert "All tests passed!" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_with_failures(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with test failures."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor with failures
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 3,
            "passed": 2,
            "failed": 1,
            "warnings": [],
            "errors": ["Test failed: not_null check"],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(mock_args)
            except SystemExit as e:
                # Should exit with 1 on failure
                assert e.code == 1

        output = fake_out.getvalue()
        assert "Failed: 1" in output
        assert "Errors (1):" in output
        assert "Test execution failed with errors" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_with_warnings(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with warnings."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor with warnings
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 2,
            "passed": 2,
            "failed": 0,
            "warnings": ["Warning: Table has no primary key"],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass  # Should not exit on warnings

        output = fake_out.getvalue()
        assert "Warnings (1):" in output
        assert "Test execution completed with warnings" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.ModelSelector")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_with_selection_patterns(self, mock_context_class, mock_selector_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with select/exclude patterns."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = ["schema1.*"]
        mock_ctx.exclude_patterns = ["*.temp"]
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        all_models = {"schema1.table1": {}, "schema1.temp": {}}
        mock_parser = Mock()
        mock_parser.collect_models.return_value = all_models
        mock_parser.get_execution_order.return_value = ["schema1.table1", "schema1.temp"]
        mock_parser_class.return_value = mock_parser

        # Mock selector
        mock_selector = Mock()
        filtered_models = {"schema1.table1": {}}
        filtered_order = ["schema1.table1"]
        mock_selector.filter_models.return_value = (filtered_models, filtered_order)
        mock_selector_class.return_value = mock_selector

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass

        output = fake_out.getvalue()
        assert "Filtered to 1 models" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_with_severity_overrides(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with severity overrides."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            try:
                cmd_test(
                    project_folder=str(mock_args.project_folder),
                    vars=None,
                    verbose=False,
                    select=None,
                    exclude=None,
                    severity=["not_null=warning", "unique=error"],
                )
            except SystemExit:
                pass

        # Verify severity overrides were parsed and passed
        call_args = mock_executor.execute_all_tests.call_args
        severity_overrides = call_args.kwargs["severity_overrides"]
        assert len(severity_overrides) == 2
        assert "not_null" in severity_overrides
        assert "unique" in severity_overrides

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_invalid_severity_override(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with invalid severity override."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(
                    project_folder=str(mock_args.project_folder),
                    vars=None,
                    verbose=False,
                    select=None,
                    exclude=None,
                    severity=["not_null=invalid"],
                )
            except SystemExit:
                pass

        output = fake_out.getvalue()
        assert "Invalid severity" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_verbose_mode(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command with verbose output."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.verbose = True
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor with detailed results
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": ["Test passed: not_null on schema1.table1.id"],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass

        output = fake_out.getvalue()
        assert "Detailed Results:" in output

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_handles_exceptions(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test test command handles exceptions gracefully."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_ctx.handle_error = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser that raises exception
        mock_parser = Mock()
        mock_parser.collect_models.side_effect = Exception("Parse failed")
        mock_parser_class.return_value = mock_parser

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            cmd_test(mock_args)

        # Verify error was handled
        mock_ctx.handle_error.assert_called_once()

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_cleanup_execution_engine(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test that execution engine is disconnected."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass

        # Verify disconnect was called
        mock_engine.disconnect.assert_called_once()

    @patch("tee.cli.commands.test.TestExecutor")
    @patch("tee.cli.commands.test.ExecutionEngine")
    @patch("tee.cli.commands.test.ProjectParser")
    @patch("tee.cli.commands.test.CommandContext")
    def test_cmd_test_relative_path_resolution(self, mock_context_class, mock_parser_class, mock_engine_class, mock_executor_class, mock_args):
        """Test that relative paths in connection config are resolved."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": "data/test.duckdb"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parser
        mock_parser = Mock()
        mock_parser.collect_models.return_value = {"schema1.table1": {}}
        mock_parser.get_execution_order.return_value = ["schema1.table1"]
        mock_parser_class.return_value = mock_parser

        # Mock execution engine
        mock_engine = Mock()
        mock_adapter = Mock()
        mock_adapter.config.type = "duckdb"
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine

        # Mock test executor
        mock_executor = Mock()
        mock_executor.execute_all_tests.return_value = {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "test_results": [],
        }
        mock_executor_class.return_value = mock_executor

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            try:
                cmd_test(mock_args)
            except SystemExit:
                pass

        # Verify ExecutionEngine was called with resolved path
        call_args = mock_engine_class.call_args
        assert call_args.kwargs["config"]["path"] == str(mock_ctx.project_path / "data/test.duckdb")

