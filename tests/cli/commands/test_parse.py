"""
Tests for the parse CLI command.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
from io import StringIO

from tee.cli.commands.parse import cmd_parse


class TestParseCommand:
    """Tests for the parse CLI command."""

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
        return args

    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_success(self, mock_context_class, mock_parse_models_only, mock_args):
        """Test successful parse command execution."""
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

        # Mock parse results
        mock_parse_models_only.return_value = {
            "parsed_models": {"schema1.table1": {}, "schema1.table2": {}},
            "execution_order": ["schema1.table1", "schema1.table2"],
            "total_models": 2,
            "total_tables": 2,
        }

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            cmd_parse(mock_args)

        output = fake_out.getvalue()
        assert "Parsing models in project:" in output
        assert "Analysis complete!" in output
        assert "Found 2 tables" in output
        assert "Execution order:" in output

        # Verify parse_models_only was called correctly
        mock_parse_models_only.assert_called_once_with(
            project_folder=str(mock_ctx.project_path),
            connection_config=mock_ctx.config["connection"],
            variables={},
            project_config=mock_ctx.config,
        )

    @patch("tee.cli.commands.parse.ModelSelector")
    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_with_selection_patterns(self, mock_context_class, mock_parse_models_only, mock_selector_class, mock_args):
        """Test parse command with select/exclude patterns."""
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

        # Mock parse results
        all_models = {"schema1.table1": {}, "schema1.table2": {}, "schema1.temp": {}}
        mock_parse_models_only.return_value = {
            "parsed_models": all_models,
            "execution_order": ["schema1.table1", "schema1.table2", "schema1.temp"],
            "total_models": 3,
            "total_tables": 3,
        }

        # Mock selector
        mock_selector = Mock()
        filtered_models = {"schema1.table1": {}, "schema1.table2": {}}
        filtered_order = ["schema1.table1", "schema1.table2"]
        mock_selector.filter_models.return_value = (filtered_models, filtered_order)
        mock_selector_class.return_value = mock_selector

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            cmd_parse(mock_args)

        output = fake_out.getvalue()
        assert "Filtered to 2 models" in output
        assert "from 3 total" in output

        # Verify selector was used
        mock_selector.filter_models.assert_called_once_with(all_models, ["schema1.table1", "schema1.table2", "schema1.temp"])

    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_verbose_mode(self, mock_context_class, mock_parse_models_only, mock_args):
        """Test parse command with verbose output."""
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

        # Mock parse results
        analysis_result = {
            "parsed_models": {"schema1.table1": {}},
            "execution_order": ["schema1.table1"],
            "total_models": 1,
            "total_tables": 1,
        }
        mock_parse_models_only.return_value = analysis_result

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            cmd_parse(mock_args)

        output = fake_out.getvalue()
        assert "Full analysis:" in output

    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_with_variables(self, mock_context_class, mock_parse_models_only, mock_args):
        """Test parse command with variables."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {"env": "production"}
        mock_ctx.select_patterns = None
        mock_ctx.exclude_patterns = None
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock parse results
        mock_parse_models_only.return_value = {
            "parsed_models": {},
            "execution_order": [],
            "total_models": 0,
            "total_tables": 0,
        }

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            cmd_parse(mock_args)

        # Verify variables were passed
        call_args = mock_parse_models_only.call_args
        assert call_args.kwargs["variables"] == {"env": "production"}

    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_handles_exceptions(self, mock_context_class, mock_parse_models_only, mock_args):
        """Test parse command handles exceptions gracefully."""
        # Setup mocks
        mock_ctx = Mock()
        mock_ctx.project_path = Path(mock_args.project_folder)
        mock_ctx.vars = {}
        mock_ctx.config = {"connection": {"type": "duckdb", "path": ":memory:"}}
        mock_ctx.print_variables_info = Mock()
        mock_ctx.print_selection_info = Mock()
        mock_ctx.handle_error = Mock()
        mock_context_class.return_value = mock_ctx

        # Mock exception
        mock_parse_models_only.side_effect = Exception("Parse failed")

        # Capture stdout
        with patch("sys.stdout", new=StringIO()):
            cmd_parse(mock_args)

        # Verify error was handled
        mock_ctx.handle_error.assert_called_once()

    @patch("tee.cli.commands.parse.parse_models_only")
    @patch("tee.cli.commands.parse.CommandContext")
    def test_cmd_parse_empty_results(self, mock_context_class, mock_parse_models_only, mock_args):
        """Test parse command with no models found."""
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

        # Mock empty parse results
        mock_parse_models_only.return_value = {
            "parsed_models": {},
            "execution_order": [],
            "total_models": 0,
            "total_tables": 0,
        }

        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            cmd_parse(mock_args)

        output = fake_out.getvalue()
        assert "Found 0 tables" in output
        assert "Execution order:" in output

