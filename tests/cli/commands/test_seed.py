"""
Tests for the seed CLI command.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from tee.cli.commands.seed import cmd_seed


class TestSeedCommand:
    """Tests for the seed CLI command."""

    def test_cmd_seed_no_seeds_folder(self, temp_project_dir):
        """Test seed command when seeds folder doesn't exist."""
        args = MagicMock()
        args.project_folder = str(temp_project_dir)
        args.verbose = False
        args.vars = None
        
        # Should not raise, just print message
        cmd_seed(args)

    def test_cmd_seed_empty_seeds_folder(self, temp_project_dir):
        """Test seed command when seeds folder is empty."""
        seeds_folder = temp_project_dir / "seeds"
        seeds_folder.mkdir()
        
        args = MagicMock()
        args.project_folder = str(temp_project_dir)
        args.verbose = False
        args.vars = None
        
        # Should not raise, just print message
        cmd_seed(args)

    @patch('tee.cli.commands.seed.CommandContext')
    @patch('tee.cli.commands.seed.ExecutionEngine')
    def test_cmd_seed_loads_seeds(self, mock_engine_class, mock_context_class, temp_project_dir):
        """Test seed command loads seeds successfully."""
        seeds_folder = temp_project_dir / "seeds"
        seeds_folder.mkdir()
        
        # Create a seed file
        csv_file = seeds_folder / "users.csv"
        csv_file.write_text("id,name\n1,Alice\n")
        
        # Mock the context
        mock_context = MagicMock()
        mock_context.project_path = temp_project_dir
        mock_context.config = {
            "connection": {"type": "duckdb", "path": ":memory:"}
        }
        mock_context.vars = {}
        mock_context.handle_error = MagicMock()
        mock_context_class.return_value = mock_context
        
        # Mock the execution engine
        mock_engine = MagicMock()
        mock_adapter = MagicMock()
        mock_adapter.config.type = "duckdb"
        mock_adapter.get_table_info.return_value = {"row_count": 1}
        mock_engine.adapter = mock_adapter
        mock_engine_class.return_value = mock_engine
        
        args = MagicMock()
        args.project_folder = str(temp_project_dir)
        args.verbose = False
        args.vars = None
        
        # Should execute without error
        cmd_seed(args)
        
        # Verify engine was created and connected
        mock_engine_class.assert_called_once()
        mock_engine.connect.assert_called_once()
        mock_engine.disconnect.assert_called_once()

    @patch('tee.cli.commands.seed.CommandContext')
    def test_cmd_seed_handles_errors(self, mock_context_class, temp_project_dir):
        """Test seed command handles errors gracefully."""
        seeds_folder = temp_project_dir / "seeds"
        seeds_folder.mkdir()
        
        # Mock context to raise error
        mock_context = MagicMock()
        mock_context.project_path = temp_project_dir
        mock_context.config = {
            "connection": {"type": "duckdb", "path": ":memory:"}
        }
        mock_context.vars = {}
        mock_context.handle_error = MagicMock()
        mock_context_class.return_value = mock_context
        
        # Make ExecutionEngine raise an error
        with patch('tee.cli.commands.seed.ExecutionEngine') as mock_engine_class:
            mock_engine_class.side_effect = Exception("Connection failed")
            
            args = MagicMock()
            args.project_folder = str(temp_project_dir)
            args.verbose = False
            args.vars = None
            
            cmd_seed(args)
            
            # Should call handle_error
            mock_context.handle_error.assert_called_once()

