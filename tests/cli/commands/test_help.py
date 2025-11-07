"""
Tests for the help CLI command.
"""

import pytest
from unittest.mock import patch
from io import StringIO

from tee.cli.commands.help import cmd_help


class TestHelpCommand:
    """Tests for the help CLI command."""

    def test_cmd_help(self):
        """Test help command."""
        # Capture stdout
        with patch("sys.stdout", new=StringIO()) as fake_out:
            cmd_help()

        output = fake_out.getvalue()
        assert "Use 't4t <command> --help' for command-specific help" in output
        assert "Or 't4t --help' to see all available commands" in output

