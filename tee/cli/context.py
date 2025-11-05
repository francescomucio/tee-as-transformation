"""
Command context for shared setup across CLI commands.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional, List

from .utils import parse_vars, load_project_config, setup_logging


class CommandContext:
    """
    Shared context for CLI commands.
    
    Handles common setup: parsing variables, loading config, setting up logging,
    and extracting selection criteria.
    """

    def __init__(self, args):
        """
        Initialize command context from parsed arguments.
        
        Args:
            args: Parsed argparse arguments
        """
        # Parse variables if provided
        vars_string = getattr(args, "vars", None)
        self.vars = parse_vars(vars_string)
        
        # Load project configuration
        self.config = load_project_config(args.project_folder, self.vars)
        
        # Set up logging
        self.verbose = getattr(args, "verbose", False)
        setup_logging(self.verbose)
        
        # Resolve project folder to absolute path
        self.project_path = Path(args.project_folder).resolve()
        
        # Extract selection criteria
        self.select_patterns = getattr(args, "select", None)
        self.exclude_patterns = getattr(args, "exclude", None)
        
        # Store original args for commands that need them
        self.args = args

    def handle_error(self, error: Exception, show_traceback: bool = None) -> None:
        """
        Handle errors consistently across commands.
        
        Args:
            error: The exception that occurred
            show_traceback: Whether to show traceback (defaults to verbose mode)
        """
        if show_traceback is None:
            show_traceback = self.verbose
        
        print(f"Error: {error}")
        if show_traceback:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    def print_variables_info(self) -> None:
        """Print variables information if verbose or if variables are set."""
        if self.vars:
            if self.verbose:
                print(f"Variables: {self.vars}")
            print(f"Using variables: {self.vars}")

    def print_selection_info(self) -> None:
        """Print selection criteria information if verbose."""
        if self.verbose:
            if self.select_patterns:
                print(f"Selection criteria: {self.select_patterns}")
            if self.exclude_patterns:
                print(f"Exclusion criteria: {self.exclude_patterns}")

