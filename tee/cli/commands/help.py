"""
Help command implementation.
"""

import typer


def cmd_help() -> None:
    """Show help information."""
    # Typer automatically generates help via --help flags
    # This command is kept for backward compatibility
    typer.echo("Use 't4t <command> --help' for command-specific help")
    typer.echo("Or 't4t --help' to see all available commands")

