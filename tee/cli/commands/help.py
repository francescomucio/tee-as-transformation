"""
Help command implementation.
"""


def cmd_help():
    """Show help information."""
    # Typer automatically generates help via --help flags
    # This command is kept for backward compatibility
    print("Use 't4t <command> --help' for command-specific help")
    print("Or 't4t --help' to see all available commands")

