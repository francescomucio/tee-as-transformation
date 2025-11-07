"""
t4t CLI Main Module

Command-line interface for the t4t SQL model execution framework.
"""

import argparse
from tee.cli.commands import cmd_run, cmd_parse, cmd_test, cmd_debug, cmd_help, cmd_build, cmd_seed


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """
    Add common arguments to a command parser.

    Args:
        parser: ArgumentParser instance to add arguments to
    """
    parser.add_argument(
        "project_folder", help="Path to the project folder containing project.toml"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument(
        "--vars",
        type=str,
        help="Variables to pass to models (JSON format, e.g., '{\"env\": \"prod\", \"start_date\": \"2024-01-01\"}')",
    )


def add_selection_args(parser: argparse.ArgumentParser) -> None:
    """
    Add selection arguments to a command parser.

    Args:
        parser: ArgumentParser instance to add arguments to
    """
    parser.add_argument(
        "-s", "--select",
        action="append",
        dest="select",
        help="Select models (e.g., --select my_model or --select tag:nightly). Can be used multiple times. Supports wildcards (*, ?).",
    )
    parser.add_argument(
        "-e", "--exclude",
        action="append",
        dest="exclude",
        help="Exclude models (e.g., --exclude deprecated or --exclude tag:test). Can be used multiple times. Supports wildcards (*, ?).",
    )


def main():
    """Main CLI entry point."""
    global parser

    parser = argparse.ArgumentParser(
        description="t4t - Transform, Extract, Execute (and t-shirts!)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  tcli run ./my_project                    # Run models in ./my_project
  tcli build ./my_project                  # Build models with tests (stops on failure)
  tcli parse ./my_project                  # Parse models in ./my_project
  tcli test ./my_project                   # Run tests on models in ./my_project
  tcli seed ./my_project                   # Load seed files into database
  tcli debug ./my_project                  # Test database connectivity
  tcli run ./my_project -v                 # Run with verbose output
  tcli run ./my_project --vars '{"env": "prod"}'  # Run with variables (JSON)
  tcli run ./my_project --select my_model  # Run only my_model
  tcli run ./my_project -s my_model  # Same as above (short flag)
  tcli run ./my_project --select tag:nightly  # Run models with tag:nightly
  tcli run ./my_project --select my_model --exclude tag:test  # Run my_model excluding test models
  tcli run ./my_project -s my_model -e tag:test  # Same as above (short flags)
  tcli test ./my_project --severity not_null=warning  # Override test severity
  tcli help                                # Show this help message
        """,
    )

    # Add subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)

    # Run command
    run_parser = subparsers.add_parser("run", help="Parse and execute SQL models")
    add_common_args(run_parser)
    add_selection_args(run_parser)
    run_parser.set_defaults(func=cmd_run)

    # Parse command
    parse_parser = subparsers.add_parser(
        "parse", help="Parse SQL models and store metadata (no execution)"
    )
    add_common_args(parse_parser)
    add_selection_args(parse_parser)
    parse_parser.set_defaults(func=cmd_parse)

    # Debug command
    debug_parser = subparsers.add_parser(
        "debug", help="Test database connectivity and configuration"
    )
    add_common_args(debug_parser)
    debug_parser.set_defaults(func=cmd_debug)

    # Test command
    test_parser = subparsers.add_parser("test", help="Run data quality tests on models")
    add_common_args(test_parser)
    test_parser.add_argument(
        "--severity",
        action="append",
        help="Override test severity (format: test_name=error|warning or table.column.test_name=error|warning). Can be used multiple times.",
    )
    add_selection_args(test_parser)
    test_parser.set_defaults(func=cmd_test)

    # Build command
    build_parser = subparsers.add_parser(
        "build", help="Build models with tests (stops on test failure)"
    )
    add_common_args(build_parser)
    add_selection_args(build_parser)
    build_parser.set_defaults(func=cmd_build)

    # Seed command
    seed_parser = subparsers.add_parser(
        "seed", help="Load seed files (CSV, JSON, TSV) into database tables"
    )
    add_common_args(seed_parser)
    seed_parser.set_defaults(func=cmd_seed)

    # Help command
    help_parser = subparsers.add_parser("help", help="Show help information")
    
    def cmd_help_with_parser(args):
        """Wrapper to pass parser to help command."""
        # Create a simple object to hold parser reference
        class ArgsWithParser:
            def __init__(self):
                self.parser = parser
        cmd_help(ArgsWithParser())
    
    help_parser.set_defaults(func=cmd_help_with_parser)

    # Parse arguments
    args = parser.parse_args()

    # Execute the appropriate command
    args.func(args)


if __name__ == "__main__":
    main()
