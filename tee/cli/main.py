"""
t4t CLI Main Module

Command-line interface for the t4t SQL model execution framework.
"""

import sys
import typer
from typing import Optional, List

from tee.cli.commands import cmd_run, cmd_parse, cmd_test, cmd_debug, cmd_help, cmd_build, cmd_seed, cmd_init

# Create Typer app
app = typer.Typer(
    name="t4t",
    help="t4t - T(ee) for Transform (and t-shirts!)",
    add_completion=False,
    rich_markup_mode="rich",
)

# Common option definitions to reduce duplication
PROJECT_FOLDER_ARG = typer.Argument(..., help="Path to the project folder containing project.toml")
VERBOSE_OPTION = typer.Option(False, "-v", "--verbose", help="Enable verbose output")
VARS_OPTION = typer.Option(None, "--vars", help="Variables to pass to models (JSON format)")
SELECT_OPTION = typer.Option(None, "-s", "--select", help="Select models. Can be used multiple times.")
EXCLUDE_OPTION = typer.Option(None, "-e", "--exclude", help="Exclude models. Can be used multiple times.")


@app.command()
def run(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
):
    """Parse and execute SQL models."""
    cmd_run(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def parse(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
):
    """Parse SQL models and store metadata (no execution)."""
    cmd_parse(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def debug(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
):
    """Test database connectivity and configuration."""
    cmd_debug(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )


@app.command()
def test(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
    severity: Optional[List[str]] = typer.Option(
        None,
        "--severity",
        help="Override test severity (format: test_name=error|warning). Can be used multiple times.",
    ),
):
    """Run data quality tests on models."""
    cmd_test(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
        severity=severity,
    )


@app.command()
def build(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
):
    """Build models with tests (stops on test failure)."""
    cmd_build(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def seed(
    project_folder: str = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
):
    """Load seed files (CSV, JSON, TSV) into database tables."""
    cmd_seed(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the project (will create a folder with this name)"),
    database_type: str = typer.Option("duckdb", "-d", "--database-type", help="Database type (duckdb, snowflake, postgresql, bigquery)"),
):
    """Initialize a new t4t project."""
    cmd_init(
        project_name=project_name,
        database_type=database_type,
    )


@app.command()
def help():
    """Show help information."""
    cmd_help()


def main():
    """Main CLI entry point."""
    # If no arguments provided, show help (same as --help)
    if len(sys.argv) == 1:
        sys.argv.append("--help")
    
    # If only --help or -h is provided, or no command is given, show help
    elif len(sys.argv) == 2 and sys.argv[1] in ["--help", "-h"]:
        pass  # Typer will handle --help
    elif len(sys.argv) > 1 and sys.argv[1].startswith("-") and sys.argv[1] not in ["--help", "-h"]:
        # Invalid flag without command, show help
        sys.argv = [sys.argv[0], "--help"]
    
    app()


if __name__ == "__main__":
    main()
