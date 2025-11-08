"""
t4t CLI Main Module

Command-line interface for the t4t SQL model execution framework.
"""

import typer
from typing import Optional, List, Any, Literal

from tee.cli.commands import cmd_run, cmd_parse, cmd_test, cmd_debug, cmd_help, cmd_build, cmd_seed, cmd_init, cmd_compile
from tee.cli.commands import ots as ots_commands
from tee.adapters import list_available_adapters, is_adapter_supported


# Type aliases for better type safety and IDE support
OutputFormat = Literal["json", "yaml"]
DatabaseType = Literal["duckdb", "snowflake", "postgresql", "bigquery"]


class AlphabeticalOrderGroup(typer.core.TyperGroup):
    """Custom Typer Group that lists commands in alphabetical order."""
    def list_commands(self, ctx):
        return sorted(self.commands.keys())


def validate_format(value: str) -> OutputFormat:
    """Validate format option (json or yaml)."""
    if value not in ["json", "yaml"]:
        raise typer.BadParameter(
            typer.style("Error: ", fg=typer.colors.RED, bold=True) +
            f"Invalid format '{value}'. Must be 'json' or 'yaml'."
        )
    return value  # type: ignore[return-value]


def validate_database_type(value: str) -> DatabaseType:
    """Validate database type option."""
    db_type = value.lower()
    if not is_adapter_supported(db_type):
        available = ", ".join(sorted(list_available_adapters()))
        raise typer.BadParameter(
            typer.style("Error: ", fg=typer.colors.RED, bold=True) +
            f"Unsupported database type '{db_type}'. "
            f"Supported: {available}"
        )
    return db_type  # type: ignore[return-value]


# Create Typer app with alphabetical command ordering
app = typer.Typer(
    name="t4t",
    help="t4t - T(ee) for Transform (and t-shirts!)",
    add_completion=False,
    rich_markup_mode="rich",
    cls=AlphabeticalOrderGroup,
    invoke_without_command=True,
)


@app.callback()
def main_callback(ctx: typer.Context) -> None:
    """Main CLI callback - shows help when no command is provided."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

# Common option definitions to reduce duplication
PROJECT_FOLDER_ARG = typer.Argument(None, help="Path to the project folder containing project.toml")
VERBOSE_OPTION = typer.Option(False, "-v", "--verbose", help="Enable verbose output")
VARS_OPTION = typer.Option(None, "--vars", help="Variables to pass to models (JSON format)")
SELECT_OPTION = typer.Option(None, "-s", "--select", help="Select models. Can be used multiple times.")
EXCLUDE_OPTION = typer.Option(None, "-e", "--exclude", help="Exclude models. Can be used multiple times.")


def _check_required_argument(ctx: typer.Context, arg_name: str, arg_value: Any) -> None:
    """Check if a required argument is provided, show help if not."""
    if arg_value is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)


@app.command()
def run(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
) -> None:
    """Parse and execute SQL models."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_run(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def parse(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
) -> None:
    """Parse SQL models and store metadata (no execution)."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_parse(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def debug(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
) -> None:
    """Test database connectivity and configuration."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_debug(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )


@app.command()
def test(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
) -> None:
    """Run data quality tests on models."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_test(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def build(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
) -> None:
    """Build models with tests (stops on test failure)."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_build(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@app.command()
def seed(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    verbose: bool = VERBOSE_OPTION,
    vars: Optional[str] = VARS_OPTION,
) -> None:
    """Load seed files (CSV, JSON, TSV) into database tables."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_seed(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )


@app.command()
def init(
    ctx: typer.Context,
    project_name: Optional[str] = typer.Argument(None, help="Name of the project (will create a folder with this name)"),
    database_type: DatabaseType = typer.Option("duckdb", "-d", "--database-type", help="Database type (duckdb, snowflake, postgresql, bigquery)", callback=validate_database_type),
) -> None:
    """Initialize a new t4t project."""
    _check_required_argument(ctx, "project_name", project_name)
    cmd_init(
        project_name=project_name,
        database_type=database_type,
    )


@app.command()
def compile(
    ctx: typer.Context,
    project_folder: Optional[str] = PROJECT_FOLDER_ARG,
    vars: Optional[str] = VARS_OPTION,
    verbose: bool = VERBOSE_OPTION,
    format: OutputFormat = typer.Option("json", "-f", "--format", help="Output format: json or yaml", callback=validate_format),
) -> None:
    """Compile t4t project to OTS modules."""
    _check_required_argument(ctx, "project_folder", project_folder)
    cmd_compile(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        format=format,
    )


@app.command()
def help() -> None:
    """Show help information."""
    cmd_help()


# OTS command group with alphabetical ordering
ots_app = typer.Typer(
    name="ots",
    help="Open Transformation Specification (OTS) module commands",
    add_completion=False,
    invoke_without_command=True,
    cls=AlphabeticalOrderGroup,
)


@ots_app.callback()
def ots_callback(ctx: typer.Context):
    """Open Transformation Specification (OTS) module commands."""
    # If no subcommand was provided, show help
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

@ots_app.command("run")
def ots_run(
    ctx: typer.Context,
    ots_path: Optional[str] = typer.Argument(None, help="Path to OTS module file (.ots.json) or directory"),
    project_folder: Optional[str] = typer.Option(None, "--project-folder", help="Project folder for connection config and merging"),
    vars: Optional[str] = VARS_OPTION,
    verbose: bool = VERBOSE_OPTION,
    select: Optional[List[str]] = SELECT_OPTION,
    exclude: Optional[List[str]] = EXCLUDE_OPTION,
) -> None:
    """Execute OTS modules."""
    _check_required_argument(ctx, "ots_path", ots_path)
    ots_commands.cmd_ots_run(
        ots_path=ots_path,
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )


@ots_app.command("validate")
def ots_validate(
    ctx: typer.Context,
    ots_path: Optional[str] = typer.Argument(None, help="Path to OTS module file (.ots.json) or directory"),
    verbose: bool = VERBOSE_OPTION,
) -> None:
    """Validate OTS modules."""
    _check_required_argument(ctx, "ots_path", ots_path)
    ots_commands.cmd_ots_validate(
        ots_path=ots_path,
        verbose=verbose,
    )


# Register OTS app as a subcommand
app.add_typer(ots_app)


def main() -> None:
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
