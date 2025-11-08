"""
Run command implementation.
"""

import typer
from typing import Optional, List
from tee.cli.context import CommandContext
from tee.engine.connection_manager import ConnectionManager
from tee import execute_models


def cmd_run(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> None:
    """Execute the run command."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )
    connection_manager = None
    
    try:
        typer.echo(f"Running t4t on project: {project_folder}")
        ctx.print_variables_info()
        ctx.print_selection_info()

        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
        )

        # Execute models using the unified connection manager
        results = execute_models(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            save_analysis=True,
            variables=ctx.vars,
            select_patterns=ctx.select_patterns,
            exclude_patterns=ctx.exclude_patterns,
            project_config=ctx.config,
        )

        # Calculate statistics
        total_tables = len(results["executed_tables"]) + len(results["failed_tables"])
        successful_count = len(results["executed_tables"])
        failed_count = len(results["failed_tables"])
        warning_count = len(results.get("warnings", []))

        typer.echo(
            f"\nCompleted! Executed {successful_count} out of {total_tables} tables successfully."
        )
        if failed_count > 0 or warning_count > 0:
            typer.echo(f"  ✅ Successful: {successful_count} tables")
            if failed_count > 0:
                typer.echo(f"  ❌ Failed: {failed_count} tables")
            if warning_count > 0:
                typer.echo(f"  ⚠️  Warnings: {warning_count} warnings")
        else:
            typer.echo(f"  ✅ All {successful_count} tables executed successfully!")

        if ctx.verbose:
            typer.echo(f"Analysis info: {results.get('analysis', {})}")

    except Exception as e:
        ctx.handle_error(e)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

