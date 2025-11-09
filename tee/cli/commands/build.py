"""
Build command implementation.

Builds models with interleaved test execution, stopping on test failures.
"""

import typer

from tee.cli.context import CommandContext
from tee.engine.connection_manager import ConnectionManager
from tee.executor import build_models


def cmd_build(
    project_folder: str,
    vars: str | None = None,
    verbose: bool = False,
    select: list[str] | None = None,
    exclude: list[str] | None = None,
) -> None:
    """Execute the build command."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )
    connection_manager = None
    
    try:
        typer.echo(f"Building project: {project_folder}")
        ctx.print_variables_info()
        ctx.print_selection_info()

        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
        )

        # Build models with interleaved tests
        results = build_models(
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
        total_tests = results.get("test_results", {}).get("total", 0)
        passed_tests = results.get("test_results", {}).get("passed", 0)
        failed_tests = results.get("test_results", {}).get("failed", 0)

        typer.echo(
            f"\nCompleted! Executed {successful_count} out of {total_tables} tables successfully."
        )
        typer.echo(f"Tests: {passed_tests} passed, {failed_tests} failed out of {total_tests} total")
        
        if failed_count > 0 or failed_tests > 0:
            if failed_count > 0:
                typer.echo(f"  ❌ Failed models: {failed_count}")
            if failed_tests > 0:
                typer.echo(f"  ❌ Failed tests: {failed_tests}")
            raise typer.Exit(1)
        else:
            typer.echo(f"  ✅ All {successful_count} tables executed successfully!")
            typer.echo(f"  ✅ All {total_tests} tests passed!")

        if ctx.verbose:
            typer.echo(f"Analysis info: {results.get('analysis', {})}")

    except KeyboardInterrupt:
        typer.echo("\n\n⚠️  Build interrupted by user")
        raise typer.Exit(130) from None
    except Exception as e:
        ctx.handle_error(e)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

