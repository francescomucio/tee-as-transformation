"""
Run command implementation.
"""

from ..context import CommandContext
from ...engine.connection_manager import ConnectionManager
from ... import execute_models


def cmd_run(args):
    """Execute the run command."""
    ctx = CommandContext(args)
    connection_manager = None
    
    try:
        print(f"Running tee on project: {args.project_folder}")
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
        )

        # Calculate statistics
        total_tables = len(results["executed_tables"]) + len(results["failed_tables"])
        successful_count = len(results["executed_tables"])
        failed_count = len(results["failed_tables"])
        warning_count = len(results.get("warnings", []))

        print(
            f"\nCompleted! Executed {successful_count} out of {total_tables} tables successfully."
        )
        if failed_count > 0 or warning_count > 0:
            print(f"  ✅ Successful: {successful_count} tables")
            if failed_count > 0:
                print(f"  ❌ Failed: {failed_count} tables")
            if warning_count > 0:
                print(f"  ⚠️  Warnings: {warning_count} warnings")
        else:
            print(f"  ✅ All {successful_count} tables executed successfully!")

        if ctx.verbose:
            print(f"Analysis info: {results.get('analysis', {})}")

    except Exception as e:
        ctx.handle_error(e)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

