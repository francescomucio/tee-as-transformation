"""
Build command implementation.

Builds models with interleaved test execution, stopping on test failures.
"""
import sys
from typing import Optional, List
from tee.cli.context import CommandContext
from tee.engine.connection_manager import ConnectionManager
from tee import build_models


def cmd_build(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
):
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
        print(f"Building project: {project_folder}")
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

        print(
            f"\nCompleted! Executed {successful_count} out of {total_tables} tables successfully."
        )
        print(f"Tests: {passed_tests} passed, {failed_tests} failed out of {total_tests} total")
        
        if failed_count > 0 or failed_tests > 0:
            if failed_count > 0:
                print(f"  ❌ Failed models: {failed_count}")
            if failed_tests > 0:
                print(f"  ❌ Failed tests: {failed_tests}")
            sys.exit(1)
        else:
            print(f"  ✅ All {successful_count} tables executed successfully!")
            print(f"  ✅ All {total_tests} tests passed!")

        if ctx.verbose:
            print(f"Analysis info: {results.get('analysis', {})}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Build interrupted by user")
        sys.exit(130)
    except Exception as e:
        ctx.handle_error(e)
        sys.exit(1)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

