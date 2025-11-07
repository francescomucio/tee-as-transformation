"""
Test command implementation.
"""

import sys
from pathlib import Path
from typing import Optional, List

from tee.cli.context import CommandContext
from tee.cli.selection import ModelSelector
from tee.parser import ProjectParser
from tee.engine.execution_engine import ExecutionEngine
from tee.testing import TestExecutor
from tee.testing.base import TestSeverity


def cmd_test(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    severity: Optional[List[str]] = None,
):
    """Execute the test command."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )
    
    try:
        print(f"Running tests for project: {project_folder}")
        ctx.print_variables_info()
        ctx.print_selection_info()
        
        # Parse models to get metadata
        parser = ProjectParser(str(ctx.project_path), ctx.config["connection"], ctx.vars)
        parsed_models = parser.collect_models()
        execution_order = parser.get_execution_order()
        
        # Apply selection filtering if specified
        if ctx.select_patterns or ctx.exclude_patterns:
            selector = ModelSelector(
                select_patterns=ctx.select_patterns,
                exclude_patterns=ctx.exclude_patterns
            )
            
            parsed_models, execution_order = selector.filter_models(parsed_models, execution_order)
            print(f"Filtered to {len(parsed_models)} models (from {len(parser.collect_models())} total)")

        # Create model executor and initialize execution engine to get adapter
        # Resolve relative paths in connection config relative to project folder
        connection_config = ctx.config["connection"].copy()
        if "path" in connection_config and connection_config["path"]:
            db_path = Path(connection_config["path"])
            if not db_path.is_absolute():
                connection_config["path"] = str(ctx.project_path / db_path)

        execution_engine = ExecutionEngine(
            config=connection_config,
            project_folder=str(ctx.project_path),
            variables=ctx.vars
        )

        try:
            # Connect adapter
            execution_engine.connect()

            # Parse severity overrides from CLI
            severity_overrides = {}
            if severity:
                for override in severity:
                    if "=" in override:
                        key, severity_str = override.split("=", 1)
                        try:
                            severity = TestSeverity(severity_str.lower())
                            severity_overrides[key.strip()] = severity
                        except ValueError:
                            print(
                                f"⚠️  Invalid severity '{severity_str}', skipping override for '{key}'"
                            )

            # Create test executor (discover SQL tests from tests/ folder)
            test_executor = TestExecutor(
                execution_engine.adapter, project_folder=str(ctx.project_path)
            )

            print("\n" + "=" * 50)
            print("EXECUTING TESTS")
            print("=" * 50)

            # Execute all tests
            test_results = test_executor.execute_all_tests(
                parsed_models=parsed_models,
                execution_order=execution_order,
                severity_overrides=severity_overrides,
            )

            # Print test results
            print(f"\nTest Results:")
            print(f"  Total tests: {test_results['total']}")
            print(f"  ✅ Passed: {test_results['passed']}")
            print(f"  ❌ Failed: {test_results['failed']}")

            if test_results["warnings"]:
                print(f"\n  ⚠️  Warnings ({len(test_results['warnings'])}):")
                for warning in test_results["warnings"]:
                    print(f"    - {warning}")

            if test_results["errors"]:
                print(f"\n  ❌ Errors ({len(test_results['errors'])}):")
                for error in test_results["errors"]:
                    print(f"    - {error}")

            # Show individual test results if verbose
            if ctx.verbose and test_results["test_results"]:
                print("\nDetailed Results:")
                for result in test_results["test_results"]:
                    print(f"  {result}")

            # Exit with error code if there are test errors
            if test_results["errors"]:
                print("\n❌ Test execution failed with errors")
                sys.exit(1)
            elif test_results["warnings"]:
                print("\n⚠️  Test execution completed with warnings")
            else:
                print("\n✅ All tests passed!")

        finally:
            if execution_engine:
                execution_engine.disconnect()

    except Exception as e:
        ctx.handle_error(e)

