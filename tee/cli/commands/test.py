"""
Test command implementation.
"""

import typer
from pathlib import Path
from typing import Optional, List

from tee.cli.context import CommandContext
from tee.cli.selection import ModelSelector
from tee.parser import ProjectParser
from tee.engine.execution_engine import ExecutionEngine
from tee.testing import TestExecutor


def cmd_test(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> None:
    """Execute the test command."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )
    
    try:
        typer.echo(f"Running tests for project: {project_folder}")
        ctx.print_variables_info()
        ctx.print_selection_info()
        
        # Step 1: Compile project to OTS modules
        typer.echo("\n" + "=" * 50)
        typer.echo("t4t: COMPILING PROJECT TO OTS MODULES")
        typer.echo("=" * 50)
        try:
            from tee.compiler import compile_project
            compile_results = compile_project(
                project_folder=str(ctx.project_path),
                connection_config=ctx.config["connection"],
                variables=ctx.vars,
                project_config=ctx.config,
            )
            typer.echo(f"✅ Compilation complete: {compile_results['ots_modules_count']} OTS module(s)")
        except Exception as e:
            typer.echo(f"❌ Compilation failed: {e}", err=True)
            raise
        
        # Step 2: Load OTS modules
        typer.echo("\n" + "=" * 50)
        typer.echo("t4t: LOADING COMPILED OTS MODULES")
        typer.echo("=" * 50)
        
        from pathlib import Path
        from tee.parser.input import OTSModuleReader, OTSConverter
        
        output_folder = ctx.project_path / "output" / "ots_modules"
        reader = OTSModuleReader()
        converter = OTSConverter()
        
        ots_files = list(output_folder.glob("*.ots.json")) + list(output_folder.glob("*.ots.yaml")) + list(output_folder.glob("*.ots.yml"))
        
        if not ots_files:
            raise RuntimeError(f"No OTS modules found in {output_folder}. Compilation may have failed.")
        
        parsed_models = {}
        for ots_file in ots_files:
            try:
                module = reader.read_module(ots_file)
                module_models = converter.convert_module(module)
                parsed_models.update(module_models)
            except Exception as e:
                raise RuntimeError(f"Failed to load OTS module {ots_file}: {e}")
        
        typer.echo(f"✅ Loaded {len(parsed_models)} transformations from {len(ots_files)} OTS module(s)")
        
        # Step 3: Build dependency graph
        parser = ProjectParser(str(ctx.project_path), ctx.config["connection"], ctx.vars, ctx.config)
        parser.parsed_models = parsed_models
        graph = parser.build_dependency_graph()
        execution_order = parser.get_execution_order()
        
        # Apply selection filtering if specified
        if ctx.select_patterns or ctx.exclude_patterns:
            selector = ModelSelector(
                select_patterns=ctx.select_patterns,
                exclude_patterns=ctx.exclude_patterns
            )
            
            parsed_models, execution_order = selector.filter_models(parsed_models, execution_order)
            typer.echo(f"Filtered to {len(parsed_models)} models")

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

            # Create test executor (discover SQL tests from tests/ folder)
            test_executor = TestExecutor(
                execution_engine.adapter, project_folder=str(ctx.project_path)
            )

            typer.echo("\n" + "=" * 50)
            typer.echo("EXECUTING TESTS")
            typer.echo("=" * 50)

            # Execute all tests
            test_results = test_executor.execute_all_tests(
                parsed_models=parsed_models,
                execution_order=execution_order,
            )

            # Print test results
            typer.echo(f"\nTest Results:")
            typer.echo(f"  Total tests: {test_results['total']}")
            typer.echo(f"  ✅ Passed: {test_results['passed']}")
            typer.echo(f"  ❌ Failed: {test_results['failed']}")

            if test_results["warnings"]:
                typer.echo(f"\n  ⚠️  Warnings ({len(test_results['warnings'])}):")
                for warning in test_results["warnings"]:
                    typer.echo(f"    - {warning}")

            if test_results["errors"]:
                typer.echo(f"\n  ❌ Errors ({len(test_results['errors'])}):")
                for error in test_results["errors"]:
                    typer.echo(f"    - {error}")

            # Show individual test results if verbose
            if ctx.verbose and test_results["test_results"]:
                typer.echo("\nDetailed Results:")
                for result in test_results["test_results"]:
                    typer.echo(f"  {result}")

            # Exit with error code if there are test errors
            if test_results["errors"]:
                typer.echo("\n❌ Test execution failed with errors", err=True)
                raise typer.Exit(1)
            elif test_results["warnings"]:
                typer.echo("\n⚠️  Test execution completed with warnings")
            else:
                typer.echo("\n✅ All tests passed!")

        finally:
            if execution_engine:
                execution_engine.disconnect()

    except Exception as e:
        ctx.handle_error(e)

