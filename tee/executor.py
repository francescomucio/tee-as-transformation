"""
t4t Executor

Handles the complete workflow of parsing and executing SQL models based on project configuration.
"""

import logging
from typing import TYPE_CHECKING, Any

from tee.engine import ModelExecutor
from tee.executor_helpers import build_helpers
from tee.parser import ProjectParser

if TYPE_CHECKING:
    from tee.adapters import AdapterConfig


def execute_models(
    project_folder: str,
    connection_config: dict[str, Any] | AdapterConfig,
    save_analysis: bool = True,
    variables: dict[str, Any] | None = None,
    select_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    project_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Execute SQL models by compiling to OTS modules and running them in dependency order.

    This function handles the complete workflow:
    1. Compile project to OTS modules (if needed)
    2. Load OTS modules from output/ots_modules/
    3. Build dependency graph and determine execution order
    4. Execute models using the execution engine
    5. Optionally save analysis files

    Note: This function does NOT execute tests. Use `t4t test` or `t4t build` to run tests.

    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        save_analysis: Whether to save parsing analysis to files
        variables: Optional variables for SQL substitution
        select_patterns: Optional list of patterns to select models
        exclude_patterns: Optional list of patterns to exclude models
        project_config: Optional project configuration

    Returns:
        Dictionary containing execution results and analysis info
    """
    logger = logging.getLogger(__name__)

    # Step 0: Compile project to OTS modules first
    print("\n" + "=" * 50)
    print("t4t: COMPILING PROJECT TO OTS MODULES")
    print("=" * 50)
    try:
        from tee.compiler import compile_project

        compile_results = compile_project(
            project_folder=project_folder,
            connection_config=connection_config,
            variables=variables,
            project_config=project_config,
        )
        print(f"✅ Compilation complete: {compile_results['ots_modules_count']} OTS module(s)")

        # Extract graph and execution order from compile results
        graph = compile_results.get("dependency_graph")
        execution_order = compile_results.get("execution_order", [])
        parsed_models = compile_results.get("parsed_models", {})

        if not graph or not execution_order:
            raise RuntimeError("Compilation did not return dependency graph or execution order")

        logger.debug(f"Using dependency graph from compilation: {len(graph['nodes'])} nodes")
        logger.debug(f"Execution order: {' -> '.join(execution_order)}")

    except Exception as e:
        logger.error(f"Compilation failed: {e}")
        raise

    # Create parser instance for model execution (needed by ModelExecutor)
    parser = ProjectParser(project_folder, connection_config, variables, project_config)
    parser.parsed_models = parsed_models
    parser.graph = graph

    # Step 2.5: Apply selection filtering if specified
    filtered_parsed_models = None
    filtered_execution_order = None

    if select_patterns or exclude_patterns:
        from .cli.selection import ModelSelector

        selector = ModelSelector(select_patterns=select_patterns, exclude_patterns=exclude_patterns)
        original_count = len(parsed_models)
        filtered_parsed_models, filtered_execution_order = selector.filter_models(
            parsed_models, execution_order
        )
        filtered_count = len(filtered_parsed_models)

        print(f"\nFiltered to {filtered_count} models (from {original_count} total)")
        if filtered_count > 0:
            print(f"Filtered execution order: {' -> '.join(filtered_execution_order)}")
        else:
            print("⚠️  No models matched the selection criteria!")
            return {
                "executed_tables": [],
                "failed_tables": [],
                "warnings": ["No models matched the selection criteria"],
                "table_info": {},
                "analysis": {
                    "total_models": 0,
                    "total_tables": 0,
                    "execution_order": [],
                    "dependency_graph": graph,
                },
            }

    # Step 3: Execute models
    print("\n" + "=" * 50)
    print("EXECUTING SQL MODELS")
    print("=" * 50)

    model_executor = ModelExecutor(project_folder, connection_config)

    try:
        # Execute models using the executor (pass filtered models if selection was applied)
        results = model_executor.execute_models(
            parser,
            variables,
            parsed_models=filtered_parsed_models,
            execution_order=filtered_execution_order,
        )

        # Step 4: Save analysis files if requested (after execution to include qualified SQL)
        if save_analysis:
            parser.save_to_json()
            print("Analysis files saved to output folder")

        # Print detailed results
        print("\n" + "=" * 50)
        print("EXECUTION RESULTS")
        print("=" * 50)

        if results.get("executed_functions"):
            print("\nSuccessfully executed functions:")
            for function in results["executed_functions"]:
                print(f"  - {function}")

        if results.get("failed_functions"):
            print("\nFailed functions:")
            for failure in results["failed_functions"]:
                print(f"  - {failure['function']}: {failure['error']}")

        if results["executed_tables"]:
            print("\nSuccessfully executed tables:")
            for table in results["executed_tables"]:
                table_info = results["table_info"].get(table, {})
                row_count = table_info.get("row_count", 0)
                print(f"  - {table}: {row_count} rows")

        if results["failed_tables"]:
            print("\nFailed tables:")
            for failure in results["failed_tables"]:
                print(f"  - {failure['table']}: {failure['error']}")

        # Get database info
        try:
            db_info = model_executor.get_database_info()
            if db_info:
                print("\nDatabase Info:")
                print(f"  Type: {db_info.get('connection_type', 'Unknown')}")
                print(f"  Connected: {db_info.get('is_connected', False)}")
        except Exception as e:
            print(f"\nDatabase Info: Error getting info - {e}")

        # Add analysis info to results (use filtered data if filtering was applied)
        final_models = filtered_parsed_models if filtered_parsed_models else parsed_models
        final_order = filtered_execution_order if filtered_execution_order else execution_order
        results["analysis"] = {
            "total_models": len(final_models),
            "total_tables": len(final_models),
            "execution_order": final_order,
            "dependency_graph": graph,
        }

        return results

    except Exception as e:
        print(f"Error during execution: {e}")
        raise


def build_models(
    project_folder: str,
    connection_config: dict[str, Any] | AdapterConfig,
    save_analysis: bool = True,
    variables: dict[str, Any] | None = None,
    select_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    project_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build models with interleaved test execution, stopping on test failures.

    This function executes models and tests interleaved:
    1. Compile project to OTS modules (if needed)
    2. Load OTS modules and build dependency graph
    3. Execute a model
    4. Run its tests immediately
    5. If any ERROR severity test fails, stop execution
    6. Skip dependents of failed models

    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        save_analysis: Whether to save parsing analysis to files
        variables: Optional variables for SQL substitution
        select_patterns: Optional list of patterns to select models
        exclude_patterns: Optional list of patterns to exclude models
        project_config: Optional project configuration

    Returns:
        Dictionary containing execution results and analysis info

    Raises:
        SystemExit: If tests fail with ERROR severity
    """
    logger = logging.getLogger(__name__)

    print("\n" + "=" * 50)
    print("t4t: BUILDING MODELS WITH TESTS")
    print("=" * 50)

    # Step 1: Compile project to OTS modules first
    print("\n" + "=" * 50)
    print("t4t: COMPILING PROJECT TO OTS MODULES")
    print("=" * 50)
    try:
        from tee.compiler import compile_project

        compile_results = compile_project(
            project_folder=project_folder,
            connection_config=connection_config,
            variables=variables,
            project_config=project_config,
        )
        print(f"✅ Compilation complete: {compile_results['ots_modules_count']} OTS module(s)")

        # Extract graph and execution order from compile results
        graph = compile_results.get("dependency_graph")
        execution_order = compile_results.get("execution_order", [])
        parsed_models = compile_results.get("parsed_models", {})

        if not graph or not execution_order:
            raise RuntimeError("Compilation did not return dependency graph or execution order")

        logger.debug(f"Using dependency graph from compilation: {len(graph['nodes'])} nodes")
        logger.debug(f"Execution order: {' -> '.join(execution_order)}")

    except Exception as e:
        logger.error(f"Compilation failed: {e}")
        raise

    # Step 2: Set up build context using compile results
    parser, parsed_models, graph, execution_order = build_helpers.setup_build_context_from_compile(
        project_folder,
        connection_config,
        variables,
        select_patterns,
        exclude_patterns,
        project_config,
        parsed_models,
        graph,
        execution_order,
    )

    # Step 2: Initialize executors
    print("\n" + "=" * 50)
    print("BUILDING MODELS AND TESTS")
    print("=" * 50)

    model_executor = None
    failed_models = set()
    skipped_models = set()
    all_test_results = []

    try:
        model_executor, test_executor = build_helpers.initialize_build_executors(
            project_folder, connection_config, variables
        )

        # Evaluate Python models before execution
        parsed_models = parser.orchestrator.evaluate_python_models(
            parsed_models, variables=variables
        )

        # Step 2.5: Execute functions before models
        # Functions must be created before models that depend on them
        parsed_functions = {}
        function_results = {"executed_functions": [], "failed_functions": []}
        try:
            parsed_functions = parser.orchestrator.discover_and_parse_functions()
            if parsed_functions:
                print(f"\n📦 Executing {len(parsed_functions)} function(s) before models...")
                function_results = model_executor.execution_engine.execute_functions(
                    parsed_functions, execution_order
                )
                if function_results.get("executed_functions"):
                    print(
                        f"  ✅ Executed {len(function_results['executed_functions'])} function(s)"
                    )
                    for func_name in function_results["executed_functions"]:
                        print(f"    - {func_name}")

                        # Execute tests for this function
                        func_test_results = build_helpers.execute_tests_for_function(
                            func_name, parsed_functions, model_executor, test_executor
                        )
                        if func_test_results:
                            # Handle test results (raises SystemExit on ERROR failures)
                            build_helpers.handle_test_results(
                                func_name, func_test_results, failed_models, parser, skipped_models
                            )
                            all_test_results.extend(func_test_results)

                if function_results.get("failed_functions"):
                    print(
                        f"  ⚠️  Failed to execute {len(function_results['failed_functions'])} function(s)"
                    )
                    for failure in function_results["failed_functions"]:
                        print(f"    - {failure['function']}: {failure['error']}")
                    # Continue with models even if some functions failed
                    # Individual function failures are logged but don't stop the build
        except Exception as e:
            # If function discovery/parsing fails, log warning but continue
            # This allows builds to work even if function parsing has issues
            # Catch specific exceptions if available, otherwise catch general Exception
            from tee.parser.shared.exceptions import ParserError
            if isinstance(e, ParserError):
                logger.warning(
                    f"Function parsing error: {e}. Continuing with model execution."
                )
            else:
                logger.warning(
                    f"Could not discover/parse functions: {e}. Continuing with model execution."
                )

        # Step 3: Execute models and tests interleaved
        for node_name in execution_order:
            # Skip functions (they were already executed in Step 2.5)
            # Check if parsed_functions is a dict and contains this node
            if isinstance(parsed_functions, dict) and node_name in parsed_functions:
                continue

            if build_helpers.should_skip_model(node_name, skipped_models, failed_models, graph):
                # Mark as skipped if it depends on a failed model
                from tee.executor_helpers.build_helpers import TEST_NODE_PREFIX
                if node_name not in skipped_models and not node_name.startswith(TEST_NODE_PREFIX):
                    node_deps = graph["dependencies"].get(node_name, [])
                    if any(
                        dep in failed_models for dep in node_deps if not dep.startswith(TEST_NODE_PREFIX)
                    ):
                        skipped_models.add(node_name)
                continue

            try:
                # Execute the model
                model_results = build_helpers.execute_single_model(
                    node_name, parsed_models, model_executor
                )

                # Handle model execution result
                if not build_helpers.handle_model_execution_result(
                    node_name, model_results, failed_models, parser, skipped_models
                ):
                    continue

                # Execute tests for this model
                test_results = build_helpers.execute_tests_for_model(
                    node_name, parsed_models, model_executor, test_executor
                )

                if test_results:
                    # Handle test results (raises SystemExit on ERROR failures)
                    build_helpers.handle_test_results(
                        node_name, test_results, failed_models, parser, skipped_models
                    )
                    all_test_results.extend(test_results)

            except SystemExit:
                raise
            except Exception as e:
                failed_models.add(node_name)
                error_msg = str(e)
                print(f"  ❌ Model execution failed: {error_msg}")
                build_helpers.mark_dependents_as_skipped(node_name, parser, skipped_models)
                print(f"\n❌ Build stopped: Model {node_name} failed")
                raise SystemExit(1) from None

        # Step 4: Save analysis files if requested
        if save_analysis:
            parser.save_to_json()
            print("\nAnalysis files saved to output folder")

        # Step 5: Compile and return results
        results = build_helpers.compile_build_results(
            execution_order, failed_models, skipped_models, all_test_results, parsed_models, graph, parsed_functions, function_results
        )
        build_helpers.print_build_summary(results, failed_models, skipped_models)

        return results

    except SystemExit:
        raise
    except Exception as e:
        print(f"Error during build: {e}")
        raise
    finally:
        # Always disconnect
        if model_executor and model_executor.execution_engine:
            model_executor.execution_engine.disconnect()
