"""
Tee Executor

Handles the complete workflow of parsing and executing SQL models based on project configuration.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional, Union, List, TYPE_CHECKING
from .parser import ProjectParser
from .engine import ModelExecutor
from .testing import TestExecutor

if TYPE_CHECKING:
    from tee.adapters import AdapterConfig


def execute_models(
    project_folder: str,
    connection_config: Union[Dict[str, Any], AdapterConfig],
    save_analysis: bool = True,
    variables: Optional[Dict[str, Any]] = None,
    select_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Execute SQL models by parsing them and running them in dependency order.

    This function handles the complete workflow:
    1. Parse SQL models from the project folder
    2. Build dependency graph and determine execution order
    3. Execute models using the execution engine
    4. Optionally save analysis files

    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        save_analysis: Whether to save parsing analysis to files

    Returns:
        Dictionary containing execution results and analysis info
    """
    logger = logging.getLogger(__name__)

    # Keep raw config dict for adapter validation
    # Adapters will handle their own validation and config creation

    print("\n" + "=" * 50)
    print("TEE: PARSING AND EXECUTING SQL MODELS")
    print("=" * 50)

    # Step 1: Parse SQL models
    logger.info("Parsing SQL models...")
    parser = ProjectParser(project_folder, connection_config, variables)

    print("\nCollecting and parsing SQL models...")
    parsed_models = parser.collect_models()
    print(f"Found {len(parsed_models)} SQL files")

    # Step 2: Build dependency graph and get execution order
    print("\nBuilding dependency graph...")
    graph = parser.build_dependency_graph()
    execution_order = parser.get_execution_order()
    print(f"Found {len(graph['nodes'])} tables")
    print(f"Execution order: {' -> '.join(execution_order)}")

    # Step 2.5: Apply selection filtering if specified
    filtered_parsed_models = None
    filtered_execution_order = None
    
    if select_patterns or exclude_patterns:
        from .cli.selection import ModelSelector
        
        selector = ModelSelector(select_patterns=select_patterns, exclude_patterns=exclude_patterns)
        original_count = len(parsed_models)
        filtered_parsed_models, filtered_execution_order = selector.filter_models(parsed_models, execution_order)
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
            parser, variables, parsed_models=filtered_parsed_models, execution_order=filtered_execution_order
        )

        # Step 4: Save analysis files if requested (after execution to include qualified SQL)
        if save_analysis:
            parser.save_to_json()
            print("Analysis files saved to output folder")

        # Step 5: Execute tests after models are created
        # Note: execution_engine.disconnect() is called in execute_models finally block,
        # so we need to reconnect the adapter for tests
        print("\n" + "=" * 50)
        print("EXECUTING TESTS")
        print("=" * 50)

        test_results = None
        try:
            # Reconnect adapter for tests
            if model_executor.execution_engine and model_executor.execution_engine.adapter:
                # Reconnect if needed
                try:
                    model_executor.execution_engine.connect()
                except Exception as e:
                    logger.debug(f"Adapter already connected or connection error: {e}")

                # Create test executor
                test_executor = TestExecutor(
                    model_executor.execution_engine.adapter, project_folder=project_folder
                )

                # Execute all tests (use filtered models if selection was applied)
                test_models = filtered_parsed_models if filtered_parsed_models else parser.collect_models()
                test_order = filtered_execution_order if filtered_execution_order else execution_order
                test_results = test_executor.execute_all_tests(
                    parsed_models=test_models, execution_order=test_order
                )

                # Print test results
                print(f"\nTest Results:")
                print(f"  Total tests: {test_results['total']}")
                print(f"  Passed: {test_results['passed']}")
                print(f"  Failed: {test_results['failed']}")

                if test_results["warnings"]:
                    print(f"\n  ⚠️  Warnings: {len(test_results['warnings'])}")
                    for warning in test_results["warnings"]:
                        print(f"    - {warning}")

                if test_results["errors"]:
                    print(f"\n  ❌ Errors: {len(test_results['errors'])}")
                    for error in test_results["errors"]:
                        print(f"    - {error}")

                # Add test results to execution results
                results["test_results"] = test_results
            else:
                logger.warning("Cannot execute tests: adapter not available")
                print("⚠️  Cannot execute tests: adapter not available")

        except Exception as e:
            logger.error(f"Error executing tests: {e}")
            print(f"⚠️  Error executing tests: {e}")
            # Don't fail the entire run if tests fail

        # Print results
        print("\n" + "=" * 50)
        print("EXECUTION RESULTS")
        print("=" * 50)
        print(f"  Successfully executed: {len(results['executed_tables'])} tables")
        print(f"  Failed: {len(results['failed_tables'])} tables")

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

        # Exit with error code if there are test errors
        # Note: We don't exit here, just log - let the caller decide
        if test_results and test_results.get("errors"):
            logger.error("Test execution failed with errors")

        return results

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        print(f"Error during execution: {e}")
        raise


def parse_models_only(
    project_folder: str,
    connection_config: Union[Dict[str, Any], AdapterConfig],
    variables: Optional[Dict[str, Any]] = None,
    project_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Parse SQL models and build dependency graph without executing them.

    Args:
        project_folder: Path to the project folder containing SQL models
        connection_config: Database connection configuration
        variables: Optional variables for SQL substitution
        project_config: Optional project configuration for OTS export

    Returns:
        Dictionary containing parsing results and dependency information
    """
    logger = logging.getLogger(__name__)

    # Keep raw config dict for adapter validation
    # Adapters will handle their own validation and config creation

    print("\n" + "=" * 50)
    print("TEE: PARSING SQL MODELS")
    print("=" * 50)

    # Parse SQL models
    logger.info("Parsing SQL models...")
    parser = ProjectParser(project_folder, connection_config, variables, project_config)

    print("\nCollecting and parsing SQL models...")
    parsed_models = parser.collect_models()
    print(f"Found {len(parsed_models)} SQL files")

    # Build dependency graph
    print("\nBuilding dependency graph...")
    graph = parser.build_dependency_graph()
    execution_order = parser.get_execution_order()
    print(f"Found {len(graph['nodes'])} tables")
    print(f"Execution order: {' -> '.join(execution_order)}")

    # Save analysis files
    parser.save_to_json()
    print("Analysis files saved to output folder")

    return {
        "parsed_models": parsed_models,
        "dependency_graph": graph,
        "execution_order": execution_order,
        "total_models": len(parsed_models),
        "total_tables": len(graph["nodes"]),
    }
