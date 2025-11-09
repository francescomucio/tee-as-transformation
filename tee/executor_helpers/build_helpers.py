"""
Build command helper functions.

This module contains helper functions used by the build_models function
to keep the main executor.py focused on the public API.
"""

from pathlib import Path
from typing import Dict, Any, Optional, Union, List, TYPE_CHECKING
from tee.parser import ProjectParser
from tee.engine import ModelExecutor
from tee.engine.seeds import SeedDiscovery, SeedLoader
from tee.testing import TestExecutor, TestSeverity

if TYPE_CHECKING:
    from tee.adapters import AdapterConfig


def setup_build_context_from_compile(
    project_folder: str,
    connection_config: Union[Dict[str, Any], AdapterConfig],
    variables: Optional[Dict[str, Any]],
    select_patterns: Optional[List[str]],
    exclude_patterns: Optional[List[str]],
    project_config: Optional[Dict[str, Any]],
    parsed_models: Dict[str, Any],
    graph: Dict[str, Any],
    execution_order: List[str],
) -> tuple[ProjectParser, Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Set up build context using compile results (graph and models).
    
    Returns:
        Tuple of (parser, parsed_models, graph, execution_order)
    """
    # Create parser instance and inject models/graph from compile
    parser = ProjectParser(project_folder, connection_config, variables, project_config)
    parser.parsed_models = parsed_models
    parser.graph = graph

    # Apply selection filters if provided
    if select_patterns or exclude_patterns:
        from tee.cli.selection import ModelSelector
        selector = ModelSelector(select_patterns=select_patterns, exclude_patterns=exclude_patterns)
        filtered_parsed_models, filtered_execution_order = selector.filter_models(
            parsed_models, execution_order
        )
        print(f"\nAfter filtering: {len(filtered_parsed_models)} models selected")
        return parser, filtered_parsed_models, graph, filtered_execution_order

    return parser, parsed_models, graph, execution_order


def initialize_build_executors(
    project_folder: str,
    connection_config: Union[Dict[str, Any], AdapterConfig],
    variables: Optional[Dict[str, Any]],
) -> tuple[ModelExecutor, TestExecutor]:
    """
    Initialize model and test executors and connect to database.
    
    Returns:
        Tuple of (model_executor, test_executor)
    """
    model_executor = ModelExecutor(project_folder, connection_config)
    
    from tee.engine.execution_engine import ExecutionEngine
    
    model_executor.execution_engine = ExecutionEngine(
        model_executor.config, project_folder=project_folder, variables=variables
    )
    
    model_executor.execution_engine.connect()
    
    # Load seeds before executing models
    _load_seeds_for_build(model_executor, project_folder)

    test_executor = TestExecutor(
        model_executor.execution_engine.adapter, project_folder=project_folder
    )
    
    return model_executor, test_executor


def _load_seeds_for_build(model_executor: ModelExecutor, project_folder: str) -> None:
    """Load seed files from the seeds folder into database tables."""
    seeds_folder = Path(project_folder) / "seeds"
    
    # Discover seed files
    seed_discovery = SeedDiscovery(seeds_folder)
    seed_files = seed_discovery.discover_seed_files()
    
    if not seed_files:
        return
    
    print(f"\nLoading {len(seed_files)} seed file(s)...")
    
    # Load seeds using the adapter
    seed_loader = SeedLoader(model_executor.execution_engine.adapter)
    seed_results = seed_loader.load_all_seeds(seed_files)
    
    # Log results
    if seed_results["loaded_tables"]:
        print(f"  ✅ Loaded {len(seed_results['loaded_tables'])} seed(s)")
        for table in seed_results["loaded_tables"]:
            print(f"    - {table}")
    
    if seed_results["failed_tables"]:
        print(f"  ⚠️  Failed to load {len(seed_results['failed_tables'])} seed(s)")
        for failure in seed_results["failed_tables"]:
            print(f"    - {failure['file']}: {failure['error']}")


def should_skip_model(
    node_name: str,
    skipped_models: set[str],
    failed_models: set[str],
    graph: Dict[str, Any],
) -> bool:
    """Check if a model should be skipped."""
    if node_name.startswith("test:"):
        return True
    if node_name in skipped_models:
        return True
    
    # Check if this model depends on any failed model
    node_deps = graph["dependencies"].get(node_name, [])
    depends_on_failed = any(
        dep in failed_models for dep in node_deps if not dep.startswith("test:")
    )
    if depends_on_failed:
        return True
    
    return False


def mark_dependents_as_skipped(
    node_name: str,
    parser: ProjectParser,
    skipped_models: set[str],
) -> None:
    """Mark all dependents of a node as skipped."""
    dependents = parser.get_table_dependents(node_name)
    for dependent in dependents:
        if not dependent.startswith("test:"):
            skipped_models.add(dependent)


def execute_single_model(
    node_name: str,
    parsed_models: Dict[str, Any],
    model_executor: ModelExecutor,
) -> Dict[str, Any]:
    """Execute a single model and return results."""
    print(f"\n📦 Executing: {node_name}")
    
    model_results = model_executor.execution_engine.execute_models(
        {node_name: parsed_models[node_name]}, [node_name]
    )
    
    return model_results


def handle_model_execution_result(
    node_name: str,
    model_results: Dict[str, Any],
    failed_models: set[str],
    parser: ProjectParser,
    skipped_models: set[str],
) -> bool:
    """
    Handle model execution result. Returns True if model succeeded, False if failed.
    """
    failed_table_names = [
        f["table"] for f in model_results.get("failed_tables", [])
    ]
    
    if node_name in failed_table_names:
        failed_models.add(node_name)
        error_msg = next(
            (
                f["error"]
                for f in model_results["failed_tables"]
                if f["table"] == node_name
            ),
            "Unknown error",
        )
        print(f"  ❌ Model failed: {error_msg}")
        mark_dependents_as_skipped(node_name, parser, skipped_models)
        return False
    
    # Model executed successfully
    table_info = model_results.get("table_info", {}).get(node_name, {})
    row_count = table_info.get("row_count", 0)
    print(f"  ✅ Model executed: {row_count} rows")
    return True


def execute_tests_for_model(
    node_name: str,
    parsed_models: Dict[str, Any],
    model_executor: ModelExecutor,
    test_executor: TestExecutor,
) -> List[Any]:
    """Execute tests for a model and return test results."""
    model_data = parsed_models[node_name]
    metadata = model_executor.execution_engine._extract_metadata(model_data)
    
    if not metadata:
        return []
    
    print(f"  🧪 Running tests for {node_name}...")
    test_results = test_executor.execute_tests_for_model(
        table_name=node_name, metadata=metadata
    )
    
    return test_results


def execute_tests_for_function(
    function_name: str,
    parsed_functions: Dict[str, Any],
    model_executor: ModelExecutor,
    test_executor: TestExecutor,
) -> List[Any]:
    """Execute tests for a function and return test results."""
    function_data = parsed_functions[function_name]
    metadata = model_executor.execution_engine._extract_function_metadata(function_data)
    
    if not metadata:
        return []
    
    print(f"  🧪 Running tests for {function_name}...")
    test_results = test_executor.execute_tests_for_function(
        function_name=function_name, metadata=metadata
    )
    
    return test_results


def handle_test_results(
    node_name: str,
    test_results: List[Any],
    failed_models: set[str],
    parser: ProjectParser,
    skipped_models: set[str],
) -> bool:
    """
    Handle test results. Returns True if tests passed (or only warnings), False if ERROR failures.
    Raises SystemExit if ERROR severity tests fail.
    """
    error_failures = [
        r for r in test_results
        if not r.passed and r.severity == TestSeverity.ERROR
    ]
    
    if error_failures:
        failed_models.add(node_name)
        print(f"  ❌ Tests failed for {node_name}:")
        for failure in error_failures:
            location = (
                f"{node_name}.{failure.column_name}"
                if failure.column_name
                else node_name
            )
            print(f"    - {failure.test_name} on {location}: {failure.message}")
        
        mark_dependents_as_skipped(node_name, parser, skipped_models)
        print(f"\n❌ Build stopped: Tests failed for {node_name}")
        raise SystemExit(1)
    
    # Tests passed or only warnings
    passed_count = sum(1 for r in test_results if r.passed)
    warning_count = sum(
        1 for r in test_results
        if not r.passed and r.severity == TestSeverity.WARNING
    )
    if warning_count > 0:
        print(f"  ⚠️  Tests: {passed_count} passed, {warning_count} warnings")
    else:
        print(f"  ✅ Tests: {passed_count} passed")
    
    return True


def compile_build_results(
    execution_order: List[str],
    failed_models: set[str],
    skipped_models: set[str],
    all_test_results: List[Any],
    parsed_models: Dict[str, Any],
    graph: Dict[str, Any],
) -> Dict[str, Any]:
    """Compile final build results."""
    executed_tables = [
        name
        for name in execution_order
        if name not in skipped_models
        and name not in failed_models
        and not name.startswith("test:")
    ]
    
    failed_tables = [
        {"table": name, "error": "Model failed or tests failed"}
        for name in failed_models
    ]
    
    # Count test results
    total_tests = len(all_test_results)
    passed_tests = sum(1 for r in all_test_results if r.passed)
    failed_tests = sum(
        1 for r in all_test_results
        if not r.passed and r.severity == TestSeverity.ERROR
    )
    warning_tests = sum(
        1 for r in all_test_results
        if not r.passed and r.severity == TestSeverity.WARNING
    )
    
    test_results_summary = {
        "total": total_tests,
        "passed": passed_tests,
        "failed": failed_tests,
        "warnings": warning_tests,
        "test_results": all_test_results,
    }
    
    return {
        "executed_tables": executed_tables,
        "failed_tables": failed_tables,
        "skipped_tables": list(skipped_models),
        "test_results": test_results_summary,
        "analysis": {
            "total_models": len(parsed_models),
            "execution_order": execution_order,
            "dependency_graph": graph,
        },
    }


def print_build_summary(results: Dict[str, Any], failed_models: set[str], skipped_models: set[str]) -> None:
    """Print build summary."""
    executed_tables = results["executed_tables"]
    test_results = results["test_results"]
    
    print("\n" + "=" * 50)
    print("BUILD RESULTS")
    print("=" * 50)
    print(f"  Models executed: {len(executed_tables)}")
    print(f"  Models failed: {len(failed_models)}")
    print(f"  Models skipped: {len(skipped_models)}")
    print(f"  Tests passed: {test_results['passed']}")
    print(f"  Tests failed: {test_results['failed']}")
    if test_results['warnings'] > 0:
        print(f"  Test warnings: {test_results['warnings']}")

