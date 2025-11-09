"""
t4t Compiler - Compiles t4t projects to OTS modules.

This module handles the compilation workflow:
1. Parse SQL/Python models
2. Load and validate imported OTS modules
3. Detect conflicts
4. Merge and convert to OTS format
5. Validate compiled modules
6. Export to output/ots_modules/
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from tee.parser import ProjectParser
from tee.parser.input import (
    OTSConverter,
    OTSConverterError,
    OTSModuleReader,
    OTSModuleReaderError,
    OTSValidationError,
    validate_ots_module_location,
)
from tee.parser.shared.exceptions import ParserError

logger = logging.getLogger(__name__)


class CompilationError(Exception):
    """Exception raised when compilation fails."""

    pass


def compile_project(
    project_folder: str,
    connection_config: dict[str, Any],
    variables: dict[str, Any] | None = None,
    project_config: dict[str, Any] | None = None,
    format: str = "json",
) -> dict[str, Any]:
    """
    Compile a t4t project to OTS modules.

    This function:
    1. Parses SQL/Python models
    2. Loads and validates imported OTS modules
    3. Detects conflicts (duplicate transformation_id)
    4. Merges all models (SQL, Python, and imported OTS)
    5. Builds dependency graph and saves analysis files
    6. Converts to OTS format
    7. Validates compiled modules
    8. Exports to output/ots_modules/

    Args:
        project_folder: Path to the project folder
        connection_config: Database connection configuration
        variables: Optional variables for SQL substitution
        project_config: Optional project configuration
        format: Output format for OTS modules ("json" or "yaml")

    Returns:
        Dictionary with compilation results including:
        - parsed_models: Merged parsed models
        - dependency_graph: Built dependency graph
        - execution_order: Execution order list
        - ots_modules_count: Number of OTS modules created
        - exported_paths: Paths to exported OTS modules

    Raises:
        CompilationError: If compilation fails
    """
    project_path = Path(project_folder)
    models_folder = project_path / "models"
    tests_folder = project_path / "tests"
    output_folder = project_path / "output" / "ots_modules"

    try:
        logger.info("Starting project compilation to OTS modules")

        # Step 1: Parse SQL/Python models and functions
        print("\n" + "=" * 50)
        print("STEP 1: Parsing SQL and Python models and functions")
        print("=" * 50)
        parser = ProjectParser(project_folder, connection_config, variables, project_config)
        parsed_models = parser.collect_models()
        print(f"✅ Parsed {len(parsed_models)} models from SQL/Python files")

        # Discover and parse functions
        parsed_functions = parser.orchestrator.discover_and_parse_functions()
        if parsed_functions:
            print(f"✅ Parsed {len(parsed_functions)} functions")
        else:
            print("✅ No functions found")

        # Step 2: Discover and validate imported OTS modules
        print("\n" + "=" * 50)
        print("STEP 2: Loading imported OTS modules")
        print("=" * 50)

        from tee.parser.processing import FileDiscovery

        file_discovery = FileDiscovery(models_folder)
        ots_files = file_discovery.discover_ots_modules()

        imported_ots_models = {}
        if ots_files:
            print(f"Found {len(ots_files)} imported OTS module(s)")
            reader = OTSModuleReader()
            converter = OTSConverter()

            for ots_file in ots_files:
                try:
                    # Validate location matches target schema
                    validate_ots_module_location(ots_file, models_folder)

                    # Load and convert
                    module = reader.read_module(ots_file)
                    module_models, module_functions = converter.convert_module(module)

                    # Merge into imported models
                    for transformation_id, parsed_model in module_models.items():
                        if transformation_id in imported_ots_models:
                            raise CompilationError(
                                f"Duplicate transformation_id '{transformation_id}' found in imported OTS modules. "
                                f"Found in both {ots_file} and another imported module."
                            )
                        imported_ots_models[transformation_id] = parsed_model

                    # Note: Functions from OTS modules are not yet integrated into the compilation flow
                    # This will be handled in Phase 8 (Execution Engine Integration)
                    if module_functions:
                        logger.debug(
                            f"Loaded {len(module_functions)} functions from OTS module {ots_file.name} (not yet integrated)"
                        )

                    print(
                        f"  ✅ Loaded {ots_file.name}: {len(module_models)} transformations", end=""
                    )
                    if module_functions:
                        print(f" and {len(module_functions)} functions")
                    else:
                        print()
                except (OTSValidationError, OTSModuleReaderError, OTSConverterError) as e:
                    raise CompilationError(
                        f"Failed to load imported OTS module {ots_file}: {e}"
                    ) from e
                except Exception as e:
                    raise CompilationError(
                        f"Unexpected error loading OTS module {ots_file}: {e}"
                    ) from e
        else:
            print("No imported OTS modules found")

        # Step 3: Detect conflicts
        print("\n" + "=" * 50)
        print("STEP 3: Detecting conflicts")
        print("=" * 50)

        conflicts = []
        for transformation_id in parsed_models.keys():
            if transformation_id in imported_ots_models:
                conflicts.append(transformation_id)

        if conflicts:
            error_msg = (
                f"Found {len(conflicts)} conflict(s): duplicate transformation_id found in both "
                f"SQL/Python models and imported OTS modules:\n"
            )
            for conflict in conflicts:
                error_msg += f"  - {conflict}\n"
            raise CompilationError(error_msg)

        print("✅ No conflicts detected")

        # Step 4: Merge all models
        print("\n" + "=" * 50)
        print("STEP 4: Merging models")
        print("=" * 50)

        all_models = parsed_models.copy()
        all_models.update(imported_ots_models)
        print(
            f"✅ Merged {len(parsed_models)} SQL/Python models with {len(imported_ots_models)} imported OTS transformations"
        )
        print(f"   Total: {len(all_models)} transformations")

        # Step 4.5: Build dependency graph and save analysis files
        print("\n" + "=" * 50)
        print("STEP 4.5: Building dependency graph")
        print("=" * 50)

        # Inject merged models into parser for dependency graph building
        parser.parsed_models = all_models
        graph = parser.build_dependency_graph()
        execution_order = parser.get_execution_order()
        print(f"✅ Built dependency graph with {len(graph['nodes'])} nodes")
        print(f"   Execution order: {' -> '.join(execution_order)}")

        # Save analysis files (dependency graph JSON, Mermaid diagram, Markdown report)
        print("\nSaving analysis files...")
        parser.save_dependency_graph()
        parser.save_mermaid_diagram()
        parser.save_markdown_report()
        parser.save_to_json()
        print("✅ Analysis files saved to output folder")

        # Step 5: Convert to OTS modules
        print("\n" + "=" * 50)
        print("STEP 5: Converting to OTS modules")
        print("=" * 50)

        from tee.parser.output import OTSTransformer

        transformer = OTSTransformer(project_config or {})

        # Load and merge test libraries
        # Collect imported OTS modules to extract their test library paths
        imported_ots_modules = []
        if ots_files:
            reader = OTSModuleReader()
            for ots_file in ots_files:
                try:
                    module = reader.read_module(ots_file)
                    imported_ots_modules.append((module, ots_file))
                except Exception:
                    # Skip if we can't read it (already validated earlier, but be safe)
                    pass

        test_library_path = _merge_test_libraries(
            project_path, tests_folder, output_folder, project_config, imported_ots_modules, format
        )

        ots_modules = transformer.transform_to_ots_modules(
            all_models, parsed_functions=parsed_functions, test_library_path=test_library_path
        )
        print(f"✅ Converted to {len(ots_modules)} OTS module(s)")

        # Step 6: Validate compiled modules
        print("\n" + "=" * 50)
        print("STEP 6: Validating compiled OTS modules")
        print("=" * 50)

        for module_name, module in ots_modules.items():
            try:
                # Re-read to validate (this will check version, structure, etc.)
                # We'll create a temporary file for validation
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".ots.json", delete=False) as tmp:
                    import json

                    json.dump(module, tmp, indent=2)
                    tmp_path = Path(tmp.name)

                reader = OTSModuleReader()
                reader.read_module(tmp_path)
                tmp_path.unlink()  # Clean up

                print(f"  ✅ Validated {module_name}")
            except Exception as e:
                raise CompilationError(f"Validation failed for module {module_name}: {e}") from e

        print("✅ All modules validated")

        # Step 7: Export to output/ots_modules/
        print("\n" + "=" * 50)
        print("STEP 7: Exporting OTS modules")
        print("=" * 50)

        output_folder.mkdir(parents=True, exist_ok=True)

        from tee.parser.output import JSONExporter

        exporter = JSONExporter(output_folder, project_config, project_path)

        # Export OTS modules in the specified format
        exported_paths = exporter.export_ots_modules(
            all_models,
            parsed_functions=parsed_functions,
            test_library_path=test_library_path,
            format=format,
        )

        print(f"✅ Exported {len(exported_paths)} OTS module(s) to {output_folder}")

        return {
            "success": True,
            "parsed_models_count": len(parsed_models),
            "imported_ots_count": len(imported_ots_models),
            "total_transformations": len(all_models),
            "ots_modules_count": len(ots_modules),
            "exported_paths": exported_paths,
            "output_folder": str(output_folder),
            "dependency_graph": graph,
            "execution_order": execution_order,
            "parsed_models": all_models,
        }

    except (CompilationError, ParserError):
        raise
    except Exception as e:
        raise CompilationError(f"Compilation failed: {e}") from e


def _merge_test_libraries(
    project_path: Path,
    tests_folder: Path,
    output_folder: Path,
    project_config: dict[str, Any] | None,
    imported_ots_modules: list[tuple],
    format: str = "json",
) -> Path | None:
    """
    Merge test libraries from project and imported OTS modules.

    Args:
        project_path: Project root path
        tests_folder: Path to tests folder
        output_folder: Output folder for compiled test library
        project_config: Project configuration
        imported_ots_modules: List of tuples (module_dict, module_file_path) from imported OTS modules

    Returns:
        Path to merged test library file, or None if no tests found
    """
    import json

    from tee.parser.output import TestLibraryExporter

    # Get project name from config or folder name
    # project.toml has "project_folder" not "name", so use that or fall back to folder name
    if project_config:
        project_name = (
            project_config.get("name") or project_config.get("project_folder") or project_path.name
        )
    else:
        project_name = project_path.name

    # Step 1: Export project's test library (or create empty structure)
    # This creates the base test library that will be merged with imported ones
    test_exporter = TestLibraryExporter(project_path, project_name)
    project_test_library_path = test_exporter.export_test_library(output_folder, format=format)
    # Note: This file will be overwritten by the merged version below

    # Load project's test library if it exists
    project_test_library = {}
    if project_test_library_path and project_test_library_path.exists():
        try:
            with open(project_test_library_path, encoding="utf-8") as f:
                # Try JSON first, then YAML
                try:
                    project_test_library = json.load(f)
                except json.JSONDecodeError:
                    # Try YAML
                    import yaml

                    f.seek(0)
                    project_test_library = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Failed to load project test library: {e}")
            project_test_library = {}

    # If no project test library was created, create empty structure
    if not project_test_library:
        project_test_library = {
            "ots_version": "0.2.0",  # Test libraries are part of OTS 0.2.0
            "test_library_version": "1.0",
            "description": f"Test library for {project_name} project",
        }

    # Step 2: Collect and load test libraries from imported OTS modules
    imported_test_libraries = []
    test_library_paths_found = set()

    for module, module_file in imported_ots_modules:
        test_library_path_str = module.get("test_library_path")
        if not test_library_path_str:
            continue

        # Resolve test library path relative to the OTS module file location
        module_dir = module_file.parent
        test_lib_path = module_dir / test_library_path_str

        # Also try relative to project root (in case path is absolute or relative to project)
        if not test_lib_path.exists():
            test_lib_path = project_path / test_library_path_str

        # Skip if we've already loaded this test library (avoid duplicates)
        test_lib_key = str(test_lib_path.resolve())
        if test_lib_key in test_library_paths_found:
            continue

        if test_lib_path.exists():
            try:
                with open(test_lib_path, encoding="utf-8") as f:
                    # Try JSON first, then YAML
                    try:
                        imported_lib = json.load(f)
                    except json.JSONDecodeError:
                        # Try YAML
                        import yaml

                        f.seek(0)
                        imported_lib = yaml.safe_load(f)

                    imported_test_libraries.append((imported_lib, test_lib_path))
                    test_library_paths_found.add(test_lib_key)
                    logger.info(f"Loaded test library from imported OTS module: {test_lib_path}")
            except Exception as e:
                logger.warning(
                    f"Failed to load test library from {test_lib_path} (referenced by {module_file}): {e}"
                )
        else:
            logger.warning(
                f"Test library path '{test_library_path_str}' referenced by {module_file} not found at {test_lib_path}"
            )

    # Step 3: Merge test libraries
    merged_generic_tests = project_test_library.get("generic_tests", {}).copy()
    merged_singular_tests = project_test_library.get("singular_tests", {}).copy()
    conflicts = []

    for imported_lib, lib_path in imported_test_libraries:
        # Merge generic tests
        imported_generic = imported_lib.get("generic_tests", {})
        for test_name, test_def in imported_generic.items():
            if test_name in merged_generic_tests:
                conflicts.append(f"generic_tests.{test_name} (from {lib_path.name})")
                # Project test library takes precedence
                logger.warning(
                    f"Test conflict: generic test '{test_name}' already exists in project test library. "
                    f"Project version will be used (imported from {lib_path.name} will be ignored)."
                )
            else:
                merged_generic_tests[test_name] = test_def

        # Merge singular tests
        imported_singular = imported_lib.get("singular_tests", {})
        for test_name, test_def in imported_singular.items():
            if test_name in merged_singular_tests:
                conflicts.append(f"singular_tests.{test_name} (from {lib_path.name})")
                # Project test library takes precedence
                logger.warning(
                    f"Test conflict: singular test '{test_name}' already exists in project test library. "
                    f"Project version will be used (imported from {lib_path.name} will be ignored)."
                )
            else:
                merged_singular_tests[test_name] = test_def

    # Step 4: Build merged test library
    merged_test_library = {
        "ots_version": "0.2.0",  # Test libraries are part of OTS 0.2.0
        "test_library_version": project_test_library.get("test_library_version", "1.0"),
        "description": project_test_library.get(
            "description", f"Test library for {project_name} project"
        ),
    }

    if merged_generic_tests:
        merged_test_library["generic_tests"] = merged_generic_tests
    if merged_singular_tests:
        merged_test_library["singular_tests"] = merged_singular_tests

    # Step 5: Write merged test library
    if not merged_generic_tests and not merged_singular_tests:
        logger.info("No tests found in project or imported libraries")
        return None

    # Use appropriate extension based on format
    if format == "yaml":
        filename = f"{project_name}_test_library.ots.yaml"
    else:
        filename = f"{project_name}_test_library.ots.json"
    output_file = output_folder / filename
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        if format == "yaml":
            import yaml

            yaml.dump(
                merged_test_library,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        else:
            json.dump(merged_test_library, f, indent=2, ensure_ascii=False)

    logger.info(f"Exported merged test library to {output_file}")
    print(f"✅ Final merged test library saved to {output_file}")
    print(
        f"   Contains {len(merged_generic_tests)} generic test(s) and {len(merged_singular_tests)} singular test(s)"
    )
    if conflicts:
        print(f"   ⚠️  {len(conflicts)} test conflict(s) resolved (project version used)")

    return output_file
