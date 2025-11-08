"""
OTS command implementations.
"""

from pathlib import Path
from typing import Optional, List
from tee.cli.context import CommandContext
from tee.parser.input import OTSModuleReader, OTSModuleReaderError, load_ots_modules
from tee.parser import ProjectParser
from tee.engine import ModelExecutor
from tee.testing import TestExecutor
from tee import execute_models


def cmd_ots_run(
    ots_path: str,
    project_folder: Optional[str] = None,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
):
    """
    Execute OTS modules.
    
    Args:
        ots_path: Path to OTS module file (.ots.json) or directory containing OTS modules
        project_folder: Optional project folder (for connection config and merging with existing models)
        vars: Optional variables for SQL substitution (JSON format)
        verbose: Enable verbose output
        select: Select models (can be used multiple times)
        exclude: Exclude models (can be used multiple times)
    """
    ots_path_obj = Path(ots_path)
    
    if not ots_path_obj.exists():
        print(f"❌ Error: OTS path not found: {ots_path}")
        return

    try:
        print(f"\n{'=' * 50}")
        print("t4t: EXECUTING OTS MODULES")
        print(f"{'=' * 50}")
        print(f"OTS path: {ots_path}")

        # Load OTS modules
        print("\nLoading OTS modules...")
        ots_parsed_models = load_ots_modules(ots_path_obj)
        print(f"✅ Loaded {len(ots_parsed_models)} transformations from OTS modules")

        # Determine connection config and project folder
        if project_folder:
            ctx = CommandContext(
                project_folder=project_folder,
                vars=vars,
                verbose=verbose,
                select=select,
                exclude=exclude,
            )
            connection_config = ctx.config["connection"]
            project_path = ctx.project_path
            variables = ctx.vars
            select_patterns = ctx.select_patterns
            exclude_patterns = ctx.exclude_patterns

            print(f"\nMerging with project models from: {project_folder}")
            
            # Parse existing models
            parser = ProjectParser(
                str(project_path),
                connection_config,
                variables,
            )
            existing_models = parser.collect_models()
            print(f"Found {len(existing_models)} existing models in project")

            # Merge OTS models with existing models
            from tee.parser.input import merge_ots_with_parsed_models
            all_models = merge_ots_with_parsed_models(existing_models, ots_parsed_models)
            print(f"Total models to execute: {len(all_models)}")

            # Inject merged models into parser
            parser.parsed_models = all_models

            # Build dependency graph with merged models
            print("\nBuilding dependency graph with merged models...")
            graph = parser.build_dependency_graph()
            execution_order = parser.get_execution_order()
            print(f"Execution order: {' -> '.join(execution_order)}")

            # Apply selection filtering if specified
            if select_patterns or exclude_patterns:
                from tee.cli.selection import ModelSelector
                selector = ModelSelector(
                    select_patterns=select_patterns,
                    exclude_patterns=exclude_patterns
                )
                all_models, execution_order = selector.filter_models(all_models, execution_order)
                print(f"Filtered to {len(all_models)} models")

            # Execute models
            from tee.engine import ModelExecutor
            model_executor = ModelExecutor(str(project_path), connection_config)
            results = model_executor.execute_models(
                parser=parser,
                variables=variables,
                parsed_models=all_models,
                execution_order=execution_order,
            )

            # Run tests
            from tee.testing import TestExecutor
            test_executor = TestExecutor(model_executor.execution_engine)
            test_results = test_executor.run_tests(all_models, variables=variables)
            results["test_results"] = test_results

            # Disconnect
            model_executor.execution_engine.disconnect()

            # Print summary
            print(f"\n✅ Execution complete!")
            print(f"   Executed: {len(results['executed_tables'])} tables")
            if results.get("failed_tables"):
                print(f"   Failed: {len(results['failed_tables'])} tables")
            if results.get("test_results"):
                test_summary = results["test_results"]
                print(f"   Tests passed: {test_summary.get('passed', 0)}")
                print(f"   Tests failed: {test_summary.get('failed', 0)}")

        else:
            # Execute OTS modules standalone
            # We need connection config - try to infer from OTS module target
            if ots_path_obj.is_file():
                reader = OTSModuleReader()
                module = reader.read_module(ots_path_obj)
                target = module.get("target", {})
                
                # Create connection config from target
                sql_dialect = target.get("sql_dialect", "duckdb")
                connection_config = {
                    "type": sql_dialect,
                }
                # Add database-specific config if available
                if target.get("database"):
                    connection_config["database"] = target["database"]
                if target.get("schema"):
                    connection_config["schema"] = target["schema"]
                
                # Use current directory as project folder
                project_path = Path.cwd()
            else:
                print("❌ Error: Cannot determine connection config for directory of OTS modules")
                print("   Please provide --project-folder or ensure OTS modules have target config")
                return

            # Create a minimal parser for dependency graph building
            parser = ProjectParser(str(project_path), connection_config, variables or {})
            parser.parsed_models = ots_parsed_models

            # Build dependency graph
            print("\nBuilding dependency graph...")
            graph = parser.build_dependency_graph()
            execution_order = parser.get_execution_order()
            print(f"Execution order: {' -> '.join(execution_order)}")

            # Execute models
            from tee.engine import ModelExecutor
            model_executor = ModelExecutor(str(project_path), connection_config)
            results = model_executor.execute_models(
                parser=parser,
                variables=variables or {},
                parsed_models=ots_parsed_models,
                execution_order=execution_order,
            )

            # Run tests
            from tee.testing import TestExecutor
            test_executor = TestExecutor(model_executor.execution_engine)
            test_results = test_executor.run_tests(ots_parsed_models, variables=variables or {})
            results["test_results"] = test_results

            # Disconnect
            model_executor.execution_engine.disconnect()

            # Print summary
            print(f"\n✅ Execution complete!")
            print(f"   Executed: {len(results['executed_tables'])} tables")
            if results.get("failed_tables"):
                print(f"   Failed: {len(results['failed_tables'])} tables")

    except OTSModuleReaderError as e:
        print(f"❌ Error reading OTS modules: {e}")
    except Exception as e:
        print(f"❌ Error executing OTS modules: {e}")
        if verbose:
            import traceback
            traceback.print_exc()


def cmd_ots_validate(
    ots_path: str,
    verbose: bool = False,
):
    """
    Validate OTS modules.
    
    Args:
        ots_path: Path to OTS module file (.ots.json) or directory containing OTS modules
        verbose: Enable verbose output
    """
    ots_path_obj = Path(ots_path)
    
    if not ots_path_obj.exists():
        print(f"❌ Error: OTS path not found: {ots_path}")
        return

    try:
        reader = OTSModuleReader()
        
        if ots_path_obj.is_file():
            print(f"\nValidating OTS module: {ots_path}")
            module = reader.read_module(ots_path_obj)
            info = reader.get_module_info(module)
            
            print("✅ OTS module is valid!")
            print(f"\nModule Information:")
            print(f"  Name: {info['module_name']}")
            print(f"  OTS Version: {info['ots_version']}")
            print(f"  Transformations: {info['transformation_count']}")
            print(f"  Target: {info['target'].get('database')}.{info['target'].get('schema')}")
            if info.get('module_tags'):
                print(f"  Tags: {', '.join(info['module_tags'])}")
            if info.get('has_test_library'):
                print(f"  Test Library: Yes")
                
        elif ots_path_obj.is_dir():
            print(f"\nValidating OTS modules in: {ots_path}")
            modules = reader.read_modules_from_directory(ots_path_obj)
            
            if not modules:
                print("⚠️  No OTS modules found")
                return
            
            print(f"✅ Found {len(modules)} OTS module(s)")
            
            for module_name, module in modules.items():
                info = reader.get_module_info(module)
                print(f"\n  {module_name}:")
                print(f"    Transformations: {info['transformation_count']}")
                print(f"    Target: {info['target'].get('database')}.{info['target'].get('schema')}")
                
        else:
            print(f"❌ Error: Path is neither a file nor a directory: {ots_path}")
            
    except OTSModuleReaderError as e:
        print(f"❌ Validation failed: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
    except Exception as e:
        print(f"❌ Error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()

