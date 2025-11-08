"""
Compile command implementation.
"""

from typing import Optional
from tee.cli.context import CommandContext
from tee.compiler import compile_project, CompilationError


def cmd_compile(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    format: str = "json",
):
    """
    Compile t4t project to OTS modules.
    
    This command:
    1. Parses SQL/Python models
    2. Loads and validates imported OTS modules
    3. Detects conflicts
    4. Merges and converts to OTS format
    5. Validates compiled modules
    6. Exports to output/ots_modules/
    
    Args:
        project_folder: Path to the project folder
        vars: Optional variables for SQL substitution (JSON format)
        verbose: Enable verbose output
        format: Output format ("json" or "yaml")
    """
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )
    
    try:
        print(f"Compiling project: {project_folder}")
        ctx.print_variables_info()
        
        # Compile project
        results = compile_project(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
            project_config=ctx.config,
            format=format,
        )
        
        print(f"\n✅ Compilation complete!")
        print(f"   Parsed models: {results['parsed_models_count']}")
        print(f"   Imported OTS: {results['imported_ots_count']}")
        print(f"   Total transformations: {results['total_transformations']}")
        print(f"   OTS modules: {results['ots_modules_count']}")
        print(f"   Output: {results['output_folder']}")
        
    except CompilationError as e:
        print(f"\n❌ Compilation failed: {e}")
        ctx.handle_error(e)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        ctx.handle_error(e)

