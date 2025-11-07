"""
Parse command implementation.
"""

from typing import Optional, List
from tee.cli.context import CommandContext
from tee.cli.selection import ModelSelector
from tee import parse_models_only


def cmd_parse(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
):
    """Execute the parse command."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
        select=select,
        exclude=exclude,
    )
    
    try:
        print(f"Parsing models in project: {project_folder}")
        ctx.print_variables_info()
        ctx.print_selection_info()
        
        # Parse models only
        analysis = parse_models_only(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
            project_config=ctx.config,
        )
        
        # Apply selection filtering if specified
        if ctx.select_patterns or ctx.exclude_patterns:
            selector = ModelSelector(
                select_patterns=ctx.select_patterns,
                exclude_patterns=ctx.exclude_patterns
            )
            
            parsed_models = analysis.get("parsed_models", {})
            execution_order = analysis.get("execution_order", [])
            filtered_models, filtered_order = selector.filter_models(parsed_models, execution_order)
            
            analysis["parsed_models"] = filtered_models
            analysis["execution_order"] = filtered_order
            analysis["total_models"] = len(filtered_models)
            analysis["total_tables"] = len(filtered_models)
            
            print(f"\nFiltered to {len(filtered_models)} models (from {len(parsed_models)} total)")

        print(f"\nAnalysis complete! Found {analysis['total_tables']} tables.")
        print(f"Execution order: {' -> '.join(analysis['execution_order'])}")

        if ctx.verbose:
            print(f"Full analysis: {analysis}")

    except Exception as e:
        ctx.handle_error(e)

