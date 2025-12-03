"""
Docs command implementation.
"""

from pathlib import Path

import typer

from tee.cli.context import CommandContext
from tee.parser.core.project_parser import ProjectParser
from tee.parser.output.docs_generator import DocsGenerator


def cmd_docs(
    project_folder: str,
    vars: str | None = None,
    verbose: bool = False,
    output_dir: str | None = None,
) -> None:
    """
    Generate static documentation site with dependency graph.

    This command:
    1. Parses SQL/Python models
    2. Builds dependency graph
    3. Generates static HTML documentation site with interactive graph

    Args:
        project_folder: Path to the project folder
        vars: Optional variables for SQL substitution (JSON format)
        verbose: Enable verbose output
        output_dir: Output directory for docs (default: output/docs)
    """
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )

    try:
        typer.echo(f"Generating documentation for project: {project_folder}")
        ctx.print_variables_info()

        # Determine output directory
        if output_dir:
            docs_output = Path(output_dir).resolve()
        else:
            docs_output = ctx.project_path / "output" / "docs"

        # Parse project
        parser = ProjectParser(
            project_folder=str(ctx.project_path),
            connection=ctx.config["connection"],
            variables=ctx.vars,
            project_config=ctx.config,
        )

        typer.echo("Parsing models...")
        parsed_models = parser.collect_models()
        typer.echo(f"  Found {len(parsed_models)} model(s)")

        typer.echo("Building dependency graph...")
        graph = parser.build_dependency_graph()
        typer.echo(f"  Graph has {len(graph['nodes'])} node(s) and {len(graph['edges'])} edge(s)")

        # Get parsed functions if available
        parsed_functions = parser.orchestrator.discover_and_parse_functions()

        # Generate documentation
        typer.echo("Generating documentation site...")
        generator = DocsGenerator(
            project_path=ctx.project_path,
            output_path=docs_output,
            parsed_models=parsed_models,
            parsed_functions=parsed_functions,
            dependency_graph=graph,
        )

        generator.generate()

        typer.echo("\n✅ Documentation generated successfully!")
        typer.echo(f"   Output: {docs_output}")
        typer.echo(f"   Open: {docs_output / 'index.html'}")

    except Exception as e:
        typer.echo(f"\n❌ Documentation generation failed: {e}", err=True)
        ctx.handle_error(e)

