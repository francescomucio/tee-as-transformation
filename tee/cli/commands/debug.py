"""
Debug command implementation.
"""

import typer
from typing import Optional
from tee.cli.context import CommandContext
from tee.engine.connection_manager import ConnectionManager


def cmd_debug(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """Execute the debug command to test database connectivity."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )
    connection_manager = None
    
    try:
        typer.echo(f"Testing database connectivity for project: {project_folder}")
        ctx.print_variables_info()

        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
        )

        typer.echo("\n" + "=" * 50)
        typer.echo("DATABASE CONNECTION TEST")
        typer.echo("=" * 50)

        # Test connection
        if connection_manager.test_connection():
            typer.echo("✅ Database connection successful!")

            # Get database info
            db_info = connection_manager.get_database_info()
            if db_info:
                typer.echo("\nDatabase Information:")
                typer.echo(f"  Type: {db_info.get('database_type', 'Unknown')}")
                typer.echo(f"  Adapter: {db_info.get('adapter_type', 'Unknown')}")
                if "version" in db_info:
                    typer.echo(f"  Version: {db_info['version']}")
                if "host" in db_info:
                    typer.echo(f"  Host: {db_info['host']}")
                if "database" in db_info:
                    typer.echo(f"  Database: {db_info['database']}")
                if "warehouse" in db_info:
                    typer.echo(f"  Warehouse: {db_info['warehouse']}")
                if "role" in db_info:
                    typer.echo(f"  Role: {db_info['role']}")

            # Test supported materializations
            typer.echo("\nSupported Materializations:")
            materializations = connection_manager.get_supported_materializations()
            for mat in materializations:
                typer.echo(f"  - {mat}")

            typer.echo("\n✅ All connectivity tests passed!")

        else:
            typer.echo("❌ Database connection failed!", err=True)
            typer.echo("Please check your connection configuration in project.toml", err=True)

    except Exception as e:
        ctx.handle_error(e)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

