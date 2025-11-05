"""
Debug command implementation.
"""

from ..context import CommandContext
from ...engine.connection_manager import ConnectionManager


def cmd_debug(args):
    """Execute the debug command to test database connectivity."""
    ctx = CommandContext(args)
    connection_manager = None
    
    try:
        print(f"Testing database connectivity for project: {args.project_folder}")
        ctx.print_variables_info()

        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=str(ctx.project_path),
            connection_config=ctx.config["connection"],
            variables=ctx.vars,
        )

        print("\n" + "=" * 50)
        print("DATABASE CONNECTION TEST")
        print("=" * 50)

        # Test connection
        if connection_manager.test_connection():
            print("✅ Database connection successful!")

            # Get database info
            db_info = connection_manager.get_database_info()
            if db_info:
                print("\nDatabase Information:")
                print(f"  Type: {db_info.get('database_type', 'Unknown')}")
                print(f"  Adapter: {db_info.get('adapter_type', 'Unknown')}")
                if "version" in db_info:
                    print(f"  Version: {db_info['version']}")
                if "host" in db_info:
                    print(f"  Host: {db_info['host']}")
                if "database" in db_info:
                    print(f"  Database: {db_info['database']}")
                if "warehouse" in db_info:
                    print(f"  Warehouse: {db_info['warehouse']}")
                if "role" in db_info:
                    print(f"  Role: {db_info['role']}")

            # Test supported materializations
            print("\nSupported Materializations:")
            materializations = connection_manager.get_supported_materializations()
            for mat in materializations:
                print(f"  - {mat}")

            print("\n✅ All connectivity tests passed!")

        else:
            print("❌ Database connection failed!")
            print("Please check your connection configuration in project.toml")

    except Exception as e:
        ctx.handle_error(e)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()

