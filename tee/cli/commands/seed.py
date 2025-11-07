"""
Seed command implementation.
"""

from typing import Optional
from pathlib import Path
from tee.cli.context import CommandContext
from tee.engine.seeds import SeedDiscovery, SeedLoader
from tee.engine.execution_engine import ExecutionEngine


def cmd_seed(
    project_folder: str,
    vars: Optional[str] = None,
    verbose: bool = False,
):
    """Execute the seed command to load seed files into the database."""
    ctx = CommandContext(
        project_folder=project_folder,
        vars=vars,
        verbose=verbose,
    )
    
    try:
        print(f"Loading seeds from project: {project_folder}")
        
        # Get seeds folder
        seeds_folder = ctx.project_path / "seeds"
        
        if not seeds_folder.exists():
            print(f"⚠️  Seeds folder not found: {seeds_folder}")
            print("   Create a 'seeds' folder in your project to use this feature.")
            return
        
        # Discover seed files
        seed_discovery = SeedDiscovery(seeds_folder)
        seed_files = seed_discovery.discover_seed_files()
        
        if not seed_files:
            print("ℹ️  No seed files found in seeds folder")
            print(f"   Supported formats: CSV, TSV, JSON")
            print(f"   Example: seeds/users.csv or seeds/my_schema/orders.json")
            return
        
        print(f"\nFound {len(seed_files)} seed file(s):")
        for file_path, schema_name in seed_files:
            if schema_name:
                print(f"  - {file_path.relative_to(seeds_folder)} → {schema_name}.{file_path.stem}")
            else:
                print(f"  - {file_path.relative_to(seeds_folder)} → {file_path.stem}")
        
        # Create execution engine to get adapter
        execution_engine = ExecutionEngine(
            ctx.config["connection"],
            project_folder=str(ctx.project_path),
            variables=ctx.vars
        )
        
        try:
            # Connect to database
            execution_engine.connect()
            print(f"\nConnected to database: {execution_engine.adapter.config.type}")
            
            # Load seeds
            print("\nLoading seeds...")
            seed_loader = SeedLoader(execution_engine.adapter)
            seed_results = seed_loader.load_all_seeds(seed_files)
            
            # Print results
            print("\n" + "=" * 50)
            print("SEED LOADING RESULTS")
            print("=" * 50)
            
            if seed_results["loaded_tables"]:
                print(f"\n✅ Successfully loaded {len(seed_results['loaded_tables'])} seed(s):")
                for table in seed_results["loaded_tables"]:
                    # Get table info to show row count
                    try:
                        table_info = execution_engine.adapter.get_table_info(table)
                        row_count = table_info.get("row_count", 0)
                        print(f"  - {table}: {row_count} rows")
                    except Exception as e:
                        print(f"  - {table} (could not get row count: {e})")
            
            if seed_results["failed_tables"]:
                print(f"\n❌ Failed to load {len(seed_results['failed_tables'])} seed(s):")
                for failure in seed_results["failed_tables"]:
                    print(f"  - {failure['file']}: {failure['error']}")
            
            if not seed_results["failed_tables"]:
                print("\n✅ All seeds loaded successfully!")
            
        finally:
            execution_engine.disconnect()
            
    except Exception as e:
        ctx.handle_error(e)

