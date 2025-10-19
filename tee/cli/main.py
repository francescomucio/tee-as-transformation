"""
Tee CLI Main Module

Command-line interface for the Tee SQL model execution framework.
"""

import argparse
import logging
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional

from .. import execute_models, parse_models_only
from ..engine.connection_manager import ConnectionManager


def parse_vars(vars_string: str) -> Dict[str, Any]:
    """
    Parse variables string into a dictionary.
    
    Supports both JSON format and key=value format.
    
    Args:
        vars_string: Variables string in JSON or key=value format
        
    Returns:
        Dictionary containing parsed variables
    """
    if not vars_string:
        return {}
    
    # Try JSON format first
    try:
        return json.loads(vars_string)
    except json.JSONDecodeError:
        pass
    
    # Try key=value format
    try:
        vars_dict = {}
        for pair in vars_string.split(','):
            if '=' not in pair:
                raise ValueError(f"Invalid variable format: {pair}")
            key, value = pair.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Try to parse as JSON value (for numbers, booleans, etc.)
            try:
                vars_dict[key] = json.loads(value)
            except json.JSONDecodeError:
                # Treat as string if not valid JSON
                vars_dict[key] = value
        
        return vars_dict
    except Exception as e:
        raise ValueError(f"Invalid variables format: {e}")


def load_project_config(project_folder: str, vars_dict: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Load project configuration from project.toml file and merge with variables.
    
    Args:
        project_folder: Path to the project folder
        vars_dict: Optional dictionary of variables to merge into config
        
    Returns:
        Dictionary containing project configuration with variables merged
    """
    project_toml_path = Path(project_folder) / "project.toml"
    
    if not project_toml_path.exists():
        raise FileNotFoundError(f"project.toml not found in {project_folder}")
    
    try:
        import tomllib
        with open(project_toml_path, 'rb') as f:
            config = tomllib.load(f)
    except ImportError:
        # Fallback for Python < 3.11
        try:
            import toml
            with open(project_toml_path, 'r') as f:
                config = toml.load(f)
        except ImportError:
            raise ImportError("toml parsing requires tomllib (Python 3.11+) or toml package")
    
    # Validate required configuration
    if 'project_folder' not in config:
        raise ValueError("project.toml must contain 'project_folder' setting")
    
    if 'connection' not in config:
        raise ValueError("project.toml must contain 'connection' configuration")
    
    # Merge variables into config
    if vars_dict:
        config['vars'] = vars_dict
    
    return config


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(levelname)s - %(name)s - %(message)s'
    )


def cmd_run(args):
    """Execute the run command."""
    connection_manager = None
    try:
        # Parse variables if provided
        vars_dict = {}
        if hasattr(args, 'vars') and args.vars:
            vars_dict = parse_vars(args.vars)
            if args.verbose:
                print(f"Variables: {vars_dict}")
        
        # Load project configuration
        config = load_project_config(args.project_folder, vars_dict)
        
        # Set up logging
        setup_logging(args.verbose)
        
        print(f"Running tee on project: {args.project_folder}")
        if vars_dict:
            print(f"Using variables: {vars_dict}")
        
        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=config["project_folder"],
            connection_config=config["connection"],
            variables=vars_dict
        )
        
        # Execute models using the unified connection manager
        results = execute_models(
            project_folder=config["project_folder"],
            connection_config=config["connection"],
            save_analysis=True,
            variables=vars_dict
        )
        
        # Calculate statistics
        total_tables = len(results['executed_tables']) + len(results['failed_tables'])
        successful_count = len(results['executed_tables'])
        failed_count = len(results['failed_tables'])
        warning_count = len(results.get('warnings', []))
        
        print(f"\nCompleted! Executed {successful_count} out of {total_tables} tables successfully.")
        if failed_count > 0 or warning_count > 0:
            print(f"  ✅ Successful: {successful_count} tables")
            if failed_count > 0:
                print(f"  ❌ Failed: {failed_count} tables")
            if warning_count > 0:
                print(f"  ⚠️  Warnings: {warning_count} warnings")
        else:
            print(f"  ✅ All {successful_count} tables executed successfully!")
        
        if args.verbose:
            print(f"Analysis info: {results.get('analysis', {})}")
            
    except Exception as e:
        print(f"Error during execution: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup
        if connection_manager:
            connection_manager.cleanup()


def cmd_parse(args):
    """Execute the parse command."""
    try:
        # Parse variables if provided
        vars_dict = {}
        if hasattr(args, 'vars') and args.vars:
            vars_dict = parse_vars(args.vars)
            if args.verbose:
                print(f"Variables: {vars_dict}")
        
        # Load project configuration
        config = load_project_config(args.project_folder, vars_dict)
        
        # Set up logging
        setup_logging(args.verbose)
        
        print(f"Parsing models in project: {args.project_folder}")
        if vars_dict:
            print(f"Using variables: {vars_dict}")
        
        # Parse models only
        analysis = parse_models_only(
            project_folder=config["project_folder"],
            connection_config=config["connection"],
            variables=vars_dict
        )
        
        print(f"\nAnalysis complete! Found {analysis['total_tables']} tables.")
        print(f"Execution order: {' -> '.join(analysis['execution_order'])}")
        
        if args.verbose:
            print(f"Full analysis: {analysis}")
            
    except Exception as e:
        print(f"Error during parsing: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def cmd_debug(args):
    """Execute the debug command to test database connectivity."""
    try:
        # Parse variables if provided
        vars_dict = {}
        if hasattr(args, 'vars') and args.vars:
            vars_dict = parse_vars(args.vars)
            if args.verbose:
                print(f"Variables: {vars_dict}")
        
        # Load project configuration
        config = load_project_config(args.project_folder, vars_dict)
        
        # Set up logging
        setup_logging(args.verbose)
        
        print(f"Testing database connectivity for project: {args.project_folder}")
        if vars_dict:
            print(f"Using variables: {vars_dict}")
        
        # Create unified connection manager
        connection_manager = ConnectionManager(
            project_folder=config["project_folder"],
            connection_config=config["connection"],
            variables=vars_dict
        )
        
        print("\n" + "="*50)
        print("DATABASE CONNECTION TEST")
        print("="*50)
        
        # Test connection
        if connection_manager.test_connection():
            print("✅ Database connection successful!")
            
            # Get database info
            db_info = connection_manager.get_database_info()
            if db_info:
                print(f"\nDatabase Information:")
                print(f"  Type: {db_info.get('database_type', 'Unknown')}")
                print(f"  Adapter: {db_info.get('adapter_type', 'Unknown')}")
                if 'version' in db_info:
                    print(f"  Version: {db_info['version']}")
                if 'host' in db_info:
                    print(f"  Host: {db_info['host']}")
                if 'database' in db_info:
                    print(f"  Database: {db_info['database']}")
                if 'warehouse' in db_info:
                    print(f"  Warehouse: {db_info['warehouse']}")
                if 'role' in db_info:
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
        print(f"❌ Error during connectivity test: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup
        if 'connection_manager' in locals():
            connection_manager.cleanup()


def cmd_help(args):
    """Show help information."""
    parser.print_help()


def main():
    """Main CLI entry point."""
    global parser
    
    parser = argparse.ArgumentParser(
        description="Tee - SQL Model Execution Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  tee run ./my_project                    # Run models in ./my_project
  tee parse ./my_project                  # Parse models in ./my_project
  tee debug ./my_project                  # Test database connectivity
  tee run ./my_project -v                 # Run with verbose output
  tee run ./my_project --vars '{"env": "prod"}'  # Run with variables (JSON)
  tee run ./my_project --vars 'env=prod,debug=true'  # Run with variables (key=value)
  tee help                                # Show this help message
        """
    )
    
    # Add subcommands
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        required=True
    )
    
    # Run command
    run_parser = subparsers.add_parser(
        'run',
        help='Parse and execute SQL models'
    )
    run_parser.add_argument(
        'project_folder',
        help='Path to the project folder containing project.toml'
    )
    run_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    run_parser.add_argument(
        '--vars',
        type=str,
        help='Variables to pass to models (JSON format or key=value,key2=value2)'
    )
    run_parser.set_defaults(func=cmd_run)
    
    # Parse command
    parse_parser = subparsers.add_parser(
        'parse',
        help='Parse SQL models and store metadata (no execution)'
    )
    parse_parser.add_argument(
        'project_folder',
        help='Path to the project folder containing project.toml'
    )
    parse_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parse_parser.add_argument(
        '--vars',
        type=str,
        help='Variables to pass to models (JSON format or key=value,key2=value2)'
    )
    parse_parser.set_defaults(func=cmd_parse)
    
    # Debug command
    debug_parser = subparsers.add_parser(
        'debug',
        help='Test database connectivity and configuration'
    )
    debug_parser.add_argument(
        'project_folder',
        help='Path to the project folder containing project.toml'
    )
    debug_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    debug_parser.add_argument(
        '--vars',
        type=str,
        help='Variables to pass to models (JSON format or key=value,key2=value2)'
    )
    debug_parser.set_defaults(func=cmd_debug)
    
    # Help command
    help_parser = subparsers.add_parser(
        'help',
        help='Show help information'
    )
    help_parser.set_defaults(func=cmd_help)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute the appropriate command
    args.func(args)


if __name__ == "__main__":
    main()
