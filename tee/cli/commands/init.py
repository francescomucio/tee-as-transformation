"""
Init command implementation.
"""

import sys
import shutil
from pathlib import Path
from typing import Dict, Any

from tee.adapters import list_available_adapters, is_adapter_supported


def _get_default_connection_config(db_type: str, project_name: str) -> Dict[str, Any]:
    """
    Get default connection configuration for a database type.
    
    Args:
        db_type: Database type identifier
        project_name: Name of the project
        
    Returns:
        Dictionary with default connection configuration
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower == "duckdb":
        return {
            "type": "duckdb",
            "path": f"data/{project_name}.duckdb",
        }
    elif db_type_lower == "snowflake":
        return {
            "type": "snowflake",
            "host": "YOUR_ACCOUNT.snowflakecomputing.com",
            "user": "YOUR_USERNAME",
            "password": "YOUR_PASSWORD",
            "role": "YOUR_ROLE",
            "warehouse": "YOUR_WAREHOUSE",
            "database": "YOUR_DATABASE",
        }
    elif db_type_lower == "postgresql":
        return {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": project_name,
            "user": "postgres",
            "password": "postgres",
        }
    elif db_type_lower == "bigquery":
        return {
            "type": "bigquery",
            "project": "YOUR_PROJECT_ID",
            "database": project_name,  # dataset name
        }
    else:
        # Generic fallback
        return {
            "type": db_type_lower,
        }


def _generate_project_toml(project_name: str, db_type: str) -> str:
    """
    Generate project.toml content.
    
    Args:
        project_name: Name of the project
        db_type: Database type identifier
        
    Returns:
        String content for project.toml
    """
    connection_config = _get_default_connection_config(db_type, project_name)
    
    # Build connection section
    connection_lines = []
    for key, value in connection_config.items():
        if isinstance(value, str):
            connection_lines.append(f'{key} = "{value}"')
        else:
            connection_lines.append(f"{key} = {value}")
    
    connection_section = "\n".join(connection_lines)
    
    # Build the TOML content
    toml_content = f'''project_folder = "{project_name}"

[connection]
{connection_section}

[flags]
materialization_change_behavior = "warn"  # Options: "warn", "error", "ignore"
'''
    
    return toml_content


def cmd_init(
    project_name: str,
    database_type: str = "duckdb",
):
    """Execute the init command to initialize a new project."""
    db_type = database_type.lower()
    
    # Validate database type
    if not is_adapter_supported(db_type):
        available = ", ".join(sorted(list_available_adapters()))
        print(f"Error: Unsupported database type '{db_type}'")
        print(f"Supported database types: {available}")
        sys.exit(1)
    
    # Validate project name (basic validation)
    if not project_name or project_name.strip() != project_name:
        print("Error: Project name cannot be empty or contain leading/trailing whitespace")
        sys.exit(1)
    
    # Create project directory (resolve to absolute path for creation)
    project_path = Path(project_name).resolve()
    
    if project_path.exists():
        print(f"Error: Directory '{project_name}' already exists")
        sys.exit(1)
    
    # Create project directory (with race condition handling)
    try:
        project_path.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        # Directory was created between check and mkdir (race condition)
        print(f"Error: Directory '{project_name}' already exists")
        sys.exit(1)
    
    try:
        print(f"Created project directory: {project_name}/")
        
        # Create default directories
        directories = ["models", "tests", "seeds"]
        if db_type == "duckdb":
            directories.append("data")
        
        for dir_name in directories:
            dir_path = project_path / dir_name
            dir_path.mkdir()
            print(f"Created directory: {project_name}/{dir_name}/")
        
        # Generate and write project.toml
        toml_content = _generate_project_toml(project_name, db_type)
        toml_path = project_path / "project.toml"
        toml_path.write_text(toml_content, encoding="utf-8")
        print(f"Created configuration file: {project_name}/project.toml")
        
        print(f"\n✅ Project '{project_name}' initialized successfully!")
        print(f"\nNext steps:")
        print(f"  1. Edit {project_name}/project.toml to configure your database connection")
        print(f"  2. Add SQL models to {project_name}/models/")
        print(f"  3. Add seed files to {project_name}/seeds/")
        print(f"  4. Run: t4t run {project_name}")
        
    except OSError as e:
        # Handle filesystem errors (permissions, disk full, etc.)
        if project_path.exists():
            shutil.rmtree(project_path)
        print(f"Error: Failed to create project directory: {e}")
        sys.exit(1)
    except Exception as e:
        # Cleanup on error
        if project_path.exists():
            shutil.rmtree(project_path)
        print(f"Error: Failed to initialize project: {e}")
        sys.exit(1)

