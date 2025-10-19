#!/usr/bin/env python3
"""
Debug script to check what the CLI is reading from project.toml
"""

import sys
from pathlib import Path

# Add the tee module to the path
sys.path.insert(0, str(Path(__file__).parent))

from tee.cli.main import load_project_config

def debug_project_config():
    """Debug project configuration loading."""
    try:
        project_folder = "t_project"
        print(f"Loading config from: {project_folder}")
        
        config = load_project_config(project_folder)
        print(f"✅ Configuration loaded successfully: {config}")
        
        # Check if project_folder is in the config
        if 'project_folder' in config:
            print(f"✅ project_folder found: {config['project_folder']}")
        else:
            print("❌ project_folder not found in config")
            print(f"Available keys: {list(config.keys())}")
        
        return True
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_project_config()
    sys.exit(0 if success else 1)
