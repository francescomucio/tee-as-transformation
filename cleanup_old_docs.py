#!/usr/bin/env python3
"""
Clean up old documentation files that have been migrated to the docs/ folder.

This script helps organize the project by removing scattered documentation files
that have been properly organized in the docs/ folder.
"""

import os
from pathlib import Path


def cleanup_old_docs():
    """Remove old documentation files that have been migrated."""
    
    # Files to remove (they've been migrated to docs/)
    files_to_remove = [
        "EXECUTION_ENGINE_README.md",
        "ADAPTER_SYSTEM_SUMMARY.md", 
        "ADAPTER_REORGANIZATION_SUMMARY.md",
        "ENGINE_CLEANUP_SUMMARY.md",
        "METADATA_EXAMPLE.md",
        "parser_reorganization_diagram.md",
        "example_report.md"
    ]
    
    # Directories to clean up
    dirs_to_clean = [
        "tee/engine/README_ADAPTERS.md"  # This is now in docs/user-guide/database-adapters.md
    ]
    
    removed_files = []
    failed_removals = []
    
    # Remove files
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                removed_files.append(file_path)
                print(f"✅ Removed: {file_path}")
            except Exception as e:
                failed_removals.append((file_path, str(e)))
                print(f"❌ Failed to remove {file_path}: {e}")
        else:
            print(f"ℹ️  File not found: {file_path}")
    
    # Remove files in subdirectories
    for file_path in dirs_to_clean:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                removed_files.append(file_path)
                print(f"✅ Removed: {file_path}")
            except Exception as e:
                failed_removals.append((file_path, str(e)))
                print(f"❌ Failed to remove {file_path}: {e}")
        else:
            print(f"ℹ️  File not found: {file_path}")
    
    # Summary
    print(f"\n📊 Cleanup Summary:")
    print(f"  ✅ Removed {len(removed_files)} files")
    print(f"  ❌ Failed to remove {len(failed_removals)} files")
    
    if failed_removals:
        print(f"\n❌ Failed removals:")
        for file_path, error in failed_removals:
            print(f"  - {file_path}: {error}")
    
    if removed_files:
        print(f"\n✅ Successfully removed:")
        for file_path in removed_files:
            print(f"  - {file_path}")
    
    print(f"\n📚 Documentation is now organized in the docs/ folder")
    print(f"🌐 Build documentation with: uv run python docs/build_docs.py build")
    print(f"🚀 Serve documentation with: uv run python docs/build_docs.py serve")


if __name__ == "__main__":
    cleanup_old_docs()
