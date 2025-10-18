#!/usr/bin/env python3
"""
Build documentation using MkDocs.

This script provides convenient commands for building and serving the documentation.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"Error: {e.stderr}")
        sys.exit(1)


def build_docs():
    """Build the documentation."""
    run_command("mkdocs build", "Building documentation")


def serve_docs():
    """Serve the documentation locally."""
    print("🚀 Starting documentation server...")
    print("📖 Documentation will be available at: http://127.0.0.1:8000")
    print("🛑 Press Ctrl+C to stop the server")
    run_command("mkdocs serve", "Serving documentation")


def clean_docs():
    """Clean the documentation build directory."""
    site_dir = Path("site")
    if site_dir.exists():
        import shutil
        shutil.rmtree(site_dir)
        print("✅ Cleaned documentation build directory")
    else:
        print("ℹ️  No build directory to clean")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python build_docs.py <command>")
        print("Commands:")
        print("  build  - Build the documentation")
        print("  serve  - Serve the documentation locally")
        print("  clean  - Clean the build directory")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "build":
        build_docs()
    elif command == "serve":
        serve_docs()
    elif command == "clean":
        clean_docs()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
