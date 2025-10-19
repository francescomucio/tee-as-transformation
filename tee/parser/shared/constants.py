"""
Constants for the parser module.
"""

# Default folder names
DEFAULT_MODELS_FOLDER = "models"

# Supported file extensions
SUPPORTED_SQL_EXTENSIONS = [".sql"]
SUPPORTED_PYTHON_EXTENSIONS = [".py"]

# SQL variable patterns
SQL_VARIABLE_PATTERNS = {
    "at_variable": r"@(\w+)",
    "jinja_variable": r"\{\{\s*(\w+(?:\.\w+)*)\s*\}\}",
    "jinja_with_default": r"\{\{\s*(\w+(?:\.\w+)*)\s*\|\s*default\s*\(\s*([^)]+)\s*\)\s*\}\}",
}



# Output file names
OUTPUT_FILES = {
    "parsed_models": "parsed_models.json",
    "dependency_graph": "dependency_graph.json",
    "mermaid_diagram": "dependency_graph.mmd",
    "markdown_report": "dependency_report.md",
}
