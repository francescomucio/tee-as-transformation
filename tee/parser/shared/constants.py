"""
Constants for the parser module.
"""

# Default folder names
DEFAULT_MODELS_FOLDER = "models"
DEFAULT_OUTPUT_FOLDER = "output"

# Supported file extensions
SUPPORTED_SQL_EXTENSIONS = [".sql"]
SUPPORTED_PYTHON_EXTENSIONS = [".py"]

# SQL variable patterns
SQL_VARIABLE_PATTERNS = {
    "at_variable": r"@(\w+)",
    "jinja_variable": r"\{\{\s*(\w+(?:\.\w+)*)\s*\}\}",
    "jinja_with_default": r"\{\{\s*(\w+(?:\.\w+)*)\s*\|\s*default\s*\(\s*([^)]+)\s*\)\s*\}\}",
}

# Connection types
CONNECTION_TYPES = {
    "duckdb": "duckdb",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "sqlite": "sqlite",
}

# Graph visualization
GRAPH_COLORS = {
    "node": "#e1f5fe",
    "edge": "#01579b",
    "cycle": "#ff5722",
    "root": "#4caf50",
}

# Output file names
OUTPUT_FILES = {
    "parsed_models": "parsed_models.json",
    "dependency_graph": "dependency_graph.json",
    "mermaid_diagram": "dependency_graph.mmd",
    "markdown_report": "dependency_report.md",
}
