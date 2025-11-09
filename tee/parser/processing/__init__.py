"""
Processing layer for variable substitution, file discovery, and model decorators.
"""

from .variable_substitution import substitute_sql_variables, validate_sql_variables
from .model_decorator import model
from .file_discovery import FileDiscovery
from .function_decorator import functions, FunctionDecoratorError

__all__ = [
    "substitute_sql_variables",
    "validate_sql_variables",
    "model",
    "FileDiscovery",
    "functions",
    "FunctionDecoratorError",
]
