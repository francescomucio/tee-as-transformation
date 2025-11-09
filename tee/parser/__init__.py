"""
Parser Module

A comprehensive tool for parsing SQL files and building dependency graphs.
Reorganized with clear layer separation for better maintainability.
"""

from .core import ProjectParser

# Backward compatibility - keep the main interface the same
__all__ = ["ProjectParser"]

# Re-export key components for advanced usage
from .analysis import DependencyGraphBuilder, TableResolver
from .output import DependencyVisualizer, JSONExporter, ReportGenerator
from .parsers import ParserFactory, PythonParser, SQLParser
from .processing import model, substitute_sql_variables, validate_sql_variables
from .shared import (
    DependencyError,
    FileDiscoveryError,
    OutputGenerationError,
    ParserError,
    PythonParsingError,
    SQLParsingError,
    TableResolutionError,
    VariableSubstitutionError,
)
