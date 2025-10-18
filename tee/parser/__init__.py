"""
Parser Module

A comprehensive tool for parsing SQL files and building dependency graphs.
Reorganized with clear layer separation for better maintainability.
"""

from .core import ProjectParser

# Backward compatibility - keep the main interface the same
__all__ = ['ProjectParser']

# Re-export key components for advanced usage
from .parsers import SQLParser, PythonParser, ParserFactory
from .analysis import DependencyGraphBuilder, TableResolver
from .processing import substitute_sql_variables, validate_sql_variables, model
from .output import DependencyVisualizer, JSONExporter, ReportGenerator
from .shared import (
    ParserError, SQLParsingError, PythonParsingError, DependencyError,
    VariableSubstitutionError, TableResolutionError, FileDiscoveryError,
    OutputGenerationError
)