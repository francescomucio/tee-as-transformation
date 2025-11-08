"""
Tee Module

A SQL model execution framework with parsing and execution capabilities.
"""

from .parser import ProjectParser
from .engine import ModelExecutor, ExecutionEngine
from .executor import execute_models, parse_models_only, build_models
from .compiler import compile_project

# Import CLI
from .cli import main as cli_main

# Import new adapter system
from .adapters import DuckDBAdapter, SnowflakeAdapter, PostgreSQLAdapter, BigQueryAdapter

__all__ = [
    "ProjectParser",
    "ModelExecutor",
    "ExecutionEngine",
    "DuckDBAdapter",
    "SnowflakeAdapter",
    "PostgreSQLAdapter",
    "BigQueryAdapter",
    "execute_models",
    "parse_models_only",
    "build_models",
    "compile_project",
    "cli_main",
]
