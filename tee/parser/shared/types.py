"""
Common type definitions for the parser module.
"""

from pathlib import Path
from typing import Any, Union

# Core data structures
ParsedModel = dict[str, Any]
ParsedFunction = dict[str, Any]
DependencyGraph = dict[str, Any]
TableReference = str
FunctionReference = str

# File paths
FilePath = str | Path

# Connection configuration
ConnectionConfig = dict[str, Any]

# Variable substitution
Variables = dict[str, Any]


# Dependency information
DependencyInfo = dict[str, list[str]]

# Execution order
ExecutionOrder = list[str]

GraphCycles = list[list[str]]
