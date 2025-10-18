"""
Common type definitions for the parser module.
"""

from typing import Dict, Any, List, Optional, Union
from pathlib import Path

# Core data structures
ParsedModel = Dict[str, Any]
DependencyGraph = Dict[str, Any]
TableReference = str

# File paths
FilePath = Union[str, Path]

# Connection configuration
ConnectionConfig = Dict[str, Any]

# Variable substitution
Variables = Dict[str, Any]

# Parsed SQL data
SQLData = Dict[str, Any]

# Python model metadata
PythonModelMetadata = Dict[str, Any]

# Dependency information
DependencyInfo = Dict[str, List[str]]

# Execution order
ExecutionOrder = List[str]

# Graph components
GraphNodes = List[str]
GraphEdges = List[tuple[str, str]]
GraphCycles = List[List[str]]
