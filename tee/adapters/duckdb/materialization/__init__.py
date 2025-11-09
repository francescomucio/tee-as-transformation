"""Materialization components for DuckDB."""

from .table_handler import TableHandler
from .view_handler import ViewHandler
from .incremental_handler import IncrementalHandler

__all__ = ["TableHandler", "ViewHandler", "IncrementalHandler"]


