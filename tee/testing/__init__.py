"""
Tee Testing Framework

Provides automated testing capabilities for data quality validation.
"""

from .base import StandardTest, TestSeverity, TestResult, TestRegistry
from .executor import TestExecutor
from .sql_test import SqlTest
from .test_discovery import TestDiscovery

# Import standard tests to trigger registration
from . import standard_tests  # noqa: F401

__all__ = [
    "StandardTest",
    "TestSeverity",
    "TestResult",
    "TestRegistry",
    "TestExecutor",
    "SqlTest",
    "TestDiscovery",
]
