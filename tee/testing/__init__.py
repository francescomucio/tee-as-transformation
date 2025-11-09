"""
Tee Testing Framework

Provides automated testing capabilities for data quality validation.
"""

# Import standard tests to trigger registration
from . import standard_tests  # noqa: F401
from .base import StandardTest, TestRegistry, TestResult, TestSeverity
from .executor import TestExecutor
from .sql_test import SqlTest
from .test_discovery import TestDiscovery

__all__ = [
    "StandardTest",
    "TestSeverity",
    "TestResult",
    "TestRegistry",
    "TestExecutor",
    "SqlTest",
    "TestDiscovery",
]
