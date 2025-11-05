"""
Test discovery for finding SQL test files in project's tests/ folder.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from .sql_test import SqlTest
from .base import TestSeverity, TestRegistry

logger = logging.getLogger(__name__)


class TestDiscovery:
    """Discovers SQL test files in the project's tests/ folder."""

    def __init__(self, project_folder: Path):
        """
        Initialize test discovery.

        Args:
            project_folder: Path to the project folder (should contain tests/ subfolder)
        """
        self.project_folder = Path(project_folder)
        self.tests_folder = self.project_folder / "tests"
        self._discovered_tests: Dict[str, SqlTest] = {}

    def discover_tests(self) -> Dict[str, SqlTest]:
        """
        Discover all SQL test files in the tests/ folder.

        Returns:
            Dictionary mapping test names to SqlTest instances

        Test names are derived from file names (without .sql extension).
        Example: tests/my_custom_test.sql -> test name "my_custom_test"
        """
        if self._discovered_tests:
            return self._discovered_tests

        if not self.tests_folder.exists():
            logger.debug(f"Tests folder not found: {self.tests_folder}")
            return {}

        discovered = {}

        # Find all .sql files in tests/ folder
        sql_files = list(self.tests_folder.rglob("*.sql"))

        for sql_file in sql_files:
            try:
                # Test name is the file name without extension and path
                # e.g., tests/my_test.sql -> "my_test"
                # e.g., tests/subfolder/test.sql -> "subfolder_test" or just "test"?
                # For now, use just the filename (without path components)
                test_name = sql_file.stem  # filename without extension

                # If there are duplicate names in subfolders, we could use path
                # For now, subfolder structure is not supported - use unique filenames
                if test_name in discovered:
                    logger.warning(
                        f"Duplicate test name '{test_name}' found. "
                        f"Using {sql_file} (previous: {discovered[test_name].sql_file_path})"
                    )

                # Create SqlTest instance
                sql_test = SqlTest(
                    name=test_name,
                    sql_file_path=sql_file,
                    project_folder=self.project_folder,
                    severity=TestSeverity.ERROR,
                )

                discovered[test_name] = sql_test
                logger.debug(f"Discovered SQL test: {test_name} from {sql_file}")

            except Exception as e:
                logger.error(f"Failed to load SQL test from {sql_file}: {e}")
                continue

        logger.info(f"Discovered {len(discovered)} SQL test(s) from {self.tests_folder}")
        self._discovered_tests = discovered
        return discovered

    def register_discovered_tests(self) -> None:
        """
        Discover and register all SQL tests with the TestRegistry.

        This should be called before test execution to ensure SQL tests
        are available in the registry.
        """
        tests = self.discover_tests()
        for test_name, sql_test in tests.items():
            TestRegistry.register(sql_test)
            logger.debug(f"Registered SQL test: {test_name}")
