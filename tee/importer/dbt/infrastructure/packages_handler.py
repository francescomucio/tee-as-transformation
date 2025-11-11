"""
dbt packages handler for importer.

Detects and documents dbt package dependencies.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

from tee.importer.dbt.constants import PACKAGES_FILE
from tee.importer.dbt.exceptions import DbtImporterError

logger = logging.getLogger(__name__)


class PackagesHandler:
    """Handles dbt package dependencies."""

    def __init__(self, verbose: bool = False) -> None:
        """
        Initialize packages handler.

        Args:
            verbose: Enable verbose logging
        """
        self.verbose = verbose

    def discover_packages(self, project_path: Path) -> dict[str, Any]:
        """
        Discover dbt packages from packages.yml or dbt_packages directory.

        Args:
            project_path: Path to dbt project root

        Returns:
            Dictionary with package information
        """
        packages_info: dict[str, Any] = {
            "packages": [],
            "has_packages": False,
            "packages_file": None,
            "warnings": [],
        }

        # Check for packages.yml
        packages_file = project_path / PACKAGES_FILE
        if packages_file.exists():
            packages_info["packages_file"] = str(packages_file)
            try:
                with packages_file.open("r", encoding="utf-8") as f:
                    content = yaml.safe_load(f)

                if isinstance(content, dict) and "packages" in content:
                    packages_list = content["packages"]
                    if isinstance(packages_list, list):
                        packages_info["packages"] = packages_list
                        packages_info["has_packages"] = len(packages_list) > 0

                        if self.verbose:
                            logger.info(f"Found {len(packages_list)} package(s) in {PACKAGES_FILE}")

                        # Extract package names and versions
                        for pkg in packages_list:
                            if isinstance(pkg, dict):
                                if "package" in pkg:
                                    pkg_name = pkg["package"]
                                    pkg_version = pkg.get("version", "latest")
                                    if self.verbose:
                                        logger.info(f"  - {pkg_name} (version: {pkg_version})")

            except Exception as e:
                error_msg = f"Error parsing {PACKAGES_FILE}: {e}"
                packages_info["warnings"].append(error_msg)
                logger.warning(error_msg)

        # Check for dbt_packages directory (installed packages)
        dbt_packages_dir = project_path / "dbt_packages"
        if dbt_packages_dir.exists():
            packages_info["has_installed_packages"] = True
            if self.verbose:
                logger.info(f"Found dbt_packages directory at: {dbt_packages_dir}")

            # Try to detect installed packages
            installed_packages = []
            for item in dbt_packages_dir.iterdir():
                if item.is_dir() and not item.name.startswith("."):
                    installed_packages.append(item.name)

            if installed_packages:
                packages_info["installed_packages"] = installed_packages
                if self.verbose:
                    logger.info(f"Detected {len(installed_packages)} installed package(s)")

        if packages_info["has_packages"]:
            packages_info["warnings"].append(
                "This project uses dbt packages. Package macros and models may need to be "
                "manually converted or inlined. Check the import report for details."
            )

        return packages_info

    def get_packages_summary(self, packages_info: dict[str, Any]) -> str:
        """
        Get human-readable summary of packages.

        Args:
            packages_info: Packages information dictionary

        Returns:
            Summary string
        """
        if not packages_info.get("has_packages"):
            return "No dbt packages detected."

        packages = packages_info.get("packages", [])
        if not packages:
            return "dbt packages detected but no package list found."

        lines = ["dbt Packages:"]
        for pkg in packages:
            if isinstance(pkg, dict):
                pkg_name = pkg.get("package", "unknown")
                pkg_version = pkg.get("version", "latest")
                lines.append(f"  - {pkg_name} (version: {pkg_version})")

        return "\n".join(lines)
