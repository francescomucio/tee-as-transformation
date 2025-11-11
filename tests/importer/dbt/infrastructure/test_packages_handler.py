"""
Tests for dbt packages handler.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from tee.importer.dbt.infrastructure import PackagesHandler


class TestPackagesHandler:
    """Tests for PackagesHandler."""

    def test_discover_packages_no_packages(self):
        """Test discovering packages when none exist."""
        handler = PackagesHandler(verbose=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            result = handler.discover_packages(project_path)
            assert result["has_packages"] is False
            assert result["packages"] == []

    def test_discover_packages_from_packages_yml(self):
        """Test discovering packages from packages.yml."""
        handler = PackagesHandler(verbose=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create packages.yml
            packages_file = project_path / "packages.yml"
            packages_data = {
                "packages": [
                    {"package": "dbt-utils", "version": "1.0.0"},
                    {"package": "dbt-date", "version": "0.7.0"},
                ]
            }
            with packages_file.open("w", encoding="utf-8") as f:
                yaml.dump(packages_data, f)

            result = handler.discover_packages(project_path)
            assert result["has_packages"] is True
            assert len(result["packages"]) == 2
            assert result["packages_file"] == str(packages_file)

    def test_discover_packages_from_dbt_packages_dir(self):
        """Test discovering installed packages from dbt_packages directory."""
        handler = PackagesHandler(verbose=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)

            # Create dbt_packages directory with some packages
            dbt_packages_dir = project_path / "dbt_packages"
            dbt_packages_dir.mkdir()
            (dbt_packages_dir / "dbt_utils").mkdir()
            (dbt_packages_dir / "dbt_date").mkdir()

            result = handler.discover_packages(project_path)
            assert result.get("has_installed_packages") is True
            assert "dbt_utils" in result.get("installed_packages", [])
            assert "dbt_date" in result.get("installed_packages", [])

    def test_get_packages_summary(self):
        """Test getting packages summary."""
        handler = PackagesHandler(verbose=False)

        packages_info = {
            "has_packages": True,
            "packages": [
                {"package": "dbt-utils", "version": "1.0.0"},
                {"package": "dbt-date", "version": "0.7.0"},
            ],
        }

        summary = handler.get_packages_summary(packages_info)
        assert "dbt Packages:" in summary
        assert "dbt-utils" in summary
        assert "1.0.0" in summary
        assert "dbt-date" in summary

    def test_get_packages_summary_no_packages(self):
        """Test getting summary when no packages."""
        handler = PackagesHandler(verbose=False)

        packages_info = {"has_packages": False}
        summary = handler.get_packages_summary(packages_info)
        assert "No dbt packages detected" in summary

