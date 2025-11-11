"""
dbt project importer.

Handles importing dbt projects into t4t format.
"""

from tee.importer.dbt.importer import import_dbt_project

__all__ = ["import_dbt_project"]
