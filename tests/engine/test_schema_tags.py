"""
Test cases for schema-level tag functionality in execution engine.
"""

import pytest
from tee.engine.execution_engine import ExecutionEngine


class TestSchemaTagExtraction:
    """Test schema-level tag extraction from project config."""

    def test_load_per_schema_tags(self, temp_project_dir):
        """Test loading tags from per-schema configuration."""
        # Create temporary project.toml
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"

[schemas.my_schema]
tags = ["analytics", "production"]
object_tags = {"sensitivity_tag" = "pii", "classification" = "public"}
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema_metadata = engine._load_schema_metadata("my_schema")
        assert schema_metadata is not None
        assert "tags" in schema_metadata
        assert schema_metadata["tags"] == ["analytics", "production"]
        assert "object_tags" in schema_metadata
        assert schema_metadata["object_tags"]["sensitivity_tag"] == "pii"
        assert schema_metadata["object_tags"]["classification"] == "public"

    def test_load_module_level_tags_as_fallback(self, temp_project_dir):
        """Test that module-level tags are used as fallback when no per-schema config."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"

[module]
tags = ["analytics", "production"]
object_tags = {"sensitivity_tag" = "pii"}
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema_metadata = engine._load_schema_metadata("my_schema")
        assert schema_metadata is not None
        assert "tags" in schema_metadata
        assert schema_metadata["tags"] == ["analytics", "production"]
        assert "object_tags" in schema_metadata
        assert schema_metadata["object_tags"]["sensitivity_tag"] == "pii"

    def test_load_root_level_tags_as_fallback(self, temp_project_dir):
        """Test that root-level tags are used as fallback when no module or per-schema config."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"
tags = ["analytics", "production"]

[connection]
type = "duckdb"
path = ":memory:"
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema_metadata = engine._load_schema_metadata("my_schema")
        assert schema_metadata is not None
        assert "tags" in schema_metadata
        assert schema_metadata["tags"] == ["analytics", "production"]

    def test_per_schema_overrides_module_level(self, temp_project_dir):
        """Test that per-schema tags override module-level tags."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"

[module]
tags = ["module_tag"]

[schemas.my_schema]
tags = ["schema_tag"]
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema_metadata = engine._load_schema_metadata("my_schema")
        assert schema_metadata is not None
        assert schema_metadata["tags"] == ["schema_tag"]  # Per-schema overrides

    def test_extract_schema_name_from_table_name(self, temp_project_dir):
        """Test extraction of schema name from table name."""
        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        assert engine._extract_schema_name("my_schema.table_name") == "my_schema"
        assert engine._extract_schema_name("schema.table") == "schema"
        assert engine._extract_schema_name("table_without_schema") is None

    def test_schema_tags_not_processed_twice(self, temp_project_dir):
        """Test that schema tags are only processed once per schema."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"

[schemas.my_schema]
tags = ["analytics"]
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        # First call should process
        engine._attach_schema_tags_if_needed("my_schema")
        assert "my_schema" in engine._processed_schemas

        # Second call should be skipped
        initial_count = len(engine._processed_schemas)
        engine._attach_schema_tags_if_needed("my_schema")
        assert len(engine._processed_schemas) == initial_count

    def test_no_schema_tags_returns_none(self, temp_project_dir):
        """Test that None is returned when no schema tags are configured."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema_metadata = engine._load_schema_metadata("my_schema")
        assert schema_metadata is None

    def test_multiple_schemas_with_different_tags(self, temp_project_dir):
        """Test that different schemas can have different tags."""
        project_toml = temp_project_dir / "project.toml"
        project_toml.write_text("""
project_folder = "test_project"

[connection]
type = "duckdb"
path = ":memory:"

[schemas.schema1]
tags = ["analytics", "production"]

[schemas.schema2]
tags = ["staging", "test"]
object_tags = {"classification" = "internal"}
""")

        engine = ExecutionEngine(
            config={"type": "duckdb", "path": ":memory:"},
            project_folder=str(temp_project_dir),
        )

        schema1_metadata = engine._load_schema_metadata("schema1")
        schema2_metadata = engine._load_schema_metadata("schema2")

        assert schema1_metadata["tags"] == ["analytics", "production"]
        assert schema2_metadata["tags"] == ["staging", "test"]
        assert "object_tags" in schema2_metadata
        assert schema2_metadata["object_tags"]["classification"] == "internal"

