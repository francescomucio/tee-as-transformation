# Handle dbt Source Metadata in t4t

## Description

Design and implement a way to preserve and handle dbt source metadata (descriptions, tags, freshness configurations, etc.) in t4t projects.

## Motivation

dbt projects define sources with rich metadata (descriptions, tags, freshness configs, etc.) that provide important documentation about data sources. Currently, the dbt importer ignores this metadata. We should preserve it in some form for documentation and governance purposes.

## Current State

- dbt sources are converted to simple table references (`schema.table`)
- Source metadata is logged but not preserved
- Source freshness tests are skipped (see issue #01)

## Proposed Solutions

### Option 1: Documentation File
Create a `SOURCES.md` or `sources.json` file that preserves all source metadata for reference.

### Option 2: Comments in SQL
Add comments to SQL files where sources are used, referencing the source documentation.

### Option 3: Metadata System
Create a t4t-native source metadata system (similar to dbt's sources concept).

### Option 4: OTS Module Metadata
Store source metadata in OTS module metadata fields.

## Questions to Answer

- Do we need a t4t-native "sources" concept, or is documentation sufficient?
- Should source metadata be queryable/usable at runtime, or just for documentation?
- How should source metadata integrate with t4t's existing metadata system?

## Related

- Part of dbt importer feature
- Related to metadata and documentation systems

## Acceptance Criteria

- [ ] Decision made on approach (documentation vs. native system)
- [ ] Source metadata is preserved during dbt import
- [ ] Source metadata is accessible/usable in t4t
- [ ] Documentation updated
- [ ] dbt importer updated to preserve source metadata

