# Add Jinja2 Template Rendering Support to t4t

## Summary

Currently, t4t supports Jinja-style variable substitution (`{{ variable }}`) using regex-based parsing, but does not support full Jinja2 templating features like:
- Control structures: `{% if %}`, `{% for %}`, `{% macro %}`
- Filters beyond `default()`
- Complex expressions and logic

This issue is needed to support the `--keep-jinja` flag in the dbt importer, which allows importing dbt projects while preserving Jinja2 templates for gradual migration.

## Background

The dbt importer now supports a `--keep-jinja` flag that:
- Converts `ref()` and `source()` calls to qualified table names
- Converts incremental configs to t4t format
- Preserves all other Jinja2 templates in the SQL

However, for these models to actually execute, t4t needs to support Jinja2 template rendering at runtime.

## Requirements

1. **Add Jinja2 as a dependency**
   - Add `jinja2` to `pyproject.toml` dependencies

2. **Add Jinja2 rendering to SQL processing pipeline**
   - Integrate Jinja2 template rendering into the SQL execution flow
   - Support rendering Jinja2 templates before SQL execution
   - Handle variables injection into Jinja2 context

3. **Support dbt-like functions in Jinja2 context**
   - `ref()` - should resolve to qualified table names (already converted by importer)
   - `source()` - should resolve to qualified table names (already converted by importer)
   - `var()` - should use t4t's variable system
   - `config()` - should use t4t's metadata system

4. **Error handling**
   - Clear error messages when Jinja2 templates fail to render
   - Support for undefined variables (with defaults)
   - Template syntax validation

5. **Documentation**
   - Document Jinja2 support in user guide
   - Examples of using Jinja2 in t4t models
   - Migration guide from dbt Jinja2 to t4t Jinja2

## Implementation Notes

- The dbt importer already converts `ref()` and `source()` to qualified names, so the Jinja2 context doesn't need to implement these functions
- Variables should be injected from t4t's `--vars` flag
- Consider caching rendered templates for performance
- Ensure Jinja2 rendering happens before SQL dialect conversion

## Related

- dbt importer `--keep-jinja` flag (Phase 2)
- Variable substitution system (`tee.parser.processing.variable_substitution`)

## Priority

Medium - Needed for full dbt import workflow with gradual migration support

