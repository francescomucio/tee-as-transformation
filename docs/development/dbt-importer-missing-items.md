# dbt Importer - Missing Items

## ✅ Completed Phases

- ✅ Phase 1: Core Infrastructure
- ✅ Phase 2: Model Conversion (Core)
- ✅ Phase 3: Advanced Features (Macros, Variables, Reports)
- ✅ Phase 4: Tests
- ✅ Phase 5: Configuration and Project Setup
- ✅ Phase 6: Validation and Reporting
- ✅ Phase 7: OTS Format Support

## 📝 Missing Documentation

### 1. CLI Reference Documentation
**Status**: ✅ Complete
**Location**: `docs/user-guide/cli-reference.md`
**Completed**: All CLI options documented with examples

### 2. User Guide for dbt Import
**Status**: ✅ Complete
**Location**: `docs/user-guide/dbt-import.md`
**Completed**: Comprehensive guide with step-by-step instructions, troubleshooting, and best practices

### 3. Limitations and Known Issues Documentation
**Status**: ✅ Complete
**Location**: `docs/user-guide/dbt-import-limitations.md`
**Completed**: Comprehensive list of unsupported features, limitations, and workarounds

## 🔧 Missing Features / Future Enhancements

### 1. Variables Conversion
**Status**: ✅ Fully Implemented
**Current State**: Variables are automatically converted in SQL models and work in Python models
**What's Actually Implemented**:
- ✅ Automatic conversion of dbt variables to t4t variables in SQL models:
  - `{{ var('name') }}` → `@name`
  - `{{ var('name', 'default') }}` → `@name:default`
- ✅ Variables work in Python models (injected into namespace by t4t)
- ✅ Variables are extracted and documented in import report

**Note**: Variables are NOT converted only when `--keep-jinja` flag is used (intentional - preserves Jinja templates). This requires Jinja2 support in t4t (coming soon).

### 2. Package Macro/Model Inlining
**Status**: ❌ Not Implemented
**Current State**: Packages are detected and documented only
**What's Missing**:
- Automatic inlining of package macros into UDFs
- Automatic inlining of package models
- Package dependency resolution

**Note**: This is complex and would require parsing package code. Current implementation detects and documents packages.

### 3. Custom `generate_schema_name` Macro Support
**Status**: ❌ Not Supported
**Current State**: Uses standard dbt schema resolution logic
**What's Missing**:
- Support for custom `generate_schema_name` macros
- Execution of dbt macros during import

**Note**: Supporting custom macros would require executing dbt macros, which is complex.

### 4. Dependency Resolution (`model+`, `+model`)
**Status**: ❌ Not Implemented
**Current State**: Basic name/tag-based selection only
**What's Missing**:
- Dependency graph traversal
- `model+` (model and all downstream)
- `+model` (model and all upstream)
- Pattern parsing for dependency selection

**Note**: GitHub Issue #8 created with analysis. Complexity: Medium (3-4 days).

### 5. Source Metadata Handling
**Status**: ⚠️ Partially Implemented
**Current State**: Source references converted to table names, metadata logged but not preserved
**What's Missing**:
- Design for handling dbt source metadata in t4t
- Preservation of source metadata
- Source freshness configuration

**Note**: GitHub issue created. Source metadata is logged but not yet integrated into t4t.

### 6. Freshness Tests
**Status**: ❌ Not Supported
**Current State**: Freshness tests are skipped with warnings
**What's Missing**:
- Freshness/recency tests in t4t
- Conversion of dbt freshness tests
- Source freshness validation

**Note**: GitHub issue created. Freshness tests are detected and skipped during import.

### 7. Missing Incremental Strategies
**Status**: ⚠️ Partially Implemented
**Current State**: Basic strategies mapped, unsupported ones logged
**What's Missing**:
- `insert_overwrite` for Spark (Issue #5)
- `on_schema_change` support (Issue #6)
- Other missing strategies (Issue #3)

**Note**: Issues created for missing strategies. Importer uses fallback with warnings.

## 🧪 Testing Gaps

### 1. Integration Tests
**Status**: ⚠️ Basic tests exist
**What's Missing**:
- More comprehensive integration tests with real dbt projects
- Tests for edge cases:
  - Complex nested folder structures
  - Multiple schema configurations
  - Large projects (100+ models)
  - Projects with many packages
  - Projects with custom macros

### 2. Error Handling Tests
**Status**: ⚠️ Basic tests exist
**What's Missing**:
- Tests for malformed dbt projects
- Tests for missing dependencies
- Tests for invalid configurations
- Tests for permission errors

## 📊 Code Quality

### 1. Code Coverage
**Status**: ⚠️ Partial
**Current Coverage**:
- `importer.py`: 62%
- `report_generator.py`: 57%
- Other modules: Varies

**What's Missing**:
- Higher coverage for critical paths
- Edge case coverage
- Error path coverage

## 🚀 Future Enhancements (Not Critical)

### 1. Progress Indicators
**Status**: ❌ Not Implemented
**What's Missing**:
- Progress bars for large imports
- Estimated time remaining
- Per-phase progress indicators

### 2. Incremental Import
**Status**: ❌ Not Implemented
**What's Missing**:
- Ability to re-import only changed models
- Diff detection
- Selective updates

### 3. Import Preview
**Status**: ⚠️ Dry-run exists but could be enhanced
**What's Missing**:
- Preview of what will be converted
- Diff view of changes
- Interactive confirmation

### 4. Batch Import
**Status**: ❌ Not Implemented
**What's Missing**:
- Import multiple dbt projects at once
- Batch validation
- Consolidated reports

## 📋 Priority Summary

### High Priority (Should be done)
1. ✅ **CLI Reference Documentation** - ✅ Complete
2. ✅ **User Guide for dbt Import** - ✅ Complete
3. ✅ **Limitations Documentation** - ✅ Complete

### Medium Priority (Nice to have)
4. ~~Variables automatic conversion~~ ✅ Already implemented (only missing when `--keep-jinja` is used, which is intentional)
5. More comprehensive integration tests
6. Higher code coverage

### Low Priority (Future enhancements)
7. Package inlining
8. Custom macro support
9. Dependency resolution (`model+`, `+model`)
10. Progress indicators
11. Incremental import

## 🎯 Recommended Next Steps

1. **Documentation** (High Priority):
   - ✅ Add `import` command to CLI reference - **COMPLETE**
   - ✅ Create user guide for dbt import - **COMPLETE**
   - ✅ Document limitations and known issues - **COMPLETE**

2. **Testing** (Medium Priority):
   - Add more integration tests
   - Improve error handling tests
   - Increase code coverage

3. **Features** (As needed):
   - ✅ Variables conversion - **COMPLETE** (only missing when `--keep-jinja` is used, which is intentional)
   - Implement dependency resolution if users request it (Issue #8)
   - Address missing incremental strategies as they come up

