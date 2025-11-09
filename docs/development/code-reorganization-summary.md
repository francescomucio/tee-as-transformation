# Code Reorganization Summary

This document identifies files that have become too long or hard to maintain and provides recommendations for refactoring.

## Files Requiring Refactoring

### 🔴 High Priority (Very Large Files - >700 lines)

#### 1. `tee/adapters/snowflake/adapter.py` - **1,124 lines, 32 methods**
**Issues:**
- Single class with too many responsibilities
- 7 methods over 50 lines (longest: `attach_object_tags` at 88 lines)
- Mixes connection management, SQL execution, materialization, tagging, and function management

**Recommendations:**
- Extract tag management: `tee/adapters/snowflake/tag_manager.py` (~200 lines)
  - `attach_tags()`, `attach_object_tags()`
- Extract materialization helpers: `tee/adapters/snowflake/materialization_helpers.py` (~150 lines)
  - `_build_view_with_column_comments()`, view creation logic
- Extract function management: `tee/adapters/snowflake/function_manager.py` (~100 lines)
  - `create_function()`, `function_exists()`, `drop_function()`
- Keep main adapter as orchestrator (~600 lines)

**Priority:** High - This is the largest file and handles multiple concerns

---

#### 2. `tee/engine/execution_engine.py` - **867 lines, 22 methods**
**Issues:**
- 5 methods over 50 lines (longest: `execute_models` at 128 lines)
- Mixes model execution, function execution, incremental logic, and metadata extraction

**Recommendations:**
- Extract function execution: `tee/engine/function_executor.py` (~150 lines)
  - `execute_functions()`, `_extract_function_sql()`, `_extract_function_metadata()`
- Extract incremental execution: `tee/engine/incremental_executor.py` (already exists, but logic could be moved)
  - `_execute_incremental_materialization()` logic
- Extract metadata extraction: `tee/engine/metadata_extractor.py` (~100 lines)
  - `_extract_metadata()`, `_load_schema_metadata()`
- Keep main engine as orchestrator (~500 lines)

**Priority:** High - Core execution logic needs better separation

---

#### 3. `tee/parser/output/ots_transformer.py` - **758 lines, 18 methods**
**Issues:**
- 4 methods over 50 lines (longest: `_transform_function` at 90 lines)
- Mixes transformation logic for models, functions, tests, and schemas

**Recommendations:**
- Extract function transformation: `tee/parser/output/ots/function_transformer.py` (~150 lines)
  - `_transform_function()`, `_merge_function_tags()`, `_extract_function_object_tags()`
- Extract test transformation: `tee/parser/output/ots/test_transformer.py` (~100 lines)
  - `_transform_tests()` and related test logic
- Extract schema inference: `tee/parser/output/ots/schema_inferencer.py` (~80 lines)
  - `_infer_schema_from_sql()` and schema-related utilities
- Extract module creation: `tee/parser/output/ots/module_builder.py` (~100 lines)
  - `_create_ots_module()` and module assembly logic
- Keep main transformer as orchestrator (~300 lines)

**Priority:** High - Already documented in `parser-refactoring-plan.md`

---

### 🟡 Medium Priority (Large Files - 500-700 lines)

#### 4. `tee/testing/executor.py` - **644 lines, 10 methods**
**Issues:**
- 5 methods over 50 lines (longest: `execute_all_tests` at 122 lines)
- Mixes test execution, function test handling, and unused test detection

**Recommendations:**
- Extract function test execution: `tee/testing/function_test_executor.py` (~150 lines)
  - `_execute_function_test()` and function test logic
- Extract unused test checker: `tee/testing/unused_test_checker.py` (~120 lines)
  - `_check_unused_generic_tests()` and related logic
- Extract test execution helpers: `tee/testing/test_execution_helpers.py` (~100 lines)
  - Common test execution utilities
- Keep main executor as orchestrator (~300 lines)

**Priority:** Medium - Testing logic is complex but less critical than core execution

---

#### 5. `tee/parser/parsers/function_sql_parser.py` - **629 lines, 11 methods**
**Issues:**
- 4 methods over 50 lines (longest: `_parse_create_function_sqlglot` at 135 lines)
- Mixes SQL parsing, dialect inference, metadata handling, and parameter parsing
- **Already documented** in `file-refactoring-proposal.md`

**Recommendations:**
- Extract dialect inference: `tee/parser/parsers/functions/dialect_inference.py` (~80 lines)
- Extract SQL parsing: `tee/parser/parsers/functions/sql_parsing.py` (~230 lines)
  - `_parse_create_function_sqlglot()`, `_parse_create_function_regex()`
- Extract parameter parsing: `tee/parser/parsers/functions/parameter_parser.py` (~100 lines)
  - `_parse_parameters()`, `_split_parameters()`
- Extract metadata handling: `tee/parser/parsers/functions/metadata_handler.py` (~70 lines)
  - `_find_metadata_file()`, `_merge_metadata()`
- Keep main parser as orchestrator (~150 lines)

**Priority:** Medium - Already has detailed refactoring plan

---

#### 6. `tee/parser/core/orchestrator.py` - **518 lines, 10 methods**
**Issues:**
- 2 methods over 50 lines (longest: `discover_and_parse_functions` at 189 lines)
- Orchestrates multiple concerns but methods are getting long

**Recommendations:**
- Extract function discovery/parsing: `tee/parser/core/function_orchestrator.py` (~200 lines)
  - `discover_and_parse_functions()` and related function logic
- Extract model discovery/parsing: `tee/parser/core/model_orchestrator.py` (~150 lines)
  - `discover_and_parse_models()` and related model logic
- Keep main orchestrator as high-level coordinator (~200 lines)

**Priority:** Medium - Orchestration logic is complex but well-structured

---

#### 7. `tee/parser/input/ots_converter.py` - **514 lines, 11 methods**
**Issues:**
- 3 methods over 50 lines (longest: `_convert_metadata` at 87 lines)
- Mixes conversion logic for models, functions, and metadata

**Recommendations:**
- Extract function conversion: `tee/parser/input/ots/function_converter.py` (~120 lines)
  - `_convert_function()` and function-related conversion
- Extract metadata conversion: `tee/parser/input/ots/metadata_converter.py` (~120 lines)
  - `_convert_metadata()` and metadata transformation
- Keep main converter as orchestrator (~250 lines)

**Priority:** Medium - Import logic is important but less frequently changed

---

### 🟢 Lower Priority (Moderate Files - 300-500 lines)

#### 8. `tee/parser/parsers/function_python_parser.py` - **360 lines, 10 methods**
**Issues:**
- Mixes AST extraction, metadata parsing, and signature extraction
- **Already documented** in `parser-refactoring-plan.md`

**Recommendations:**
- Extract AST utilities: `tee/parser/shared/ast_extractor.py` (~100 lines)
- Extract signature extraction: `tee/parser/parsers/functions/signature_extractor.py` (~80 lines)
- Keep main parser as orchestrator (~180 lines)

**Priority:** Low - Smaller file, less urgent

---

## Summary Statistics

| File | Lines | Methods | Long Methods (>50 lines) | Priority |
|------|-------|---------|-------------------------|----------|
| `snowflake/adapter.py` | 1,124 | 32 | 7 | 🔴 High |
| `execution_engine.py` | 867 | 22 | 5 | 🔴 High |
| `ots_transformer.py` | 758 | 18 | 4 | 🔴 High |
| `testing/executor.py` | 644 | 10 | 5 | 🟡 Medium |
| `function_sql_parser.py` | 629 | 11 | 4 | 🟡 Medium |
| `orchestrator.py` | 518 | 10 | 2 | 🟡 Medium |
| `ots_converter.py` | 514 | 11 | 3 | 🟡 Medium |
| `function_python_parser.py` | 360 | 10 | 0 | 🟢 Low |

## Refactoring Strategy

### Phase 1: High Priority Files (Do First)
1. **`snowflake/adapter.py`** - Extract tag management, materialization helpers, function manager
2. **`execution_engine.py`** - Extract function executor, metadata extractor
3. **`ots_transformer.py`** - Extract function transformer, test transformer, schema inferencer

### Phase 2: Medium Priority Files
4. **`testing/executor.py`** - Extract function test executor, unused test checker
5. **`function_sql_parser.py`** - Follow existing refactoring plan
6. **`orchestrator.py`** - Extract function and model orchestrators
7. **`ots_converter.py`** - Extract function and metadata converters

### Phase 3: Lower Priority Files
8. **`function_python_parser.py`** - Extract AST utilities and signature extractor

## Benefits of Refactoring

1. **Improved Maintainability**: Smaller, focused files are easier to understand and modify
2. **Better Testability**: Individual components can be tested in isolation
3. **Reduced Complexity**: Each file has a single, clear responsibility
4. **Easier Navigation**: Developers can quickly find code by feature
5. **Reusability**: Extracted utilities can be shared across modules
6. **Parallel Development**: Multiple developers can work on different modules

## Implementation Guidelines

1. **Maintain Backward Compatibility**: Keep public APIs unchanged
2. **Incremental Refactoring**: Extract one module at a time
3. **Comprehensive Testing**: Ensure all tests pass after each extraction
4. **Documentation**: Update docstrings and module documentation
5. **Code Review**: Review each extraction before moving to the next

