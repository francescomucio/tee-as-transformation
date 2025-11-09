# UDF Implementation Plan

This document outlines the step-by-step implementation plan for adding User-Defined Functions (UDFs) support to t4t.

## Overview

The implementation is divided into 9 phases, each building on the previous ones. The plan is designed to be incremental, allowing for testing and validation at each stage.

**Execution Order:** Seeds → Functions → Models

---

## Phase 0: Foundation & Type System

**Goal:** Establish the type system and basic infrastructure for functions.

**Deliverables:**
1. Type definitions for functions in `tee/typing/metadata.py`
2. Constants for function discovery in `tee/parser/shared/constants.py`
3. Basic exception classes for function-related errors

**Tasks:**

### 0.1 Type Definitions
- [ ] Add `FunctionType` enum: `scalar`, `aggregate`, `table`
- [ ] Add `FunctionParameter` TypedDict
- [ ] Add `FunctionMetadataDict` TypedDict (with `tags` and `object_tags` like models)
- [ ] Add `ParsedFunction` TypedDict (similar to `ParsedModel`)
- [ ] Add `OTSFunction` TypedDict for OTS 0.2.0 (with tags support)
- [ ] Update `OTSModule` to include optional `functions` field

### 0.2 Constants
- [ ] Add `DEFAULT_FUNCTIONS_FOLDER = "functions"` to constants
- [ ] Add function-related file patterns (`.sql`, `.py`, `.js` for database overrides)

### 0.3 Exception Classes
- [ ] Add `FunctionParsingError` exception
- [ ] Add `FunctionExecutionError` exception
- [ ] Add `FunctionMetadataError` exception

**Testing:**
- Unit tests for type validation
- Type checking with mypy

**Dependencies:** None

**Estimated Time:** 1-2 days

---

## Phase 1: File Discovery

**Goal:** Discover function files in the `functions/` folder.

**Deliverables:**
1. Extended `FileDiscovery` class to discover function files
2. Support for both flat and folder-based structures

**Tasks:**

### 1.1 Extend FileDiscovery
- [ ] Add `discover_function_files()` method
- [ ] Support flat structure: `functions/{schema}/{function_name}.sql`
- [ ] Support folder structure: `functions/{schema}/{function_name}/{function_name}.sql`
- [ ] Support database overrides: `{function_name}.{database}.sql` or `.js`
- [ ] Add caching (similar to model discovery)
- [ ] Update `discover_all_files()` to include functions

### 1.2 Function File Patterns
- [ ] SQL functions: `*.sql`
- [ ] Python functions: `*.py`
- [ ] Database-specific: `*.{database}.sql`, `*.{database}.js`
- [ ] Metadata files: `{function_name}.py` (for SQL functions)

**Testing:**
- Test discovery of flat structure
- Test discovery of folder structure
- Test discovery of database overrides
- Test schema extraction from folder structure
- Test edge cases (nested folders, invalid names)

**Dependencies:** Phase 0

**Estimated Time:** 1-2 days

---

## Phase 2: Function Parsers

**Goal:** Parse function definitions from SQL and Python files.

**Deliverables:**
1. `FunctionSQLParser` for parsing SQL function definitions
2. `FunctionPythonParser` for parsing Python function definitions
3. Function decorators (`@functions.sql()`, `@functions.python()`)

**Tasks:**

### 2.1 SQL Function Parser
- [ ] Create `tee/parser/parsers/function_sql_parser.py`
- [ ] Parse `CREATE FUNCTION` statements using SQLglot
- [ ] Extract:
  - Function name (qualified: `schema.function_name`)
  - Parameters (name, type, default, mode)
  - Return type (scalar or table schema)
  - Language
  - Function body
- [ ] Extract dependencies:
  - Table references in function body
  - Function calls (filter out built-ins)
- [ ] Handle database-specific syntax variations

### 2.2 Python Function Parser
- [ ] Create `tee/parser/parsers/function_python_parser.py`
- [ ] AST parsing for `@functions.sql()` decorator
- [ ] AST parsing for `@functions.python()` decorator
- [ ] Extract metadata from decorator arguments
- [ ] Extract function signature (type hints)
- [ ] Extract docstring (description)
- [ ] Support multiple functions per file
- [ ] Handle metadata-only files (`FunctionMetadataDict`)

### 2.3 Function Decorators
- [ ] Create `tee/parser/processing/function_decorator.py`
- [ ] Implement `@functions.sql()` decorator
- [ ] Implement `@functions.python()` decorator
- [ ] Store metadata on function objects
- [ ] Support `database_name` parameter for overloading
- [ ] Support `tags` parameter (list of strings, dbt-style)
- [ ] Support `object_tags` parameter (dict, database-style)

### 2.4 Metadata Extraction
- [ ] Extract metadata from Python files (similar to model metadata)
- [ ] Merge SQL function code with Python metadata
- [ ] Validate consistency (raise error on conflicts)
- [ ] Support metadata-only definitions

**Testing:**
- Test SQL function parsing (various dialects)
- Test Python function parsing (decorators)
- Test metadata extraction and merging
- Test multiple functions per file
- Test error handling (invalid syntax, conflicts)

**Dependencies:** Phase 0, Phase 1

**Estimated Time:** 3-4 days

---

## Phase 3: Function Orchestration & Integration

**Goal:** Integrate function parsing into the main parser workflow.

**Deliverables:**
1. Extended `ParserOrchestrator` to handle functions
2. Function discovery and parsing in project workflow
3. Function metadata standardization

**Tasks:**

### 3.1 Extend ParserOrchestrator
- [x] Add `discover_and_parse_functions()` method
- [x] Integrate with existing `discover_and_parse_models()`
- [x] Handle function file discovery
- [x] Parse SQL functions
- [x] Parse Python functions
- [x] Merge SQL + metadata files
- [x] Standardize function structure (similar to models)

### 3.2 Function Metadata Standardization
- [x] Create `standardize_parsed_function()` utility (already existed in function_utils.py)
- [x] Ensure consistent structure across SQL/Python functions
- [x] Compute function hash for change detection
- [x] Store function code (SQL or Python)

### 3.3 Table Resolver Integration
- [x] Extend `TableResolver` to handle function names
- [x] Generate qualified function names (`schema.function_name`)
- [x] Resolve function references in SQL

**Testing:**
- [x] Test function discovery in orchestrator
- [x] Test parsing integration
- [x] Test metadata standardization
- [x] Test qualified name generation
- [x] Test with existing model parsing (no regressions)

**Dependencies:** Phase 1, Phase 2

**Estimated Time:** 2-3 days

---

## Phase 4: Dependency Graph Integration

**Goal:** Add functions to the dependency graph.

**Deliverables:**
1. Extended dependency graph to include functions
2. Function-to-function dependencies
3. Function-to-table dependencies
4. Model-to-function dependencies

**Tasks:**

### 4.1 Extend DependencyGraphBuilder
- [x] Add function nodes to dependency graph
- [x] Extract function dependencies:
  - From SQL function bodies (parse with SQLglot/regex)
  - From Python function code (via metadata)
- [x] Extract model dependencies on functions:
  - Parse model SQL for function calls (SQLglot with regex fallback)
  - Filter out built-in functions
- [x] Handle function overloading (by signature) - via qualified names

### 4.2 Dependency Extraction
- [x] SQL function body parsing for dependencies
- [x] Function call detection in SQL (SQLglot with regex fallback)
- [x] Function call detection in Python (via metadata from parser)
- [x] Table reference extraction from function bodies
- [x] Seed dependency support (via table resolution)

### 4.3 Execution Order
- [x] Functions before models in execution order
- [x] Function dependency resolution
- [x] Topological sort including functions
- [x] Handle cycles (functions → functions)

**Testing:**
- [x] Test function dependency extraction
- [x] Test function-to-function dependencies
- [x] Test function-to-table dependencies
- [x] Test model-to-function dependencies
- [x] Test execution order (functions before models)
- [x] Test cycle detection

**Dependencies:** Phase 3

**Estimated Time:** 3-4 days

---

## Phase 5: OTS 0.2.0 Support

**Goal:** Add function support to OTS specification (version 0.2.0).

**Deliverables:**
1. OTS 0.2.0 type definitions
2. Function export to OTS modules
3. Function import from OTS modules

**Tasks:**

### 5.1 OTS Type Updates
- [x] Update `OTSModule` to include `functions` field
- [x] Add `OTSFunction` type definition
- [x] Support OTS version 0.2.0
- [x] Backward compatibility with 0.1.0

### 5.2 Function Export (OTS Transformer)
- [x] Extend `OTSTransformer` to export functions
- [x] Include functions in OTS module compilation
- [x] Export function code (generic + database-specific)
- [x] Export function metadata (including tags and object_tags)
- [x] Export function dependencies
- [x] Merge function tags (like model tags: schema-level, module-level, function-level)

### 5.3 Function Import (OTS Reader/Converter)
- [x] Update `OTSModuleReader` to read 0.2.0 modules
- [x] Update `OTSConverter` to convert functions to `ParsedFunction`
- [x] Handle function imports from OTS modules
- [x] Merge imported functions with project functions

### 5.4 OTS Version Handling
- [x] Version detection and validation
- [x] Support both 0.1.0 and 0.2.0
- [x] Clear error messages for unsupported versions

**Testing:**
- [x] Test function export to OTS
- [x] Test function import from OTS
- [x] Test OTS 0.2.0 module structure
- [x] Test backward compatibility (0.1.0)
- [x] Test version validation

**Dependencies:** Phase 3, Phase 4

**Estimated Time:** 3-4 days

---

## Phase 6: Adapter Interface & Base Implementation

**Goal:** Define adapter interface for function creation.

**Deliverables:**
1. Abstract methods in `DatabaseAdapter` base class
2. Base implementation with error handling

**Tasks:**

### 6.1 Base Adapter Interface
- [x] Add `create_function()` abstract method
- [x] Add `function_exists()` abstract method
- [x] Add `drop_function()` abstract method
- [x] Add `get_function_info()` method (non-abstract, with default implementation)
- [x] Update `attach_tags()` and `attach_object_tags()` documentation to include 'FUNCTION' object type
- [x] Define function definition structure (function_name, function_sql, metadata)

### 6.2 Base Implementation
- [x] Default implementation for `get_function_info()` with debug logging
- [x] Error handling and logging (NotImplementedError stubs in adapters)
- [x] Function name qualification (accepts qualified names like schema.function_name)
- [x] Schema handling (via qualified function names)

**Testing:**
- [x] Test abstract method definitions
- [x] Test base implementation (get_function_info default)
- [x] Test error handling
- [x] Test metadata handling
- [x] Test qualified and unqualified function names

**Dependencies:** Phase 0

**Estimated Time:** 1-2 days

---

## Phase 7: Adapter Implementations

**Goal:** Implement function creation for each database adapter.

**Deliverables:**
1. DuckDB function implementation
2. PostgreSQL function implementation
3. Snowflake function implementation
4. BigQuery function implementation (if applicable)

**Tasks:**

### 7.1 DuckDB Adapter
- [x] Implement `create_function()` for SQL functions (executes CREATE OR REPLACE FUNCTION SQL)
- [x] Implement `create_function()` for Python UDFs (same method, different SQL syntax)
- [x] Handle function overloading (database handles it via signature matching)
- [x] Implement `function_exists()` (queries information_schema.routines)
- [x] Implement `drop_function()` (DROP FUNCTION IF EXISTS)
- [x] Implement `attach_tags()` for functions (logs debug, DuckDB doesn't natively support tags)

### 7.2 PostgreSQL Adapter
- [x] Implement `create_function()` for SQL functions (executes CREATE OR REPLACE FUNCTION SQL)
- [x] Implement `create_function()` for PL/pgSQL functions (same method, different SQL syntax)
- [x] Handle function overloading (database handles it via signature matching)
- [x] Implement `function_exists()` (queries information_schema.routines)
- [x] Implement `drop_function()` (DROP FUNCTION IF EXISTS)
- [x] Implement `attach_tags()` for functions (logs debug, PostgreSQL doesn't natively support tags)

### 7.3 Snowflake Adapter
- [x] Implement `create_function()` for SQL functions (executes CREATE OR REPLACE FUNCTION SQL with qualified names)
- [x] Implement `create_function()` for JavaScript UDFs (same method, different SQL syntax)
- [x] Implement `create_function()` for Python UDFs (same method, different SQL syntax)
- [x] Handle function overloading (database handles it via signature matching)
- [x] Implement `function_exists()` (queries information_schema.routines)
- [x] Implement `drop_function()` (DROP FUNCTION IF EXISTS with qualified names)
- [x] Implement `attach_tags()` for functions (full support via existing attach_tags/attach_object_tags methods)

### 7.4 BigQuery Adapter
- [x] Implement `create_function()` for SQL functions (executes CREATE OR REPLACE FUNCTION SQL)
- [x] Implement `create_function()` for JavaScript UDFs (same method, different SQL syntax)
- [x] Handle function overloading (database handles it via signature matching)
- [x] Implement `function_exists()` (queries INFORMATION_SCHEMA.ROUTINES)
- [x] Implement `drop_function()` (DROP FUNCTION IF EXISTS)

**Testing:**
- [x] Test function creation for each adapter (via interface tests)
- [x] Test function existence checks (via interface tests)
- [x] Test function dropping (via interface tests)
- [x] Test database-specific syntax (handled by executing SQL as-is)
- [ ] Integration tests with actual databases (deferred to Phase 8 when execution is integrated)

**Dependencies:** Phase 6

**Estimated Time:** 4-5 days per adapter (can be done in parallel)

---

## Phase 8: Execution Engine Integration ✅ COMPLETE

**Goal:** Execute functions in dependency order before models.

**Deliverables:**
1. ✅ Function execution in `ExecutionEngine`
2. ✅ Function execution in `ModelExecutor`
3. ✅ Integration with build workflow
4. ✅ Integration tests for DuckDB and Snowflake

**Tasks:**

### 8.1 ExecutionEngine Extensions
- [x] Add `execute_functions()` method
- [x] Execute functions in dependency order (filters execution_order to get functions)
- [x] Handle function creation/updates (always CREATE OR REPLACE via adapter)
- [x] Always use `CREATE OR REPLACE` (functions are always overwritten)
- [x] Raise error if function creation fails (FunctionExecutionError, but continues with other functions)
- [x] Attach tags to functions after creation (via adapter.attach_tags/attach_object_tags)

### 8.2 ModelExecutor Integration
- [x] Add function execution before model execution (in execute_models method)
- [x] Integrate with seed loading (Seeds → Functions → Models order maintained)
- [x] Update execution order logic (uses existing execution_order from dependency graph)
- [x] Handle function execution errors (logged but doesn't stop model execution)

### 8.3 Build Workflow Integration
- [x] Update `build_models()` to execute functions before model loop
- [x] Ensure execution order: Seeds → Functions → Models → Tests
- [x] Update CLI commands if needed (no changes needed, build_models handles it)

### 8.4 Function State Management
- [ ] Track function creation state (deferred - functions always use CREATE OR REPLACE)
- [ ] Detect function changes (hash-based) (deferred - can be added later if needed)
- [ ] Support incremental updates (deferred - functions are always overwritten)

**Testing:**
- [x] Test function execution order (via existing dependency graph tests)
- [x] Test function creation before models (via build_models tests)
- [x] Test function dependency resolution (via dependency graph tests)
- [x] Test error handling (functions continue on error, don't stop build)
- [x] Test with existing model execution (all 662 tests pass, no regressions)
- [x] Integration tests with full workflow (DuckDB: 6 tests passing, Snowflake: 2 tests ready)

**Dependencies:** Phase 4, Phase 7

**Estimated Time:** 3-4 days

---

## Phase 9: Testing Framework & Documentation ✅ COMPLETE

**Goal:** Add function testing support and complete documentation.

**Deliverables:**
1. ✅ Function testing framework
2. ✅ CLI command for function testing
3. ✅ Complete documentation
4. ✅ Examples

**Tasks:**

### 9.1 Function Testing Framework
- [x] Extend `TestExecutor` to handle function tests
- [x] Discover tests from `tests/functions/` folder (via `TestDiscovery`)
- [x] Support two test patterns:
  - **Assertion-based**: Test SQL returns boolean (TRUE = pass, FALSE = fail)
  - **Expected value**: Test SQL returns result, compare to `expected` in metadata
- [x] Support generic tests (with `@function_name` placeholder) - referenced in metadata
- [x] Support singular tests (hardcoded function name) - always executed
- [x] Parse singular test SQL to find function calls (match to functions)
- [x] Support function parameter placeholders (`@param1`, `@param2`, etc.)
- [x] Handle boolean representations across databases (TRUE/1/truthy)
- [x] Support scalar, aggregate, and table-valued function tests
- [x] Integration with `execute_all_tests()` to handle both models and functions

### 9.2 TestExecutor Extensions
- [x] Extend `execute_all_tests()` to handle both models and functions
- [x] Add `execute_tests_for_function()` method (similar to `execute_tests_for_model()`)
- [x] Discover function tests from `tests/functions/` folder
- [x] Parse singular test SQL to extract function dependencies
- [x] Support test metadata with `expected` values
- [x] Handle boolean assertion pattern (default)
- [x] Integration with existing test command (no separate command needed)

### 9.3 Documentation
- [x] User guide for functions (`docs/user-guide/functions.md`)
- [x] API reference for decorators (`docs/api-reference/functions.md`)
- [x] Examples for SQL functions (`docs/user-guide/examples/functions.md`)
- [x] Examples for Python functions (`docs/user-guide/examples/functions.md`)
- [x] Examples for database-specific functions (`docs/user-guide/examples/functions.md`)
- [x] Migration guide (not needed - functions are additive)
- [x] Update main README

### 9.4 Examples
- [x] Example project with SQL functions (`examples/t_project`)
- [x] Example project with Python functions (documented in examples guide)
- [x] Example project with database overrides (`examples/t_project_sno`)
- [x] Example project with function dependencies (documented in examples guide)
- [x] Example project with function tests (`examples/t_project` and `examples/t_project_sno`)

**Testing:**
- [x] Test function testing framework (all 668 tests passing)
- [x] Test CLI commands (integrated into existing test command)
- [x] Test documentation examples (function test files added to example projects)
- [x] End-to-end examples (example projects updated)

**Dependencies:** All previous phases

**Estimated Time:** 3-4 days

---

## Implementation Strategy

### Incremental Approach
1. **Phase 0-2:** Core parsing (can be developed and tested independently)
2. **Phase 3-4:** Integration (builds on parsing)
3. **Phase 5:** OTS support (can be developed in parallel with Phase 6-7)
4. **Phase 6-7:** Adapter implementations (can be done in parallel)
5. **Phase 8:** Execution (requires all previous phases)
6. **Phase 9:** Polish and documentation

### Testing Strategy
- Unit tests for each component
- Integration tests for each phase
- End-to-end tests with example projects
- Backward compatibility tests (ensure models still work)

### Backward Compatibility
- All existing functionality must continue to work
- Functions are additive (no breaking changes)
- OTS 0.1.0 modules should still work
- Models should work exactly as before

### Rollout Plan
1. **Alpha:** Phases 0-4 (parsing and dependency graph)
2. **Beta:** Phases 5-7 (OTS and adapters)
3. **RC:** Phase 8 (execution)
4. **Release:** Phase 9 (documentation and examples)

---

## Estimated Total Time

- **Phase 0:** 1-2 days
- **Phase 1:** 1-2 days
- **Phase 2:** 3-4 days
- **Phase 3:** 2-3 days
- **Phase 4:** 3-4 days
- **Phase 5:** 3-4 days
- **Phase 6:** 1-2 days
- **Phase 7:** 4-5 days per adapter (can parallelize)
- **Phase 8:** 3-4 days
- **Phase 9:** 3-4 days

**Total:** ~30-40 days (with 2-3 adapters implemented)

---

## Risk Mitigation

### High-Risk Areas
1. **SQL Function Parsing:** Different databases have different syntax
   - *Mitigation:* Start with simple SQL functions, add complexity incrementally
   
2. **Dependency Extraction:** Complex function bodies may have hidden dependencies
   - *Mitigation:* Conservative approach, allow manual dependency specification
   
3. **Function Overloading:** Database-specific behavior
   - *Mitigation:* Clear documentation, adapter-specific handling

4. **Python UDF Execution:** Database-specific Python runtime requirements
   - *Mitigation:* Start with SQL-generating functions, add Python UDFs later

### Rollback Plan
- Each phase should be mergeable independently
- Feature flags could be added to disable function support
- Backward compatibility ensures existing workflows continue

---

## Success Criteria

1. ✅ Functions can be defined in SQL and Python
2. ✅ Functions are discovered and parsed correctly
3. ✅ Function dependencies are extracted and resolved
4. ✅ Functions are created in the database before models
5. ✅ Models can use functions in their SQL
6. ✅ Functions are exported/imported in OTS 0.2.0 modules
7. ✅ All adapters support function creation
8. ✅ Functions can be tested
9. ✅ Documentation is complete
10. ✅ No regressions in existing functionality

---

## Questions to Resolve During Implementation

**RESOLVED:**
1. ✅ **Function variables:** Not supported for now
2. ✅ **Function versioning:** Functions are always overwritten (CREATE OR REPLACE)
3. ✅ **Function errors:** Raise an error if function creation fails
4. ✅ **Function tags:** Functions should have tags like models (both `tags` and `object_tags`)

**REMAINING:**
1. ✅ **Function tests:** Integrated into existing test framework, in `tests/functions/` folder
2. ✅ **Function test patterns:** Hybrid approach (assertion-based + expected value in metadata)
3. ✅ **Function test types:** Generic (with `@function_name`) and singular (hardcoded function name)
4. ✅ **Function test execution:** Boolean assertion (TRUE=pass) or compare to expected value
5. ✅ **Function permissions/security:** Tagging is sufficient

---

## Next Steps

1. Review and approve this plan
2. Set up project tracking (GitHub issues, milestones)
3. Start with Phase 0 (foundation)
4. Create feature branch: `feature/udf-support`
5. Begin incremental implementation

