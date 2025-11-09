# File Refactoring Proposal

## Current State

- `function_sql_parser.py`: **618 lines** (11 methods)
- `dependency_graph.py`: **523 lines** (9 methods)

## Key Improvement: Extract Function Calls During Model Parsing

**Current Issue**: Function dependencies are extracted from model SQL during graph building (re-parsing), creating duplication.

**Solution**: Extract function calls during model parsing (like we do for tables), then read them during graph building.

**Benefits**:
- ✅ No SQL re-parsing in graph builder
- ✅ Consistent with table dependency extraction
- ✅ Simpler graph builder code
- ✅ Better performance (parse once, use many times)
- ✅ Removes `_extract_function_dependencies_from_sql()` method (~70 lines)

## Option 1: Feature-Based Split (Recommended ⭐)

**Philosophy**: Split by feature/concern - each file handles one specific aspect.

### `function_sql_parser.py` → Split into:

1. **`function_sql_parser.py`** (~150 lines)
   - Main `FunctionSQLParser` class
   - `__init__`, `parse()` (orchestration)
   - Delegates to specialized modules

2. **`function_dialect_inference.py`** (~80 lines)
   - `_infer_dialect_from_connection()`
   - `_infer_dialect_from_filename()`
   - Dialect mapping logic

3. **`function_metadata_handler.py`** (~70 lines)
   - `_find_metadata_file()`
   - `_merge_metadata()`
   - Metadata file operations

4. **`function_sql_parsing.py`** (~230 lines)
   - `_parse_create_function_sqlglot()`
   - `_parse_create_function_regex()`
   - SQL parsing logic

5. **`function_parameter_parser.py`** (~100 lines)
   - `_parse_parameters()`
   - `_split_parameters()`
   - Parameter parsing logic

6. **`function_dependency_extractor.py`** (~60 lines) - **SHARED with SQLParser**
   - `_extract_dependencies()` (for function bodies)
   - Function extraction utilities
   - **Also used by `SQLParser` to extract function calls from model SQL**

### `dependency_graph.py` → Split into:

1. **`dependency_graph.py`** (~120 lines) - **SIMPLIFIED**
   - Main `DependencyGraphBuilder` class
   - `build_graph()` (orchestration)
   - Reads pre-extracted dependencies (tables AND functions)
   - **No SQL re-parsing needed!**

2. **`test_dependency_parser.py`** (~180 lines)
   - `_parse_test_dependencies()`
   - `_parse_test_instance_dependencies()`
   - Test dependency extraction

3. **`graph_cycle_detection.py`** (~70 lines)
   - `_detect_cycles()`
   - `_detect_cycles_with_graphlib()`
   - Cycle detection logic

4. **`graph_topological_sort.py`** (~60 lines)
   - `_topological_sort_with_graphlib()`
   - Topological sorting logic

5. ~~**`function_dependency_extractor.py`**~~ - **REMOVED**
   - Function dependencies extracted during model parsing (like tables)
   - No need to re-parse SQL in graph builder
   - **Simplifies graph builder significantly!**

**Pros:**
- ✅ Clear separation of concerns
- ✅ Easy to find code by feature
- ✅ Small, focused files (~50-230 lines each)
- ✅ Easy to test individual features
- ✅ Reusable components (e.g., dependency extractor)

**Cons:**
- ⚠️ More files to navigate
- ⚠️ Need to manage imports carefully

---

## Option 2: Layer-Based Split

**Philosophy**: Split by abstraction level - high-level orchestration vs low-level utilities.

### `function_sql_parser.py` → Split into:

1. **`function_sql_parser.py`** (~200 lines)
   - Main `FunctionSQLParser` class
   - `parse()` method (high-level orchestration)
   - Public API

2. **`function_sql_parser_utils.py`** (~420 lines)
   - All private helper methods
   - Dialect inference
   - SQL parsing
   - Parameter parsing
   - Dependency extraction
   - Metadata handling

### `dependency_graph.py` → Split into:

1. **`dependency_graph.py`** (~200 lines)
   - Main `DependencyGraphBuilder` class
   - `build_graph()` method (high-level orchestration)
   - Public API

2. **`dependency_graph_utils.py`** (~323 lines)
   - All private helper methods
   - Test dependency parsing
   - Cycle detection
   - Topological sort
   - Function dependency extraction

**Pros:**
- ✅ Simple two-file structure
- ✅ Clear public/private separation
- ✅ Minimal refactoring needed

**Cons:**
- ⚠️ Utils files still large (~300-400 lines)
- ⚠️ Less granular organization
- ⚠️ Harder to find specific functionality

---

## Option 3: Domain-Based Split

**Philosophy**: Split by domain - SQL parsing domain vs graph building domain.

### `function_sql_parser.py` → Split into:

1. **`function_sql_parser.py`** (~250 lines)
   - Main `FunctionSQLParser` class
   - `parse()` orchestration
   - SQL parsing methods (`_parse_create_function_sqlglot`, `_parse_create_function_regex`)

2. **`function_metadata.py`** (~200 lines)
   - Metadata file finding
   - Metadata merging
   - Dialect inference
   - Metadata operations

3. **`function_parameters.py`** (~100 lines)
   - Parameter parsing
   - Parameter splitting
   - Parameter validation

4. **`function_dependencies.py`** (~70 lines)
   - Dependency extraction
   - Dependency analysis

### `dependency_graph.py` → Split into:

1. **`dependency_graph.py`** (~200 lines)
   - Main `DependencyGraphBuilder` class
   - `build_graph()` orchestration
   - Graph building logic

2. **`dependency_extraction.py`** (~180 lines)
   - Test dependency parsing
   - Function dependency extraction
   - All dependency extraction logic

3. **`graph_algorithms.py`** (~140 lines)
   - Cycle detection
   - Topological sort
   - Graph algorithms

**Pros:**
- ✅ Domain-focused organization
- ✅ Clear boundaries between domains
- ✅ Moderate file sizes

**Cons:**
- ⚠️ Some cross-domain dependencies
- ⚠️ Less granular than Option 1

---

## Recommendation: **Option 1 (Feature-Based Split)** ⭐

### Why Option 1?

1. **Best maintainability**: Each file has a single, clear responsibility
2. **Optimal file sizes**: Files range from 50-230 lines (very manageable)
3. **Easier testing**: Can test each feature independently
4. **Reusability**: Components like `function_dependency_extractor.py` can be shared
5. **Future-proof**: Easy to add new features without bloating existing files
6. **Clear navigation**: Developers can quickly find code by feature

### Implementation Strategy

**Phase 0: Extract Function Calls During Model Parsing** (NEW - Simplifies everything!)
   - Update `SQLParser._parse_sqlglot_expression()` to extract function calls
   - Store in `code_data["sql"]["source_functions"]`
   - Update `DependencyGraphBuilder` to read pre-extracted functions
   - Remove `_extract_function_dependencies_from_sql()` method
   - **Result**: Graph builder simplified, no re-parsing needed

1. **Phase 1**: Extract shared utilities
   - Create `function_dependency_extractor.py` (shared utility)
   - Used by both `SQLParser` and `FunctionSQLParser`
   - Create `function_dialect_inference.py`

2. **Phase 2**: Extract parsing logic
   - Create `function_sql_parsing.py`
   - Create `function_parameter_parser.py`

3. **Phase 3**: Extract metadata handling
   - Create `function_metadata_handler.py`

4. **Phase 4**: Refactor main parser
   - Update `function_sql_parser.py` to use extracted modules

5. **Phase 5**: Refactor dependency graph (SIMPLIFIED)
   - Extract test dependency parser
   - Extract cycle detection
   - Extract topological sort
   - Update `dependency_graph.py` (now much simpler - no function extraction!)

### File Structure After Refactoring

```
tee/parser/
├── parsers/
│   ├── function_sql_parser.py          (~150 lines)
│   ├── function_dialect_inference.py   (~80 lines)
│   ├── function_metadata_handler.py     (~70 lines)
│   ├── function_sql_parsing.py         (~230 lines)
│   ├── function_parameter_parser.py    (~100 lines)
│   └── sql_parser.py                   (~260 lines) - UPDATED to extract functions
├── shared/
│   └── function_dependency_extractor.py (~60 lines) - SHARED utility
└── analysis/
    ├── dependency_graph.py              (~120 lines) - SIMPLIFIED (no re-parsing!)
    ├── test_dependency_parser.py        (~180 lines)
    ├── graph_cycle_detection.py         (~70 lines)
    └── graph_topological_sort.py       (~60 lines)
```

### Key Changes

1. **`SQLParser`** now extracts function calls during model parsing (like tables)
2. **`DependencyGraphBuilder`** simplified - just reads pre-extracted data
3. **Shared utility** for function extraction used by both parsers
4. **Net result**: Cleaner, more consistent, better performance

### Migration Path

- Maintain backward compatibility by keeping main classes in original files
- Gradually move methods to new modules
- Update imports incrementally
- Run tests after each extraction
- Remove old code once migration is complete

---

## Comparison Table

| Aspect | Option 1 (Feature) | Option 2 (Layer) | Option 3 (Domain) |
|--------|-------------------|------------------|-------------------|
| **File Count** | 10 files | 4 files | 7 files |
| **Max File Size** | ~230 lines | ~420 lines | ~250 lines |
| **Maintainability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Testability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Reusability** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| **Refactoring Effort** | Medium | Low | Medium |
| **Navigation** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |

---

## Next Steps

If Option 1 is chosen:
1. Create extraction plan for each module
2. Start with shared utilities (dependency extractor)
3. Extract one module at a time
4. Update tests incrementally
5. Document new structure

