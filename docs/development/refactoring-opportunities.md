# Code Refactoring and Optimization Opportunities

## Issues Found

### 1. **BUG: Missing parameter in `_topological_sort_with_graphlib()` call**
   - **Location**: `tee/parser/analysis/dependency_graph.py:132`
   - **Issue**: Method is called without `parsed_functions` parameter, but signature requires it
   - **Impact**: Functions may not be prioritized correctly in execution order
   - **Fix**: Add `parsed_functions` parameter to the call

### 2. **Code Duplication: Built-in Functions List**
   - **Locations**: 
     - `tee/parser/parsers/function_sql_parser.py:566-590` (25 lines)
     - `tee/parser/analysis/dependency_graph.py:477` (1 line, smaller set)
   - **Issue**: Built-in function names are duplicated
   - **Fix**: Extract to shared constant in `tee/parser/shared/constants.py`

### 3. **Code Duplication: Function Dependency Extraction**
   - **Locations**:
     - `tee/parser/parsers/function_sql_parser.py:_extract_dependencies()` (55 lines)
     - `tee/parser/analysis/dependency_graph.py:_extract_function_dependencies_from_sql()` (70 lines)
   - **Issue**: Similar logic for extracting function calls from SQL
   - **Fix**: Extract to shared utility function

### 4. **Large File: `function_sql_parser.py` (642 lines)**
   - **Opportunities**:
     - Extract dialect inference logic (lines 44-115) → `tee/parser/shared/dialect_inference.py`
     - Extract parameter parsing logic (lines 456-541) → `tee/parser/shared/parameter_parser.py`
     - Extract dependency extraction (lines 543-599) → `tee/parser/shared/dependency_extractor.py`

### 5. **Large File: `dependency_graph.py` (530 lines)**
   - **Opportunities**:
     - Extract function dependency extraction (lines 418-488) → `tee/parser/shared/dependency_extractor.py`
     - Extract test dependency parsing (lines 102-327) → `tee/parser/analysis/test_dependency_parser.py`

### 6. **Unused Method: `_detect_cycles()`**
   - **Location**: `tee/parser/analysis/dependency_graph.py:350-387`
   - **Issue**: Method is only used as fallback in `_detect_cycles_with_graphlib()`
   - **Status**: Actually used, but could be simplified since graphlib handles most cases

## Recommended Refactoring Priority

### High Priority (Fixes Bugs)
1. ✅ **FIXED** - Fix missing `parsed_functions` parameter in `_topological_sort_with_graphlib()` call
2. ✅ **FIXED** - Fix execution order logic to respect dependencies (removed incorrect function/model separation)

### Medium Priority (Reduces Duplication)
3. ✅ **FIXED** - Extract built-in functions list to shared constant (`SQL_BUILT_IN_FUNCTIONS`)

### Low Priority (Improves Maintainability)
4. Extract function dependency extraction to shared utility (still duplicated between `function_sql_parser.py` and `dependency_graph.py`)
5. Split large files into smaller modules:
   - `function_sql_parser.py` (617 lines) - could extract dialect inference, parameter parsing
   - `dependency_graph.py` (522 lines) - could extract test dependency parsing
6. Extract dialect inference logic
7. Extract parameter parsing logic

## Implementation Plan

### Step 1: Fix Critical Bug
- Fix `_topological_sort_with_graphlib()` call to include `parsed_functions`

### Step 2: Extract Shared Constants
- Create `SQL_BUILT_IN_FUNCTIONS` constant in `tee/parser/shared/constants.py`
- Update both files to use the constant

### Step 3: Extract Dependency Extraction Utility
- Create `tee/parser/shared/dependency_extractor.py`
- Move common function extraction logic there
- Update both files to use the utility

### Step 4: (Optional) Split Large Files
- Extract dialect inference
- Extract parameter parsing
- Extract test dependency parsing

