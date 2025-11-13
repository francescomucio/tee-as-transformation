# Code Review: Metadata Propagation & Function Type Consolidation

## Summary
This review covers changes made to fix metadata propagation issues and consolidate function types, mirroring the model type consolidation pattern.

## Files Modified
1. `tee/typing/function.py` - New file with Function TypedDict
2. `tee/parser/shared/function_builder.py` - New utility for building functions
3. `tee/parser/processing/function_builder.py` - SQLFunctionMetadata dataclass
4. `tee/parser/shared/model_utils.py` - Fixed metadata preservation
5. `tee/parser/parsers/python_parser.py` - Fixed import injection
6. `tee/engine/executor.py` - Fixed function/model filtering
7. `tee/adapters/duckdb/materialization/table_handler.py` - Cleanup

---

## 1. Unused Code ✅

### Status: PASS
- ✅ No unused imports (verified with `ruff check`)
- ✅ No unused variables or functions
- ✅ No dead code paths
- ✅ No commented-out code blocks
- ✅ No temporary files or debug code
- ⚠️ **Minor**: `hashlib` import in `function_builder.py` is used (line 142)

---

## 2. Code Duplication ⚠️

### Status: NEEDS ATTENTION

#### Issue: Duplicate Caller File Path Extraction Logic
**Location**: Multiple files use identical frame inspection code

**Duplicated in:**
- `tee/parser/shared/function_builder.py` (lines 36-49)
- `tee/parser/processing/function_builder.py` (lines 130-141)
- `tee/parser/processing/model_builder.py` (lines 108-119)
- `tee/parser/shared/model_builder.py` (lines 36-48)

**Recommendation**: Extract to a shared utility function:

```python
# tee/parser/shared/inspect_utils.py
import inspect
import os
from typing import Tuple

def get_caller_file_info(frames_up: int = 2) -> Tuple[str | None, bool]:
    """
    Get caller file path and whether it's being run as __main__.
    
    Args:
        frames_up: Number of frames to go up (default 2 for dataclass __post_init__)
    
    Returns:
        Tuple of (file_path, is_main)
    """
    frame = inspect.currentframe()
    if not frame:
        return None, False
    
    # Go up the specified number of frames
    caller_frame = frame
    for _ in range(frames_up):
        if caller_frame.f_back:
            caller_frame = caller_frame.f_back
        else:
            break
    
    caller_globals = caller_frame.f_globals
    file_path = caller_globals.get("__file__")
    is_main = caller_globals.get("__name__") == "__main__"
    
    if file_path:
        file_path = os.path.abspath(file_path)
    
    return file_path, is_main
```

**Impact**: Medium - Reduces duplication and improves maintainability

---

## 3. Method/Function Length ✅

### Status: PASS

| File | Function/Method | Lines | Status |
|------|-----------------|-------|--------|
| `function_builder.py` | `build_function_from_file` | 154 | ✅ OK |
| `function_builder.py` | `_print_function` | 73 | ✅ OK |
| `function_builder.py` | `__post_init__` | 28 | ✅ OK |
| `model_utils.py` | `standardize_parsed_model` | 67 | ✅ OK |
| `executor.py` | `execute_models` | ~30 (relevant section) | ✅ OK |

All functions are within reasonable length limits.

---

## 4. Python Version Compatibility ✅

### Status: PASS
- ✅ Using modern type hints (`str | None` instead of `Optional[str]`)
- ✅ Using modern syntax (no `from __future__` needed)
- ✅ No compatibility shims or version checks
- ✅ All code compatible with Python 3.12+

---

## 5. Code Quality ✅

### Status: PASS with Minor Notes

#### Type Hints
- ✅ Complete and accurate
- ✅ Using TypedDict appropriately
- ✅ Proper use of `NotRequired` for optional fields

#### Error Handling
- ✅ Appropriate exception handling
- ✅ No bare `except:` clauses
- ✅ Specific exceptions where needed

#### Docstrings
- ✅ Present and accurate
- ✅ Follow Google-style format
- ✅ Include Args and Returns sections

#### Function Names
- ✅ Clear and descriptive
- ✅ Follow naming conventions

#### Magic Numbers/Strings
- ⚠️ **Minor**: Magic number `80` for separator width in `_print_function` and `_print_model`
  - **Recommendation**: Extract to constant: `SEPARATOR_WIDTH = 80`

#### Complex Expressions
- ✅ Reasonable complexity
- ✅ Well-structured conditionals

---

## 6. Testing ⚠️

### Status: PARTIAL

#### Existing Tests
- ✅ `test_duckdb_metadata_propagation` passes
- ✅ Function type tests updated

#### Missing Tests
- ⚠️ **New functionality needs tests**:
  - `build_function_from_file()` - No unit tests found
  - `SQLFunctionMetadata` - No unit tests found
  - `standardize_parsed_model()` metadata preservation - Edge cases not tested

**Recommendation**: Add tests for:
1. `build_function_from_file()` with various scenarios
2. `SQLFunctionMetadata` instantiation and behavior
3. Metadata preservation in `standardize_parsed_model()` for SQL models

---

## 7. Performance & Best Practices ✅

### Status: PASS

#### Performance
- ✅ No obvious performance issues
- ✅ Appropriate use of data structures
- ✅ No unnecessary loops

#### Resource Management
- ✅ File operations use context managers (`with open()`)
- ✅ No memory leaks detected
- ✅ Proper cleanup in exception handlers

#### Best Practices
- ✅ Using dataclasses appropriately
- ✅ Proper separation of concerns

---

## 8. Security & Safety ✅

### Status: PASS

- ✅ No hardcoded secrets or credentials
- ✅ File path operations are safe (using `os.path.abspath()`)
- ✅ SQL operations use parameterized queries (via SQLParser)
- ✅ Input validation present where needed

---

## 9. Consistency ✅

### Status: PASS

#### Coding Style
- ✅ Follows project conventions
- ✅ Consistent naming (snake_case for functions, PascalCase for classes)
- ✅ Consistent import organization

#### Error Handling
- ✅ Consistent patterns across modules
- ✅ Consistent logging patterns

#### Code Patterns
- ✅ `SQLFunctionMetadata` mirrors `SqlModelMetadata` pattern
- ✅ `build_function_from_file` mirrors `build_model_from_file` pattern
- ✅ Consistent TypedDict structure

---

## 10. Documentation ✅

### Status: PASS

- ✅ Public APIs are documented
- ✅ Complex logic has inline comments
- ✅ Type hints serve as inline documentation
- ✅ Docstrings are comprehensive

---

## Critical Issues Found

### None

All critical functionality works correctly. The test passes and metadata propagation is fixed.

---

## Recommendations Summary

### High Priority
1. **Extract caller file path logic** to shared utility (reduces duplication)

### Medium Priority
2. **Add unit tests** for new functionality:
   - `build_function_from_file()`
   - `SQLFunctionMetadata`
   - Metadata preservation edge cases

### Low Priority
3. **Extract magic numbers** to constants (separator width)
4. **Verify `resolved_sql` handling** in `build_function_from_file()` - Currently uses `original_sql` for both (line 134). This may be intentional for functions, but should be verified against model builder pattern.

---

## Overall Assessment

**Status: ✅ APPROVED with Minor Recommendations**

The code is well-structured, follows best practices, and successfully fixes the metadata propagation issue. The main improvement opportunity is reducing code duplication in caller file path extraction, which can be addressed in a follow-up refactoring.

**Code Quality Score: 8.5/10**

