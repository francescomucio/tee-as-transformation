# Test Typing Review & Recommendations

## Current State

### ✅ What's Working Well

1. **Metadata Types**: Tests correctly use `ModelMetadata` and `FunctionMetadata` from `tee.typing.metadata`
2. **Type Tests**: `tests/typing/test_function_metadata.py` properly tests the consolidated types
3. **Backward Compatibility**: `ParsedModel` and `ParsedFunction` aliases work correctly

### ⚠️ Areas for Improvement

## 1. Inconsistent Type Imports

### Issue
Tests import `ParsedModel` and `ParsedFunction` from `tee.parser.shared.types` instead of using `Model` and `Function` directly from `tee.typing`.

**Current Pattern:**
```python
from tee.parser.shared.types import ParsedModel, ParsedFunction
```

**Recommended Pattern:**
```python
from tee.typing import Model, Function
```

### Files Affected
- `tests/parser/output/test_ots_function_export.py`
- `tests/parser/output/test_ots_tags.py`
- `tests/parser/output/test_json_exporter_yaml.py`
- `tests/parser/integration/test_function_end_to_end.py`
- `tests/parser/integration/test_ots_round_trip.py`
- `tests/parser/analysis/test_function_dependency_graph.py`
- `tests/parser/core/test_function_orchestration.py`

### Impact
- **Low**: Functionally equivalent (aliases work), but less clear
- **Medium**: Inconsistent with production code patterns
- **Low**: Makes it harder to see that these are the same types

## 2. Using Old-Style Type Hints

### Issue
Tests use `Dict[str, ParsedModel]` instead of modern `dict[str, Model]`.

**Current:**
```python
parsed_models: Dict[str, ParsedModel] = {...}
```

**Recommended:**
```python
parsed_models: dict[str, Model] = {...}
```

### Files Affected
- `tests/parser/output/test_ots_tags.py` (5 occurrences)
- `tests/parser/output/test_ots_function_export.py` (13 occurrences)
- `tests/parser/output/test_json_exporter_yaml.py` (1 occurrence)

### Impact
- **Low**: Works but not modern Python style
- **Low**: Inconsistent with codebase (uses `dict` not `Dict`)

## 3. Missing Type Hints in Test Fixtures

### Issue
Metadata fixtures in `tests/adapters/fixtures/metadata_fixtures.py` don't have type hints.

**Current:**
```python
USERS_METADATA = {
    "schema": [...]
}
```

**Recommended:**
```python
from tee.typing import ModelMetadata

USERS_METADATA: ModelMetadata = {
    "schema": [...]
}
```

### Impact
- **Medium**: No type checking for test data
- **Medium**: Could catch errors in test fixtures
- **Low**: Less IDE support

## 4. Test Fixture Return Types

### Issue
Test fixtures that return models/functions don't have explicit return types.

**Example:**
```python
@pytest.fixture
def sample_function(self) -> ParsedFunction:  # Uses ParsedFunction instead of Function
    return {...}
```

**Recommended:**
```python
from tee.typing import Function

@pytest.fixture
def sample_function(self) -> Function:
    return {...}
```

### Impact
- **Low**: Works but inconsistent
- **Low**: Less clear that it's the same type

## Recommendations

### Priority 1: High Impact, Low Effort

1. **Add type hints to metadata fixtures**
   - File: `tests/adapters/fixtures/metadata_fixtures.py`
   - Add `ModelMetadata` type hints to all metadata constants
   - **Benefit**: Type checking for test data, catches errors early

### Priority 2: Medium Impact, Medium Effort

2. **Update type imports in test files**
   - Replace `from tee.parser.shared.types import ParsedModel, ParsedFunction`
   - With `from tee.typing import Model, Function`
   - Update all type annotations to use `Model` and `Function`
   - **Benefit**: Consistency with production code, clearer intent

3. **Modernize type hints**
   - Replace `Dict[str, ParsedModel]` with `dict[str, Model]`
   - Replace `List[...]` with `list[...]` (if any)
   - **Benefit**: Modern Python style, consistent with codebase

### Priority 3: Low Impact, Low Effort

4. **Update fixture return types**
   - Change `-> ParsedFunction` to `-> Function`
   - Change `-> ParsedModel` to `-> Model`
   - **Benefit**: Consistency

## Implementation Plan

### Step 1: Fix Metadata Fixtures (Quick Win)
```python
# tests/adapters/fixtures/metadata_fixtures.py
from tee.typing import ModelMetadata

USERS_METADATA: ModelMetadata = {
    "schema": [...]
}
```

### Step 2: Update Test Imports (Bulk Change)
```python
# Before
from tee.parser.shared.types import ParsedModel, ParsedFunction

# After
from tee.typing import Model, Function
```

### Step 3: Update Type Annotations
```python
# Before
parsed_models: Dict[str, ParsedModel] = {...}

# After
parsed_models: dict[str, Model] = {...}
```

## Summary

**Current Status**: ✅ Functionally correct, but inconsistent

**Recommended Changes**:
1. Add type hints to metadata fixtures (high value, low effort)
2. Update imports to use `Model`/`Function` directly (consistency)
3. Modernize type hints to use `dict`/`list` (modern Python)

**Estimated Impact**: 
- Better type safety
- Improved consistency
- Better IDE support
- Easier maintenance

**Risk**: Low - All changes are type-only, no functional changes

