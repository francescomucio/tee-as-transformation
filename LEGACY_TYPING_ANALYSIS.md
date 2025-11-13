# Legacy Typing Support Analysis

## Answer: **No Legacy Typing Support**

The project **does not support legacy typing** and requires Python 3.14+, which fully supports modern typing syntax.

## Current State

### Python Version Requirements
- **`requires-python = ">=3.14"`** (from `pyproject.toml`)
- **Ruff target**: `target-version = "py314"`
- **No compatibility shims** or version checks

### Modern Typing Features Used
✅ **Built-in generics**: `dict[str, int]`, `list[str]` (not `Dict`, `List`)  
✅ **Union syntax**: `str | None` (not `Optional[str]`)  
✅ **Type parameters**: `[T]` syntax (Python 3.12+)  
✅ **No `from __future__ import annotations`** needed

## Legacy Typing Patterns Found

Despite requiring Python 3.14, there are **inconsistencies** where legacy patterns are still used:

### 1. Production Code

**File**: `tee/parser/parsers/function_sql_parser/__init__.py`
```python
from typing import Any, Dict, Optional  # ❌ Legacy
```

**Files**: `tee/adapters/bigquery/adapter.py` (4 occurrences)
```python
Optional[Dict[str, Any]]  # ❌ Legacy
```

### 2. Test Code

Multiple test files use legacy patterns:
- `Dict[str, ParsedModel]` instead of `dict[str, Model]`
- `from typing import Dict, List, Optional`
- `List[...]` instead of `list[...]`

### 3. Documentation Examples

Some docs show legacy patterns in examples.

## Why This Matters

### Ruff Configuration
- **`UP` (pyupgrade) rules enabled** - Should catch legacy typing
- **Target version**: `py314` - Fully supports modern syntax
- **No exceptions** for legacy typing

### Impact
- **Low risk**: Functionally equivalent, but inconsistent
- **Code quality**: Mixed patterns reduce clarity
- **Maintenance**: Should be modernized for consistency

## Recommendations

### 1. Fix Production Code (High Priority)
```python
# Before
from typing import Any, Dict, Optional
def func(x: Optional[Dict[str, Any]]) -> None: ...

# After
from typing import Any
def func(x: dict[str, Any] | None) -> None: ...
```

### 2. Modernize Test Code (Medium Priority)
```python
# Before
from typing import Dict
parsed_models: Dict[str, ParsedModel] = {...}

# After
from tee.typing import Model
parsed_models: dict[str, Model] = {...}
```

### 3. Update Documentation (Low Priority)
- Ensure examples use modern typing
- Update contributing guide if needed

## Automated Fix

Ruff can automatically fix most of these:

```bash
# Check for legacy typing
ruff check --select UP

# Auto-fix (safe)
ruff check --select UP --fix

# Auto-fix (including unsafe)
ruff check --select UP --fix --unsafe-fixes
```

## Summary

| Aspect | Status |
|--------|--------|
| **Legacy typing support** | ❌ None - Python 3.14+ only |
| **Compatibility shims** | ❌ None |
| **Legacy patterns in code** | ⚠️ Some inconsistencies found |
| **Should be fixed** | ✅ Yes - for consistency |

**Conclusion**: The project doesn't support legacy typing, but has some legacy patterns that should be modernized for consistency.

