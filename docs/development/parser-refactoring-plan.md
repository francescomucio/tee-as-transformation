# Parser Module Refactoring Plan

## Overview

Several files in the parser module are quite long (300-640 lines) and could benefit from refactoring to improve maintainability. This document outlines a plan to break down these files into smaller, more focused modules.

## Files to Refactor

### 1. `function_sql_parser.py` (641 lines, 11 methods)
**Current Issues:**
- Mixes multiple responsibilities: parsing, dialect inference, metadata merging
- Large methods: `_parse_create_function_sqlglot` (101 lines), `parse` (80 lines)
- Complex regex parsing logic mixed with SQLglot parsing

**Proposed Structure:**
```
tee/parser/parsers/functions/
├── __init__.py
├── sql_parser.py              # Main FunctionSQLParser class (orchestration)
├── dialect_inference.py      # Dialect inference logic
├── sqlglot_parser.py         # SQLglot-based parsing
├── regex_parser.py           # Regex fallback parsing
├── parameter_parser.py       # Parameter parsing logic
└── dependency_extractor.py   # Dependency extraction
```

**Benefits:**
- Clear separation of concerns
- Easier to test individual components
- Can swap parsing strategies without touching main class
- Dialect inference can be reused by other parsers

---

### 2. `function_python_parser.py` (362 lines, 10 methods)
**Current Issues:**
- AST extraction logic mixed with parsing logic
- Metadata parsing could be separate
- Function signature extraction is complex

**Proposed Structure:**
```
tee/parser/parsers/functions/
├── python_parser.py          # Main FunctionPythonParser class
├── ast_extractor.py          # AST value extraction utilities
├── metadata_parser.py        # Metadata-only file parsing
└── signature_extractor.py    # Function signature extraction
```

**Benefits:**
- AST utilities can be reused
- Metadata parsing logic is isolated
- Easier to extend with new AST node types

---

### 3. `ots_transformer.py` (542 lines, 14 methods)
**Current Issues:**
- Multiple transformation concerns in one class
- Test transformation is complex
- Schema inference is separate concern

**Proposed Structure:**
```
tee/parser/output/ots/
├── __init__.py
├── transformer.py             # Main OTSTransformer (orchestration)
├── module_builder.py         # OTS module creation
├── test_transformer.py       # Test transformation logic
├── schema_inferencer.py      # Schema inference from SQL
└── tag_merger.py             # Tag merging utilities
```

**Benefits:**
- Test transformation can evolve independently
- Schema inference can be improved/tested separately
- Tag merging logic can be reused

---

### 4. `python_parser.py` (428 lines, 10 methods)
**Current Issues:**
- Function execution mixed with parsing
- Model metadata extraction could be separate

**Proposed Structure:**
```
tee/parser/parsers/
├── python_parser.py          # Main PythonParser (orchestration)
├── model_metadata.py         # Model metadata extraction
└── function_executor.py      # Function execution logic
```

**Benefits:**
- Execution logic separated from parsing
- Metadata extraction can be tested independently
- Easier to add execution strategies

---

## Refactoring Strategy

### Phase 1: Extract Utilities (Low Risk)
1. **Dialect Inference** → `tee/parser/shared/dialect_inference.py`
   - Used by multiple parsers
   - Low risk, high reuse value

2. **AST Extractor** → `tee/parser/shared/ast_extractor.py`
   - Generic AST utilities
   - Can be used by both model and function parsers

3. **Parameter Parser** → `tee/parser/parsers/functions/parameter_parser.py`
   - Reusable for SQL function parsing
   - Clear, isolated responsibility

### Phase 2: Split Parsing Strategies (Medium Risk)
1. **SQL Function Parsing**
   - Extract SQLglot parser
   - Extract regex parser
   - Keep main class as orchestrator

2. **Python Function Parsing**
   - Extract metadata parser
   - Extract signature extractor
   - Keep main class as orchestrator

### Phase 3: Split Output Transformers (Low Risk)
1. **OTS Transformer**
   - Extract test transformer
   - Extract schema inferencer
   - Extract tag merger

### Phase 4: Split Model Parsing (Medium Risk)
1. **Python Parser**
   - Extract function executor
   - Extract model metadata extractor

---

## Implementation Guidelines

### 1. Maintain Backward Compatibility
- Keep existing public APIs unchanged
- Use internal refactoring (private methods → separate modules)
- Update imports gradually

### 2. Testing Strategy
- Extract tests for each new module
- Ensure existing tests still pass
- Add integration tests for refactored components

### 3. Code Organization
```
tee/parser/
├── parsers/
│   ├── functions/          # NEW: Function parsing subpackage
│   │   ├── __init__.py
│   │   ├── sql_parser.py
│   │   ├── python_parser.py
│   │   ├── dialect_inference.py
│   │   ├── sqlglot_parser.py
│   │   ├── regex_parser.py
│   │   ├── parameter_parser.py
│   │   └── dependency_extractor.py
│   ├── sql_parser.py       # Keep for models
│   ├── python_parser.py    # Keep for models
│   └── ...
├── shared/
│   ├── ast_extractor.py    # NEW: Shared AST utilities
│   └── ...
└── output/
    └── ots/                # NEW: OTS transformation subpackage
        ├── __init__.py
        ├── transformer.py
        ├── test_transformer.py
        ├── schema_inferencer.py
        └── tag_merger.py
```

### 4. Migration Path
1. Create new modules alongside existing files
2. Move code gradually, keeping old methods as wrappers
3. Update tests to use new modules
4. Remove old code after full migration
5. Update documentation

---

## Priority Order

### High Priority (Do First)
1. **Dialect Inference** - Used by multiple parsers, easy to extract
2. **Parameter Parser** - Clear, isolated responsibility
3. **AST Extractor** - Reusable utility

### Medium Priority
1. **SQL Function Parsing Strategies** - Improves testability
2. **OTS Test Transformer** - Complex logic that benefits from isolation

### Low Priority (Can Wait)
1. **Python Parser Splits** - Already reasonably organized
2. **Schema Inferencer** - Less frequently changed

---

## Example: Refactoring `function_sql_parser.py`

### Before (641 lines)
```python
class FunctionSQLParser(BaseParser):
    def parse(self, ...):
        # 80 lines of orchestration
    
    def _parse_create_function_sqlglot(self, ...):
        # 101 lines of SQLglot parsing
    
    def _parse_create_function_regex(self, ...):
        # 63 lines of regex parsing
    
    def _parse_parameters(self, ...):
        # 35 lines of parameter parsing
    
    def _extract_dependencies(self, ...):
        # 44 lines of dependency extraction
```

### After (Split into modules)
```python
# tee/parser/parsers/functions/sql_parser.py
class FunctionSQLParser(BaseParser):
    def __init__(self, ...):
        self._sqlglot_parser = SQLglotFunctionParser()
        self._regex_parser = RegexFunctionParser()
        self._dialect_inferrer = DialectInferrer(connection, project_config)
        self._parameter_parser = ParameterParser()
        self._dependency_extractor = DependencyExtractor()
    
    def parse(self, ...):
        # ~30 lines of orchestration
        dialect = self._dialect_inferrer.infer(...)
        function_data = self._sqlglot_parser.parse(...) or self._regex_parser.parse(...)
        # ... merge and return

# tee/parser/parsers/functions/sqlglot_parser.py
class SQLglotFunctionParser:
    def parse(self, content, dialect):
        # 101 lines of SQLglot-specific logic

# tee/parser/parsers/functions/regex_parser.py
class RegexFunctionParser:
    def parse(self, content):
        # 63 lines of regex-specific logic

# tee/parser/shared/dialect_inference.py
class DialectInferrer:
    def infer(self, connection, project_config, filename, metadata):
        # Dialect inference logic
```

---

## Benefits Summary

1. **Maintainability**: Smaller files are easier to understand and modify
2. **Testability**: Isolated components are easier to test
3. **Reusability**: Shared utilities can be used across modules
4. **Readability**: Clear separation of concerns
5. **Extensibility**: Easy to add new parsing strategies or transformers

---

## Next Steps

1. Review and approve this plan
2. Start with Phase 1 (low-risk utilities)
3. Create PRs for each phase
4. Update tests as we go
5. Document new module structure

