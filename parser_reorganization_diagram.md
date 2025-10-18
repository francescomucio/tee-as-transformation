# Parser Module Reorganization

## Current Structure (Issues)
```
tee/parser/
├── __init__.py                    # Only exports ProjectParser
├── core.py                       # 300 lines - does everything
├── sql_parser.py                 # Thin wrapper around sqlglot_parser
├── sqlglot_parser.py             # Core SQL parsing logic
├── python_parser.py              # 359 lines - Python model parsing
├── dependency_graph.py           # Graph building
├── visualization.py              # Output generation
├── utils.py                      # TableNameGenerator
├── qualified_sql.py              # SQL qualification
├── sql_variable_substitution.py  # Variable handling
└── model_decorator.py            # Python decorators
```

**Problems:**
- ❌ Mixed abstraction levels
- ❌ Tight coupling between files
- ❌ Large files with multiple responsibilities
- ❌ Inconsistent naming (sql_parser vs sqlglot_parser)
- ❌ No clear layer separation

## Proposed Structure (Improved)
```
tee/parser/
├── __init__.py                   # Clean public API
├── core/                        # Core orchestration layer
│   ├── __init__.py
│   ├── project_parser.py        # Simplified main class
│   └── orchestrator.py          # High-level coordination
├── parsers/                     # Parsing layer
│   ├── __init__.py
│   ├── base.py                  # Abstract base parser
│   ├── sql_parser.py            # Unified SQL parsing
│   ├── python_parser.py         # Python model parsing
│   └── parser_factory.py        # Parser creation
├── analysis/                    # Analysis layer
│   ├── __init__.py
│   ├── dependency_graph.py      # Dependency analysis
│   ├── table_resolver.py        # Table name resolution
│   └── sql_qualifier.py         # SQL qualification
├── processing/                  # Processing layer
│   ├── __init__.py
│   ├── variable_substitution.py # SQL variable handling
│   ├── model_decorator.py       # Python decorators
│   └── file_discovery.py        # File finding logic
├── output/                      # Output layer
│   ├── __init__.py
│   ├── visualizer.py            # Visualization
│   ├── json_exporter.py         # JSON export
│   └── report_generator.py      # Reports
└── shared/                      # Shared utilities
    ├── __init__.py
    ├── types.py                 # Type definitions
    ├── exceptions.py            # Custom exceptions
    └── constants.py             # Constants
```

**Benefits:**
- ✅ Clear separation of concerns
- ✅ Consistent abstraction levels
- ✅ Loose coupling between layers
- ✅ Smaller, focused files
- ✅ Better testability
- ✅ Easier to extend

## Migration Strategy

### Phase 1: Create new structure
1. Create new directory structure
2. Move files to appropriate layers
3. Update imports

### Phase 2: Refactor large files
1. Split `core.py` into `project_parser.py` + `orchestrator.py`
2. Split `python_parser.py` into focused components
3. Merge `sql_parser.py` + `sqlglot_parser.py`

### Phase 3: Clean up
1. Remove old files
2. Update `__init__.py` files
3. Update tests
4. Update documentation

## Key Improvements

1. **Layer-based architecture**: Clear separation between parsing, analysis, processing, and output
2. **Single responsibility**: Each file has one clear purpose
3. **Dependency direction**: Dependencies flow downward (core → parsers → analysis → processing)
4. **Consistent naming**: All files follow the same naming convention
5. **Better testability**: Smaller, focused components are easier to test
6. **Easier maintenance**: Changes are isolated to specific layers
