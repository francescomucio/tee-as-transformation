# OTS Transformer Refactoring - 3 Approaches

## Current State Analysis

**File:** `tee/parser/output/ots_transformer.py`
- **Lines:** 758
- **Methods:** 18
- **Longest methods:**
  - `_transform_function`: 90 lines
  - `_create_ots_module`: 80 lines
  - `_infer_schema_from_sql`: 53 lines
  - `_transform_tests`: 52 lines

**Key Responsibilities:**
1. Module orchestration (grouping, assembly)
2. Model transformation (schema, materialization, tests, tags)
3. Function transformation (code, dependencies, tags)
4. Schema inference (SQL parsing, datatype inference)
5. Tag management (merging, extraction for models and functions)
6. Test transformation (column and table tests)

**Issues:**
- Duplication: Tag merging/extraction logic duplicated for models and functions
- Mixed concerns: Schema inference mixed with transformation
- Large methods: Several methods over 50 lines
- Hard to test: Complex methods with multiple responsibilities

---

## Approach 1: Feature-Based Split (Recommended ⭐)

**Philosophy:** Split by transformation type - each transformer handles one entity type.

### Structure

```
tee/parser/output/ots/
├── __init__.py
├── transformer.py              # Main orchestrator (~200 lines)
├── module_builder.py           # Module assembly (~100 lines)
├── model_transformer.py         # Model → OTS Transformation (~200 lines)
├── function_transformer.py      # Function → OTS Function (~150 lines)
├── schema_inferencer.py         # Schema inference from SQL (~100 lines)
├── test_transformer.py          # Test transformation (~100 lines)
└── tag_manager.py              # Shared tag management (~100 lines)
```

### Details

**1. `transformer.py` (Main Orchestrator)**
```python
class OTSTransformer:
    """Main orchestrator for OTS transformation."""
    
    def __init__(self, project_config):
        self.project_config = project_config
        self.model_transformer = ModelTransformer(project_config)
        self.function_transformer = FunctionTransformer(project_config)
        self.module_builder = ModuleBuilder(project_config)
    
    def transform_to_ots_modules(self, parsed_models, parsed_functions, test_library_path):
        # Grouping logic
        # Delegate to module_builder
```

**2. `model_transformer.py`**
```python
class ModelTransformer:
    """Transforms parsed models to OTS transformations."""
    
    def transform(self, model_id, model_data, schema):
        # Transform model structure
        # Delegate to schema_inferencer, test_transformer, tag_manager
```

**3. `function_transformer.py`**
```python
class FunctionTransformer:
    """Transforms parsed functions to OTS functions."""
    
    def transform(self, function_id, function_data, schema):
        # Transform function structure
        # Delegate to tag_manager
```

**4. `tag_manager.py` (Shared)**
```python
class TagManager:
    """Manages tag merging and extraction for models and functions."""
    
    def merge_tags(self, module_tags, entity_tags):
        # Unified tag merging logic
    
    def extract_object_tags(self, metadata):
        # Unified object tag extraction
```

**5. `schema_inferencer.py`**
```python
class SchemaInferencer:
    """Infers schema from SQL using sqlglot."""
    
    def infer_from_sql(self, model_data):
        # SQL parsing and schema inference
    
    def infer_datatype(self, col_expr):
        # Datatype inference
```

**6. `test_transformer.py`**
```python
class TestTransformer:
    """Transforms tests to OTS format."""
    
    def transform(self, model_data):
        # Extract and normalize tests
```

**7. `module_builder.py`**
```python
class ModuleBuilder:
    """Builds OTS modules from transformations and functions."""
    
    def build(self, module_name, schema, models, functions, test_library_path):
        # Assemble module structure
```

### Pros
- ✅ Clear separation by entity type
- ✅ Easy to find code (model logic in model_transformer, etc.)
- ✅ Reusable components (tag_manager shared)
- ✅ Testable: Each transformer can be tested independently
- ✅ Scalable: Easy to add new entity types

### Cons
- ⚠️ More files to navigate
- ⚠️ Need to manage dependencies between transformers

### File Sizes After Refactoring
- `transformer.py`: ~200 lines
- `model_transformer.py`: ~200 lines
- `function_transformer.py`: ~150 lines
- `schema_inferencer.py`: ~100 lines
- `test_transformer.py`: ~100 lines
- `tag_manager.py`: ~100 lines
- `module_builder.py`: ~100 lines

**Total:** ~950 lines (slight increase due to imports/docstrings, but much better organized)

---

## Approach 2: Component-Based Split (Selected ⭐)

**Philosophy:** Split by component/concern - transformers, taggers, inferencers, builders.

### Structure

```
tee/parser/output/ots/
├── __init__.py                 # Public API exports
├── transformer.py              # Main orchestrator (~150 lines)
├── transformers/
│   ├── __init__.py
│   ├── base.py                 # Base transformer class (~50 lines)
│   ├── model_transformer.py    # Model transformation (~250 lines)
│   └── function_transformer.py  # Function transformation (~200 lines)
├── taggers/
│   ├── __init__.py
│   └── tag_manager.py          # Tag merging/extraction (~150 lines)
├── inferencers/
│   ├── __init__.py
│   └── schema_inferencer.py    # Schema inference (~100 lines)
├── builders/
│   ├── __init__.py
│   └── module_builder.py       # Module assembly (~100 lines)
└── utils/
    ├── __init__.py
    ├── grouping.py             # Schema grouping utilities (~50 lines)
    └── dialect_inference.py    # SQL dialect inference (~30 lines)
```

### Details

**1. `transformer.py` (Main Orchestrator)**
```python
class OTSTransformer:
    """Main orchestrator for OTS transformation."""
    
    def __init__(self, project_config):
        self.project_config = project_config
        self.database = project_config.get("project_folder", "unknown")
        self.sql_dialect = infer_sql_dialect(project_config)
        
        # Initialize components
        self.model_transformer = ModelTransformer(project_config, self.sql_dialect)
        self.function_transformer = FunctionTransformer(project_config)
        self.module_builder = ModuleBuilder(project_config, self.database, self.sql_dialect)
    
    def transform_to_ots_modules(self, parsed_models, parsed_functions, test_library_path):
        # Group by schema using utils.grouping
        models_by_schema = group_models_by_schema(parsed_models)
        functions_by_schema = group_functions_by_schema(parsed_functions or {})
        
        # Build modules
        return self.module_builder.build_modules(
            models_by_schema, functions_by_schema, test_library_path,
            self.model_transformer, self.function_transformer
        )
```
- Coordinates all components
- Handles high-level orchestration
- Delegates grouping to utils
- Delegates module building to ModuleBuilder

**2. `transformers/base.py`**
```python
class BaseTransformer(ABC):
    """Base class for transformation strategies."""
    
    def __init__(self, project_config):
        self.project_config = project_config
        self.tag_manager = TagManager(project_config)
    
    @abstractmethod
    def transform(self, entity_id, entity_data, schema):
        pass
```
- Common base class for transformers
- Shared initialization
- Common tag manager access

**3. `transformers/model_transformer.py`**
```python
class ModelTransformer(BaseTransformer):
    """Transforms parsed models to OTS transformations."""
    
    def __init__(self, project_config, sql_dialect):
        super().__init__(project_config)
        self.sql_dialect = sql_dialect
        self.schema_inferencer = SchemaInferencer()
        self.test_transformer = TestTransformer()
    
    def transform(self, model_id, model_data, schema):
        # Transform model structure
        # Use schema_inferencer for schema
        # Use test_transformer for tests
        # Use tag_manager for tags
```
- All model transformation logic
- Uses inferencers, test transformer, tag manager
- Handles schema, materialization, tests, tags

**4. `transformers/function_transformer.py`**
```python
class FunctionTransformer(BaseTransformer):
    """Transforms parsed functions to OTS functions."""
    
    def transform(self, function_id, function_data, schema):
        # Transform function structure
        # Use tag_manager for tags
```
- All function transformation logic
- Uses tag manager
- Handles code, dependencies, parameters, tags

**5. `taggers/tag_manager.py`**
```python
class TagManager:
    """Unified tag management for models and functions."""
    
    def __init__(self, project_config):
        self.project_config = project_config
        self._module_tags = self._extract_module_tags()
    
    def merge_tags(self, entity_tags):
        """Merge module tags with entity-specific tags."""
    
    def extract_object_tags(self, metadata):
        """Extract and validate object tags."""
```
- Unified tag management
- Works for both models and functions
- Handles module-level tag extraction
- Validates tag formats

**6. `inferencers/schema_inferencer.py`**
```python
class SchemaInferencer:
    """Infers schema from SQL using sqlglot."""
    
    def infer_from_sql(self, model_data):
        """Infer schema from SQL query."""
    
    def infer_datatype(self, col_expr):
        """Infer OTS datatype from SQL column expression."""
```
- Schema inference from SQL
- Datatype inference
- SQLglot integration
- Independent utility (no project config needed)

**7. `builders/module_builder.py`**
```python
class ModuleBuilder:
    """Builds OTS modules from transformations and functions."""
    
    def __init__(self, project_config, database, sql_dialect):
        self.project_config = project_config
        self.database = database
        self.sql_dialect = sql_dialect
    
    def build_modules(self, models_by_schema, functions_by_schema, 
                     test_library_path, model_transformer, function_transformer):
        """Build OTS modules for all schemas."""
    
    def build_module(self, module_name, schema, models, functions, test_library_path,
                    model_transformer, function_transformer):
        """Build a single OTS module."""
```
- Module assembly logic
- OTS version determination (0.1.0 vs 0.2.0)
- Target configuration
- Module-level metadata

**8. `utils/grouping.py`**
```python
def group_models_by_schema(parsed_models):
    """Group models by schema."""
    
def group_functions_by_schema(parsed_functions):
    """Group functions by schema."""
```
- Pure utility functions
- No dependencies on project config
- Reusable grouping logic

**9. `utils/dialect_inference.py`**
```python
def infer_sql_dialect(project_config):
    """Infer SQL dialect from connection type."""
```
- Pure utility function
- No class needed
- Simple mapping logic

### Pros
- ✅ Clear component boundaries
- ✅ Easy to understand component roles
- ✅ Good for team organization (different developers work on different components)
- ✅ Components can be swapped/replaced independently
- ✅ Logical grouping (all transformers together, all taggers together)
- ✅ Easy to extend (add new transformer type, new inferencer, etc.)

### Cons
- ⚠️ More nested structure (subdirectories)
- ⚠️ Some components might be thin (e.g., builders, utils)
- ⚠️ Need to manage imports across subdirectories

### Additional Considerations

**1. Test Organization**
```
tests/parser/output/ots/
├── test_transformer.py         # Test main orchestrator
├── transformers/
│   ├── test_model_transformer.py
│   └── test_function_transformer.py
├── taggers/
│   └── test_tag_manager.py
├── inferencers/
│   └── test_schema_inferencer.py
└── builders/
    └── test_module_builder.py
```
- Mirror the source structure
- Each component tested independently
- Integration tests in main test_transformer.py

**2. Import Structure**
```python
# tee/parser/output/ots/__init__.py
from .transformer import OTSTransformer
from .transformers import ModelTransformer, FunctionTransformer
from .taggers import TagManager
from .inferencers import SchemaInferencer
from .builders import ModuleBuilder

__all__ = [
    "OTSTransformer",
    "ModelTransformer",
    "FunctionTransformer",
    "TagManager",
    "SchemaInferencer",
    "ModuleBuilder",
]
```

**3. Shared Utilities**
- `utils/grouping.py`: Pure functions, no state
- `utils/dialect_inference.py`: Pure function, no state
- Can be easily tested and reused

**4. Component Dependencies**
```
transformer.py
  ├── transformers/ (model, function)
  │     ├── taggers/ (tag_manager)
  │     ├── inferencers/ (schema_inferencer)
  │     └── builders/ (module_builder)
  └── utils/ (grouping, dialect_inference)
```
- Clear dependency hierarchy
- No circular dependencies
- Easy to understand data flow

**5. Backward Compatibility**
- Keep old `OTSTransformer` class as thin wrapper initially
- Gradually migrate callers to new structure
- Maintain same public API during transition

**6. Materialization Logic**
- Currently in `_transform_materialization()` and `_transform_incremental_details()`
- Consider: Extract to `transformers/materialization_transformer.py` if it grows
- For now: Keep in `model_transformer.py` (it's model-specific)

**7. Test Transformation**
- Currently in `_transform_tests()`
- Consider: Extract to `transformers/test_transformer.py` if it grows
- For now: Keep as helper in `model_transformer.py` or extract to separate class

### File Sizes After Refactoring
- `transformer.py`: ~150 lines
- `transformers/base.py`: ~50 lines
- `transformers/model_transformer.py`: ~250 lines
- `transformers/function_transformer.py`: ~200 lines
- `taggers/tag_manager.py`: ~150 lines
- `inferencers/schema_inferencer.py`: ~100 lines
- `builders/module_builder.py`: ~100 lines
- `utils/grouping.py`: ~50 lines
- `utils/dialect_inference.py`: ~30 lines

**Total:** ~1,080 lines (includes imports, docstrings, but better organized)

### Implementation Order (Recommended)

1. **Extract utilities first** (no dependencies)
   - `utils/dialect_inference.py` - Simple function extraction
   - `utils/grouping.py` - Grouping logic extraction

2. **Extract independent components**
   - `inferencers/schema_inferencer.py` - No project config needed
   - `taggers/tag_manager.py` - Used by both transformers

3. **Extract transformers**
   - `transformers/base.py` - Base class
   - `transformers/function_transformer.py` - Simpler, fewer dependencies
   - `transformers/model_transformer.py` - More complex, uses all other components

4. **Extract builders**
   - `builders/module_builder.py` - Orchestrates transformers

5. **Refactor main transformer**
   - `transformer.py` - Becomes thin orchestrator

### Migration Strategy

**Phase 1: Extract Utilities (No Breaking Changes)**
- Create `utils/` directory
- Move grouping and dialect inference
- Update imports in main file
- Run tests to verify

**Phase 2: Extract Components (Backward Compatible)**
- Create component directories
- Move code to new modules
- Keep old methods as wrappers calling new code
- Update tests incrementally

**Phase 3: Refactor Main Class (Breaking Changes)**
- Update `OTSTransformer` to use new components
- Update all callers
- Remove old code
- Final test verification

**Phase 4: Cleanup**
- Remove wrapper methods
- Update documentation
- Add integration tests

---

## Approach 3: Strategy Pattern with Mixins

**Philosophy:** Use strategy pattern for transformation types, mixins for shared behavior.

### Structure

```
tee/parser/output/ots/
├── __init__.py
├── transformer.py              # Main orchestrator (~150 lines)
├── strategies/
│   ├── __init__.py
│   ├── base.py                 # Base transformation strategy (~100 lines)
│   ├── model_strategy.py       # Model transformation strategy (~200 lines)
│   └── function_strategy.py    # Function transformation strategy (~150 lines)
├── mixins/
│   ├── __init__.py
│   ├── taggable.py             # Tag management mixin (~100 lines)
│   └── testable.py             # Test transformation mixin (~100 lines)
└── utils/
    ├── __init__.py
    ├── schema_inferencer.py    # Schema inference (~100 lines)
    └── module_builder.py       # Module assembly (~100 lines)
```

### Details

**1. `strategies/base.py`**
```python
class BaseTransformationStrategy(ABC):
    """Base class for transformation strategies."""
    
    @abstractmethod
    def transform(self, entity_id, entity_data, schema):
        pass
```

**2. `strategies/model_strategy.py`**
```python
class ModelTransformationStrategy(BaseTransformationStrategy, TaggableMixin, TestableMixin):
    """Strategy for transforming models."""
    
    def transform(self, model_id, model_data, schema):
        # Model-specific transformation
        # Use mixins for tags and tests
```

**3. `strategies/function_strategy.py`**
```python
class FunctionTransformationStrategy(BaseTransformationStrategy, TaggableMixin):
    """Strategy for transforming functions."""
    
    def transform(self, function_id, function_data, schema):
        # Function-specific transformation
        # Use mixin for tags
```

**4. `mixins/taggable.py`**
```python
class TaggableMixin:
    """Mixin for tag management."""
    
    def merge_tags(self, module_tags, entity_tags):
        # Shared tag merging logic
    
    def extract_object_tags(self, metadata):
        # Shared object tag extraction
```

**5. `mixins/testable.py`**
```python
class TestableMixin:
    """Mixin for test transformation."""
    
    def transform_tests(self, model_data):
        # Test transformation logic
```

**6. `transformer.py`**
```python
class OTSTransformer:
    """Main transformer using strategy pattern."""
    
    def __init__(self, project_config):
        self.model_strategy = ModelTransformationStrategy(project_config)
        self.function_strategy = FunctionTransformationStrategy(project_config)
    
    def transform_to_ots_modules(self, ...):
        # Use appropriate strategy for each entity
```

### Pros
- ✅ Flexible: Easy to add new transformation types
- ✅ Reusable: Mixins provide shared behavior
- ✅ Extensible: Strategy pattern allows swapping implementations
- ✅ Object-oriented: Follows design patterns

### Cons
- ⚠️ More complex: Requires understanding of mixins and strategy pattern
- ⚠️ Might be over-engineered for current needs
- ⚠️ More abstract: Harder for new developers to understand

### File Sizes After Refactoring
- `transformer.py`: ~150 lines
- `strategies/base.py`: ~100 lines
- `strategies/model_strategy.py`: ~200 lines
- `strategies/function_strategy.py`: ~150 lines
- `mixins/taggable.py`: ~100 lines
- `mixins/testable.py`: ~100 lines
- `utils/schema_inferencer.py`: ~100 lines
- `utils/module_builder.py`: ~100 lines

---

## Comparison Table

| Aspect | Approach 1 (Feature) | Approach 2 (Component) | Approach 3 (Strategy) |
|--------|----------------------|------------------------|----------------------|
| **Complexity** | Low | Medium | High |
| **Maintainability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Testability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Extensibility** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Learning Curve** | Low | Medium | High |
| **File Count** | 7 files | 6 files (3 subdirs) | 8 files (3 subdirs) |
| **Max File Size** | ~200 lines | ~250 lines | ~200 lines |
| **Code Duplication** | Minimal | Minimal | None (mixins) |
| **Best For** | Most projects | Large teams | Complex/extensible systems |

---

## Additional Considerations for Approach 2

### Edge Cases to Handle

1. **Empty Schemas**
   - What if a schema has only models or only functions?
   - Module builder should handle both cases gracefully

2. **Default Schema**
   - Models/functions without schema prefix go to "default"
   - Ensure consistent handling across grouping utilities

3. **OTS Version Selection**
   - 0.2.0 if functions present, 0.1.0 otherwise
   - Module builder should determine this correctly

4. **Tag Merging Edge Cases**
   - Empty tags lists
   - None values
   - Non-list types
   - TagManager should handle all gracefully

5. **Schema Inference Failures**
   - SQL parsing errors
   - Missing SQL content
   - SchemaInferencer should return None gracefully

### Performance Considerations

1. **Lazy Initialization**
   - Components initialized once in `__init__`
   - No repeated initialization during transformation

2. **Caching Opportunities**
   - Module tags extracted once, cached in TagManager
   - SQL dialect inferred once, cached in transformer

3. **Batch Processing**
   - Grouping done once for all models/functions
   - Transformations done in batches per schema

### Testing Strategy

1. **Unit Tests**
   - Each component tested independently
   - Mock dependencies where needed
   - Test edge cases for each component

2. **Integration Tests**
   - Test full transformation pipeline
   - Test with real project data
   - Verify OTS output structure

3. **Regression Tests**
   - Compare output before/after refactoring
   - Ensure identical OTS modules generated

### Documentation Updates

1. **Module Docstrings**
   - Document each component's purpose
   - Document dependencies between components

2. **API Documentation**
   - Update public API docs
   - Document new component structure

3. **Migration Guide**
   - Document breaking changes (if any)
   - Provide examples of new usage

### Error Handling Strategy

1. **Component-Level Errors**
   - Each component should handle its own errors gracefully
   - Return None or empty dicts for optional fields
   - Log warnings for recoverable errors
   - Raise exceptions only for critical failures

2. **Validation Points**
   - Validate inputs at component boundaries
   - Type checking with TypedDict where possible
   - Schema validation for OTS structures

3. **Error Propagation**
   - Main transformer catches and logs component errors
   - Continue processing other schemas if one fails
   - Return partial results with error information

### Logging Considerations

1. **Component-Level Logging**
   - Each component has its own logger
   - Log at appropriate levels (debug, info, warning)
   - Include context (schema, entity_id) in log messages

2. **Transformation Tracking**
   - Log start/end of transformations
   - Log counts (models transformed, functions transformed)
   - Log schema grouping results

### Type Safety

1. **Type Hints**
   - Use TypedDict for all OTS structures
   - Type hints for all method signatures
   - Return type annotations

2. **Validation**
   - Validate OTS structures match TypedDict definitions
   - Runtime type checking for critical paths
   - Use mypy for static type checking

### Future Extensibility

1. **Adding New Entity Types**
   - Create new transformer in `transformers/` directory
   - Follow BaseTransformer pattern
   - Add to module builder

2. **Adding New Inferencers**
   - Create new inferencer in `inferencers/` directory
   - Follow SchemaInferencer pattern
   - Use in appropriate transformer

3. **Adding New Tag Types**
   - Extend TagManager with new methods
   - Or create new tagger in `taggers/` directory
   - Keep backward compatible

### Code Quality Considerations

1. **Documentation**
   - Docstrings for all public methods
   - Type hints in docstrings
   - Examples in docstrings where helpful

2. **Code Style**
   - Follow existing codebase conventions
   - Consistent naming (transform, build, infer, merge)
   - Consistent error handling patterns

3. **Dependencies**
   - Minimize dependencies between components
   - Use dependency injection where possible
   - Avoid circular dependencies

### Potential Issues & Solutions

1. **Circular Dependencies**
   - **Issue**: Components might need each other
   - **Solution**: Use dependency injection, pass dependencies in __init__
   - **Example**: TagManager passed to transformers, not imported

2. **Test Complexity**
   - **Issue**: Many components to test
   - **Solution**: Mock dependencies, test in isolation
   - **Benefit**: Easier to test, faster test execution

3. **Import Paths**
   - **Issue**: Longer import paths (e.g., `from tee.parser.output.ots.transformers import ModelTransformer`)
   - **Solution**: Use `__init__.py` to provide shorter aliases
   - **Example**: `from tee.parser.output.ots import ModelTransformer`

4. **Initialization Overhead**
   - **Issue**: Many components to initialize
   - **Solution**: Lazy initialization where possible, or initialize once in __init__
   - **Benefit**: Components initialized once, reused many times

---

## Recommendation: Approach 2 (Component-Based Split) ⭐

**Why:**
1. **Clear component boundaries** - Easy to understand what each directory does
2. **Good for team work** - Different developers can work on different components
3. **Logical organization** - Related functionality grouped together
4. **Extensible** - Easy to add new transformer types or inferencers
5. **Maintainable** - Changes to one component don't affect others
6. **Testable** - Each component can be tested independently
7. **Scalable** - Structure supports growth without becoming unwieldy

**Implementation Order:**
1. Extract utilities first (no dependencies)
   - `utils/dialect_inference.py` - Simple function extraction
   - `utils/grouping.py` - Grouping logic extraction

2. Extract independent components
   - `inferencers/schema_inferencer.py` - No project config needed
   - `taggers/tag_manager.py` - Used by both transformers

3. Extract transformers
   - `transformers/base.py` - Base class
   - `transformers/function_transformer.py` - Simpler, fewer dependencies
   - `transformers/model_transformer.py` - More complex, uses all other components

4. Extract builders
   - `builders/module_builder.py` - Orchestrates transformers

5. Refactor main transformer
   - `transformer.py` - Becomes thin orchestrator

**Migration Strategy:**
- Keep old class as wrapper initially
- Gradually move methods to new modules
- Update tests incrementally
- Remove old code once migration complete
- Maintain backward compatibility during transition

