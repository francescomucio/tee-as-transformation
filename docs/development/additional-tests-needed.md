# Additional Tests Needed for Function Implementation

## Current Test Coverage Summary

We have good unit test coverage for:
- ✅ Function file discovery
- ✅ Function decorators
- ✅ SQL function parsing
- ✅ Python function parsing
- ✅ Function orchestration (basic)
- ✅ Dependency graph integration
- ✅ OTS export/import (unit tests)
- ✅ Type definitions

## Recommended Additional Tests

### 1. End-to-End Integration Tests

**Priority: High**

Test the complete workflow using real example projects:

```python
# tests/parser/integration/test_function_end_to_end.py

def test_full_function_workflow_with_example_project():
    """Test complete workflow: discovery → parsing → dependency graph → OTS export"""
    # Use examples/t_project or examples/t_project_sno
    # 1. Discover functions
    # 2. Parse functions
    # 3. Build dependency graph
    # 4. Export to OTS
    # 5. Verify OTS structure
    # 6. Import from OTS
    # 7. Verify round-trip consistency

def test_function_and_model_integration():
    """Test functions and models working together in a real project"""
    # Create a project with:
    # - Functions that depend on tables
    # - Models that use functions
    # - Verify execution order
    # - Verify dependency resolution
```

### 2. Database-Specific Override Tests

**Priority: Medium**

```python
# tests/parser/parsers/test_function_database_overrides.py

def test_postgresql_specific_function():
    """Test .postgresql.sql override file"""
    
def test_snowflake_specific_function():
    """Test .snowflake.sql override file"""
    
def test_duckdb_specific_function():
    """Test .duckdb.sql override file"""
    
def test_override_priority():
    """Test that database-specific overrides take precedence"""
    
def test_multiple_database_overrides():
    """Test function with multiple database-specific files"""
```

### 3. Complex Dependency Scenarios

**Priority: High**

```python
# tests/parser/analysis/test_complex_function_dependencies.py

def test_function_chain_with_models():
    """Test: table → func1 → func2 → model"""
    
def test_circular_dependency_detection_functions():
    """Test cycle detection with functions"""
    
def test_function_depends_on_multiple_functions():
    """Test function calling multiple other functions"""
    
def test_model_uses_multiple_functions():
    """Test model calling multiple functions"""
    
def test_function_and_model_mixed_dependencies():
    """Test complex mixed dependencies"""
```

### 4. Function Name Resolution Edge Cases

**Priority: Medium**

```python
# tests/parser/analysis/test_function_name_resolution.py

def test_unqualified_function_name_resolution():
    """Test resolving unqualified function names"""
    
def test_qualified_function_name_resolution():
    """Test resolving qualified function names (schema.func)"""
    
def test_function_name_collision():
    """Test handling function name collisions"""
    
def test_function_name_with_special_characters():
    """Test function names with special characters"""
```

### 5. Metadata Merging Edge Cases

**Priority: Low**

```python
# tests/parser/output/test_function_metadata_merging.py

def test_complex_tag_merging():
    """Test merging tags from schema, module, and function levels"""
    
def test_object_tags_merging():
    """Test merging object_tags with conflicts"""
    
def test_metadata_override_priority():
    """Test metadata override priority (SQL vs Python)"""
```

### 6. Error Handling Edge Cases

**Priority: Medium**

```python
# tests/parser/parsers/test_function_error_handling.py

def test_malformed_sql_function():
    """Test handling of malformed CREATE FUNCTION statements"""
    
def test_missing_metadata_file():
    """Test function without metadata file"""
    
def test_invalid_metadata_structure():
    """Test function with invalid metadata structure"""
    
def test_function_with_missing_dependencies():
    """Test function referencing non-existent tables/functions"""
    
def test_parse_error_does_not_crash_orchestrator():
    """Test that one function parse error doesn't stop all parsing"""
```

### 7. OTS Round-Trip Tests

**Priority: High**

```python
# tests/parser/integration/test_ots_round_trip.py

def test_function_export_import_round_trip():
    """Test: Parse → Export → Import → Verify consistency"""
    
def test_ots_version_downgrade():
    """Test importing 0.2.0 module into 0.1.0 context"""
    
def test_ots_version_upgrade():
    """Test importing 0.1.0 module into 0.2.0 context"""
    
def test_function_ots_with_complex_dependencies():
    """Test OTS export/import with complex function dependencies"""
```

### 8. Function Execution Order Tests

**Priority: Medium**

```python
# tests/parser/analysis/test_function_execution_order.py

def test_functions_before_models_in_execution_order():
    """Test that functions come before models when no dependencies"""
    
def test_function_depends_on_table_execution_order():
    """Test execution order when function depends on table"""
    
def test_model_depends_on_function_execution_order():
    """Test execution order when model depends on function"""
    
def test_complex_execution_order():
    """Test execution order with complex dependency chains"""
```

### 9. Function Overloading Tests

**Priority: Low** (Future feature)

```python
# tests/parser/parsers/test_function_overloading.py

def test_function_overloading_by_signature():
    """Test multiple functions with same name, different signatures"""
    
def test_function_overloading_resolution():
    """Test resolving which overloaded function to use"""
```

### 10. Real Project Integration Tests

**Priority: High**

```python
# tests/integration/test_real_projects_with_functions.py

def test_t_project_with_functions():
    """Test parsing and compiling t_project with functions"""
    
def test_t_project_sno_with_functions():
    """Test parsing and compiling t_project_sno with functions"""
    
def test_compile_project_with_functions():
    """Test compile_project() with functions included"""
```

## Test Implementation Priority

1. **High Priority** (Implement first):
   - End-to-end integration tests
   - OTS round-trip tests
   - Complex dependency scenarios
   - Real project integration tests

2. **Medium Priority**:
   - Database-specific override tests
   - Function name resolution edge cases
   - Error handling edge cases
   - Function execution order tests

3. **Low Priority** (Can wait):
   - Metadata merging edge cases
   - Function overloading tests (future feature)

## Test File Structure

```
tests/
├── parser/
│   ├── integration/                    # NEW
│   │   ├── test_function_end_to_end.py
│   │   ├── test_ots_round_trip.py
│   │   └── test_real_projects_with_functions.py
│   ├── parsers/
│   │   ├── test_function_database_overrides.py  # NEW
│   │   └── test_function_error_handling.py      # NEW
│   └── analysis/
│       ├── test_complex_function_dependencies.py  # NEW
│       ├── test_function_name_resolution.py       # NEW
│       └── test_function_execution_order.py       # NEW
```

## Notes

- Most critical missing tests are **end-to-end integration tests** that verify the complete workflow
- **OTS round-trip tests** are important to ensure export/import consistency
- **Real project tests** using `t_project` and `t_project_sno` would catch integration issues early
- Consider adding these tests before moving to Phase 6 (Adapter Interface)

