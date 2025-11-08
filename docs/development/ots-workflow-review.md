# OTS Workflow Review

This document reviews the workflow of t4t commands to ensure they correctly handle OTS compilation and execution.

## Command Workflows

### `t4t compile`
**Purpose**: Compile t4t project to OTS modules

**Workflow**:
1. Parse SQL/Python models from `models/`
2. Discover and validate imported OTS modules in `models/`
3. Detect conflicts (duplicate `transformation_id`)
4. Merge all models
5. Convert to OTS format
6. Validate compiled modules
7. Export to `output/ots_modules/`
8. Export test library to `output/ots_modules/`

**Implementation**: `tee/cli/commands/compile.py` → `tee/compiler.py`

**Status**: ✅ Correct

---

### `t4t run`
**Purpose**: Execute models

**Workflow**:
1. Call `compile_project()` (always compiles, overwrites existing)
2. Load OTS modules from `output/ots_modules/`
3. Build dependency graph
4. Execute models
5. Run tests

**Implementation**: 
- `tee/cli/commands/run.py` → `tee/executor.execute_models()`
- `execute_models()` always compiles first, then loads

**Status**: ✅ Correct - Always compiles, no unnecessary checks

---

### `t4t build`
**Purpose**: Build models with interleaved tests

**Workflow**:
1. Call `compile_project()` (always compiles, overwrites existing)
2. Load OTS modules from `output/ots_modules/`
3. Build dependency graph
4. Execute models with interleaved tests

**Implementation**:
- `tee/cli/commands/build.py` → `tee/executor.build_models()`
- `build_models()` always compiles first, then loads
- Passes `project_config` ✅

**Status**: ✅ Correct - Always compiles, no unnecessary checks

---

### `t4t test`
**Purpose**: Run tests only

**Workflow**:
1. Call `compile_project()` (always compiles, overwrites existing)
2. Load OTS modules from `output/ots_modules/`
3. Build dependency graph
4. Run tests only (no execution)

**Implementation**: 
- `tee/cli/commands/test.py`
- Always compiles first, then loads from OTS modules ✅

**Status**: ✅ Correct - Always compiles, no unnecessary checks

---

### `t4t parse`
**Purpose**: Parse models without execution

**Workflow**:
1. Parse SQL/Python models (for analysis only)
2. Build dependency graph
3. Export analysis files
4. Export OTS modules (if project_config provided)

**Implementation**: `tee/cli/commands/parse.py` → `tee/executor.parse_models_only()`

**Status**: ✅ Correct - This is for analysis, not execution, so it doesn't need to load from OTS

---

## Key Functions

### `execute_models()`
**Location**: `tee/executor.py`

**Workflow**:
1. ✅ Always compile (calls `compile_project()`)
2. ✅ Load OTS modules from `output/ots_modules/`
3. ✅ Build dependency graph
4. ✅ Execute models

**Status**: ✅ Correct - Always compiles, simple and clean

---

### `build_models()`
**Location**: `tee/executor.py`

**Workflow**:
1. ✅ Always compile (calls `compile_project()`)
2. ✅ Load OTS modules via `setup_build_context_from_ots()`
3. ✅ Build dependency graph
4. ✅ Execute models with interleaved tests

**Status**: ✅ Correct - Always compiles, simple and clean

---

### `compile_project()`
**Location**: `tee/compiler.py`

**Workflow**:
1. ✅ Parse SQL/Python models
2. ✅ Discover imported OTS modules
3. ✅ Validate OTS module locations
4. ✅ Detect conflicts
5. ✅ Merge models
6. ✅ Convert to OTS format
7. ✅ Validate compiled modules
8. ✅ Export to `output/ots_modules/`

**Status**: ✅ Correct - Single source of truth for compilation

---

## Verification Checklist

- [x] `t4t compile` - Standalone compilation command
- [x] `t4t run` - Always compiles, then executes from OTS
- [x] `t4t build` - Always compiles, then executes from OTS
- [x] `t4t test` - Always compiles, then loads from OTS
- [x] No duplicate compilation logic
- [x] All commands use same compilation function
- [x] All commands load from same location (`output/ots_modules/`)
- [x] No legacy parsing paths in execution commands
- [x] No unnecessary file existence checks

## Summary

✅ **All workflows are correct and consistent:**

1. **Compilation**: Single function `compile_project()` used by all commands
   - Always compiles (overwrites existing files)
2. **Execution**: All execution commands (`run`, `build`, `test`) follow the same pattern:
   - Always compile (calls `compile_project()`)
   - Load from OTS modules
   - Execute/Test

3. **No duplication**: Compilation logic is centralized in `compile_project()`

4. **Clean separation**: 
   - `compile` = compile only
   - `run/build/test` = compile → execute from OTS

5. **Simple and efficient**: No unnecessary file checks - just compile and execute

