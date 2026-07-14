# Skill: Find and Remove Dead Code

## Objective

Systematically identify and remove dead code from the codebase. This includes:
- **Unused code**: Functions, classes, methods, variables never called/used
- **Backward compatibility code**: We do NOT provide backward compatibility - remove it
- **Dead code paths**: Unreachable code, obsolete feature flags
- **Deprecated features**: Old implementations kept "just in case"

## CRITICAL: No Backward Compatibility

**This project does NOT maintain backward compatibility.** Any code that exists solely for:
- Supporting old API versions
- Legacy function signatures
- Deprecated parameter handling
- "Keep for now" comments

Should be **immediately removed**. Breaking changes are acceptable and expected.

---

## Required Tools

### Install Development Dependencies

Add these tools to `pyproject.toml` under `[project.optional-dependencies]` dev section:

```bash
# Add vulture for dead code detection
uv add --dev vulture
```

### Available Tools

| Tool | Purpose | Command |
|------|---------|---------|
| **vulture** | Find unused code (functions, variables, imports) | `uv run vulture src/gsql2rsql --exclude=grammar,cli.py` |
| **ruff** | Unused imports (F401), unused variables (F841) | `uv run ruff check src/gsql2rsql --select=F401,F841 --exclude=grammar` |
| **coverage** | Find code never executed by tests | `uv run pytest --cov=src/gsql2rsql --cov-report=html` |

### Excluded Paths

**Always exclude these from analysis:**

| Path | Reason |
|------|--------|
| `parser/grammar/` | ANTLR auto-generated code - do NOT modify |
| `cli.py` | TUI application with dynamic widget imports - many false positives |

```bash
# Correct exclusion syntax
uv run vulture src/gsql2rsql --exclude=grammar,cli.py
uv run ruff check src/gsql2rsql --select=F401,F841 --exclude=grammar
```

---

## Before You Start

### Step 1: Read Architecture Documentation

```bash
cat docs_help_dev/02-architecture.md
```

Understanding the 5-phase architecture helps identify:
- Which code belongs to which phase
- What patterns are expected vs. accidental
- Where dead code typically accumulates

### Step 2: Understand Phase Boundaries

| Phase | Location | Common Dead Code |
|-------|----------|------------------|
| Parser | `parser/` | Unused AST node types, deprecated visitor methods |
| Planner | `planner/` | Unused operator types, old join strategies |
| Optimizer | `planner/subquery_flattening.py`, `planner/selection_pushdown.py` | Disabled optimization passes |
| Resolver | `planner/column_resolver.py` | Unused resolution helpers |
| Enrichment | `renderer/sql_enrichment.py` | Unused enriched dataclass fields |
| Renderer | `renderer/` | Old SQL generation patterns, leftover `db_schema` calls (should be migrated to enrichment), unused helpers |

---

## Analysis Process

### Phase 1: Automated Detection

#### 1.1 Run vulture (Primary Tool)

```bash
# Basic scan (excludes grammar/ and cli.py)
uv run vulture src/gsql2rsql --exclude=grammar,cli.py

# With confidence threshold (lower = more aggressive)
uv run vulture src/gsql2rsql --exclude=grammar,cli.py --min-confidence 80

# Generate whitelist for false positives (if needed)
uv run vulture src/gsql2rsql --exclude=grammar,cli.py --make-whitelist > vulture_whitelist.py
```

**What vulture finds:**
- Unused functions and methods
- Unused classes
- Unused variables
- Unused imports
- Unreachable code

#### 1.2 Run ruff for Unused Imports/Variables

```bash
# Find unused imports (excludes grammar/ and cli.py)
uv run ruff check src/gsql2rsql --select=F401 --exclude="grammar,cli.py"

# Find unused variables
uv run ruff check src/gsql2rsql --select=F841 --exclude="grammar,cli.py"

# Auto-fix unused imports
uv run ruff check src/gsql2rsql --select=F401 --exclude="grammar,cli.py" --fix
```

#### 1.3 Analyze Coverage Gaps (CRITICAL - vulture can't find this)

Coverage analysis finds dead code that vulture misses:
- **Tested but unused**: Code that tests cover but production code never calls
- **Designed but never integrated**: Methods created for future features that were never wired in
- **Orphaned helpers**: Utility methods whose callers were deleted

```bash
# Generate coverage report with term-missing to see uncovered lines
uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=html --cov-report=term-missing

# Open report
xdg-open htmlcov/index.html  # Linux
open htmlcov/index.html      # macOS
```

**Coverage Analysis Strategy:**

1. **Look for files with < 80% coverage** - Focus on these first
2. **Identify 0% coverage methods** - These are likely dead code
3. **Check if uncovered code has tests** - If tests exist but code is uncovered, the code may be dead

**Key insight**: If a method has 0% coverage but tests import its class/module, the method is likely dead (tests don't exercise it, and neither does production).

**Example from this codebase:**
```
src/gsql2rsql/planner/schema.py    69%   Missing: 93-96, 188-205, 234-236, 259-269
```
This revealed methods like `copy_from()`, `add_field()`, `get_entity_fields()` that:
- Were defined and documented
- Had 0% coverage
- Were never called anywhere in production code
- Some even had tests, but those tests only tested the dead code itself

**Cross-reference with grep:**
```bash
# For each 0% coverage method, verify it's never called
grep -rn "\.method_name(" src/gsql2rsql/
# If no results (only definition), it's dead code
```

### Phase 2: Manual Inspection

#### 2.1 Search for Dead Code Patterns

```bash
# Find "TODO: remove" comments
grep -rn "TODO.*remove\|FIXME.*remove\|XXX.*remove" src/gsql2rsql/

# Find deprecated markers
grep -rn "@deprecated\|DEPRECATED\|# deprecated" src/gsql2rsql/

# Find backward compatibility markers
grep -rn "backward.*compat\|legacy\|for.*compatibility\|keep.*for" src/gsql2rsql/

# Find commented-out code blocks
grep -rn "^#.*def \|^#.*class " src/gsql2rsql/

# Find "just in case" comments
grep -rn "just in case\|might need\|keep for now\|temporary" src/gsql2rsql/
```

#### 2.2 Check for Unused Enum Values

```bash
# List all enum classes
grep -rn "class.*Enum" src/gsql2rsql/

# For each enum, search for usage of its values
# Example: grep -rn "JoinKeyPairType\." src/gsql2rsql/
```

#### 2.3 Check for Unused Exception Types

```bash
# List custom exceptions
grep -rn "class.*Exception\|class.*Error" src/gsql2rsql/

# Search for raises/catches
grep -rn "raise.*Exception\|except.*Exception" src/gsql2rsql/
```

### Phase 3: Coverage-Based Deep Analysis

This phase catches dead code that vulture misses (60% confidence items that are actually dead).

#### 3.1 Run Full Coverage Analysis

```bash
uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=term-missing 2>&1 | tail -60
```

#### 3.2 Identify Suspicious Patterns

Look for methods with 0% coverage in the `Missing` column:

| Coverage Pattern | Likely Cause | Action |
|-----------------|--------------|--------|
| Class at 100%, specific methods at 0% | Dead methods | Verify with grep, remove |
| Entire class at 0% | Dead class | Check imports, may be dead |
| Abstract method implementations at 0% | Never instantiated subclass | Verify hierarchy |

#### 3.3 Verify Each Uncovered Method

```bash
# For each method with 0% coverage:
grep -rn "\.method_name(" src/gsql2rsql/

# If only the definition appears, it's dead code
# If calls appear, investigate why coverage is 0%
```

#### 3.4 Handle Tests That Only Test Dead Code

**IMPORTANT**: Sometimes tests exist that only test dead code. When you remove dead code:

1. Tests may fail with `AttributeError` or `ImportError`
2. These tests should be removed or updated
3. Replace deprecated methods with their working alternatives (e.g., `add_field()` → `append()`)

```python
# Before (testing dead code)
schema.add_field(field)  # add_field() is dead

# After (use the actual API)
schema.append(field)  # Schema extends list, use list methods
```

### Phase 4: Categorize Findings

Create a list categorized by:

1. **Safe to Remove** (no references, clear dead code)
2. **Likely Dead** (vulture found, manual verification needed)
3. **Coverage Dead** (0% coverage, grep confirms no calls)
4. **False Positives** (used dynamically, pytest fixtures, etc.)
5. **Backward Compatibility** (remove per policy)

---

## Common False Positives

vulture may incorrectly flag:

| Pattern | Why False Positive | Solution |
|---------|-------------------|----------|
| `pytest` fixtures | Dynamic usage | Add to whitelist |
| `__init__.py` exports | Public API | Add to whitelist |
| Abstract methods | Implemented by subclasses | Add to whitelist |
| Dataclass fields | Accessed dynamically | Verify usage |
| CLI entrypoints | Called by Click | Add to whitelist |
| ANTLR `visit_*` methods | Called dynamically by visitor pattern | Ignore these |
| `__hash__`, `__eq__` | Called by Python internals | Ignore these |
| Public API re-exports | Imported by external code/tests | Verify with tests |

### ANTLR Visitor Methods (Important!)

All `visit_oC_*` methods in `visitor.py` are **false positives**. They are called dynamically by ANTLR's visitor pattern:

```python
# These look unused but are called via visitChildren() / accept()
def visit_oC_Cypher(self, ctx): ...
def visit_oC_Statement(self, ctx): ...
def visit_oC_Query(self, ctx): ...
```

**Never remove these methods** - they are the parser's entry points.

### Creating a Whitelist

If needed, create `vulture_whitelist.py`:

```python
# vulture_whitelist.py
# False positives for vulture

# pytest fixtures
_.spark  # unused fixture
_.graph_context  # unused fixture

# Public API exports in __init__.py
_.transpile  # public API
_.parse_cypher  # public API

# Abstract methods (implemented by subclasses)
_.render_operator  # abstract method
_.bind  # abstract method
```

Run with whitelist:
```bash
uv run vulture src/gsql2rsql vulture_whitelist.py
```

---

## Removal Process

### Step 1: Verify Before Removing

For each candidate:

1. **Search for all usages**:
   ```bash
   grep -rn "function_name" src/ tests/
   ```

2. **Check for dynamic access**:
   ```bash
   grep -rn "getattr.*function_name\|__dict__.*function_name" src/
   ```

3. **Check test coverage** - is it tested but main code doesn't use it?

### Step 2: Remove Incrementally

1. Remove ONE piece of dead code
2. Run type checks:
   ```bash
   uv run pyright src/gsql2rsql
   uv run mypy src/gsql2rsql
   ```
3. Run tests:
   ```bash
   uv run pytest tests/ -n 4 -q
   ```
4. If all pass, commit
5. Repeat

### Step 3: Document Removals

For significant removals, note in commit message:
```
refactor: remove dead code from operators.py

Removed:
- UnusedOperatorType enum value (never used)
- _legacy_join_helper() method (backward compat, not needed)
- OldJoinStrategy class (replaced by JoinKeyPairType)

vulture confidence: 100%
Tests: all passing
```

---

## Verification Commands

After each removal:

```bash
# Type checks (MANDATORY)
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql

# Tests (parallelized)
uv run pytest tests/ -n 4 -q

# Re-run vulture to confirm removal (excludes grammar/ and cli.py)
uv run vulture src/gsql2rsql --exclude=grammar,cli.py

# Full verification
uv run pyright src/gsql2rsql && uv run mypy src/gsql2rsql && uv run pytest tests/ -n 4 -q
```

---

## Output Document (Optional)

If doing a comprehensive cleanup, create `docs_help_dev/dead-code-audit.md`:

```markdown
# Dead Code Audit

**Date**: YYYY-MM-DD
**Tools Used**: vulture, ruff, coverage

## Summary

- Dead code items found: N
- Items removed: N
- False positives: N
- Lines of code removed: ~N

## Removed Items

### Parser Phase
- `_unused_visitor_method()` in visitor.py (vulture 100%)

### Planner Phase
- `OldJoinType` enum (backward compat, policy removal)
- `_legacy_bind()` method (never called)

### Renderer Phase
- `_render_old_style()` (replaced by new implementation)

## False Positives (Whitelisted)

- pytest fixtures: spark, graph_context
- Public API: transpile, parse_cypher

## Remaining Technical Debt

- [ ] Large commented blocks in renderer modules (manual review needed)
- [ ] Unused enum values in JoinKeyPairType (verify with team)
```

---

## Red Flags to Watch For

### Code Smells That Indicate Dead Code

1. **Methods that only call another method**
   ```python
   def old_name(self, x):
       return self.new_name(x)  # Likely backward compat
   ```

2. **Conditionals that are always true/false**
   ```python
   if True:  # Dead else branch
       ...
   ```

3. **Unreachable code after return**
   ```python
   return result
   print("Never executed")  # Dead code
   ```

4. **Unused parameters**
   ```python
   def func(self, used, unused):  # unused is dead
       return used
   ```

5. **Empty exception handlers**
   ```python
   except SomeError:
       pass  # Might be intentional, verify
   ```

---

## Success Criteria

- [ ] vulture reports 0 items at 80%+ confidence (excluding grammar/ and cli.py)
- [ ] ruff F401/F841 reports 0 issues (excluding grammar/)
- [ ] No "TODO remove" or "deprecated" comments remain
- [ ] No backward compatibility code remains
- [ ] All tests pass
- [ ] Type checks pass (pyright + mypy)
- [ ] Coverage analysis: no methods with 0% coverage that grep shows are never called

**Acceptable Exceptions (False Positives to Ignore):**
- `visitor.py` methods (`visit_oC_*`) - ANTLR visitor pattern
- `__hash__`, `__eq__`, `__str__` - Python magic methods
- Dataclass fields used in `clone()` methods
- Public API exports used by tests

**Note:** `parser/grammar/` is ANTLR-generated and `cli.py` has many dynamic imports for the TUI - both are excluded from dead code analysis.

---

## Notes for the Agent

- **Be aggressive**: We don't maintain backward compatibility
- **Verify before removing**: Always search for usages first
- **Incremental commits**: Remove one thing at a time, verify, commit
- **False positives exist**: pytest fixtures, public API, abstract methods, ANTLR visitors
- **Document significant removals**: Future devs should know what was removed
- **Run all checks**: Type checks + tests after every removal
- **Ask if uncertain**: If a removal seems risky, ask the user first

---

## Recommended Workflow (Summary)

```
1. RUN AUTOMATED TOOLS
   ├── uv run ruff check src/gsql2rsql --select=F401,F841 --exclude=grammar,cli.py --fix
   ├── uv run vulture src/gsql2rsql --exclude=grammar,cli.py --min-confidence 80
   └── uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=term-missing

2. ANALYZE RESULTS
   ├── ruff: Auto-fix unused imports (safe)
   ├── vulture 80%+: Verify with grep, likely dead
   ├── vulture 60%: Many false positives, be careful
   └── coverage 0%: Cross-reference with grep to confirm

3. FOR EACH CANDIDATE
   ├── grep -rn "\.method_name(" src/gsql2rsql/
   ├── If only definition found → DEAD CODE
   ├── If calls found → NOT dead, investigate why coverage is 0%
   └── If tests use it but src/ doesn't → DEAD CODE (remove tests too)

4. REMOVE INCREMENTALLY
   ├── Remove one item
   ├── uv run mypy src/gsql2rsql
   ├── uv run pytest tests/ -n 4 -q
   ├── If tests fail with AttributeError → update tests or remove dead test code
   └── Repeat

5. FINAL VERIFICATION
   ├── uv run ruff check src/gsql2rsql --select=F401,F841 --exclude=grammar,cli.py
   ├── uv run vulture src/gsql2rsql --exclude=grammar,cli.py --min-confidence 80
   ├── uv run mypy src/gsql2rsql
   └── uv run pytest tests/ -n 4 -q
```

**Key Insight**: The combination of vulture + coverage + grep is more powerful than any single tool. vulture finds obvious dead code, coverage finds designed-but-never-integrated code, and grep confirms the findings.
