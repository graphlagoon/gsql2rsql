---
name: analyze_soc_violations

description: Analyze the transpiler codebase (especially renderer) for Separation of Concerns violations, document them with impact assessment, solutions, and create an action plan for refactoring. Note: Do not worry about deprecated features or backward compatibility; refactoring can be aggressive and breaking changes are acceptable.

---

## Skill metadata

* **Name:** SoC Violations Analyzer
* **Intent:** Identify architectural violations, assess their impact, and create a prioritized refactoring roadmap
* **Primary output:** `docs_help_dev/soc-violations.md` with comprehensive analysis and action plan
* **Focus areas:** Renderer (primary), Planner-Renderer boundary, Schema Provider usage
* **Key Assumption:** No backward compatibility required; deprecated features can be ignored, allowing for more radical refactoring without legacy constraints.

---

## Activation / When to use

Invoke this skill when you need to:
* Audit the codebase for Separation of Concerns violations
* Create a refactoring roadmap for architectural improvements
* Assess technical debt related to SoC violations
* Plan architectural improvements before major features

---

## Required Reading Before Analysis

The skill **must** read these documents first:

1. **[docs_help_dev/how-to-respect-soc.md](../../docs_help_dev/how-to-respect-soc.md)**
   - Decision framework for SoC
   - The Adjacency List Thought Experiment
   - Red flags and patterns

2. **[docs_help_dev/02-architecture.md](../../docs_help_dev/02-architecture.md)**
   - Phase responsibilities
   - Design principles
   - File responsibilities

3. **[.claude/skills/fix_transpiler_bugs/SKILL.md](../fix_transpiler_bugs/SKILL.md)**
   - Known case studies
   - Architectural lessons

---

## Analysis Methodology

### Phase 1: Pattern Detection

**Red Flags to Look For:**

In **Renderer** (`src/gsql2rsql/renderer/` — `sql_renderer.py`, `join_renderer.py`, `recursive_cte_renderer.py`, `procedural_bfs_renderer.py`, `expression_renderer.py`):
```python
# ❌ Semantic decisions
if op.direction == RelationshipDirection.BOTH:
if relationship_type in ["KNOWS", "FRIENDS"]:
if self._undirected_strategy == "union_edges":

# ❌ Schema consultation (RESOLVED — all db_schema calls now in sql_enrichment.py)
# Previously: edge_schema = self.schema.get_edge_definition(...)
# Now: enriched = self._ctx.enriched.exists_exprs.get(id(expr))

# ❌ Storage model decisions
if edge_strategy == EdgeAccessStrategy.EDGE_LIST:
    # Generate UNION ALL

# ❌ Business logic in SQL generation
if node.label == "Person" and relationship == "KNOWS":
```

**Note:** As of ADR-009, all `db_schema` calls have been migrated out of renderers into `sql_enrichment.py`. The renderer now reads from `EnrichedPlanData` (immutable frozen dataclasses). If you find a new `db_schema` call in a renderer, it should be migrated to enrichment.

In **Planner** (`src/gsql2rsql/planner/logical_plan.py`):
```python
# ❌ SQL string building
sql_fragment = "SELECT * FROM ..."
op.sql_hint = sql_fragment

# ❌ Dialect-specific decisions (should be in renderer)
if self.dialect == "databricks":
```

### Phase 2: Impact Assessment

For each violation found, assess:

1. **Severity** (Critical / High / Medium / Low):
   - **Critical**: Blocks major features or storage model changes
   - **High**: Significant coupling, hard to extend
   - **Medium**: Code smell, increases maintenance cost
   - **Low**: Minor issue, easy workaround

2. **Impact Radius**:
   - How many files/modules affected?
   - How many features depend on this code?
   - What breaks if we change storage model?

3. **Technical Debt**:
   - How much harder does this make future changes?
   - Does it prevent planned features?

### Phase 3: Solution Design

For each violation, propose:

1. **Ideal Solution**: Perfect SoC following architecture
2. **Pragmatic Solution**: If ideal is blocked by SQL constraints
3. **Intermediate Steps**: Can we improve incrementally?

### Phase 4: Prioritization

Rank violations by:
- **Impact × Difficulty matrix**
- **Feature dependencies** (blocking planned work?)
- **Refactoring risk** (how much can break?)

---

## Output Document Structure

Create `docs_help_dev/soc-violations.md` with:

```markdown
# Separation of Concerns Violations: Analysis & Remediation Plan

**Analysis Date**: [DATE]
**Analyzer**: [WHO/WHAT]
**Codebase Version**: [COMMIT/VERSION]

---

## Executive Summary

- Total violations found: [N]
- Critical: [N] | High: [N] | Medium: [N] | Low: [N]
- Estimated refactoring effort: [PERSON-DAYS]
- Priority 1 violations blocking: [FEATURES/CHANGES]

---

## Violation Categories

### 1. Renderer Making Semantic Decisions

**Pattern**: Renderer checks semantic intent and decides SQL structure

**Instances Found**: [N]

#### V1.1: Undirected VLP with EdgeAccessStrategy

**Location**: `src/gsql2rsql/renderer/recursive_cte_renderer.py`

**Code**:
```python
is_undirected = op.direction == RelationshipDirection.BOTH
edge_strategy = self._graph_def.get_edge_access_strategy()
needs_union = is_undirected and edge_strategy == EdgeAccessStrategy.EDGE_LIST
if needs_union:
    # Generate UNION ALL
```

**Why This Violates SoC**:
- Renderer decides "undirected + edge_list = UNION ALL"
- This is a semantic decision based on storage model
- Blocks: Switching to adjacency lists without touching renderer

**Severity**: HIGH
**Impact Radius**:
- Files: renderer modules (`recursive_cte_renderer.py`, `join_renderer.py`)
- Features: All VLP undirected queries
- Storage models: EDGE_LIST vs ADJACENCY_BIDIRECTIONAL

**Current Workarounds**: EdgeAccessStrategy abstraction (partial fix)

**Ideal Solution**:
```python
# Planner creates specialized operator
if rel.direction == BOTH and edge_strategy == EDGE_LIST:
    return BidirectionalRecursiveTraversalOperator(...)

# Renderer just checks operator type
if isinstance(op, BidirectionalRecursiveTraversalOperator):
    return self._render_with_internal_union(op)
```

**Difficulty**: MEDIUM
- Requires: New operator type, planner changes, renderer simplification
- Risk: Medium (tests cover behavior well)
- Estimated effort: 4-6 hours

**SQL Constraint**: WITH RECURSIVE structure requires UNION ALL inside CTE
**Pragmatic Status**: DOCUMENTED, abstraction added, future refactoring planned

---

### 2. [Other Violation Category]

[Continue pattern for each category...]

---

## Architectural Debt Hotspots

### Hotspot 1: Renderer Undirected Logic

**Lines of Code**: ~200 lines across multiple methods
**Violation Count**: 3-5 instances
**Total Effort to Fix**: 8-12 hours

**Methods Affected**:
- `_render_recursive_cte()` in `recursive_cte_renderer.py`: EdgeAccessStrategy check
- `_render_join()` in `join_renderer.py`: JoinKeyPairType.EITHER_AS_SOURCE/SINK handling
- `_should_use_undirected_union_optimization()` in `join_renderer.py`: Semantic decision helper

**Refactoring Approach**:
1. Create `BidirectionalRecursiveTraversalOperator`
2. Move logic to planner's `_build_variable_length_path()`
3. Simplify renderer to type-check operators
4. Update tests (should pass without changes)

---

## Action Plan

### Phase 1: Quick Wins (Low-Hanging Fruit)
**Estimated Effort**: 2-4 hours
**Risk**: Low

- [ ] **V3.2**: Extract hardcoded SQL fragments to constants
- [ ] **V5.1**: Move dialect checks from planner to renderer
- [ ] **V2.3**: Consolidate schema lookups in planner

### Phase 2: Medium Priority (Architectural Improvements)
**Estimated Effort**: 8-12 hours
**Risk**: Medium

- [ ] **V1.1**: Refactor undirected VLP with BidirectionalRecursiveTraversalOperator
- [ ] **V1.2**: Consolidate JoinKeyPairType handling in single-hop joins
- [ ] **V4.1**: Abstract edge property access patterns

### Phase 3: Major Refactoring (Requires Design Review)
**Estimated Effort**: 16-24 hours
**Risk**: High

- [ ] **V1.3**: Redesign WITH RECURSIVE operator representation
- [ ] **V6.1**: Split renderer into dialect-specific subclasses (renderer is now modular: `sql_renderer.py`, `join_renderer.py`, `recursive_cte_renderer.py`, `expression_renderer.py`, `dialect.py`)
- [x] **V7.1**: ~~Create RenderContext to eliminate renderer state~~ (DONE — `render_context.py` exists)

### Phase 4: Future Considerations
**Blocked By**: Feature requirements, design decisions

- [ ] Support for adjacency list storage
- [ ] Support for multiple SQL dialects (Postgres, MySQL)
- [ ] Property graph model changes

---

## Testing Strategy

For each refactoring:

1. **Pre-Refactoring**:
   - Run full test suite, capture coverage
   - Document current SQL output for key queries
   - Identify integration test gaps

2. **During Refactoring**:
   - TDD: Write tests for new operators/abstractions first
   - Incremental changes with test runs
   - SQL diff validation (before/after should match semantically)

3. **Post-Refactoring**:
   - Full regression suite (PySpark + unit tests)
   - Performance benchmarks (ensure no degradation)
   - Manual smoke testing of edge cases

---

## Risk Assessment

### High-Risk Refactorings

**V1.1: BidirectionalRecursiveTraversalOperator**
- **Risk**: Breaking VLP queries in production
- **Mitigation**: Feature flag, extensive testing, gradual rollout
- **Rollback Plan**: Keep old code path behind flag for 2 releases

**V1.3: WITH RECURSIVE redesign**
- **Risk**: Fundamental change to operator semantics
- **Mitigation**: Prototype in separate branch, design review, RFC
- **Decision**: Requires stakeholder buy-in

### Medium-Risk Refactorings

**V1.2: JoinKeyPairType consolidation**
- **Risk**: Breaking single-hop undirected queries
- **Mitigation**: Comprehensive test coverage, SQL diff validation

---

## Success Metrics

Track progress with:

1. **Violation Count**: Decrease from [N] to target
2. **Test Coverage**: Maintain >90% during refactoring
3. **Code Complexity**: Reduce cyclomatic complexity in renderer
4. **Maintainability Index**: Improve from [X] to [Y]

**Definition of Done**:
- ✅ All Critical and High severity violations resolved
- ✅ Documented pragmatic compromises for Medium/Low
- ✅ No test regressions
- ✅ Updated architecture docs reflect changes
- ✅ Team review and sign-off

---

## References

- [how-to-respect-soc.md](how-to-respect-soc.md): SoC principles and patterns
- [02-architecture.md](02-architecture.md): System architecture
- [fix_transpiler_bugs/SKILL.md](../.claude/skills/fix_transpiler_bugs/SKILL.md): Case studies

---

## Maintenance

**Review Frequency**: Quarterly or before major features
**Owner**: Architecture team
**Next Review**: [DATE + 3 months]
```

---

## Analysis Process

### Step 1: Code Discovery

```bash
# Find renderer files
find src/gsql2rsql/renderer -name "*.py"

# Find planner files
find src/gsql2rsql/planner -name "*.py"

# Search for red flag patterns
grep -r "direction ==" src/gsql2rsql/renderer/
grep -r "get_edge_access_strategy" src/gsql2rsql/
grep -r "isinstance.*Operator" src/gsql2rsql/renderer/
```

### Step 2: Pattern Analysis

For each file, look for:
1. **Conditional logic** on semantic fields (direction, relationship types)
2. **Schema queries** for business logic (not just structure)
3. **Storage model decisions** (EDGE_LIST vs ADJACENCY)
4. **Hardcoded SQL patterns** that should be operator-driven
5. **Dual-renderer inconsistencies** — VLP bugs may exist in `recursive_cte_renderer.py` but not in `procedural_bfs_renderer.py` (or vice versa). Both consume the same `EnrichedRecursiveOp` and should apply the same SoC rules

### Step 3: Trace Impact

For each violation:
```python
# Ask: What breaks if we change storage model?
# Ask: Can renderer generate different SQL without semantic knowledge?
# Ask: Should planner have made this decision?
```

### Step 4: Solution Design

Use the **Adjacency List Thought Experiment**:
1. Assume edges are now stored bidirectionally
2. What code needs to change?
3. Is it in the renderer? → **VIOLATION**
4. Should it be in planner? → **SOLUTION**

---

## Deliverables

1. **`docs_help_dev/soc-violations.md`**
   - Comprehensive violation catalog
   - Impact assessment
   - Solution designs
   - Action plan with effort estimates

2. **Executive Summary** (in conversation)
   - Top 3 critical violations
   - Quick wins (< 4 hours each)
   - Recommended priority order

3. **Optional: Refactoring Tickets** (if requested)
   - GitHub issues for each Phase 1-2 item
   - Acceptance criteria
   - Test requirements

---

## Example Analysis Output

```
Found 12 SoC violations in renderer:
- 3 Critical (blocking adjacency list support)
- 4 High (significant coupling)
- 3 Medium (code smells)
- 2 Low (minor issues)

Top Critical Violations:
1. V1.1: Undirected VLP EdgeAccessStrategy check
   → Solution: BidirectionalRecursiveTraversalOperator
   → Effort: 6 hours, Risk: Medium

2. V1.2: JoinKeyPairType semantic handling
   → Solution: Move to planner join creation
   → Effort: 4 hours, Risk: Medium

3. V2.1: Edge property access patterns
   → Solution: PropertyAccessStrategy abstraction
   → Effort: 8 hours, Risk: High

Quick Wins (Phase 1):
- V3.2: Extract SQL constants (1 hour)
- V5.1: Move dialect checks (2 hours)

Recommended Next Steps:
1. Review findings with team
2. Prioritize based on upcoming features
3. Start with Quick Wins to build momentum
4. Schedule design review for V1.1 refactoring
```

---

## Quality Checks

Before finalizing the document:

- [ ] All violations have severity rating
- [ ] Solutions include effort estimates
- [ ] Action plan is prioritized
- [ ] SQL constraints are documented
- [ ] Testing strategy is clear
- [ ] Rollback plans for high-risk items
- [ ] References to architecture docs
- [ ] Maintenance schedule defined

## Verification Commands

After any refactoring from this analysis:

```bash
# Type checks (MANDATORY - must pass before committing)
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql

# Tests (parallelized)
uv run pytest tests/ -n 4 -q

# Full verification
uv run pyright src/gsql2rsql && uv run mypy src/gsql2rsql && uv run pytest tests/ -n 4 -q
```

---

## Final Instruction

After reading the required docs, systematically:

1. Search codebase for red flag patterns
2. Document each violation with location, severity, impact
3. Design solutions (ideal + pragmatic)
4. Create prioritized action plan
5. Output comprehensive `soc-violations.md`
6. Provide executive summary to user

Focus on **renderer** but include planner-renderer boundary violations.
