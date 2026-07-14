# Skill: Transpiler Optimization Investigation

## Objective

Guide a compiler/transpiler engineer through systematic investigation of optimization opportunities in the gsql2rsql Cypher-to-SQL transpiler. This skill creates a structured approach for analyzing, benchmarking, and implementing optimizations while preserving correctness.

---

## CRITICAL: Before Any Optimization

**Read and internalize these principles:**

1. **Correctness > Performance** — A fast wrong answer is worse than a slow correct one
2. **Conservative by default** — Only optimize patterns proven 100% semantically equivalent
3. **Separation of Concerns** — Each phase has ONE job. Don't let optimizations blur boundaries
4. **Measure first** — Never optimize without benchmarks showing the problem

---

## Required Background Knowledge

### The 5-Phase Pipeline

```
┌─────────┐   ┌─────────┐   ┌──────────┐   ┌──────────┐   ┌───────────┐   ┌──────────┐
│ Parser  │ → │ Planner │ → │Optimizer │ → │ Resolver │ → │Enrichment │ → │ Renderer │
│(Syntax) │   │(Semant.)│   │(Rewrites)│   │(Validat.)│   │(SQL Meta) │   │(SQL Emit)│
└─────────┘   └─────────┘   └──────────┘   └──────────┘   └───────────┘   └──────────┘
     │              │              │              │              │              │
     ↓              ↓              ↓              ↓              ↓              ↓
  QueryNode    LogicalPlan    Optimized     Resolution    Enriched        SQL String
   (AST)     (Operator Tree)    Plan          Result      PlanData
```

**Phase Responsibilities (MEMORIZE THIS):**

| Phase | CAN DO | CANNOT DO |
|-------|--------|-----------|
| **Parser** | Validate Cypher syntax, build AST | Access schema, resolve references |
| **Planner** | Create operators, bind entities, semantic decisions | Validate columns, generate SQL |
| **Optimizer** | Rewrite operator tree (safely), push predicates | Change semantics, access renderer |
| **Resolver** | Validate column refs, provide metadata | Modify plan, generate SQL |
| **Enrichment** | Resolve SQL tables/columns from schema, produce `EnrichedPlanData` | Make semantic decisions, build SQL strings |
| **Renderer** | Generate SQL from operators + enriched data | Make semantic decisions, call `db_schema`, modify plan |

### Key Files to Understand

```bash
# Architecture documentation
cat docs_help_dev/02-architecture.md

# Decision rationale
cat docs_help_dev/03-decision-log.md

# Existing optimizations
cat src/gsql2rsql/planner/subquery_flattening.py       # Subquery flattening
cat src/gsql2rsql/planner/selection_pushdown.py         # Predicate pushdown
cat src/gsql2rsql/planner/pass_manager.py               # Optimization pass orchestrator

# SQL enrichment (schema resolution, runs before rendering)
cat src/gsql2rsql/renderer/sql_enrichment.py            # EnrichedPlanData, SQLEnrichmentPass (~680 lines)

# SQL generation (split into modules — all read from enriched data, never call db_schema)
cat src/gsql2rsql/renderer/sql_renderer.py              # Orchestrator (~1,306 lines)
cat src/gsql2rsql/renderer/expression_renderer.py       # Expression-to-SQL (~2,092 lines)
cat src/gsql2rsql/renderer/recursive_cte_renderer.py    # WITH RECURSIVE CTE (~1,350 lines)
cat src/gsql2rsql/renderer/join_renderer.py             # JOIN rendering (~1,118 lines)

# Logical operator definitions (modular package)
cat src/gsql2rsql/planner/operators/__init__.py
```

---

## Investigation Framework

### Step 1: Define the Investigation Scope

**Ask yourself (or the user):**

1. **What optimization are we investigating?**
   - SQL verbosity reduction?
   - Query execution performance?
   - Recursive CTE efficiency?
   - Join strategy improvements?
   - Memory usage?

2. **What is the current state?**
   - Run example queries and examine output
   - Measure execution time if performance-related
   - Count SQL lines/nesting levels if verbosity-related

3. **What is the success criteria?**
   - X% reduction in SQL lines?
   - X% faster execution?
   - All tests still pass?

### Step 2: Understand Current Implementation

#### 2.1 Current Optimizer Passes

The codebase has **two optimizer passes** already implemented:

```python
# In subquery_optimizer.py

class SelectionPushdownOptimizer:
    """Pushes predicates to DataSourceOperator.

    SAFE to push:
    - Single-variable predicates: p.age > 30
    - AND conjunctions with single vars

    NEVER push:
    - Through LEFT JOINs (changes OPTIONAL MATCH semantics)
    - Volatile functions (rand(), now())
    - Aggregations
    - OR expressions
    - EXISTS subqueries
    """

class SubqueryFlatteningOptimizer:
    """Merges consecutive operators to reduce nesting.

    SAFE to flatten:
    - Selection → Projection
    - Selection → Selection

    NEVER flatten:
    - Projection → Projection (aliases!)
    - Anything with LIMIT/OFFSET/DISTINCT
    - Patterns crossing aggregation boundaries
    """
```

#### 2.2 Run Benchmarks

```bash
# Generate SQL for sample queries and inspect
uv run python -c "
from gsql2rsql import GraphContext
graph = GraphContext(nodes_table='n', edges_table='e')
sql = graph.transpile('''
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    WHERE a.age > 30
    RETURN b.name
''')
print(sql)
print(f'Lines: {len(sql.splitlines())}')
"

# Run tests with timing
uv run pytest tests/transpile_tests/ -v --durations=10
```

### Step 3: Identify Optimization Category

#### Category A: SQL Generation Verbosity

**Symptoms:**
- Generated SQL has 3-5x more nesting than necessary
- Simple queries produce 50+ lines of SQL
- Hard to debug/read generated SQL

**Current State:**
- Each operator wraps its input in a subquery
- DataSource → Join → Projection creates 3 nesting levels

**Investigation Path:**
1. Read `docs_help_dev/subquery-optimization-analysis.md`
2. Study `_render_join()` in `join_renderer.py`
3. Identify safe flattening patterns
4. Prototype in renderer (careful: don't add semantic decisions)

**Key Trade-off:**
- Databricks optimizer already handles nesting well
- Benefit is primarily readability/debuggability
- Risk: introducing semantic bugs

#### Category B: Recursive Query Optimization

**Symptoms:**
- Variable-length path queries (VLP) are slow
- Large intermediate result sets before filtering
- Predicates evaluated too late

**Current State:**
- Predicates partially pushed into recursive CTEs
- Source/sink node filtering implemented
- Edge predicate pushdown partially implemented

**Investigation Path:**
1. Read `src/gsql2rsql/planner/recursive_traversal.py`
2. Study `_render_recursive_cte()` in `recursive_cte_renderer.py`
3. Analyze which predicates could be pushed deeper
4. Benchmark with/without pushdown

**Key Concepts:**
```sql
-- Current: filter AFTER recursion
WITH RECURSIVE paths AS (
    SELECT ... FROM edges WHERE ...
    UNION ALL
    SELECT ... FROM paths p JOIN edges e ON ...
)
SELECT * FROM paths WHERE expensive_predicate;

-- Optimized: filter DURING recursion
WITH RECURSIVE paths AS (
    SELECT ... FROM edges WHERE ... AND cheap_predicate
    UNION ALL
    SELECT ... FROM paths p JOIN edges e ON ... WHERE cheap_predicate
)
SELECT * FROM paths WHERE expensive_predicate_only;
```

#### Category C: Join Strategy Optimization

**Symptoms:**
- Suboptimal join order for multi-hop patterns
- Missing broadcast hints for small tables
- Undirected edges causing performance issues

**Current State:**
- Undirected optimization uses UNION ALL expansion (Option A)
- No cardinality-based join reordering
- No broadcast hints

**Investigation Path:**
1. Read `docs_help_dev/development/UNDIRECTED_OPTIMIZATION_STRATEGIES.md`
2. Study `JoinKeyPairType` in operators.py
3. Analyze join patterns in `join_renderer.py`
4. Consider adding Spark hints (`/*+ BROADCAST(table) */`)

**Critical Understanding - JoinKeyPairType:**
```python
class JoinKeyPairType(Enum):
    SOURCE = auto()          # node.id = edge.source_id (directed forward)
    SINK = auto()            # node.id = edge.sink_id (directed backward)
    EITHER = auto()          # node.id = edge.source_id OR edge.sink_id (undirected, slow)
    EITHER_AS_SOURCE = auto() # Use UNION ALL, join on source_id
    EITHER_AS_SINK = auto()   # Use UNION ALL, join on sink_id
```

#### Category D: Schema-Driven Optimization

**Symptoms:**
- Same plan regardless of table sizes
- No cardinality estimation
- Suboptimal for skewed data

**Current State:**
- No statistics collection
- No cost-based optimization
- Relies entirely on Spark optimizer

**Investigation Path:**
1. Study ISQLDBSchemaProvider interface
2. Consider adding cardinality hints
3. Prototype cost model for join ordering

**Effort Level:** Very high - requires schema statistics infrastructure

---

## Implementation Guidelines

### The Conservative Approach

```python
# ❌ BAD: Aggressive optimization that might break edge cases
def optimize(plan):
    # "This should work for most queries..."
    return flatten_everything(plan)

# ✅ GOOD: Conservative optimization with explicit safety checks
def optimize(plan):
    if not self._is_safe_to_flatten(plan):
        return plan  # Don't optimize if unsure

    # Explicit list of SAFE patterns
    if self._is_selection_projection_pattern(plan):
        return self._flatten_selection_projection(plan)

    return plan  # Default: don't touch it
```

### Adding a New Optimization Pass

1. **Create feature flag:**
```python
# In GraphContext or renderer
self._enable_new_optimization = True  # Default off initially
```

2. **Add comprehensive tests FIRST (TDD):**
```python
def test_new_optimization_basic():
    """Test the optimization works for simple case."""

def test_new_optimization_does_not_break_edge_case():
    """Ensure edge case still works."""

def test_new_optimization_disabled_produces_same_result():
    """Verify semantics unchanged."""
```

3. **Implement with explicit safety checklist:**
```python
def _should_apply_new_optimization(self, plan):
    """
    Safety Checklist:
    - [ ] No aggregation boundaries crossed
    - [ ] No DISTINCT/LIMIT/OFFSET affected
    - [ ] No alias shadowing
    - [ ] No LEFT JOIN semantics changed
    """
    # ... explicit checks
```

4. **Document the optimization:**
```python
"""
New Optimization: XYZ Flattening

Motivation:
    Current SQL generates 5 levels of nesting for pattern X.

Transformation:
    Before: SELECT * FROM (SELECT * FROM (SELECT * FROM ...))
    After: SELECT ... FROM ... JOIN ...

Safety Proof:
    - Pattern X always has Y property
    - Y property guarantees semantic equivalence
    - Verified by tests: test_xyz_1, test_xyz_2, ...

Limitations:
    - Does NOT apply when Z is present
    - Does NOT cross aggregation boundaries
"""
```

---

## Verification Commands

```bash
# Type checks (MANDATORY)
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql

# Run all tests
uv run pytest tests/ -n 4 -q

# Run specific optimization tests
uv run pytest tests/transpile_tests/test_subquery_flattening.py -v

# Benchmark SQL generation
uv run python scripts/benchmark_sql_generation.py  # if exists

# Compare SQL output before/after
uv run python -c "
from gsql2rsql import GraphContext
# Test query here
"
```

---

## Study Resources

### Compiler/Transpiler Concepts

| Concept | Why It Matters | Where to Learn |
|---------|---------------|----------------|
| **Relational Algebra** | Foundation of query optimization | Any database textbook |
| **Query Optimization** | Cost-based vs rule-based | "Database System Concepts" Ch. 15 |
| **Recursive CTEs** | Variable-length path implementation | Databricks docs |
| **Visitor Pattern** | AST traversal (used in parser) | Design Patterns book |
| **Dataflow Analysis** | Predicate pushdown correctness | Compiler textbooks |

### Graph Query Concepts

| Concept | Why It Matters | Where to Learn |
|---------|---------------|----------------|
| **OpenCypher** | Query language we're transpiling | opencypher.org |
| **Graph Patterns** | MATCH semantics, path patterns | Neo4j docs |
| **Variable-Length Paths** | VLP syntax and semantics | Cypher spec |
| **Undirected Edges** | Direction ambiguity handling | This codebase's decision log |

### SQL Dialect (Databricks)

| Feature | Usage | Documentation |
|---------|-------|---------------|
| **WITH RECURSIVE** | Variable-length paths | Databricks SQL reference |
| **LATERAL VIEW** | UNWIND implementation | Spark SQL docs |
| **STRUCT/ARRAY** | Edge collection | Databricks SQL types |
| **Query Hints** | `/*+ BROADCAST */` | Spark SQL hints |

---

## Common Pitfalls

### Pitfall 1: Optimizing in the Wrong Phase

```python
# ❌ BAD: Renderer making semantic decisions
def _render_join(self, op):
    # "Let's reorder these joins for efficiency..."
    # NO! Join order is a PLANNER decision

# ✅ GOOD: Optimizer handling rewrites, Renderer just translating
class JoinReorderingOptimizer:
    def optimize(self, plan):
        # Reorder joins here, in optimizer phase
```

### Pitfall 2: Breaking Optional Match Semantics

```python
# ❌ BAD: Pushing predicate through LEFT JOIN
# OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.age > 30
# Pushing "b.age > 30" to DataSource changes semantics!

# Correct behavior:
# - Without pushdown: Returns 'a' even if no matching 'b'
# - With pushdown: Might not return 'a' at all
```

### Pitfall 3: Ignoring Alias Shadowing

```python
# ❌ BAD: Flattening when aliases conflict
# SELECT x FROM (SELECT y AS x FROM (SELECT z AS y FROM t))
# Cannot flatten: x, y, z are different columns!

# ✅ GOOD: Check for alias conflicts before flattening
if self._has_alias_conflicts(inner, outer):
    return plan  # Don't flatten
```

### Pitfall 4: Forgetting Aggregation Boundaries

```python
# ❌ BAD: Flattening across GROUP BY
# MATCH (a)-[:KNOWS]->(b)
# WITH a, count(b) AS cnt  <- Aggregation boundary
# WHERE cnt > 5
# RETURN a

# Cannot push "cnt > 5" before aggregation!
```

---

## Investigation Template

When starting a new optimization investigation, fill out:

```markdown
## Optimization Investigation: [NAME]

### 1. Problem Statement
What specific issue are we addressing?
- Symptom:
- Impact:
- Example query showing the problem:

### 2. Current Behavior
How does the system currently handle this?
- Code location:
- Current output:
- Benchmark baseline:

### 3. Proposed Optimization
What transformation do we want to apply?
- Before → After transformation:
- Safety proof:
- Affected code paths:

### 4. Edge Cases
What edge cases must we handle?
- [ ] Aggregation boundaries
- [ ] DISTINCT/LIMIT/OFFSET
- [ ] LEFT JOINs (OPTIONAL MATCH)
- [ ] Alias shadowing
- [ ] Recursive patterns
- [ ] Set operations (UNION)

### 5. Implementation Plan
- [ ] Add tests first (TDD)
- [ ] Implement with feature flag
- [ ] Verify all tests pass
- [ ] Benchmark improvement
- [ ] Document in decision log

### 6. Rollback Plan
If optimization causes issues:
- Feature flag to disable
- Revert commit hash: ___
```

---

## Success Criteria

- [ ] Investigation documented using template above
- [ ] Baseline benchmarks recorded
- [ ] Safety checklist completed
- [ ] Tests written BEFORE implementation
- [ ] Feature flag added for A/B testing
- [ ] All existing tests pass
- [ ] Type checks pass (pyright + mypy)
- [ ] Improvement measured and documented
- [ ] Decision log updated if significant change

---

## Notes for the Agent

- **Ask clarifying questions first** — What optimization? What's the symptom? What's the goal?
- **Measure before optimizing** — Generate baseline SQL, count lines, measure time
- **Read existing optimizers** — SubqueryFlatteningOptimizer is template-quality code
- **Respect phase boundaries** — Optimizer optimizes, Renderer renders
- **Conservative by default** — If unsure, don't optimize
- **Document everything** — Future engineers need to understand WHY
- **TDD workflow** — Tests first, implementation second
- **Feature flags** — Allow disabling new optimizations easily
