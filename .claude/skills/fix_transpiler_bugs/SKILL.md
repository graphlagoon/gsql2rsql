---
name: investigate_and_solve_transpiler_bugs

description:  guide an engineer (or an assistant-agent) through  a transpiler bugfix. Emphasizes TDD, root-cause fixes (not workarounds), respect for the current Separation of Concerns, running the PySpark-focused tests, and verifying no regressions by running the non-PySpark test suite. The skill **must** ask the user immediately what they are trying to solve and which PySpark test(s) to run.

---

## Skill metadata

* **Name:** TDD Transpiler Fixes
* **Intent:** Produce a correct, maintainable fix for transpiler issues while following TDD and respect for existing architecture.
* **Primary tests:** tests specified by user and `uv run pytest tests/ -n 4`.
* **Required behavior:** Root-cause diagnosis, trade-off analysis, explicit restatement of Separation of Concerns, follow-up verification via both test suites.

---

## Activation / When to use

Invoke this skill whenever you are asked to investigate, fix, or adjust transpiler code related to the PySpark pipeline or related components and you must:

* follow TDD,
* avoid temporary workarounds that break architecture,
* run the PySpark test suite and the non-PySpark suite to detect regressions.

---

## Immediate user questions (the skill **must** ask these now)

When activated, the assistant **must** ask the user these two questions before making code changes:

1. **"What problem are you trying to solve?** Please describe the bug/behavior you want fixed, including symptoms, logs, failing tests, or reproduction steps."
2. **"Which PySpark test(s) should I run first?** (e.g., `tests` — or provide the exact test target(s)). or create new tests"

> The assistant SHOULD wait for the user's answers to those two questions before proceeding with implementation steps. (The rest of the skill describes what to do after those answers are provided.)

---

## Architecture: The 6-Phase Pipeline (MUST READ)

**Key Principle:** Never guess. Always investigate the pipeline phase-by-phase before touching code.

```
Parser → Planner → Optimizer → Resolver → Enrichment → Renderer
```

| Phase | Responsibility | Key Files |
|-------|----------------|-----------|
| **Parser** | Cypher AST generation | `parser/` |
| **Planner** | Logical operators, binding entities | `planner/operators/`, `planner/logical_plan.py` |
| **Optimizer** | Query optimization | `planner/subquery_flattening.py`, `planner/selection_pushdown.py`, `planner/pass_manager.py` |
| **Resolver** | Column resolution | `planner/column_resolver.py` |
| **Enrichment** | SQL metadata resolution (tables, columns, filters) | `renderer/sql_enrichment.py` |
| **Renderer** | SQL emission (mechanical) | `renderer/sql_renderer.py` (orchestrator), `renderer/expression_renderer.py`, `renderer/join_renderer.py`, `renderer/recursive_cte_renderer.py`, `renderer/procedural_bfs_renderer.py` |

**Separation of Concerns (SoC):**
- **Parser:** Only parses, never validates schema
- **Planner:** Creates logical plan, binds entities to schema. **Does NOT generate SQL**
- **Enrichment:** Resolves all SQL-specific metadata (table names, columns) from `db_schema`. Produces immutable `EnrichedPlanData`
- **Renderer:** Generates SQL from logical plan + enriched data. **Does NOT modify logical semantics. Does NOT call `db_schema` directly** — reads from `EnrichedPlanData` instead

### File Responsibilities

| File | Adds/Modifies | Never Does |
|------|---------------|------------|
| `planner/operators/` | Entity binding, JoinKeyPairType (modular package) | SQL generation |
| `planner/logical_plan.py` | Operator creation, direction handling | SQL generation |
| `renderer/sql_enrichment.py` | SQL metadata resolution: table names, columns, filters. All `db_schema` calls | Semantic decisions, SQL string building |
| `renderer/sql_renderer.py` | Orchestrator: `render_plan`, `_render_operator` dispatcher, data source, projection, set ops | Semantic decisions, `db_schema` calls |
| `renderer/join_renderer.py` | All JOIN rendering (single-hop, recursive, boundary, undirected UNION) | Semantic decisions, `db_schema` calls |
| `renderer/recursive_cte_renderer.py` | WITH RECURSIVE CTE generation for VLP | Semantic decisions, `db_schema` calls |
| `renderer/procedural_bfs_renderer.py` | Procedural BFS for VLP: `BEGIN...END` + `WHILE` loop (Databricks/PySpark 4.2+) | Semantic decisions, `db_schema` calls |
| `renderer/expression_renderer.py` | Expression-to-SQL translation (values, predicates, functions, EXISTS) | Semantic decisions, `db_schema` calls |
| `common/schema.py` | Schema types, EdgeAccessStrategy, constants | SQL or planning |
| `renderer/schema_provider.py` | Table descriptors, storage strategies (consumed by enrichment) | Planning or semantic decisions |

---

## Required checklist / behavior after getting the answers

### 1. Read Existing Documentation First if necessary

```bash
# ALWAYS check docs before investigating
ls docs_help_dev/
cat docs_help_dev/02-architecture.md
```

**Why:** The codebase has design decisions documented. Don't reinvent or contradict them.

### 2. Write a Failing Test (RED Phase)

Create a PySpark test that demonstrates the bug with **real data verification**:

```python
# tests/test_<feature>_pyspark.py

@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    spark = (
        SparkSession.builder
        .appName("Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph with KNOWN data."""
    # Document the graph structure in comments!
    """
    Test Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                 ^
          v                 |
        Dave ---KNOWS---> Eve
    """
    nodes_data = [
        ("Alice", "Person", 25),
        ("Bob", "Person", 30),
        # ...
    ]
    edges_data = [
        ("Alice", "Bob", "KNOWS"),
        # ...
    ]
    # Create DataFrames and views
    return {"nodes": nodes_df, "edges": edges_df}

def test_specific_case(self, spark, graph_context):
    """Test description with expected behavior."""
    query = """
    MATCH (a:Person)-[:KNOWS]-(b:Person)
    WHERE a.node_id = 'Bob'
    RETURN a.node_id AS src, b.node_id AS dst
    """

    sql = graph_context.transpile(query)
    print(f"\n=== SQL ===\n{sql}")  # ALWAYS print SQL for debugging

    result = spark.sql(sql)
    rows = result.collect()

    # Verify EXACT expected values, not just "it runs"
    results = {row["dst"] for row in rows}
    expected = {"Alice", "Carol", "Eve"}  # Document WHY these are expected
    assert results == expected, f"Expected {expected}, got {results}"
```

**Test Data Design Guidelines:**
- Small (5-10 nodes, 5-10 edges)
- Each node/edge serves a purpose
- Document the graph visually in comments
- Include edge cases (leaf nodes, cycles if relevant)

### 3. Investigate the Pipeline (Diagnose)

#### 3.1 Check Parser Output

```python
from gsql2rsql.parser.cypher_parser import parse_cypher

query = "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a"
ast = parse_cypher(query)
print(ast)  # Check entity_name, direction, etc.
```

**What to verify:**
- `RelationshipDirection` is correct (FORWARD, BACKWARD, BOTH)
- `entity_name` has the relationship type
- Node labels are captured

#### 3.2 Check Planner Output

**What to verify:**
- `DataSourceOperator` has correct `join_key_pair_type`
- `RecursiveTraversalOperator` has correct `direction` (for VLP)
- Entity bindings are correct

#### 3.3 Check Renderer Output

```python
sql = graph_context.transpile(query)
print(sql)
```

**What to verify:**
- JOIN conditions use correct columns (src vs dst)
- WHERE clauses have correct filters
- UNION ALL is present for undirected (if applicable)

### 4. Identify the Faulty Phase

**Decision Tree:**

```
Is the AST correct?
├─ NO → Fix Parser (rare)
└─ YES → Is the logical plan correct?
         ├─ NO → Fix Planner (operators.py or logical_plan.py)
         └─ YES → Is the SQL correct?
                  ├─ NO → Fix Renderer (see renderer/ modules above)
                  └─ YES → Test expectation is wrong
```

**Common Bug Patterns:**

| Symptom | Likely Cause | Fix Location |
|---------|--------------|--------------|
| Wrong direction in SQL | Planner sets wrong `JoinKeyPairType` | `logical_plan.py` |
| Missing UNION ALL | Renderer doesn't detect undirected | `join_renderer.py` |
| Wrong column in JOIN | Renderer uses wrong key | `join_renderer.py` |
| Entity not bound | Planner can't find schema | `operators.py` |
| Missing WHERE filter | Renderer skips filter | `expression_renderer.py` or `sql_renderer.py` |
| Wrong recursive CTE | VLP base/recursive case wrong | `recursive_cte_renderer.py` |
| Wrong procedural BFS | Edge expansion or visited-set logic wrong | `procedural_bfs_renderer.py` |
| Operator precedence in multi-type filter | OR filter not parenthesized, bypasses AND conditions | `procedural_bfs_renderer.py` or `recursive_cte_renderer.py` |
| DISTINCT on MAP/ARRAY columns | Spark `UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE` | `procedural_bfs_renderer.py` (remove DISTINCT from edge expansion) |

### 4.1. Dual-Renderer VLP Check (CRITICAL)

> **When fixing a VLP bug, ALWAYS check both `recursive_cte_renderer.py` AND `procedural_bfs_renderer.py`.**

Both renderers consume the same `EnrichedRecursiveOp` from enrichment, but they generate different SQL structures. A bug in one often exists in the other. Known examples:
- Operator precedence in multi-type edge filters (OR not parenthesized)
- `SELECT DISTINCT` on edge expansion with MAP columns (CTE uses `NOT ARRAY_CONTAINS`, procedural BFS uses visited-set table)

### 5. Verify the Fix

#### Run Type Checks First (MANDATORY)

```bash
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql
```

**Both must report 0 errors.** Type errors indicate potential runtime bugs.

#### Run the Specific Test

```bash
uv run pytest tests/test_<feature>_pyspark.py::<TestClass>::<test_name> -v -s
```

#### Run All  Tests

```bash
uv run pytest tests -n 8 -v
```


**All existing tests must pass.** If they don't, your fix broke something.

### 6. Refactor (REFACTOR Phase)


**Add documentation for non-obvious decisions:**

```python
# EITHER_AS_SOURCE/SINK are used for undirected single-hop.
# They differ from EITHER (used in VLP) because:
# - EITHER: VLP handles direction in recursive CTE
# - EITHER_AS_SOURCE/SINK: Single-hop uses UNION ALL expansion
```

---

## Test Categories to Cover

### 1. Directed Tests
```python
def test_directed_from_alice(self):
    """Alice -[:KNOWS]-> should return outgoing neighbors."""
```

### 2. Undirected Tests
```python
def test_undirected_from_bob(self):
    """Bob -[:KNOWS]- should return BOTH directions."""
```

### 3. Untyped Edge Tests
```python
def test_untyped_directed(self):
    """(a)-[]->(b) should match ALL edge types."""
```

### 4. OR Relationship Type Tests
```python
def test_or_types(self):
    """[:KNOWS|DEFRAUDED] should match both types."""
```

### 5. No-Label Node Tests
```python
def test_no_label_source(self):
    """(a)-[:KNOWS]->(b:Person) with no label on a."""
```

### 6. Exact Count Tests
```python
def test_undirected_doubles_count(self):
    """Undirected count = 2x directed count."""
```

---

## Architectural Lessons: When to Refactor

For detailed guidance on making architectural decisions that respect Separation of Concerns, see [docs_help_dev/how-to-respect-soc.md](../docs_help_dev/how-to-respect-soc.md).

---

## Common Mistakes to Avoid

### 1. Guessing Without Investigation
**Wrong:**
```
"I think the renderer is wrong, let me change the renderer"
```

**Right:**
```
"Let me check what the planner outputs first..."
```

### 2. Fixing Symptoms Instead of Root Cause
**Wrong:**
```python
# In renderer: hack to swap columns
if self._is_undirected:
    src, dst = dst, src  # HACK!
```

**Right:**
```python
# In planner: set correct join type
join_type = JoinKeyPairType.EITHER_AS_SINK  # Semantic meaning
```

### 3. Not Verifying Exact Values
**Wrong:**
```python
assert len(rows) > 0  # Just checks it returns something
```

**Right:**
```python
assert results == {"Alice", "Carol", "Eve"}  # Exact expected values
```

### 4. Wrong Test Expectations
**Common error:** Test comment says "Dave->Bob DEFRAUDED edge" but no such edge exists in test data.

**Always:** Verify test expectations match actual test data.

---

## Acceptance criteria

* **Type checks pass**: Both `pyright` and `mypy` report 0 errors on `src/gsql2rsql`.
* The originally specified PySpark test(s) pass locally after the change.
* Non-PySpark suite also passes locally.
* The fix is explained with a root-cause diagnosis and a trade-off analysis.
* The solution preserves (or documents deliberate, justified changes to) the Separation of Concerns.
* Tests added/changed are reviewed and are robust (not flaky).

---

## Running Tests & Type Checks (Quick Reference)

```bash
# Type checking (ALWAYS run before committing)
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql

# Single test
uv run pytest tests/test_single_hop_pyspark.py::TestClass::test_name -v -s

# All PySpark single-hop tests
uv run pytest tests/test_single_hop_pyspark.py -v

# PySpark procedural BFS tests
uv run pytest tests/pyspark_tests/test_procedural_bfs_pyspark.py -v -s

# Procedural BFS rendering tests (non-PySpark, golden SQL assertions)
uv run pytest tests/test_procedural_bfs_rendering.py -v

# All tests except PySpark (faster, parallelized)
uv run pytest tests/ --ignore=tests/pyspark_tests -n 4 -q

# With coverage (parallelized)
uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=term-missing

# Full verification (type check + tests)
uv run pyright src/gsql2rsql && uv run mypy src/gsql2rsql && uv run pytest tests/ -n 4 -q
```

---

## Final instruction to the agent

**Now ask the user**:

1. "What problem are you trying to solve?"
2. "Which PySpark test(s) should I run first?"

After the user answers, follow the checklist above, and report results (failing logs, root cause, proposed fix, trade-offs, and both test-suite runs).
