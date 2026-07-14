---
name: implement_tests

description: Implement new tests or improve existing tests for the gsql2rsql Cypher-to-SQL transpiler. Covers unit tests, integration tests, PySpark execution tests, golden file tests, and structural SQL assertions. Follows TDD principles and respects the 6-phase pipeline architecture.

---

## Skill metadata

* **Name:** Test Implementation & Improvement
* **Intent:** Create high-quality, maintainable tests that verify transpiler correctness across all 6 pipeline phases
* **Primary test commands:** `uv run pytest tests/ -n 4 -q` (non-PySpark) and `uv run pytest tests/pyspark_tests/ -v -s` (PySpark)
* **Key constraint:** Tests must verify exact values/structure, not just "it runs"

---

## Activation / When to use

Invoke this skill when you need to:
* Write new tests for transpiler features or bug fixes
* Improve existing tests (better assertions, more coverage, clearer structure)
* Add PySpark execution tests that verify actual query results
* Create golden file tests for SQL regression detection
* Add YAML example test cases
* Increase coverage for under-tested modules

---

## Immediate user questions (the skill **must** ask these now)

When activated, the assistant **must** ask the user:

1. **"What feature, module, or behavior do you want to test?"** Describe the Cypher query pattern, operator, or pipeline phase you want covered.
2. **"What type of test do you want?"** Options:
   - **Unit test** — Test a single function/method in isolation
   - **Integration test** — Test transpilation end-to-end (Cypher → SQL)
   - **PySpark execution test** — Test that generated SQL produces correct results
   - **Golden file test** — Regression test comparing SQL output against baseline
   - **Structural assertion test** — Verify SQL structure (has JOIN, has CTE, etc.)
   - **Improve existing tests** — Add assertions, fix flaky tests, or increase coverage

> The assistant SHOULD wait for the user's answers before proceeding.

---

## The 6-Phase Pipeline (Context for Test Design)

```
Parser → Planner → Optimizer → Resolver → Enrichment → Renderer
```

| Phase | What to Test | Test Location |
|-------|-------------|---------------|
| **Parser** | AST correctness, direction parsing, label extraction | `tests/test_parser*.py` |
| **Planner** | Operator creation, entity binding, JoinKeyPairType | `tests/test_planner*.py` |
| **Optimizer** | Predicate pushdown, subquery flattening | `tests/test_subquery_*.py`, `tests/test_*_pushdown*.py` |
| **Resolver** | Column resolution, reference validation | `tests/test_column_res*.py` |
| **Enrichment** | Schema resolution, `EnrichedPlanData` correctness | `tests/test_enrichment*.py` |
| **Renderer** | SQL generation, expression rendering, CTE structure | `tests/test_renderer*.py`, `tests/test_defensive_*.py` |
| **Renderer (Procedural BFS)** | Procedural BFS SQL generation, edge expansion, visited-set logic | `tests/test_procedural_bfs_rendering.py` |
| **End-to-end** | Full Cypher → SQL → execution with PySpark | `tests/pyspark_tests/`, `tests/test_*_pyspark.py` |
| **End-to-end (Procedural BFS)** | Procedural BFS with PySpark execution | `tests/pyspark_tests/test_procedural_bfs_pyspark.py` |

---

## Test Infrastructure Reference

### Available Utilities

All utilities are importable from `tests.utils`:

```python
from tests.utils import (
    # Golden file management
    normalize_sql,
    assert_sql_equal,
    write_golden_if_missing,
    load_expected_sql,
    TranspilerTestCase,
    # Structural SQL assertions
    assert_has_select,
    assert_has_from_table,
    assert_has_where,
    assert_has_join,
    assert_no_join,
    assert_has_order_by,
    assert_has_limit_offset,
    assert_has_group_by,
    assert_has_recursive_cte,
    assert_has_array_agg,
    assert_projected_columns,
    assert_no_cartesian_join,
    SQLStructure,
)
```

### Key Imports by Test Type

#### Schema Setup

```python
from gsql2rsql.common.schema import NodeSchema, EdgeSchema
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor
```

#### Operator Tests

```python
from gsql2rsql.planner.operators import (
    DataSourceOperator, JoinOperator, JoinType, JoinKeyPair, JoinKeyPairType,
    ProjectionOperator, SelectionOperator, RecursiveTraversalOperator,
    AggregationBoundaryOperator, SetOperator, UnwindOperator,
)
from gsql2rsql.planner.schema import EntityField, Schema, ValueField, EntityType
```

#### Renderer Tests

```python
from gsql2rsql.renderer.render_context import RenderContext
from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
from gsql2rsql.renderer.join_renderer import JoinRenderer
from gsql2rsql.renderer.sql_renderer import SQLRenderer
```

#### End-to-End Tests

```python
from gsql2rsql import GraphContext
# or full pipeline:
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
```

#### PySpark Tests

```python
from pyspark.sql import SparkSession
from gsql2rsql.pyspark_executor import transpile_query, load_schema_from_yaml, create_spark_session
from tests.utils.sample_data_generator import SchemaDataGenerator
```

---

## Test Patterns (Templates)

### Pattern 1: Unit Test — Renderer Component

Test a single renderer method in isolation.

```python
"""Tests for [component] rendering."""

import pytest
from gsql2rsql.renderer.render_context import RenderContext
from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor
from gsql2rsql.common.schema import NodeSchema, EdgeSchema


class TestComponentRendering:
    """Tests for [specific component]."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.schema = SimpleSQLSchemaProvider()
        self.schema.add_node(
            NodeSchema(name="Person"),
            SQLTableDescriptor(
                entity_id="Person",
                table_name="dbo.Person",
                node_id_columns=["id"],
            ),
        )
        # Add more nodes/edges as needed...

        self.ctx = RenderContext(
            db_schema=self.schema,
            resolution_result=None,
            required_columns=set(),
            required_value_fields=set(),
            enable_column_pruning=False,
            config={},
            enriched=None,  # Set to EnrichedPlanData mock if testing enrichment-dependent code
        )
        self.expr_renderer = ExpressionRenderer(self.ctx)

    def test_specific_behavior(self):
        """Describe what behavior is verified and WHY."""
        # Arrange
        # ...

        # Act
        result = self.expr_renderer._some_method(...)

        # Assert — verify EXACT expected values
        assert result == "expected_sql_fragment"
```

### Pattern 2: Integration Test — Cypher to SQL

Test full transpilation pipeline.

```python
"""Integration tests for [feature] transpilation."""

import pytest
from tests.utils import assert_has_join, assert_has_where, assert_has_recursive_cte


class TestFeatureTranspilation:
    """Tests for [feature] Cypher → SQL transpilation."""

    def test_basic_case(self, movie_graph_schema):
        """[Feature] basic case produces correct SQL structure."""
        from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer

        cypher = """
        MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
        WHERE p.name = 'Tom'
        RETURN m.title
        """

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, movie_graph_schema)
        plan.resolve(original_query=cypher)
        renderer = SQLRenderer(db_schema_provider=movie_graph_schema)
        sql = renderer.render_plan(plan)

        print(f"\n=== Generated SQL ===\n{sql}")  # ALWAYS print for debugging

        # Structural assertions — verify SQL shape, not exact string
        assert_has_join(sql, join_type="INNER")
        assert_has_where(sql, condition="name")
        assert "dbo.Person" in sql
        assert "dbo.ActedIn" in sql
```

### Pattern 3: GraphContext End-to-End

Simplified API for end-to-end testing.

```python
"""End-to-end tests using GraphContext API."""

import pytest
from gsql2rsql import GraphContext
from gsql2rsql.common.schema import NodeSchema, EdgeSchema
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


@pytest.fixture
def graph_context():
    """Create a GraphContext with test schema."""
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(name="Person"),
        SQLTableDescriptor(entity_id="Person", table_name="nodes", node_id_columns=["id"]),
    )
    schema.add_edge(
        EdgeSchema(name="KNOWS", source_node_id="Person", sink_node_id="Person"),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="edges",
            node_id_columns=["src_id", "dst_id"],
        ),
    )
    gc = GraphContext.from_schema_provider(schema)
    return gc


class TestFeatureEndToEnd:
    def test_directed_query(self, graph_context):
        """Directed relationship produces correct SQL."""
        sql = graph_context.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            RETURN a.name, b.name
        """)
        print(f"\n=== SQL ===\n{sql}")
        assert "INNER JOIN" in sql.upper()
```

### Pattern 4: PySpark Execution Test

Verify generated SQL produces correct results with real data.

```python
"""PySpark execution tests for [feature]."""

import pytest
from pyspark.sql import SparkSession
from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests."""
    session = (
        SparkSession.builder
        .appName("TestFeature")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def graph_data(spark):
    """Create test graph with KNOWN data.

    Test Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                                   ^
          v                                   |
        Dave ---KNOWS---> Eve ----------------+
    """
    nodes_data = [
        ("Alice", "Person", 25),
        ("Bob", "Person", 30),
        ("Carol", "Person", 35),
        ("Dave", "Person", 28),
        ("Eve", "Person", 32),
    ]
    nodes_df = spark.createDataFrame(nodes_data, ["node_id", "label", "age"])
    nodes_df.createOrReplaceTempView("nodes")

    edges_data = [
        ("Alice", "Bob", "KNOWS"),
        ("Bob", "Carol", "KNOWS"),
        ("Alice", "Dave", "KNOWS"),
        ("Dave", "Eve", "KNOWS"),
        ("Eve", "Carol", "KNOWS"),
    ]
    edges_df = spark.createDataFrame(edges_data, ["src_id", "dst_id", "rel_type"])
    edges_df.createOrReplaceTempView("edges")

    gc = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        node_id_col="node_id",
        extra_node_attrs={"label": str, "age": int},
    )
    gc.set_types(node_types=["Person"], edge_types=["KNOWS"])
    return gc


class TestFeaturePySpark:
    def test_directed_from_alice(self, spark, graph_data):
        """Alice -[:KNOWS]-> should return outgoing neighbors."""
        sql = graph_data.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            WHERE a.node_id = 'Alice'
            RETURN b.node_id AS dst
        """)
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql).collect()
        results = {row["dst"] for row in result}
        expected = {"Bob", "Dave"}  # Alice -> Bob, Alice -> Dave
        assert results == expected, f"Expected {expected}, got {results}"
```

**Test Data Design Guidelines:**
- Small graphs: 5-10 nodes, 5-10 edges
- Each node/edge serves a purpose (document it)
- Draw the graph visually in comments
- Include edge cases (leaf nodes, cycles if relevant)
- Use deterministic data (no random generation in assertions)

### Pattern 5: Golden File Test

Regression test comparing SQL output against baseline.

```python
"""Golden file regression tests for [feature]."""

from tests.utils import TranspilerTestCase


class TestFeatureGolden:
    def test_basic_pattern(self, movie_graph_schema):
        """Verify SQL output matches golden file."""
        test_case = TranspilerTestCase(
            test_id="feature_01",
            test_name="basic_pattern",
            cypher_query="""
                MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
                RETURN p.name, m.title
            """,
            schema=movie_graph_schema,
            description="Basic directed relationship pattern",
        )
        # First run: creates tests/output/expected/feature_01_basic_pattern.sql
        # Subsequent runs: compares against golden file
        test_case.assert_transpiles_correctly()
```

### Pattern 6: Defensive Programming Test

Test error handling and edge cases in specific components.

```python
"""Defensive tests for [component]."""

import pytest
from gsql2rsql.renderer.join_renderer import JoinRenderer
from gsql2rsql.renderer.render_context import RenderContext


def _make_renderer():
    """Create minimal renderer for unit testing."""
    ctx = RenderContext.__new__(RenderContext)
    return JoinRenderer(ctx, None, None, None)


class TestDefensiveComponent:
    def test_invalid_input_raises_error(self):
        """Invalid input is caught early with clear error message."""
        renderer = _make_renderer()
        with pytest.raises(RuntimeError, match="expected pattern"):
            renderer._some_method(invalid_input)

    def test_edge_case_handled_gracefully(self):
        """Edge case produces sensible default, not crash."""
        renderer = _make_renderer()
        result = renderer._some_method(edge_case_input)
        assert result is not None
```

---

## Improving Existing Tests

### Checklist for Test Improvement

1. **Weak assertions** — Replace `assert len(rows) > 0` with exact value checks
2. **Missing SQL verification** — Add `print(f"\n=== SQL ===\n{sql}")` to all transpile tests
3. **Missing structural assertions** — Add `assert_has_join`, `assert_has_where`, etc.
4. **Missing edge cases** — Add tests for boundary conditions (empty results, single node, cycles)
5. **Flaky tests** — Replace non-deterministic assertions with deterministic ones
6. **Coverage gaps** — Use `uv run pytest --cov=src/gsql2rsql --cov-report=term-missing` to find uncovered lines

### Finding Coverage Gaps

```bash
# Generate coverage report
uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=term-missing 2>&1 | tail -60

# Focus on specific module
uv run pytest tests/ -n 4 --cov=src/gsql2rsql/renderer/expression_renderer --cov-report=term-missing
```

### Common Weak Patterns to Fix

```python
# BAD: Only checks "it runs"
sql = graph_context.transpile(query)
assert sql is not None

# GOOD: Verifies SQL structure
sql = graph_context.transpile(query)
assert_has_join(sql, join_type="INNER")
assert_has_where(sql, condition="age > 30")

# BAD: Checks length only
result = spark.sql(sql).collect()
assert len(result) == 3

# GOOD: Checks exact values
result = spark.sql(sql).collect()
results = {(row["src"], row["dst"]) for row in result}
expected = {("Alice", "Bob"), ("Alice", "Dave"), ("Bob", "Carol")}
assert results == expected
```

---

## YAML Example Tests

### Adding New YAML Examples

YAML files live in `examples/` and are consumed by parametrized PySpark tests.

```yaml
# examples/my_feature_queries.yaml
schema:
  nodes:
    - name: Person
      tableName: catalog.demo.Person
      idProperty: { name: id, type: int }
      properties:
        - { name: name, type: string }
        - { name: age, type: int }
  edges:
    - name: KNOWS
      sourceNode: Person
      sinkNode: Person
      tableName: catalog.demo.Knows
      sourceIdProperty: { name: src_id, type: int }
      sinkIdProperty: { name: dst_id, type: int }

examples:
  - description: "Simple directed traversal"
    application: "Demo: Basic"
    query: |
      MATCH (a:Person)-[:KNOWS]->(b:Person)
      RETURN a.name, b.name
    notes: |
      Basic directed single-hop pattern.
    expected_error: false

  - description: "Query with known error"
    application: "Demo: Error case"
    query: |
      MATCH (a:Person) OVER (PARTITION BY a.name)
      RETURN a
    notes: |
      Uses SQL OVER syntax, not valid Cypher.
    expected_error: true
```

---

## Schema Setup Patterns

### Minimal Schema (1 node, 1 edge)

```python
schema = SimpleSQLSchemaProvider()
schema.add_node(
    NodeSchema(name="Person"),
    SQLTableDescriptor(entity_id="Person", table_name="person_table", node_id_columns=["id"]),
)
schema.add_edge(
    EdgeSchema(name="KNOWS", source_node_id="Person", sink_node_id="Person"),
    SQLTableDescriptor(
        entity_id="Person@KNOWS@Person",
        table_name="knows_table",
        node_id_columns=["src_id", "dst_id"],
    ),
)
```

### Multi-Type Schema

```python
schema = SimpleSQLSchemaProvider()
# Nodes
for node_name, table_name in [("Person", "dbo.Person"), ("Movie", "dbo.Movie")]:
    schema.add_node(
        NodeSchema(name=node_name),
        SQLTableDescriptor(entity_id=node_name, table_name=table_name, node_id_columns=["id"]),
    )
# Edges
schema.add_edge(
    EdgeSchema(name="ACTED_IN", source_node_id="Person", sink_node_id="Movie"),
    SQLTableDescriptor(
        entity_id="Person@ACTED_IN@Movie",
        table_name="dbo.ActedIn",
        node_id_columns=["person_id", "movie_id"],
    ),
)
```

### Using conftest Fixture

The `movie_graph_schema` fixture (in `tests/conftest.py`) provides:
- Nodes: Person, Movie
- Edges: ACTED_IN, DIRECTED, PRODUCED, WROTE, FOLLOWS, REVIEWED

```python
def test_something(self, movie_graph_schema):
    # movie_graph_schema is a SimpleSQLSchemaProvider
    renderer = SQLRenderer(db_schema_provider=movie_graph_schema)
```

---

## Testing Enrichment

When testing code that depends on `EnrichedPlanData`:

```python
from gsql2rsql.renderer.sql_enrichment import (
    EnrichedPlanData,
    EnrichedDataSource,
    EnrichedRecursiveOp,
    EnrichedExistsExpr,
    SQLEnrichmentPass,
)

# Option 1: Run enrichment pass (integration test)
enrichment = SQLEnrichmentPass(db_schema=schema)
enriched = enrichment.enrich(plan)
ctx = RenderContext(..., enriched=enriched)

# Option 2: Construct enriched data directly (unit test)
enriched = EnrichedPlanData(
    data_sources={"op_0": EnrichedDataSource(table_name="dbo.Person", ...)},
    recursive_ops={},
    node_infos={},
    recursive_edges={},
    exists_exprs={},
)
ctx = RenderContext(..., enriched=enriched)
```

---

## Naming Conventions

| Type | File Pattern | Class Pattern |
|------|-------------|---------------|
| Unit test | `test_{module}.py` | `Test{Component}` |
| Integration | `test_{feature}_integration.py` | `Test{Feature}Integration` |
| PySpark | `test_{feature}_pyspark.py` or `pyspark_tests/test_{feature}.py` | `Test{Feature}PySpark` |
| Golden file | `test_{feature}_golden.py` | `Test{Feature}Golden` |
| Defensive | `test_defensive_{component}.py` | `TestDefensive{Component}` |
| Regression | `test_{issue_description}.py` | `Test{IssueDescription}` |

---

## Verification Commands

```bash
# Run specific test file
uv run pytest tests/test_my_feature.py -v -s

# Run specific test method
uv run pytest tests/test_my_feature.py::TestClass::test_method -v -s

# Run all non-PySpark tests (fast, parallelized)
uv run pytest tests/ --ignore=tests/pyspark_tests -n 4 -q

# Run PySpark tests
uv run pytest tests/pyspark_tests/ -v -s

# Run procedural BFS PySpark tests
uv run pytest tests/pyspark_tests/test_procedural_bfs_pyspark.py -v -s

# Run procedural BFS rendering tests (golden SQL assertions)
uv run pytest tests/test_procedural_bfs_rendering.py -v

# Run with coverage for specific module
uv run pytest tests/ -n 4 --cov=src/gsql2rsql --cov-report=term-missing

# Type checks (MANDATORY before committing)
uv run pyright src/gsql2rsql
uv run mypy src/gsql2rsql

# Full verification
uv run pyright src/gsql2rsql && uv run mypy src/gsql2rsql && uv run pytest tests/ -n 4 -q
```

**Baseline:** 1465 passed, 12 skipped, 3 xfailed (non-PySpark suite)

---

## Common Mistakes to Avoid

### 1. Non-Deterministic Assertions

```python
# BAD: Order-dependent
assert result == [("Bob",), ("Alice",)]

# GOOD: Order-independent
assert set(result) == {("Bob",), ("Alice",)}
# or
assert sorted(result) == sorted([("Bob",), ("Alice",)])
```

### 2. Testing Implementation Instead of Behavior

```python
# BAD: Brittle string match
assert sql == "SELECT p.name FROM dbo.Person p WHERE p.age > 30"

# GOOD: Structural assertion
assert_has_where(sql, condition="age")
assert_has_from_table(sql, "Person")
```

### 3. Missing Print for Debugging

```python
# BAD: No way to debug failures
sql = graph_context.transpile(query)
assert "JOIN" in sql

# GOOD: SQL visible in test output
sql = graph_context.transpile(query)
print(f"\n=== SQL ===\n{sql}")
assert "JOIN" in sql
```

### 4. Over-Scoped Fixtures

```python
# BAD: Module-scoped SparkSession for a single test
@pytest.fixture(scope="module")
def spark():
    ...  # expensive for one test

# GOOD: Module-scoped when multiple tests share it
# Function-scoped when only one test needs it
```

### 5. Wrong Test Expectations

```python
# BAD: Assertion doesn't match test data
# Data has: Alice->Bob, Alice->Dave
assert results == {"Bob", "Dave", "Carol"}  # Carol not reachable from Alice!

# GOOD: Verify expectations against the graph you defined
# Alice -> Bob (direct), Alice -> Dave (direct)
assert results == {"Bob", "Dave"}
```

### 6. Testing Reserved Words

```python
# BAD: Uses Cypher reserved word as alias
query = "MATCH (n) RETURN n.name AS end"  # 'end' is reserved!

# GOOD: Use non-reserved aliases
query = "MATCH (n) RETURN n.name AS name_result"
```

---

## Process

### For New Tests

1. **Identify what to test** — Feature, bug fix, or coverage gap
2. **Choose test type** — Unit, integration, PySpark, golden, or structural
3. **Design test data** — Small, deterministic, documented graph
4. **Write the test** — Follow patterns above
5. **Run and verify** — `uv run pytest tests/test_my_file.py -v -s`
6. **Check no regressions** — `uv run pytest tests/ -n 4 -q`

### For Test Improvement

1. **Run coverage** — Identify gaps
2. **Read existing test** — Understand current assertions
3. **Add stronger assertions** — Exact values, structural checks
4. **Add edge cases** — Empty results, single nodes, cycles
5. **Run and verify** — Same as above

---

## Notes for the Agent

- **Always print SQL** — Every transpile test should `print(f"\n=== SQL ===\n{sql}")`
- **Exact values over counts** — `assert results == {expected}` not `assert len(results) == N`
- **Document test data** — Draw the graph in comments
- **Use existing fixtures** — Check `conftest.py` before creating new schemas
- **Structural assertions** — Use `tests.utils.sql_assertions` instead of string matching
- **Keep tests focused** — One behavior per test method
- **Run the full suite** — After adding tests, verify no regressions
- **Respect the baseline** — 1465 passed, 12 skipped, 3 xfailed
- **Dual-mode VLP testing** — When writing VLP tests, verify both CTE (`recursive_cte_renderer`) and procedural BFS (`procedural_bfs_renderer`) renderers produce correct results. Use `procedural_bfs_mode="numbered_views"` for PySpark tests
