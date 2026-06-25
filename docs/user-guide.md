# User Guide

This guide covers everything you need to start using gsql2rsql to transpile OpenCypher queries to Databricks SQL.

!!! tip "Looking for examples?"
    After learning the basics, check out complete query examples:

    - [**Fraud Detection**](examples/fraud.md) - Fraud rings, money laundering patterns
    - [**Credit Analysis**](examples/credit.md) - Risk assessment, guarantor chains
    - [**All Features**](examples/features.md) - 69 queries covering every feature

---

## Installation

```bash
pip install gsql2rsql
```

**Requirements:**

- Python 3.12+
- Databricks Runtime 15+ only needed to **execute** the generated SQL

---

## Try It Now

No database needed! Just define a schema and generate SQL:

```python
from gsql2rsql import GraphContext

# Table names are just strings - no database connection required
graph = GraphContext(
    nodes_table="my_nodes",
    edges_table="my_edges",
)
graph.set_types(
    node_types=["Person", "Company"],
    edge_types=["WORKS_AT"],
)

# Generate SQL from OpenCypher
sql = graph.transpile("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    RETURN p.node_id, c.node_id
""")

print(sql)  # Copy this SQL to run on Databricks
```

??? example "Generated SQL Output"

{{ userguide_try_it_now_sql(indent=4) }}

---

## GraphContext: Full Configuration

**GraphContext** is the recommended API for graph data stored in the Triple Store pattern (one nodes table + one edges table). It eliminates ~100 lines of schema boilerplate.

### With Node/Edge Attributes

```python
from gsql2rsql import GraphContext

# Define schema with additional attributes
graph = GraphContext(
    nodes_table="catalog.schema.nodes",
    edges_table="catalog.schema.edges",
    extra_node_attrs={"name": str, "age": int, "score": float},
    extra_edge_attrs={"weight": float, "timestamp": str},
)

graph.set_types(
    node_types=["Person", "Company", "Account"],
    edge_types=["KNOWS", "WORKS_AT", "OWNS"],
)

# Now you can use those attributes in queries
sql = graph.transpile("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    WHERE c.industry = 'Technology'
    RETURN p.name, c.name AS company
    LIMIT 100
""")

print(sql)
```

??? example "Generated SQL"

{{ userguide_with_attrs_sql(indent=4) }}

### GraphContext Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `nodes_table` | str | Full path to nodes table (e.g., `catalog.schema.nodes`) |
| `edges_table` | str | Full path to edges table |
| `node_type_col` | str | Column for node type (default: `"node_type"`) |
| `edge_type_col` | str | Column for edge type (default: `"relationship_type"`) |
| `node_id_col` | str | Column for node ID (default: `"node_id"`) |
| `edge_src_col` | str | Column for edge source (default: `"src"`) |
| `edge_dst_col` | str | Column for edge destination (default: `"dst"`) |
| `extra_node_attrs` | dict | Additional node properties `{name: type}` |
| `extra_edge_attrs` | dict | Additional edge properties `{name: type}` |
| `spark` | SparkSession | Optional: for auto-discovery and execution |
| `optimize_dead_tables` | bool | Enable dead table elimination optimization (default: `True`) |

### Automatic System Properties

GraphContext automatically exposes system properties for use in queries, without needing to declare them in `extra_node_attrs` or `extra_edge_attrs`:

**Node system properties:**

| Property | Default Column | Description |
|----------|---------------|-------------|
| `node_id` | `node_id` | Unique node identifier |
| `node_type` | `node_type` | Node label/type |

**Edge system properties:**

| Property | Default Column | Description |
|----------|---------------|-------------|
| `src` | `src` | Source node ID |
| `dst` | `dst` | Target node ID |
| `relationship_type` | `relationship_type` | Edge label/type |

This allows queries like:

```python
# Filter by node_type without declaring it in extra_node_attrs
sql = graph.transpile("""
    MATCH (a)-[r]->(b)
    WHERE a.node_type IN ['Person', 'Company']
    RETURN a.node_id, r.relationship_type, b.node_id
""")
```

### Expected Table Structure

**Nodes table:**

| Column | Description |
|--------|-------------|
| `node_id` | Unique node identifier |
| `node_type` | Node label (Person, Company, etc.) |
| `name`, `age`, ... | Node properties |

**Edges table:**

| Column | Description |
|--------|-------------|
| `src` | Source node ID |
| `dst` | Destination node ID |
| `relationship_type` | Edge label (KNOWS, WORKS_AT, etc.) |
| `weight`, ... | Edge properties |

---

## No-Label Nodes (Wildcard Matching)

GraphContext automatically enables **no-label support**, allowing nodes without explicit labels in queries:

```python
# Node 'a' has no label - matches ANY node type
sql = graph.transpile("""
    MATCH (a)-[:WORKS_AT]->(c:Company)
    RETURN a, c.name
""")
```

This is useful when:

- You don't know or care about the source node type
- You want to match multiple node types at once
- You're exploring relationships without type constraints

!!! warning "Performance Impact"
    No-label nodes cause **full table scans** on the nodes table (no `WHERE node_type = '...'` filter). Use explicit labels whenever possible for production queries.

---

## Variable-Length Paths

One of gsql2rsql's most powerful features is support for variable-length paths using `WITH RECURSIVE`.

```python
sql = graph.transpile("""
    MATCH path = (origin:Person {id: 12345})-[:KNOWS*1..3]->(friend:Person)
    RETURN friend.name, length(path) AS hops
    ORDER BY hops
""")
```

**Key features:**

- **Depth bounds**: `*1..3` means 1 to 3 hops
- **Cycle detection**: Automatic `ARRAY_CONTAINS` checks prevent infinite loops
- **Path functions**: `length(path)`, `nodes(path)`, `relationships(path)`
- **Path variable**: Captures the entire path for analysis

### Bidirectional BFS Optimization

When BOTH source AND target have equality filters on ID, bidirectional BFS can enable queries that would otherwise **fail due to Spark's recursion limits**.

```python
sql = graph.transpile(
    """
    MATCH path = (a:Person)-[:KNOWS*1..4]->(b:Person)
    WHERE a.node_id = 'alice' AND b.node_id = 'dave'
    RETURN nodes(path) AS path_nodes
    """,
    bidirectional_mode="recursive"  # Enable optimization
)
```

??? example "Generated SQL (bidirectional off vs recursive)"

    === "bidirectional_mode='off'"

{{ bidirectional_example_sql(mode="off", include_fence=True) | indent(8, first=True) }}

    === "bidirectional_mode='recursive'"

{{ bidirectional_example_sql(mode="recursive", include_fence=True) | indent(8, first=True) }}

**Why use it:**

- **Unidirectional BFS** explores ALL paths from source (exponential growth)
- **Bidirectional BFS** explores from both ends and prunes paths that don't meet
- For small graphs: bidirectional has ~20-30% overhead
- For large graphs: unidirectional **fails** (hits `maxRowsPerIteration` limit), bidirectional **succeeds**

**Modes:**

| Mode | Description |
|------|-------------|
| `"off"` | Standard unidirectional BFS |
| `"recursive"` | WITH RECURSIVE forward/backward CTEs (default) |
| `"unrolling"` | Unrolled CTEs (best for depth ≤6) |
| `"auto"` | Auto-select based on max_hops |

---

## Inline Filters

Use inline property filters for better performance:

```python
# Inline filter (optimized - pushed to source scan)
sql = graph.transpile("""
    MATCH (p:Person {status: 'active'})-[:KNOWS]->(friend)
    RETURN p.name, friend.name
""")

# Equivalent WHERE clause (less optimized)
sql = graph.transpile("""
    MATCH (p:Person)-[:KNOWS]->(friend)
    WHERE p.status = 'active'
    RETURN p.name, friend.name
""")
```

Inline filters are pushed down to the data source scan, reducing the amount of data processed.

---

## Undirected Relationships

Use `-[:REL]-` (without arrow) for undirected relationships:

```python
sql = graph.transpile("""
    MATCH (a:Person)-[:KNOWS]-(b:Person)
    WHERE a.id = 123
    RETURN b.name
""")
```

This generates a `UNION ALL` query to match both directions efficiently.

---

## Aggregations

Standard aggregation functions are supported:

```python
sql = graph.transpile("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    RETURN c.name, COUNT(p) AS employees, AVG(p.salary) AS avg_salary
    ORDER BY employees DESC
""")
```

**Supported functions:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`, `COUNT(DISTINCT ...)`

---

## WITH Clauses

Chain multiple query stages with `WITH`:

```python
sql = graph.transpile("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    WITH c, COUNT(p) AS employee_count
    WHERE employee_count > 100
    MATCH (c)-[:LOCATED_IN]->(city:City)
    RETURN c.name, employee_count, city.name
""")
```

---

## UNION Queries

Combine multiple result sets:

```python
sql = graph.transpile("""
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    RETURN p.name AS source, friend.name AS target, 'KNOWS' AS type
    UNION
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    RETURN p.name AS source, c.name AS target, 'WORKS_AT' AS type
""")
```

---

## Execution on Databricks

If you provide a SparkSession, you can execute queries directly:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

graph = GraphContext(
    spark=spark,  # Enable execution
    nodes_table="catalog.schema.nodes",
    edges_table="catalog.schema.edges",
)
graph.set_types(node_types=["Person"], edge_types=["KNOWS"])

# Transpile and execute
sql = graph.transpile("MATCH (p:Person) RETURN p.name LIMIT 10")
df = spark.sql(sql)
df.show()
```

---

## Low-Level API (Without GraphContext)

For complex scenarios where graph data is spread across multiple tables (not a simple Triple Store), or when you need full control, use the components directly:

```python
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# 1. Define schema (SimpleSQLSchemaProvider)
schema = SimpleSQLSchemaProvider()

person = NodeSchema(
    name="Person",
    properties=[
        EntityProperty(property_name="id", data_type=int),
        EntityProperty(property_name="name", data_type=str),
    ],
    node_id_property=EntityProperty(property_name="id", data_type=int)
)
schema.add_node(
    person,
    SQLTableDescriptor(
        table_name="catalog.schema.people",  # Separate table for Person nodes
        node_id_columns=["id"],
    )
)

knows = EdgeSchema(
    name="KNOWS",
    source_node_id="Person",
    sink_node_id="Person",
    source_id_property=EntityProperty(property_name="person_id", data_type=int),
    sink_id_property=EntityProperty(property_name="friend_id", data_type=int),
)
schema.add_edge(
    knows,
    SQLTableDescriptor(
        entity_id="Person@KNOWS@Person",
        table_name="catalog.schema.friendships",  # Separate table for KNOWS edges
    )
)

# 2. Transpile
parser = OpenCypherParser()
ast = parser.parse("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name")
plan = LogicalPlan.process_query_tree(ast, schema)
plan.resolve(original_query="...")

renderer = SQLRenderer(db_schema_provider=schema)
sql = renderer.render_plan(plan)
```

This approach is useful when:

- **Different tables for different node/edge types** (e.g., `people`, `companies`, `friendships`)
- **Custom column mappings** beyond what GraphContext supports
- **Integration with existing schemas** that don't follow Triple Store pattern

---

## CLI Usage

gsql2rsql includes a command-line interface:

```bash
# Transpile a query
echo "MATCH (p:Person) RETURN p.name" | gsql2rsql translate --schema schema.json

# Interactive TUI
gsql2rsql tui --schema schema.json
```


---

## OpenCypher Grammar Notes

gsql2rsql follows the **standard OpenCypher grammar**. Some patterns that might seem intuitive are actually invalid Cypher syntax.

### WHERE Clause Placement

`WHERE` can only appear directly after `MATCH` or `WITH` clauses - **not after `UNWIND`**:

```python
# ❌ INVALID - WHERE cannot follow UNWIND directly
sql = graph.transpile("""
    MATCH (s)-[e:KNOWS*1..3]->(o)
    UNWIND e AS r
    WHERE r.weight > 5
    RETURN r.src, r.dst
""")
# Error: no viable alternative at input 'WHERE'

# ✅ CORRECT - Use WITH to enable WHERE filtering
sql = graph.transpile("""
    MATCH (s)-[e:KNOWS*1..3]->(o)
    UNWIND e AS r
    WITH r WHERE r.weight > 5
    RETURN r.src, r.dst
""")
```

This is standard Cypher behavior (same as Neo4j).

### Reserved Words

Some words are reserved and cannot be used as variable names:

| Reserved Word | Used In | Workaround |
|---------------|---------|------------|
| `end` | `CASE ... END` | Use `target`, `endpoint`, `dest` |
| `start` | Legacy syntax | Use `source`, `origin`, `from_node` |
| `match` | `MATCH` clause | Use `matched`, `result` |
| `return` | `RETURN` clause | Use `result`, `output` |

```python
# ❌ INVALID - 'end' is a reserved word
sql = graph.transpile("""
    MATCH (start)-[:KNOWS]->(end)
    RETURN end.name
""")
# Error: no viable alternative at input '...)->(end'

# ✅ CORRECT - Use a different variable name
sql = graph.transpile("""
    MATCH (start)-[:KNOWS]->(target)
    RETURN target.name
""")
```

### String Concatenation

Cypher uses `+` for string concatenation, but Spark SQL uses `CONCAT()`. Use the SQL function directly:

```python
# ❌ May fail in Spark - '+' not supported for strings
sql = graph.transpile("""
    MATCH (p:Person)
    RETURN p.first_name + ' ' + p.last_name AS full_name
""")

# ✅ CORRECT - Use CONCAT function
sql = graph.transpile("""
    MATCH (p:Person)
    RETURN CONCAT(p.first_name, ' ', p.last_name) AS full_name
""")
```

---

## Limitations

- **Databricks new Runtime** required for `WITH RECURSIVE` and HoF
- **Write operations** not supported (`CREATE`, `DELETE`, `SET`)
- **Multi-label node syntax** not yet supported:
    - `(a:Person|Company)` - pipe OR syntax causes parser error
    - `(a:Person:Company)` - colon AND syntax silently ignores additional labels
    - **Workaround:** Use `WHERE a.node_type IN ['Person', 'Company']`
- **Some Cypher features** not yet implemented

---

## Next Steps

- [**Examples Gallery**](examples/index.md) - query examples
