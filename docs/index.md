# gsql2rsql

**Query your Delta Tables as a Graph**

No need for a separate graph database. Write intuitive OpenCypher queries, get Databricks SQL automatically.

!!! success "Why Databricks?"
    Databricks provides tables designed for massive scale, enabling efficient storage and querying of tens of billions of triples with features like time travel No ETL or migration needed—just query your data lake as a graph. Recently, Databricks released support for recursive queries, unlocking the use of SQL warehouses for graph-type queries.

---

## Why gsql2rsql?

| Challenge | Solution |
|-----------|----------|
| Graph queries require complex SQL with `WITH RECURSIVE` | Write 5 lines of Cypher instead |
| Need to maintain a separate graph database | Query Delta Lake directly |
| LLM-generated complex SQL is hard to audit | Human-readable Cypher + deterministic transpilation (optionally pass to LLM for final optimization) |
| Scaling to tens of billions of triples is costly in graph DBs | Delta Lake stores billions of triples efficiently, with Spark scalability |



## See It in Action

```bash
pip install gsql2rsql
```

```python
from gsql2rsql import GraphContext

# Point to your existing Delta tables - no migration needed
graph = GraphContext(
    nodes_table="catalog.fraud.nodes",
    edges_table="catalog.fraud.edges",
)

# Write graph queries with familiar Cypher syntax
sql = graph.transpile("""
    MATCH path = (origin:Person {id: 12345})-[:TRANSACTION*1..4]->(dest:Person)
    WHERE dest.risk_score > 0.8
    RETURN dest.id, dest.name, dest.risk_score, length(path) AS depth
    ORDER BY depth, dest.risk_score DESC
    LIMIT 3
""")

print(sql)
```

**5 lines of Cypher → optimized Databricks SQL with recursive CTEs**

??? example "Click to see the generated SQL (auto-generated from transpiler)"

{{ fraud_example_sql(indent=4, include_fence=True) }}

---

!!! warning "Early Stage Project — Not for OLTP or end-user queries"
    This project is in **early development**. APIs may change, features may be incomplete, and bugs are expected. Contributions and feedback are welcome!

    This transpiler is for **internal analytics and exploration** (data science, engineering, analysis). It obviously makes no sense for OLTP! If you plan to expose transpiled queries to end users, be careful: implement validation, rate limiting, and security. Use common sense.


## Real-World Examples

=== "Fraud Detection"

    ```cypher
    -- Find fraud rings: accounts connected through suspicious transactions
    MATCH (a:Account)-[:TRANSFER*2..4]->(b:Account)
    WHERE a.flagged = true AND b.flagged = true
    RETURN DISTINCT a.id, b.id, length(path) AS hops
    ```

    [See more fraud detection queries →](examples/fraud.md)

=== "Credit Analysis"

    ```cypher
    -- Analyze credit exposure through guarantor chains
    MATCH path = (borrower:Customer)-[:GUARANTEES*1..3]->(guarantor:Customer)
    WHERE borrower.credit_score < 600
    RETURN borrower.id, COLLECT(guarantor.id) AS chain
    ```

    [See more credit analysis queries →](examples/credit.md)

=== "Social Network"

    ```cypher
    -- Friends of friends who work at tech companies
    MATCH (me:Person {id: 123})-[:KNOWS*1..2]->(friend)-[:WORKS_AT]->(c:Company)
    WHERE c.industry = 'Technology'
    RETURN DISTINCT friend.name, c.name
    ```

    [See all feature examples →](examples/features.md)

---




**That's it!** No schema boilerplate, no complex setup.

[Full User Guide →](user-guide.md)

---

## Low-Level API (Without GraphContext)

For advanced use cases or non-Triple-Store schemas, use the components directly:

```python
from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor

# 1. Define schema (SimpleSQLSchemaProvider)
schema = SimpleSQLSchemaProvider()

person = NodeSchema(
    name="Person",
    node_id_property=EntityProperty("id", int),
    properties=[EntityProperty("name", str)],
)
schema.add_node(
    person,
    SQLTableDescriptor(table_name="people", node_id_columns=["id"]),
)

knows = EdgeSchema(
    name="KNOWS",
    source_node_id="Person",
    sink_node_id="Person",
)
schema.add_edge(
    knows,
    SQLTableDescriptor(table_name="friendships"),
)

# 2. Transpile
parser = OpenCypherParser()
ast = parser.parse("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name")
plan = LogicalPlan.process_query_tree(ast, schema)
plan.resolve(original_query="...")

renderer = SQLRenderer(db_schema_provider=schema)
sql = renderer.render_plan(plan)
```


---

## Key Features

| Feature | Description |
|---------|-------------|
| **Variable-length paths** | `[:REL*1..5]` via `WITH RECURSIVE` |
| **Cycle detection** | Automatic `ARRAY_CONTAINS` checks |
| **Path functions** | `length(path)`, `nodes(path)`, `relationships(path)` |
| **No-label nodes** | `(a)-[:REL]->(b:Label)` matches any node type for `a` |
| **Inline filters** | `(n:Person {id: 123})` pushes predicates to source |
| **Undirected edges** | `(a)-[:KNOWS]-(b)` via optimized UNION ALL |
| **Aggregations** | COUNT, SUM, AVG, COLLECT, etc. |
| **Type safety** | Schema validation before SQL generation |

---

## Architecture

gsql2rsql uses a **4-phase pipeline** for correctness:

```
OpenCypher → Parser → Planner → Resolver → Renderer → SQL
```

1. **Parser**: Cypher → AST (syntax only, no schema)
2. **Planner**: AST → Logical operators (semantics)
3. **Resolver**: Validate columns & types against schema
4. **Renderer**: Operators → Databricks SQL

This separation ensures each phase has clear responsibilities and can be tested independently.


---

## Documentation

| Section | Description |
|---------|-------------|
| [**User Guide**](user-guide.md) | Getting started, GraphContext, schema setup |
| [**Examples**](examples/index.md) | queries with generated SQL |


---

## Project Status

!!! info "Research Project"
 **Contributions welcome!**


- [GitHub Repository](https://github.com/devmessias/gsql2rsql)
- [Issue Tracker](https://github.com/devmessias/gsql2rsql/issues)
- [Contributing Guide](contributing.md)

---

## License

MIT License - see [LICENSE](https://github.com/devmessias/gsql2rsql/blob/main/LICENSE)

---
--8<-- "inspiration.md"
