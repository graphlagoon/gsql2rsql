# Limitations

What the transpiler does not support, what only works in specific rendering
modes, and how to read the error messages designed to catch these cases
early.

The transpiler is **read-only**: it turns openCypher `MATCH` queries into
Databricks SQL. Anything that mutates the graph is out of scope by design.

---

## Quick Reference

| Feature | Status | Notes |
|---|---|---|
| `MATCH`, `WHERE`, `RETURN`, `WITH`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT` | ✅ | Core pipeline |
| Variable-length paths `-[:TYPE*1..N]->` | ✅ | Requires `WITH RECURSIVE` (Databricks Runtime 17+) |
| Undirected and multi-type edges `-[:A\|B]-` | ✅ | UNION ALL expansion |
| `relationships(p)`, `length(p)`, `nodes(p)` | ✅ CTE mode | See [rendering-mode matrix](#cte-vs-procedural-rendering-modes) |
| Backtick-escaped identifiers `` (n:`my label`) `` | ✅ | Delimiters stripped per openCypher |
| `CREATE`, `DELETE`, `SET`, `REMOVE`, `MERGE`, `FOREACH` | ❌ | Write operations — use SQL DML directly |
| `shortestPath()`, `allShortestPaths()` | ❌ | Workaround: bounded BFS + `ORDER BY length(p) LIMIT 1` |
| `CALL` procedures (APOC) | ❌ | Neo4j-specific |
| Multi-label nodes `(a:Person:Company)` | ❌ | Workaround: `WHERE a.node_type IN [...]` |
| Map projections `n{.id, .name}` | ❌ | Workaround: `{id: n.id, name: n.name}` |
| `duration()`, temporal arithmetic | ⚠️ | Basic datetime functions only |
| Geospatial (`point()`, `distance()`) | ❌ | Use Databricks geospatial SQL directly |
| `!=` operator | ❌ deliberate | Use `<>` — `!=` is rejected as a syntax error |
| Standalone `UNWIND ... RETURN` (no `MATCH`) | ❌ | Fails with "Empty partial query" |
| `percentileCont()`, `percentileDisc()` | ✅ | `PERCENTILE_CONT/DISC ... WITHIN GROUP` |
| `LIMIT $n` / `SKIP $n` (parameters) | ❌ | Inline the value — rejected with a clear error |
| Label predicates `WHERE a:Person` | ❌ | Move the label into the pattern: `MATCH (a:Person)` |
| `split()`, `substring()`, `replace()`, `reverse()`, `head()`, `tail()`, `keys()`, `labels()`, `id()`, `properties()` | ❌ | Rejected with an error naming the function |
| Unbounded VLP `-[:TYPE*]->` | ⚠️ | Silently capped at 10 hops — write an explicit bound (`*1..15` is honoured as-is) |
| Relationship uniqueness (edge isomorphism) | ⚠️ | Not enforced — see [Correctness Caveats](#correctness-caveats) |

---

## Runtime Requirements

- **Databricks Runtime 17+** (or Spark 4.x) for `WITH RECURSIVE` — required
  by every variable-length path query.
- **Spark SQL scripting** (`spark.sql.scripting.enabled=true`, PySpark 4.2+)
  for the procedural BFS mode (`vlp_rendering_mode="procedural"`).
- Generated SQL uses Databricks-specific functions (`array_contains`,
  `CONCAT` on arrays, `STRUCT`, `NAMED_STRUCT`) and is **not portable** to
  other databases without modification.

Recursion limits are Spark-version dependent: Spark 4.x defaults to 100
iterations (`RECURSION_LEVEL_LIMIT_EXCEEDED`); older versions cap rows per
iteration. The [bidirectional BFS](#deep-variable-length-paths) option
exists specifically to work around these limits.

---

## CTE vs Procedural Rendering Modes

Variable-length paths can be rendered two ways, selected per query:

```python
graph.transpile(query)                                   # CTE (default)
graph.transpile(query, vlp_rendering_mode="procedural",
                materialization_strategy="numbered_views")
```

**CTE mode** (`WITH RECURSIVE`) is the default and supports every VLP
construct. **Procedural mode** (a `BEGIN…END` BFS loop) is a performance
option for deep traversals from a **single start node** — its architecture
keeps one global visited set and attributes every discovered node to the
seed, which makes several constructs unrepresentable. Those cases fail
loudly instead of returning wrong results:

| Construct | CTE | Procedural |
|---|---|---|
| `count(DISTINCT d)`, filters, aggregations over VLP | ✅ | ✅ |
| `relationships(p)` + `UNWIND` | ✅ | ✅ |
| `ALL(n IN nodes(p) WHERE ...)` predicate pushdown | ✅ | ✅ |
| `length(p)` | ✅ | ❌ transpile-time error |
| `nodes(p)` / `size(nodes(p))` | ✅ | ❌ transpile-time error |
| `*0..N` (zero-length paths include the start node) | ✅ | ❌ transpile-time error |
| Unfiltered / chained VLP source (`MATCH (a)-[*]->(b) MATCH (b)-[*]->(c)`) | ✅ | ❌ transpile-time error |
| Start filter matching **multiple** nodes | ✅ | ❌ runtime `RAISE_ERROR` |
| `is_terminator()` barriers, edge filters, sink filters | ✅ | ✅ |

If procedural mode rejects your query, the error message says so
explicitly — switch to `vlp_rendering_mode="cte"` (or drop the argument)
and the query works.

!!! note "Why a runtime guard?"
    Whether a start filter matches one node or many is only known when the
    query runs. The generated script counts the seed frontier and raises
    `gsql2rsql procedural BFS is single-source but the start filter matched
    multiple nodes` rather than fabricating unreachable (start, end) pairs.

---

## Schema Binding Errors

Node labels and relationship types in your Cypher are matched against the
registered schema **exactly** — case-sensitive, whitespace-sensitive. When
a lookup misses, the error lists what is registered and flags
case-insensitive near-matches:

```text
Failed to bind entity 'root' of type 'cnpj_raiz'. Did you mean 'CNPJ_RAIZ'?
Node type names are matched exactly (case-sensitive, whitespace-sensitive).
Available node types: 'CNPJ_RAIZ', 'socio'
```

```text
Failed to bind relationship '_anon1' of type 'owns'. Did you mean 'OWNS'?
Edge type names are matched exactly (case-sensitive, whitespace-sensitive).
Available edge types: 'OWNS', 'PARTICIPA'
```

Two edge-type situations behave differently on purpose:

- A type that exists for **no** endpoint pair anywhere in the schema is
  treated as a typo and raises — including inside OR lists (`[:OWNS|KNOWS]`).
- A type that exists but not between the pattern's endpoint node types is
  silently dropped from an OR list. This is intentional: with restricted
  `edge_combinations`, `[:X|Y]` between `A` nodes legitimately reduces to
  `X` when `Y` only connects `A → B`.

---

## Parser Notes

- **`!=` is rejected on purpose** — use `<>`. This avoids silent confusion
  with other dialects; the parser error tells you the replacement.
- **Backticks are fully supported** for labels, relationship types,
  variables, property names, and aliases (`` (n:`my label`) ``,
  ``RETURN n.`first name` AS x``). Doubled backticks escape a literal one.
- **Unaliased projections get generated column names** derived from the
  expression text: `RETURN count(DISTINCT d)` produces a column called
  `count_DISTINCT_d`, `count(*)` produces `count_star`. Add `AS name` to
  control the output column name.
- **Colliding unaliased property projections are qualified**: in
  `RETURN a.name, b.name` both columns would be called `name`, so they
  become `a_name` and `b_name`. A single `RETURN p.name` still produces
  `name`. Explicit `AS` aliases are never rewritten.
- **`RETURN *` / `WITH *` expand to the named variables in scope** —
  nodes and relationships project as structs of their properties, the
  same as `RETURN a`. Anonymous pattern parts and named path variables
  are not included.
- **Multi-label syntax is not supported**: `(a:Person:Company)` silently
  uses only the first label and `(a:Person|Company)` is a parser error.
  Workaround: `WHERE a.node_type IN ['Person', 'Company']`.
- **Standalone `UNWIND`** (a query with no `MATCH` clause) fails with
  "Empty partial query". Add a `MATCH` or run plain SQL for constant data.
- **Physical columns with spaces or special characters** (e.g. a column
  literally named `first name`) are not supported end-to-end: the Cypher
  side parses, but the generated SQL does not quote such columns. Table
  *names* with backticks (`` `my-catalog`.schema.nodes ``) work fine —
  they come from your schema configuration and are emitted verbatim.

---

## Correctness Caveats

- **Aggregation after aggregation** (`WITH ... COUNT(...)` followed by
  another aggregating `WITH`) has known edge cases that can surface as
  Databricks `UNRESOLVED_COLUMN` at execution. Prefer flattening into a
  single aggregation; explicitly alias every aggregated column.
- **Self-loops in undirected patterns** (`(a)-[:KNOWS]-(a)`) may appear
  twice in results due to the UNION ALL expansion.
- **Relationship uniqueness (edge isomorphism) is not enforced** in
  fixed-length patterns. openCypher requires each relationship in a
  `MATCH` pattern to bind a distinct edge; the transpiler can reuse one
  edge for several pattern relationships. Example: with a single
  self-loop edge `A→A`, `MATCH (a)-[:T]->(b)-[:T]->(a)` matches by using
  that edge for both hops, where openCypher would return nothing.
  Variable-length paths are unaffected (the `visited` array prevents
  node revisits).
- **Unbounded variable-length paths are depth-capped at 10**: `*` and
  `*2..` are rewritten to a maximum of 10 hops with no warning, so paths
  longer than 10 are silently absent. Explicit bounds are honoured
  exactly, including above 10 (`*1..15` renders `depth < 15`). Always
  write an explicit upper bound for graphs whose diameter may exceed 10.

---

## Performance Caveats

### Deep Variable-Length Paths

`WITH RECURSIVE` for deep traversals (`*1..20`) can be slow or hit Spark's
recursion limits. Keep max depth ≤ 10 where possible, `LIMIT` your result
sets, and when **both endpoints are filtered by ID**, use bidirectional
BFS — its purpose is not speed but enabling queries that would otherwise
exceed the recursion limit:

```python
sql = graph.transpile(query, bidirectional_mode="auto")
```

| Mode | Description |
|---|---|
| `"off"` | Standard unidirectional (default) |
| `"recursive"` | Forward/backward recursive CTEs |
| `"unrolling"` | Unrolled CTEs (better for depth ≤ 6) |
| `"auto"` | Selects based on max hops |

Restrictions: equality filters only, both endpoints filtered, single edge
type.

### Other

- **`COLLECT()` over large groups** builds in-memory arrays — filter before
  aggregating, or use `COUNT()` when only the size matters.
- **Disconnected patterns** (`MATCH (p:Person), (c:Company)`) produce
  cartesian products; always connect patterns through shared variables.
- **`relationships(p)` is not free**: referencing it makes the recursive
  CTE collect edge structs per path. Use `length(p)` when you only need
  the hop count.

---

## Where to Go Next

- [Getting Started](user-guide.md) — schema setup and first queries
- [Functions Reference](functions.md) — supported openCypher functions
- [Procedural BFS example](examples/procedural_bfs.md) — when and how to
  use the procedural mode
