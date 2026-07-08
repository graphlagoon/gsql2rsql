# Running Graph Queries on Databricks

A short series on how to ask graph questions of a Databricks lakehouse — using Cypher syntax, generating Spark SQL underneath, and not standing up a separate graph database for it. The series follows the engineering choices the [gsql2rsql](../../README.md) transpiler makes.

## The posts

1. [**Graph queries on a Databricks lakehouse with `WITH RECURSIVE`**](01-databricks-graphs-and-with-recursive.md) — why Databricks for graphs in the first place, the storage layout, the mechanical `MATCH → JOIN` translation, and variable-length paths with `WITH RECURSIVE`. Ends on the row-explosion failure mode.
2. [**Frontier tables: SQL/PGQ, procedural BFS, and Databricks temp tables**](02-frontier-tables-pgq-and-temp-tables.md) — what the SQL/PGQ standard does and doesn't help with, an explicit BFS with frontier and visited tables, and what specifically about `CREATE TEMPORARY TABLE` semantics on Databricks makes the approach practical.
3. [**Inside the transpiler: a 6-phase pipeline**](03-the-6-phase-pipeline.md) — Parser → Planner → Optimizer → Resolver → Enrichment → Renderer, what each phase is allowed and forbidden to do, and how a query flows through all six.

## Who this is for

- **Data engineers** with a Databricks lakehouse considering graph queries. Posts 1 and 2 are the entry point.
- **Compiler and database engineers** curious about a real-world Cypher-to-SQL translation. Post 2 has the most original SQL; post 3 explains the engineering that holds it together.

The posts assume working SQL knowledge and familiarity with Cypher's `MATCH (a)-[:REL]->(b)` syntax.

## Conventions

- SQL examples target **Databricks SQL** (Spark SQL with Databricks extensions like `CREATE TEMPORARY TABLE` and SQL scripting `BEGIN…END`).
- Cypher examples follow **OpenCypher**.
- Code links go to the project source so you can read the actual implementation behind each idea.
