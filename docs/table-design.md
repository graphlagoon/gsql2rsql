# Making Databricks Delta  Tables Graph Friendly

How to structure your Delta tables to enable graph queries with gsql2rsql.

---

## Why Delta Lake for Graphs?

Delta Lake can store **terabytes of graph data** efficiently while still enabling fast queries:

- **ACID transactions**
- **Time travel** for auditing and debugging
- **Efficient storage**
- **Data skipping** to reduce I/O

Instead of maintaining a separate graph database, you can query your existing Delta tables as a graph.

---

## The Triple Store Pattern

One approach I find convenient is the **Triple Store** pattern: one table for nodes, one table for edges.

```
┌─────────────────┐         ┌─────────────────┐
│   nodes table   │         │   edges table   │
├─────────────────┤         ├─────────────────┤
│ node_id (PK)    │◄────────│ src (FK)        │
│ node_type       │         │ dst (FK)        │
│ ...properties   │         │ relationship_type│
└─────────────────┘         │ ...properties   │
                            └─────────────────┘
```

**This is not the only approach**, and may not be optimal for your use case. But it's flexible and works well with gsql2rsql's `GraphContext` API.

### Nodes Table

```sql
CREATE TABLE catalog.schema.nodes (
    node_id STRING NOT NULL,
    node_type STRING NOT NULL,  -- 'Person', 'Company', etc.
    name STRING,
    -- ... other properties
)
USING DELTA;
```

### Edges Table

```sql
CREATE TABLE catalog.schema.edges (
    src STRING NOT NULL,
    dst STRING NOT NULL,
    relationship_type STRING NOT NULL,  -- 'KNOWS', 'WORKS_AT', etc.
    -- ... edge properties
)
USING DELTA;
```

---

## Alternative: Separate Tables

If you already have separate tables per entity type, gsql2rsql supports that too via the low-level API:

```
people (id, name, email)
companies (id, name, industry)
employment (person_id, company_id, role)
```

See [Low-Level API](user-guide.md#low-level-api-without-graphcontext) for how to configure this.

---

## Databricks Optimizations

!!! warning "Under Construction"
    This section covers Databricks-specific optimizations. The best approach depends on many factors: data size, query patterns, cluster configuration, and whether you're using Photon.


## Liquid Clustering vs Partitioning with Z-ORDER

!!! danger "HARD TO PREDICT"
    **IN MY EXPERIENCE**, Liquid Clustering performance is **very hard to predict**. It can be great or terrible depending on your data distribution, query patterns, and table size. **Always benchmark with your actual workload before committing.**

### Which Columns to Optimize?

For graph queries, the most important columns are:

| Table | Key Columns | Why |
|-------|-------------|-----|
| **Edges** | `src`, `dst` | Join predicates in traversals |
| **Nodes** | `node_id` | Join target from edges |

Secondary columns (less impact):

| Table | Column | When Useful |
|-------|--------|-------------|
| Edges | `relationship_type` | If you filter by edge type frequently |
| Nodes | `node_type` | If you filter by node type frequently |

### Liquid Clustering

```sql
CREATE TABLE catalog.schema.edges (
    src STRING,
    dst STRING,
    relationship_type STRING
)
USING DELTA
CLUSTER BY (src, dst);
```

**Pros:**
- Automatic optimization (no manual `OPTIMIZE` runs)
- Adapts to changing data patterns

**Cons:**
- Performance can be unpredictable
- Less control over clustering behavior
- May not work well with high-cardinality columns

### Partitioning + Z-ORDER

```sql
-- Create with partitioning (if you have clear access patterns)
CREATE TABLE catalog.schema.edges (...)
USING DELTA
PARTITIONED BY (relationship_type);

-- Then Z-ORDER on join columns
OPTIMIZE catalog.schema.edges
ZORDER BY (src, dst);
```

**Pros:**
- More predictable behavior
- Fine-grained control
- Well-understood optimization

**Cons:**
- Requires periodic `OPTIMIZE` runs
- Partitioning can create small file problems

### When to Use What?

| Scenario | Recommendation |
|----------|----------------|
| New table, uncertain query patterns | Start with Z-ORDER only, benchmark LC later |
| Existing table | Z-ORDER on `(src, dst)` |
| Very large table (TB+) with clear partition key | Consider partitioning + Z-ORDER |
| Queries always filter by edge type first | Maybe partition by `relationship_type` |

!!! tip "My Recommendation"
    **Start simple**: no partitioning, just `ZORDER BY (src, dst)`. Measure performance. Only add complexity (LC or partitioning) if you have a clear problem to solve.

---

## Data Skipping

Delta Lake automatically tracks min/max statistics for data skipping. To maximize effectiveness:

1. **Z-Order on join columns** (`src`, `dst`, `node_id`) - this is the most important
2. **Keep column cardinality reasonable** - very high cardinality reduces skipping effectiveness

### Bloom Filters

HELP WANTED!

## Data Skew

Graph data is often highly skewed - some nodes have many more connections than others (e.g., celebrity accounts, hub nodes). Also, some relationship types are far more frequent than others

### Why Skew Matters

| Problem | Impact |
|---------|--------|
| Hot partitions | Some tasks take much longer than others |
| Uneven file sizes | Z-ORDER/LC less effective |
| Join explosions | Popular nodes cause massive intermediate results |

### Detecting Skew


### Mitigating Skew

HELP WANTED

## Column Naming

gsql2rsql uses sensible defaults. Override them if your tables use different names:

```python
graph = GraphContext(
    nodes_table="my_nodes",
    edges_table="my_edges",

    # Custom column names
    node_id_col="id",              # default: "node_id"
    node_type_col="type",          # default: "node_type"
    edge_src_col="source",         # default: "src"
    edge_dst_col="target",         # default: "dst"
    edge_type_col="rel_type",      # default: "relationship_type"
)
```


---

## What's Missing

!!! info "Topics to be expanded (Help wanted)"
    - Benchmarks comparing optimization strategies
    - Guidance for very large graphs (billions of edges)
    - Cost-based optimization considerations

---

## Next Steps

- [User Guide](user-guide.md) - Start using gsql2rsql
- [Examples](examples/index.md) - Real-world query examples
