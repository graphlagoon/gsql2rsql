"""PySpark integration tests for procedural BFS rendering mode.

Requires PySpark 4.2+ with SQL scripting support.
Tests execute real queries on Spark and verify exact result values.

Test Graph (single-type: KNOWS only):
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                 ^
      +---KNOWS---> Dave ---KNOWS---> Eve ---KNOWS---> Bob
                                       (amount=75)

Edges:
    Alice -> Bob   (amount=100)
    Bob   -> Carol (amount=200)
    Alice -> Dave  (amount=150)
    Dave  -> Eve   (amount=50)
    Eve   -> Bob   (amount=75)

Multi-type Test Graph (KNOWS + WORKS_AT):
    Alice ---KNOWS---> Bob ---KNOWS---> Carol
      |                                  |
      +---WORKS_AT---> Acme <---WORKS_AT-+
      |
      +---KNOWS---> Dave ---WORKS_AT---> BigCo
"""

import pytest

try:
    from pyspark.sql import SparkSession

    _spark = (
        SparkSession.builder
        .master("local[1]")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    _spark.sql("BEGIN DECLARE x INT DEFAULT 1; END")
    HAS_PYSPARK_SCRIPTING = True
    _spark.stop()
except Exception:
    HAS_PYSPARK_SCRIPTING = False

pytestmark = pytest.mark.skipif(
    not HAS_PYSPARK_SCRIPTING,
    reason="Requires PySpark 4.2+ with SQL scripting support",
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("ProceduralBFS_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    # Create test graph
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW test_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person', 25),
            ('Bob',   'Person', 30),
            ('Carol', 'Person', 35),
            ('Dave',  'Person', 28),
            ('Eve',   'Person', 22)
        AS t(node_id, node_type, age)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW test_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS', 100),
            ('Bob',   'Carol', 'KNOWS', 200),
            ('Alice', 'Dave',  'KNOWS', 150),
            ('Dave',  'Eve',   'KNOWS', 50),
            ('Eve',   'Bob',   'KNOWS', 75)
        AS t(src, dst, relationship_type, amount)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="test_nodes",
        edges_table="test_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"age": int},
        extra_edge_attrs={"amount": int},
    )
    return ctx


class TestProceduralBFSPySpark:
    """End-to-end procedural BFS tests with PySpark execution."""

    def test_directed_bfs_basic(self, spark, graph):
        """Basic directed BFS from Alice should find reachable nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Bob, Alice->Dave, Bob->Carol, Dave->Eve, Eve->Bob (already visited)
        # With global visited: Bob at depth 1, Dave at depth 1,
        #   Carol at depth 2, Eve at depth 2
        # Bob is NOT re-discovered at depth 3 (global visited)
        expected = {"Bob", "Carol", "Dave", "Eve"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_directed_with_sink_filter(self, spark, graph):
        """Sink filter: only Carol should be returned."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}
        assert results == {"Carol"}

    def test_backward_bfs(self, spark, graph):
        """Backward BFS from Carol should find nodes that reach Carol."""
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Carol'
        RETURN DISTINCT b.node_id AS src
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["src"] for row in rows}

        # Carol<-Bob, Bob<-Alice, Bob<-Eve, Eve<-Dave
        expected = {"Bob", "Alice", "Eve", "Dave"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_min_hops_filtering(self, spark, graph):
        """*2..3 should skip depth-1 results."""
        query = """
        MATCH (a:Person)-[:KNOWS*2..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = graph.transpile(query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views")
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Depth-1: Bob, Dave (skipped)
        # Depth-2: Carol, Eve
        # Depth-3: (Bob already visited)
        expected = {"Carol", "Eve"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_global_visited_vs_cte_paths(self, spark, graph):
        """Procedural BFS should return fewer rows than CTE (unique nodes vs all paths)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id AS dst
        """

        sql_proc = graph.transpile(
            query, vlp_rendering_mode="procedural", materialization_strategy="numbered_views",
        )
        sql_cte = graph.transpile(
            query, vlp_rendering_mode="cte",
        )
        print(f"\n=== Procedural SQL ===\n{sql_proc}")
        print(f"\n=== CTE SQL ===\n{sql_cte}")

        rows_proc = spark.sql(sql_proc).collect()
        rows_cte = spark.sql(sql_cte).collect()

        # CTE mode finds all paths (may include Bob at depth 1 AND depth 3)
        # Procedural mode with global visited finds each node once
        assert len(rows_proc) <= len(rows_cte), (
            f"Procedural ({len(rows_proc)} rows) should have "
            f"<= CTE ({len(rows_cte)} rows)"
        )

    def test_unwind_relationships_returns_edges(self, spark, graph):
        """UNWIND relationships(path) should return edge data."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src AS edge_src, r.dst AS edge_dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        edges = {(row["edge_src"], row["edge_dst"]) for row in rows}

        # Alice->Bob, Alice->Dave (depth 1)
        # Bob->Carol, Dave->Eve (depth 2)
        # Eve->Bob already visited, not discovered (depth 3)
        expected = {
            ("Alice", "Bob"),
            ("Alice", "Dave"),
            ("Bob", "Carol"),
            ("Dave", "Eve"),
        }
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_canonical_undirected_unwind_equivalence(self, spark, graph):
        """Canonical query: undirected VLP + UNWIND relationships(p) RETURN r.

        This is the primary real-world shape and exercises all three procedural
        memory fixes at once — the SET->INTO loop-termination fix, the
        NOT EXISTS visited exclusion (O5), and the UNION-ALL-of-equi-joins
        undirected expansion (O7). It asserts the fixes preserve results: the
        procedural (numbered_views) renderer's DISTINCT discovered-edge set
        equals the CTE renderer's.

        Only numbered_views is executed here — temp_tables emits Databricks
        ``CREATE TEMPORARY TABLE`` which open-source Spark cannot parse (its
        equivalence is covered by the golden-SQL tests and the shared
        _direction_branches / _BFSParams logic). Raw multiplicity legitimately
        differs between procedural (global visited) and CTE (all paths), so only
        the DISTINCT set is compared — see
        docs_help_dev/analysis_canonical_query_bfs_memory.md.
        """
        query = """
        MATCH ( root {node_id: 'Alice'} )
        MATCH p = (root)-[*1..2]-()
        UNWIND relationships(p) AS r
        RETURN r.src AS edge_src, r.dst AS edge_dst
        """

        def edges(**kwargs):
            sql = graph.transpile(query, **kwargs)
            rows = spark.sql(sql).collect()
            return {(row["edge_src"], row["edge_dst"]) for row in rows}

        nv = edges(
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        cte = edges(vlp_rendering_mode="cte")

        # non-empty sanity (undirected *1..2 from Alice discovers edges)
        assert nv, "procedural numbered_views returned no edges"
        # distinct discovered edges must match CTE (results preserved)
        assert nv == cte, f"procedural edges {nv} != CTE edges {cte}"

    def test_optimization_flags_on_off_result_equality(self, spark, graph):
        """Optimized (default flags ON) and legacy (all_off) procedural SQL
        must return byte-identical results — same rows, same multiplicity.

        This is the executable guarantee that the O5/O7/loop-control fixes are
        result-preserving on real data: the flags change only HOW the SQL
        computes (NOT EXISTS vs NOT IN, UNION ALL vs OR-join, INTO vs SET),
        never WHAT it returns.
        """
        from collections import Counter

        from gsql2rsql import ProceduralBFSOptimizations

        query = """
        MATCH ( root {node_id: 'Alice'} )
        MATCH p = (root)-[*1..3]-()
        UNWIND relationships(p) AS r
        RETURN r.src AS edge_src, r.dst AS edge_dst
        """

        def edge_multiset(opts):
            sql = graph.transpile(
                query,
                vlp_rendering_mode="procedural",
                materialization_strategy="numbered_views",
                procedural_optimizations=opts,
            )
            rows = spark.sql(sql).collect()
            return Counter((row["edge_src"], row["edge_dst"]) for row in rows)

        optimized = edge_multiset(ProceduralBFSOptimizations())
        legacy = edge_multiset(ProceduralBFSOptimizations.all_off())

        assert optimized, "optimized SQL returned no edges"
        assert optimized == legacy, (
            f"flags ON {dict(optimized)} != flags OFF {dict(legacy)}"
        )


class TestBidirectionalProceduralBFSPySpark:
    """Bidirectional procedural BFS tests with PySpark execution.

    When both source AND target have equality filters, the
    bidirectional optimizer activates automatically. These tests
    verify that the reachable-set pruning approach produces
    correct results.

    Test Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                 ^
          +---KNOWS---> Dave ---KNOWS---> Eve ---KNOWS---> Bob
    """

    def test_bidir_directed_alice_to_carol(self, spark, graph):
        """Bidirectional: Alice->Carol should find Carol at depth 2."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Bob->Carol: depth 2
        assert results == {"Carol"}, (
            f"Expected {{'Carol'}}, got {results}"
        )

    def test_bidir_directed_alice_to_eve(self, spark, graph):
        """Bidirectional: Alice->Eve via Alice->Dave->Eve."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Eve'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Alice->Dave->Eve: depth 2
        assert results == {"Eve"}, (
            f"Expected {{'Eve'}}, got {results}"
        )

    def test_bidir_no_path(self, spark, graph):
        """Bidirectional: Carol->Alice has no directed path."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Carol' AND b.node_id = 'Alice'
        RETURN b.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Bidir SQL ===\n{sql}")

        result = spark.sql(sql)
        rows = result.collect()
        assert len(rows) == 0, (
            f"Expected empty result, got {rows}"
        )

    def test_bidir_same_result_as_unidirectional(
        self, spark, graph
    ):
        """Bidirectional should produce same reachable set."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
        RETURN DISTINCT b.node_id AS dst
        """
        # Bidirectional (auto mode — optimizer will enable it)
        sql_bidir = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        # Force unidirectional
        sql_unidir = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            bidirectional_mode="off",
        )
        print(f"\n=== Bidir SQL ===\n{sql_bidir}")
        print(f"\n=== Unidir SQL ===\n{sql_unidir}")

        rows_bidir = spark.sql(sql_bidir).collect()
        rows_unidir = spark.sql(sql_unidir).collect()

        set_bidir = {row["dst"] for row in rows_bidir}
        set_unidir = {row["dst"] for row in rows_unidir}

        assert set_bidir == set_unidir, (
            f"Bidir {set_bidir} != Unidir {set_unidir}"
        )


# ======================================================================
# Multi-type edge fixtures
# ======================================================================


@pytest.fixture(scope="module")
def multi_spark():
    """Spark session for multi-type tests (separate to avoid view conflicts)."""
    spark = (
        SparkSession.builder
        .appName("ProceduralBFS_MultiType_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )
    # Create multi-type graph:
    #   Nodes: Alice(Person), Bob(Person), Carol(Person),
    #          Dave(Person), Acme(Company), BigCo(Company)
    #   Edges:
    #     Alice -KNOWS->   Bob   (amount=100)
    #     Bob   -KNOWS->   Carol (amount=200)
    #     Alice -WORKS_AT-> Acme (amount=50)
    #     Carol -WORKS_AT-> Acme (amount=60)
    #     Alice -KNOWS->   Dave  (amount=150)
    #     Dave  -WORKS_AT-> BigCo(amount=70)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW mt_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person',  25),
            ('Bob',   'Person',  30),
            ('Carol', 'Person',  35),
            ('Dave',  'Person',  28),
            ('Acme',  'Company', 0),
            ('BigCo', 'Company', 0)
        AS t(node_id, node_type, age)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW mt_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',   'KNOWS',    100),
            ('Bob',   'Carol', 'KNOWS',    200),
            ('Alice', 'Acme',  'WORKS_AT', 50),
            ('Carol', 'Acme',  'WORKS_AT', 60),
            ('Alice', 'Dave',  'KNOWS',    150),
            ('Dave',  'BigCo', 'WORKS_AT', 70)
        AS t(src, dst, relationship_type, amount)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def multi_graph(multi_spark):
    """GraphContext with Person + Company node types, KNOWS + WORKS_AT edges."""
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=multi_spark,
        nodes_table="mt_nodes",
        edges_table="mt_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"age": int},
        extra_edge_attrs={"amount": int},
    )
    return ctx


class TestMultiTypeProceduralBFSPySpark:
    """Procedural BFS tests with multi-type edges (KNOWS + WORKS_AT).

    These tests verify the operator-precedence fix for WHERE clauses
    with OR-joined type filters AND other conditions.

    Graph:
        Alice ---KNOWS---> Bob ---KNOWS---> Carol
          |                                  |
          +---WORKS_AT---> Acme <---WORKS_AT-+
          |
          +---KNOWS---> Dave ---WORKS_AT---> BigCo
    """

    def test_multi_type_directed_bfs(self, multi_spark, multi_graph):
        """BFS with KNOWS|WORKS_AT should find all reachable nodes."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT*1..3]->(b)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Multi-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Depth 1: Bob (KNOWS), Acme (WORKS_AT), Dave (KNOWS)
        # Depth 2: Carol (KNOWS from Bob), BigCo (WORKS_AT from Dave)
        #          Acme (WORKS_AT from Carol) — already visited
        # Depth 3: Acme from Carol — already visited
        expected = {"Bob", "Acme", "Dave", "Carol", "BigCo"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_multi_type_single_type_filter(self, multi_spark, multi_graph):
        """BFS with only KNOWS should not follow WORKS_AT edges."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Single-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["dst"] for row in rows}

        # Only KNOWS edges: Alice->Bob, Alice->Dave, Bob->Carol
        expected = {"Bob", "Dave", "Carol"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )

    def test_multi_type_vs_cte(self, multi_spark, multi_graph):
        """Multi-type procedural BFS should match CTE results (modulo duplicates)."""
        query = """
        MATCH (a:Person)-[:KNOWS|WORKS_AT*1..3]->(b)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS dst
        """
        sql_proc = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        sql_cte = multi_graph.transpile(
            query,
            vlp_rendering_mode="cte",
        )
        print(f"\n=== Procedural SQL ===\n{sql_proc}")
        print(f"\n=== CTE SQL ===\n{sql_cte}")

        rows_proc = multi_spark.sql(sql_proc).collect()
        rows_cte = multi_spark.sql(sql_cte).collect()

        set_proc = {row["dst"] for row in rows_proc}
        set_cte = {row["dst"] for row in rows_cte}

        # Both modes should find the same set of reachable nodes
        assert set_proc == set_cte, (
            f"Procedural {set_proc} != CTE {set_cte}"
        )

    def test_multi_type_cross_type_traversal(self, multi_spark, multi_graph):
        """Cross-type BFS: Person -WORKS_AT-> Company."""
        query = """
        MATCH (a:Person)-[:WORKS_AT*1..2]->(b:Company)
        WHERE a.node_id = 'Alice'
        RETURN DISTINCT b.node_id AS company
        """
        sql = multi_graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== Cross-type SQL ===\n{sql}")

        result = multi_spark.sql(sql)
        rows = result.collect()
        results = {row["company"] for row in rows}

        # Alice -WORKS_AT-> Acme (depth 1)
        # No WORKS_AT->WORKS_AT paths exist, so only depth 1
        expected = {"Acme"}
        assert results == expected, (
            f"Expected {expected}, got {results}"
        )


# ======================================================================
# NULL-endpoint semantics: the visited NOT EXISTS IS NOT NULL guard
# ======================================================================


@pytest.fixture(scope="module")
def null_edge_graph(spark):
    """Graph with an edge whose dst is NULL.

    Legacy ``NOT IN`` (null-aware) drops that edge; the default
    ``NOT EXISTS`` must drop it too via its ``IS NOT NULL`` guard —
    without the guard the NULL leaks into visited/frontier/result.
    """
    from gsql2rsql import GraphContext

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW null_test_nodes AS
        SELECT * FROM VALUES
            ('Alice', 'Person', 25),
            ('Bob',   'Person', 30),
            ('Carol', 'Person', 35)
        AS t(node_id, node_type, age)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW null_test_edges AS
        SELECT * FROM VALUES
            ('Alice', 'Bob',                  'KNOWS', 100),
            ('Bob',   'Carol',                'KNOWS', 200),
            ('Bob',   CAST(NULL AS STRING),   'KNOWS', 50)
        AS t(src, dst, relationship_type, amount)
    """)
    return GraphContext(
        spark=spark,
        nodes_table="null_test_nodes",
        edges_table="null_test_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"age": int},
        extra_edge_attrs={"amount": int},
    )


class TestNullEndpointSemantics:
    """Default NOT EXISTS (+ IS NOT NULL guard) must equal legacy NOT IN
    on data containing NULL edge endpoints."""

    QUERY = """
    MATCH ( root {node_id: 'Alice'} )
    MATCH p = (root)-[*1..3]-()
    UNWIND relationships(p) AS r
    RETURN r.src AS edge_src, r.dst AS edge_dst
    """

    def _edge_multiset(self, spark, null_edge_graph, opts):
        from collections import Counter

        sql = null_edge_graph.transpile(
            self.QUERY,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            procedural_optimizations=opts,
        )
        rows = spark.sql(sql).collect()
        return Counter((row["edge_src"], row["edge_dst"]) for row in rows)

    def test_guarded_not_exists_equals_legacy_not_in(
        self, spark, null_edge_graph,
    ):
        from gsql2rsql import ProceduralBFSOptimizations

        guarded = self._edge_multiset(
            spark, null_edge_graph, ProceduralBFSOptimizations(),
        )
        legacy = self._edge_multiset(
            spark, null_edge_graph, ProceduralBFSOptimizations.all_off(),
        )
        assert guarded, "guarded SQL returned no edges"
        assert guarded == legacy, (
            f"guarded {dict(guarded)} != legacy NOT IN {dict(legacy)}"
        )

    def test_null_endpoint_edge_is_dropped(self, spark, null_edge_graph):
        """The Bob->NULL edge must never surface (Cypher: an edge connects
        two nodes) — under BOTH flag settings."""
        from gsql2rsql import ProceduralBFSOptimizations

        for opts in (
            ProceduralBFSOptimizations(),
            ProceduralBFSOptimizations.all_off(),
        ):
            edges = self._edge_multiset(spark, null_edge_graph, opts)
            nulls = {e: c for e, c in edges.items() if None in e}
            assert not nulls, f"NULL-endpoint edge leaked: {nulls}"


# ======================================================================
# New opt-in flags: executable result-equivalence
# ======================================================================


class TestNewFlagResultEquivalence:
    """Each new opt-in rewrite must return the exact same rows (same
    multiplicity) as the default and legacy SQL.

    numbered_views executes natively; temp_tables SQL is executed by
    rewriting ``TEMPORARY TABLE`` -> ``TABLE`` (the bench_flags_ab_tt.py
    trick — same statements, materialized regime), since open-source Spark
    cannot parse Databricks ``CREATE TEMPORARY TABLE``.
    """

    UNDIRECTED_UNWIND_QUERY = """
    MATCH ( root {node_id: 'Alice'} )
    MATCH p = (root)-[*1..3]-()
    UNWIND relationships(p) AS r
    RETURN r.src AS edge_src, r.dst AS edge_dst, r.amount AS amount
    """

    BARRIER_QUERY = """
    MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE a.node_id = 'Alice' AND is_terminator(b.age > 29)
    RETURN DISTINCT b.node_id AS dst
    """

    _BFS_TABLES = (
        "bfs_visited_1", "bfs_frontier_1", "bfs_frontier_1_init",
        "bfs_result_1", "bfs_edges_1", "bfs_adj_1",
        "bfs_edges_keyed_1", "bfs_barrier_1",
    )

    def _drop_bfs_tables(self, spark):
        for t in self._BFS_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
            spark.sql(f"DROP VIEW IF EXISTS {t}")
        spark.sql("DROP VIEW IF EXISTS paths_1")

    def _run_tt(self, spark, graph, query, opts):
        """Execute temp_tables SQL on OSS Spark via the TABLE rewrite."""
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="temp_tables",
            procedural_optimizations=opts,
        ).replace("TEMPORARY TABLE", "TABLE")
        self._drop_bfs_tables(spark)
        return spark.sql(sql).collect()

    def _run_nv(self, spark, graph, query, opts):
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
            procedural_optimizations=opts,
        )
        return spark.sql(sql).collect()

    def test_doubled_adjacency_nv_equivalence(self, spark, graph):
        """undirected_doubled_adjacency on numbered_views: identical
        multiset vs default (OR-join) and legacy all_off."""
        from collections import Counter

        from gsql2rsql import ProceduralBFSOptimizations

        def ms(opts):
            rows = self._run_nv(
                spark, graph, self.UNDIRECTED_UNWIND_QUERY, opts,
            )
            return Counter(
                (r["edge_src"], r["edge_dst"], r["amount"]) for r in rows
            )

        adj = ms(ProceduralBFSOptimizations(
            undirected_doubled_adjacency=True,
        ))
        default = ms(ProceduralBFSOptimizations())
        legacy = ms(ProceduralBFSOptimizations.all_off())

        assert adj, "adjacency SQL returned no edges"
        assert adj == default == legacy, (
            f"adj {dict(adj)} != default {dict(default)} "
            f"!= legacy {dict(legacy)}"
        )

    def test_doubled_adjacency_tt_equivalence(self, spark, graph):
        """undirected_doubled_adjacency on temp_tables (TABLE rewrite):
        identical multiset vs default."""
        from collections import Counter

        from gsql2rsql import ProceduralBFSOptimizations

        def ms(opts):
            rows = self._run_tt(
                spark, graph, self.UNDIRECTED_UNWIND_QUERY, opts,
            )
            return Counter(
                (r["edge_src"], r["edge_dst"], r["amount"]) for r in rows
            )

        adj = ms(ProceduralBFSOptimizations(
            undirected_doubled_adjacency=True,
        ))
        default = ms(ProceduralBFSOptimizations())
        assert adj, "adjacency SQL returned no edges"
        assert adj == default, (
            f"adj {dict(adj)} != default {dict(default)}"
        )

    def test_deferred_payload_tt_equivalence(self, spark, graph):
        """deferred_edge_payload (default ON; alone and composed with the
        default-ON adjacency) vs the legacy ``all_off()`` SQL: identical
        rows, including the re-attached payload column values.

        Values are compared as strings: the legacy result table stores
        every property column as STRING, while the deferred re-attach
        keeps the ORIGINAL schema type (int here) — an intentional,
        documented side effect that matches CTE mode.
        """
        from collections import Counter

        from gsql2rsql import ProceduralBFSOptimizations

        def rows_for(opts):
            return self._run_tt(
                spark, graph, self.UNDIRECTED_UNWIND_QUERY, opts,
            )

        def ms(rows):
            return Counter(
                (r["edge_src"], r["edge_dst"], str(r["amount"]))
                for r in rows
            )

        legacy_rows = rows_for(ProceduralBFSOptimizations.all_off())
        default_rows = rows_for(ProceduralBFSOptimizations())
        deferred_only_rows = rows_for(ProceduralBFSOptimizations(
            deferred_edge_payload=True,
            undirected_doubled_adjacency=False,
        ))

        legacy = ms(legacy_rows)
        default = ms(default_rows)
        deferred_only = ms(deferred_only_rows)

        assert legacy, "legacy SQL returned no edges"
        assert default == legacy, (
            f"default {dict(default)} != legacy {dict(legacy)}"
        )
        assert deferred_only == legacy, (
            f"deferred-only {dict(deferred_only)} != legacy {dict(legacy)}"
        )
        # Type fidelity: deferral restores the original schema type (int),
        # legacy coerces to STRING.
        assert all(isinstance(r["amount"], int) for r in default_rows)
        assert all(isinstance(r["amount"], int) for r in deferred_only_rows)
        assert all(isinstance(r["amount"], str) for r in legacy_rows)

    def test_barrier_precompute_tt_equivalence_and_semantics(
        self, spark, graph,
    ):
        """barrier_precompute: identical rows vs default, and the exact
        expected barrier semantics.

        Graph (directed): Alice->Bob, Bob->Carol, Alice->Dave, Dave->Eve,
        Eve->Bob. Ages: Bob=30 (barrier, age>29), others below.
        BFS from Alice: depth1 discovers Bob (barrier: recorded, NOT
        expanded — so Carol is never reached) and Dave; depth2 Dave->Eve;
        depth3 Eve->Bob is excluded (visited). Expected {Bob, Dave, Eve}.
        """
        from gsql2rsql import ProceduralBFSOptimizations

        def result_set(opts):
            rows = self._run_tt(spark, graph, self.BARRIER_QUERY, opts)
            return {row["dst"] for row in rows}

        default = result_set(ProceduralBFSOptimizations())
        precomputed = result_set(ProceduralBFSOptimizations(
            barrier_precompute=True,
        ))

        expected = {"Bob", "Dave", "Eve"}
        assert default == expected, (
            f"default barrier semantics broken: {default} != {expected}"
        )
        assert precomputed == expected, (
            f"precomputed barrier diverged: {precomputed} != {expected}"
        )
