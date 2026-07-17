"""PySpark tests for list predicates over nodes(path) with property access.

Bug: ``all(n IN nodes(p) WHERE n.prop ...)`` used to render as
``FORALL(path, n -> n.prop)``, but the path array holds scalar node IDs,
so Spark failed with INVALID_EXTRACT_BASE_FIELD_TYPE. The fix renders a
correlated subquery against the node table instead.

Test Graph (types PF/PJ, status, degree — each node exercises one branch
of the reported filter):

    A(PF, ATIVA,    deg 2)  --- B(PF, ATIVA, deg 3) --- C(PJ, ATIVA, deg 10)
    A --- D(PF, SUSPENSA, deg 1)     <- fails status filter
    B --- E(PF, ATIVA,    deg 99)    <- fails degree filter (PF must be <= 15)
    C --- F(PJ, ATIVA,    deg 25)    <- passes (PJ <= 30)
    F --- G(PJ, ATIVA,    deg 40)    <- fails degree filter (PJ must be <= 30)
    H(PF, NULL status, deg 1) --- A  <- NULL status: coalesce(...,'ATIVA') passes

Filter under test (the user's reported shape):
    coalesce(n.status, 'ATIVA') = 'ATIVA'
    AND ((n.node_type = 'PF' AND n.degree <= 15) OR
         (n.node_type = 'PJ' AND n.degree <= 30))

Nodes passing the filter: A, B, C, F, H.
Nodes failing it:         D (status), E (PF degree), G (PJ degree).
"""

import pytest

try:
    from pyspark.sql import SparkSession

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False

pytestmark = pytest.mark.skipif(not HAS_PYSPARK, reason="Requires PySpark")


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("NodesPathPredicates_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW npp_nodes AS
        SELECT * FROM VALUES
            ('A', 'PF', 'ATIVA',    2),
            ('B', 'PF', 'ATIVA',    3),
            ('C', 'PJ', 'ATIVA',   10),
            ('D', 'PF', 'SUSPENSA', 1),
            ('E', 'PF', 'ATIVA',   99),
            ('F', 'PJ', 'ATIVA',   25),
            ('G', 'PJ', 'ATIVA',   40),
            ('H', 'PF', CAST(NULL AS STRING), 1)
        AS t(node_id, node_type, status, degree)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW npp_edges AS
        SELECT * FROM VALUES
            ('A', 'B', 'REL'),
            ('B', 'C', 'REL'),
            ('A', 'D', 'REL'),
            ('B', 'E', 'REL'),
            ('C', 'F', 'REL'),
            ('F', 'G', 'REL'),
            ('H', 'A', 'REL')
        AS t(src, dst, relationship_type)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="npp_nodes",
        edges_table="npp_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"status": str, "degree": int},
    )
    ctx.set_types(node_types=["PF", "PJ"], edge_types=["REL"])
    return ctx


STATUS_DEGREE_FILTER = """
    coalesce(n.status, 'ATIVA') = 'ATIVA'
    AND (
        (n.node_type = 'PF' AND n.degree <= 15) OR
        (n.node_type = 'PJ' AND n.degree <= 30)
    )
"""


class TestAllNodesPathPropertyPredicate:
    """The reported crash: all(n IN nodes(p) WHERE <property predicate>)."""

    def test_reported_query_shape_undirected(self, spark, graph):
        """The user's query shape: undirected *1..3 + all() + UNWIND rels.

        Paths start at A. Every node on the path must pass the filter, so
        traversal is confined to {A, B, C, F, H}. The distinct edges among
        those nodes are A-B, B-C, C-F, H-A. D/E/G paths must be pruned.
        """
        query = f"""
        MATCH (root {{ node_id: 'A' }})
        MATCH p = (root)-[*1..3]-(d)
        WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS src, r.dst AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {(row["src"], row["dst"]) for row in rows}
        expected = {("A", "B"), ("B", "C"), ("C", "F"), ("H", "A")}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_all_directed(self, spark, graph):
        """Directed *1..3 from A: A->B->C->F all pass; D/E/G branches pruned."""
        query = f"""
        MATCH p = (root {{ node_id: 'A' }})-[*1..3]->(d)
        WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        expected = {"B", "C", "F"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_all_null_property_interior_node(self, spark, graph):
        """H has NULL status; coalesce(status,'ATIVA') passes, so H->A->B ok.

        Verifies NULL properties flow through COALESCE inside the subquery.
        """
        query = f"""
        MATCH p = (root {{ node_id: 'H' }})-[*1..2]->(d)
        WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        expected = {"A", "B"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_all_null_predicate_filters_path(self, spark, graph):
        """A predicate that is NULL for some node must reject the path.

        n.status = 'ATIVA' is NULL for H (status NULL). Cypher all() then
        yields NULL, which filters the row. Paths from A through H must
        disappear; A->B->C etc. survive (all statuses TRUE).
        """
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]-(d)
        WHERE all(n IN nodes(p) WHERE n.status = 'ATIVA')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # From A (undirected, depth 1-2): B, C (via B), E (via B, status
        # ATIVA deg irrelevant here), D fails (SUSPENSA), H fails (NULL).
        expected = {"B", "C", "E"}
        assert results == expected, f"Expected {expected}, got {results}"


class TestOtherQuantifiersOverNodesPath:
    def test_any_nodes_path_property(self, spark, graph):
        """any(n IN nodes(p) WHERE n.node_type = 'PJ'): path touches a PJ."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE any(n IN nodes(p) WHERE n.node_type = 'PJ')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # Directed from A: A->B (no PJ), A->D (no), A->B->C (C is PJ),
        # A->B->E (no PJ).
        expected = {"C"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_none_nodes_path_property(self, spark, graph):
        """none(n IN nodes(p) WHERE n.status = 'SUSPENSA'): avoid D."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE none(n IN nodes(p) WHERE n.status = 'SUSPENSA')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # Directed from A depth 1-2: B, C, E reachable without D.
        # D itself is SUSPENSA -> excluded. NOTE: H is not reachable
        # (H->A is inbound), so NULL-status handling isn't hit here.
        expected = {"B", "C", "E"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_single_nodes_path_property(self, spark, graph):
        """single(n IN nodes(p) WHERE n.node_type = 'PJ'): exactly one PJ."""
        query = """
        MATCH p = (root { node_id: 'B' })-[*1..2]->(d)
        WHERE single(n IN nodes(p) WHERE n.node_type = 'PJ')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # Directed from B: B->C (one PJ: C) ok; B->E (zero PJ) no;
        # B->C->F (two PJ) no.
        expected = {"C"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_none_null_predicate_element_rejects_path(self, spark, graph):
        """none() must reject a path when the predicate is NULL for a node.

        H.status is NULL, so `H.status = 'SUSPENSA'` evaluates to NULL.
        Cypher none() with a NULL element yields NULL -> the row is
        filtered. Undirected from H reaches A (H-A), then B (H-A-B).
        Both paths contain H (NULL) -> ALL such paths must be dropped.
        """
        query = """
        MATCH p = (root { node_id: 'H' })-[*1..2]-(d)
        WHERE none(n IN nodes(p) WHERE n.status = 'SUSPENSA')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # Every path from H contains H whose predicate is NULL -> none()
        # is NULL -> all rows filtered. Empty result.
        assert results == set(), f"Expected empty, got {results}"

    def test_single_null_predicate_element(self, spark, graph):
        """single() with a NULL predicate element: NULL approximates to
        non-matching, so it does not count toward the exactly-one total."""
        # From H undirected: H(status NULL), A(ATIVA), B(ATIVA), D(SUSPENSA
        # via A), etc. single(n WHERE n.status='SUSPENSA') = exactly one
        # SUSPENSA node on the path. H-A-D contains D (SUSPENSA) exactly
        # once; H's NULL status is not counted.
        query = """
        MATCH p = (root { node_id: 'H' })-[*1..2]-(d)
        WHERE single(n IN nodes(p) WHERE n.status = 'SUSPENSA')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # H-A-D: exactly one SUSPENSA (D). Other H-paths have zero SUSPENSA.
        assert results == {"D"}, f"Expected {{'D'}}, got {results}"


class TestRegressionGuards:
    def test_pure_id_lambda_keeps_array_contains(self, spark, graph):
        """any(n IN nodes(p) WHERE n = <id>) keeps HOF/ARRAY_CONTAINS.

        No property access on the lambda var -> no subquery rewrite.
        """
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE any(n IN nodes(p) WHERE n = 'C')
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        assert "ARRAY_CONTAINS" in sql
        # The rewrite would put the node table inside an EXISTS subquery;
        # a pure-ID lambda must not do that.
        assert "EXISTS (SELECT 1 FROM npp_nodes" not in sql

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        expected = {"C"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_all_under_or_returns_both_branches(self, spark, graph):
        """Phase-0 regression: ALL(...) OR <other> must not prune traversal.

        all(n ... degree <= 3) holds for A->B (degs 2,3) and A->D
        (degs 2,1); d.node_id = 'E' matches A->B->E even though
        E.degree = 99 violates the ALL branch. All three must be returned.
        """
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE all(n IN nodes(p) WHERE n.degree <= 3) OR d.node_id = 'E'
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        expected = {"B", "D", "E"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_relationships_all_under_or_not_pruned(self, spark, graph):
        """Phase-0 regression for edge predicates: ALL(rels) OR <other>."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE all(r IN relationships(p) WHERE r.src = 'ZZZ') OR d.node_id = 'C'
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # ALL branch matches nothing (no edge has src 'ZZZ'), but the OR
        # branch keeps A->B->C. Before the fix the CTE was pruned to empty.
        expected = {"C"}
        assert results == expected, f"Expected {expected}, got {results}"

    def test_nodes_id_comprehension_untouched(self, spark, graph):
        """[n IN nodes(p) | n.id] optimization still returns the path array."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..1]->(d)
        RETURN d.node_id AS dst, [n IN nodes(p) | n.id] AS ids
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        by_dst = {row["dst"]: list(row["ids"]) for row in rows}
        assert by_dst == {"B": ["A", "B"], "D": ["A", "D"]}


class TestNodePushdownParity:
    """The filtered-edges pushdown must never change results.

    It only shrinks the traversed edge set with a condition implied by the
    residual ALL; pushdown ON and OFF must be row-identical.
    """

    PARITY_QUERIES = [
        # the reported shape (undirected)
        f"""
        MATCH (root {{ node_id: 'A' }})
        MATCH p = (root)-[*1..3]-(d)
        WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
        UNWIND relationships(p) AS r
        RETURN DISTINCT r.src AS a, r.dst AS b
        """,
        # directed
        f"""
        MATCH p = (root {{ node_id: 'A' }})-[*1..3]->(d)
        WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
        RETURN DISTINCT d.node_id AS a, 'x' AS b
        """,
    ]

    @pytest.mark.parametrize("query_idx", [0, 1])
    def test_pushdown_on_off_row_identical(self, spark, graph, query_idx):
        query = self.PARITY_QUERIES[query_idx]

        sql_on = graph.transpile(query)
        assert "vlp_filtered_edges_" in sql_on
        # Safety invariant: the residual subquery is never removed.
        assert "NOT EXISTS (SELECT 1 FROM npp_nodes" in sql_on

        sql_off = graph.transpile(
            query, config={"enable_vlp_node_pushdown": False}
        )
        assert "vlp_filtered_edges_" not in sql_off

        rows_on = {tuple(r) for r in spark.sql(sql_on).collect()}
        rows_off = {tuple(r) for r in spark.sql(sql_off).collect()}
        assert rows_on == rows_off, (
            f"Pushdown changed results!\nON:  {rows_on}\nOFF: {rows_off}"
        )

    def test_all_under_or_gets_no_pushdown(self, graph):
        """ALL under OR is not conjunctive -> no filtered-edges CTE."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE all(n IN nodes(p) WHERE n.degree <= 3) OR d.node_id = 'E'
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        assert "vlp_filtered_edges_" not in sql

    def test_outer_variable_predicate_gets_no_pushdown(self, spark, graph):
        """Predicate referencing an outer variable is not pushable."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        WHERE all(n IN nodes(p) WHERE n.degree <= d.degree)
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        assert "vlp_filtered_edges_" not in sql

        rows = spark.sql(sql).collect()
        results = {row["dst"] for row in rows}
        # Directed from A: B(3>=2,3 ok), D(1>=2? no), C(10>=2,3,10 ok),
        # E(99>=2,3,99 ok).
        expected = {"B", "C", "E"}
        assert results == expected, f"Expected {expected}, got {results}"


def _has_scripting(spark) -> bool:
    try:
        spark.sql("BEGIN DECLARE _probe INT DEFAULT 1; END")
        return True
    except Exception:
        return False


class TestProceduralNodePushdown:
    """Procedural BFS enforces all(n IN nodes(p) WHERE pred) by filtering
    the frontier against a precomputed passing-node set. The path array is
    never collected in this mode, so the pushdown IS the semantic fix — the
    residual is rendered as TRUE only when the frontier filter is active.
    """

    _BFS_TABLES = (
        "bfs_visited_1", "bfs_frontier_1", "bfs_frontier_1_init",
        "bfs_result_1", "bfs_edges_1", "bfs_edges_keyed_1", "bfs_adj_1",
        "bfs_barrier_1", "bfs_vlp_nodes_ok_1",
    )

    def _drop_bfs_tables(self, spark):
        for t in self._BFS_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
            spark.sql(f"DROP VIEW IF EXISTS {t}")
        spark.sql("DROP VIEW IF EXISTS paths_1")

    REPORTED_QUERY = f"""
    MATCH (root {{ node_id: 'A' }})
    MATCH p = (root)-[*1..3]-(d)
    WHERE all(n IN nodes(p) WHERE {STATUS_DEGREE_FILTER})
    UNWIND relationships(p) AS r
    RETURN DISTINCT r.src AS src, r.dst AS dst
    """

    # Undirected *1..3 from A within passing nodes {A,B,C,F,H}.
    # NOTE: not identical to the CTE result set. Procedural BFS uses a
    # GLOBAL visited set (shortest-path semantics): each node is expanded
    # once, so edges only reachable via longer alternative paths may be
    # dropped. The CTE enumerates all simple paths. This difference is
    # inherent to the mode (documented), not introduced by the pushdown.
    EXPECTED_EDGES = {("A", "B"), ("B", "C"), ("C", "F"), ("H", "A")}

    def test_temp_tables_enforces_predicate(self, spark, graph):
        """temp_tables (TABLE rewrite for OSS Spark): D/E/G pruned."""
        sql = graph.transpile(
            self.REPORTED_QUERY,
            vlp_rendering_mode="procedural",
            materialization_strategy="temp_tables",
        )
        print(f"\n=== SQL ===\n{sql}")
        assert "bfs_vlp_nodes_ok_1" in sql
        # Drop AFTER too (try/finally): the TABLE rewrite creates managed
        # tables whose warehouse directories outlive this module's Spark
        # session and would collide with other test modules' CREATE TABLE.
        self._drop_bfs_tables(spark)
        try:
            rows = spark.sql(
                sql.replace("TEMPORARY TABLE", "TABLE")
            ).collect()
        finally:
            self._drop_bfs_tables(spark)
        results = {(r["src"], r["dst"]) for r in rows}
        assert results == self.EXPECTED_EDGES, (
            f"Expected {self.EXPECTED_EDGES}, got {results}"
        )

    def test_numbered_views_enforces_predicate(self, spark, graph):
        """numbered_views (native scripting): D/E/G pruned."""
        if not _has_scripting(spark):
            pytest.skip("Requires PySpark 4.2+ SQL scripting")
        sql = graph.transpile(
            self.REPORTED_QUERY,
            vlp_rendering_mode="procedural",
            materialization_strategy="numbered_views",
        )
        print(f"\n=== SQL ===\n{sql}")
        assert "bfs_vlp_nodes_ok_1" in sql
        rows = spark.sql(sql).collect()
        results = {(r["src"], r["dst"]) for r in rows}
        assert results == self.EXPECTED_EDGES, (
            f"Expected {self.EXPECTED_EDGES}, got {results}"
        )

    def test_non_pushable_still_raises(self, graph):
        """any()/outer-variable predicates cannot be enforced by the
        frontier filter and the path array is unavailable -> clear error."""
        from gsql2rsql.common.exceptions import (
            TranspilerNotSupportedException,
        )

        non_pushable = [
            # existential quantifier: frontier filter would change semantics
            """
            MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
            WHERE any(n IN nodes(p) WHERE n.node_type = 'PJ')
            RETURN DISTINCT d.node_id AS dst
            """,
            # outer variable in the lambda body
            """
            MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
            WHERE all(n IN nodes(p) WHERE n.degree <= d.degree)
            RETURN DISTINCT d.node_id AS dst
            """,
            # ALL under OR: not conjunctive, cannot prune traversal
            """
            MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
            WHERE all(n IN nodes(p) WHERE n.degree <= 3) OR d.node_id = 'E'
            RETURN DISTINCT d.node_id AS dst
            """,
        ]
        for query in non_pushable:
            with pytest.raises(TranspilerNotSupportedException):
                graph.transpile(query, vlp_rendering_mode="procedural")

    def test_procedural_without_pattern_has_no_nodes_ok_table(self, graph):
        """Queries without the pattern generate byte-identical scripts to
        before: no bfs_vlp_nodes_ok table appears."""
        query = """
        MATCH p = (root { node_id: 'A' })-[*1..2]->(d)
        RETURN DISTINCT d.node_id AS dst
        """
        sql = graph.transpile(
            query,
            vlp_rendering_mode="procedural",
            materialization_strategy="temp_tables",
        )
        assert "bfs_vlp_nodes_ok" not in sql
