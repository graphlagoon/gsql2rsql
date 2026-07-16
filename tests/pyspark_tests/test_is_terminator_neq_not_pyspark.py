"""PySpark execution tests for is_terminator() with `<>` and NOT forms.

Covers two user-reported scenarios on an undirected VLP (*1..3):

1. is_terminator(... coalesce(d.something, 'ATIVA') <> 'ATIVA') — the
   inequality operator inside a complex OR + coalesce predicate. (`!=` is
   not valid Cypher; it must raise a clear syntax error — also tested.)
2. NOT is_terminator(P) — negated directive: the barrier fires where P
   is FALSE (NOT is_terminator(P) == is_terminator(NOT P)).

Both run through BOTH VLP renderers: WITH RECURSIVE CTE and procedural
BFS (numbered_views).

Test Graph (undirected traversal from ROOT, depth 1..3):

    ROOT --- A  (PF, degree=20, ATIVA)      terminator (PF & degree>15)
    A    --- A2 (PF, degree=1,  ATIVA)
    ROOT --- B  (PF, degree=5,  ATIVA)      expandable
    B    --- C  (PJ, degree=40, ATIVA)      terminator (PJ & degree>30)
    C    --- C2 (PF, degree=1,  ATIVA)
    B    --- D  (PF, degree=2,  INATIVA)    terminator (something <> ATIVA)
    D    --- D2 (PF, degree=1,  ATIVA)
    B    --- E  (PF, degree=3,  NULL)       coalesce->'ATIVA', expandable
    E    --- E2 (PF, degree=1,  ATIVA)
"""

from __future__ import annotations

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

# (mode-kwargs for GraphContext.transpile, label)
RENDER_MODES = [
    pytest.param({}, id="recursive_cte"),
    pytest.param(
        {
            "vlp_rendering_mode": "procedural",
            "materialization_strategy": "numbered_views",
        },
        id="procedural_numbered_views",
    ),
]


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("IsTerminator_Neq_Not_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .getOrCreate()
    )

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW term_nodes AS
        SELECT * FROM VALUES
            ('ROOT', 'PF', 1,  'ATIVA'),
            ('A',    'PF', 20, 'ATIVA'),
            ('A2',   'PF', 1,  'ATIVA'),
            ('B',    'PF', 5,  'ATIVA'),
            ('C',    'PJ', 40, 'ATIVA'),
            ('C2',   'PF', 1,  'ATIVA'),
            ('D',    'PF', 2,  'INATIVA'),
            ('D2',   'PF', 1,  'ATIVA'),
            ('E',    'PF', 3,  CAST(NULL AS STRING)),
            ('E2',   'PF', 1,  'ATIVA')
        AS t(id_hash, node_type, degree, something)
    """)

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW term_edges AS
        SELECT * FROM VALUES
            ('ROOT', 'A',  'REL'),
            ('A',    'A2', 'REL'),
            ('ROOT', 'B',  'REL'),
            ('B',    'C',  'REL'),
            ('C',    'C2', 'REL'),
            ('B',    'D',  'REL'),
            ('D',    'D2', 'REL'),
            ('B',    'E',  'REL'),
            ('E',    'E2', 'REL')
        AS t(src, dst, relationship_type)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    return GraphContext(
        spark=spark,
        nodes_table="term_nodes",
        edges_table="term_edges",
        node_id_col="id_hash",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"degree": int, "something": str},
    )


def _edge_set(rows) -> set[frozenset[str]]:
    """Undirected edge set from UNWIND relationships(p) rows."""
    out: set[frozenset[str]] = set()
    for row in rows:
        r = row["r"]
        if hasattr(r, "asDict"):
            r = r.asDict()
        out.add(frozenset((r["src"], r["dst"])))
    return out


def _fs(pairs) -> set[frozenset[str]]:
    return {frozenset(p) for p in pairs}


class TestIsTerminatorWithNeqOperator:
    """User case 1 (rewritten with <>): OR + coalesce + <> predicate."""

    QUERY = """
    MATCH (root { id_hash: "ROOT" })
    MATCH p = (root)-[*1..3]-(d)
    WHERE is_terminator(
        (d.node_type = 'PF' AND d.degree > 15) OR
        (d.node_type = 'PJ' AND d.degree > 30) OR
        coalesce(d.something, 'ATIVA') <> 'ATIVA'
    )
    UNWIND relationships(p) AS r
    RETURN DISTINCT r
    """

    # A (PF hub), C (PJ hub) and D (INATIVA) are terminators: reached but
    # never expanded past. E has something=NULL -> coalesce yields 'ATIVA',
    # so E is expandable and E2 is reached at depth 3.
    EXPECTED = _fs([
        ("ROOT", "A"), ("ROOT", "B"),
        ("B", "C"), ("B", "D"), ("B", "E"),
        ("E", "E2"),
    ])
    FORBIDDEN = _fs([("A", "A2"), ("C", "C2"), ("D", "D2")])

    @pytest.mark.parametrize("mode_kwargs", RENDER_MODES)
    def test_barrier_with_neq_or_coalesce(self, spark, graph, mode_kwargs):
        sql = graph.transpile(self.QUERY, **mode_kwargs)
        rows = spark.sql(sql).collect()
        got = _edge_set(rows)

        assert got == self.EXPECTED, (
            f"Expected {self.EXPECTED}, got {got}"
        )
        assert not (got & self.FORBIDDEN), (
            f"Barrier leaked past terminator nodes: {got & self.FORBIDDEN}"
        )

    def test_bang_equals_raises_clear_syntax_error(self, graph):
        """`!=` is not valid Cypher — must fail at parse, not deep in
        the pipeline. Users must rewrite `!=` as `<>`."""
        from gsql2rsql.common.exceptions import (
            TranspilerSyntaxErrorException,
        )

        bad_query = self.QUERY.replace("<>", "!=")
        with pytest.raises(TranspilerSyntaxErrorException, match="'!'"):
            graph.transpile(bad_query)


class TestNotIsTerminator:
    """User case 2: NOT is_terminator(P) — barrier fires where P is FALSE."""

    QUERY = """
    MATCH (root { id_hash: "ROOT" })
    MATCH p = (root)-[*1..3]-(d)
    WHERE NOT is_terminator(
        (d.node_type = 'PF' AND d.degree > 15) OR
        (d.node_type = 'PJ' AND d.degree > 30) OR
        coalesce(d.something, 'ATIVA') = 'ATIVA'
    )
    UNWIND relationships(p) AS r
    RETURN DISTINCT r
    """

    # Barrier = NOT(P) = not-a-hub AND something is not 'ATIVA'.
    # Only D (PF, degree=2, INATIVA) satisfies it. Everything else
    # expands: A-A2 (depth 2), C-C2 and E-E2 (depth 3). D-D2 is blocked.
    EXPECTED = _fs([
        ("ROOT", "A"), ("A", "A2"),
        ("ROOT", "B"),
        ("B", "C"), ("C", "C2"),
        ("B", "D"),
        ("B", "E"), ("E", "E2"),
    ])
    FORBIDDEN = _fs([("D", "D2")])

    @pytest.mark.parametrize("mode_kwargs", RENDER_MODES)
    def test_not_terminator_user_shape(self, spark, graph, mode_kwargs):
        sql = graph.transpile(self.QUERY, **mode_kwargs)
        rows = spark.sql(sql).collect()
        got = _edge_set(rows)

        assert got == self.EXPECTED, (
            f"Expected {self.EXPECTED}, got {got}"
        )
        assert not (got & self.FORBIDDEN), (
            f"Barrier leaked past terminator node D: {got & self.FORBIDDEN}"
        )

    SIMPLE_QUERY = """
    MATCH (root { id_hash: "ROOT" })
    MATCH p = (root)-[*1..3]-(d)
    WHERE NOT is_terminator(d.degree <= 15)
    UNWIND relationships(p) AS r
    RETURN DISTINCT r
    """

    # Barrier = NOT(degree <= 15) = degree > 15 -> stops at A and C only.
    SIMPLE_EXPECTED = _fs([
        ("ROOT", "A"), ("ROOT", "B"),
        ("B", "C"), ("B", "D"), ("B", "E"),
        ("D", "D2"), ("E", "E2"),
    ])
    SIMPLE_FORBIDDEN = _fs([("A", "A2"), ("C", "C2")])

    @pytest.mark.parametrize("mode_kwargs", RENDER_MODES)
    def test_not_terminator_simple_predicate(self, spark, graph, mode_kwargs):
        sql = graph.transpile(self.SIMPLE_QUERY, **mode_kwargs)
        rows = spark.sql(sql).collect()
        got = _edge_set(rows)

        assert got == self.SIMPLE_EXPECTED, (
            f"Expected {self.SIMPLE_EXPECTED}, got {got}"
        )
        assert not (got & self.SIMPLE_FORBIDDEN), (
            f"Barrier leaked past hubs A/C: {got & self.SIMPLE_FORBIDDEN}"
        )
