"""Execution tests: a repeated variable must constrain both pattern endpoints.

Fixture A -- ``payments``, an account-transfer graph::

                   (frozen)
    A1 ──────► A2 ──────► A3 ──────► A4
    ▲ │          ▲         │          │
    │ │          │         │          ▼
    │ └────────► A5 ──────► A6       A7  (leaf, no outgoing edges)
    │                       │
    └───────── A3           └──► A6   (SELF-LOOP)
      (A3 -> A1 closes the A1->A2->A3->A1 triangle)

    A8  (isolated -- no edges at all)
    A9  (NULL risk_score, no edges)

The self-loop ``A6 -> A6`` and the single genuine 3-cycle
``A1 -> A2 -> A3 -> A1`` are the two load-bearing properties for this
module.

Structural counterpart: ``tests/test_repeated_variable_join.py`` (runs in CI;
the PySpark suites do not).
"""

import pytest

from tests.utils.spark_session import new_test_session

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    """Isolated child session. Never call spark.stop() -- see tests/utils."""
    session = new_test_session()
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW rv_nodes AS
        SELECT * FROM VALUES
            ('A1', 'Account', 90,   'active', 'Lisboa'),
            ('A2', 'Account', 20,   'frozen', 'Lisboa'),
            ('A3', 'Account', 55,   'active', 'Porto'),
            ('A4', 'Account', 70,   'active', 'Porto'),
            ('A5', 'Account', 40,   'active', 'Lisboa'),
            ('A6', 'Account', 85,   'active', 'Porto'),
            ('A7', 'Account', 30,   'active', 'Braga'),
            ('A8', 'Account', 10,   'active', 'Braga'),
            ('A9', 'Account', CAST(NULL AS INT), 'active', 'Lisboa')
        AS t(node_id, node_type, risk_score, status, city)
    """)
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW rv_edges AS
        SELECT * FROM VALUES
            ('A1', 'A2', 'TRANSFER', 1000),
            ('A1', 'A5', 'TRANSFER',  500),
            ('A2', 'A3', 'TRANSFER', 2000),
            ('A3', 'A1', 'TRANSFER',  400),
            ('A3', 'A4', 'TRANSFER',  300),
            ('A4', 'A7', 'TRANSFER',  750),
            ('A5', 'A2', 'TRANSFER',  600),
            ('A5', 'A6', 'TRANSFER',  900),
            ('A6', 'A6', 'TRANSFER',  250)
        AS t(src, dst, relationship_type, amount)
    """)
    return session


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="rv_nodes",
        edges_table="rv_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"risk_score": int, "status": str, "city": str},
        extra_edge_attrs={"amount": int},
    )
    ctx.set_types(node_types=["Account"], edge_types=["TRANSFER"])
    return ctx


class TestSelfLoopRepeatedVariable:
    """``(a)-[:TRANSFER]->(a)`` returns only accounts that transfer to self."""

    def test_self_loop_only(self, spark, graph):
        """Expected: {A6}.

        A6 -> A6 is the only edge whose src equals its dst.  A1, A2, A3, A4
        and A5 all have outgoing transfers but none to themselves, so a
        transpiler that constrains only ``a.node_id = edge.src`` returns all
        six -- the silent wrong answer this test exists to catch.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(a)
        RETURN a.node_id AS id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == {"A6"}, (
            f"Expected only the self-loop account A6, got {sorted(results)}. "
            f"Accounts with any outgoing transfer are A1-A6; if all of them "
            f"appear, the edge's sink was never bound back to 'a'."
        )

    def test_self_loop_with_property_filter(self, spark, graph):
        """Same pattern plus a filter -- must still be self-loops only.

        A6 has risk_score 85 > 50, so the expected set is unchanged.  A
        transpiler returning all senders would also return A1 (90) and A4
        (70), which pass the filter too.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(a)
        WHERE a.risk_score > 50
        RETURN a.node_id AS id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == {"A6"}, f"Expected {{A6}}, got {sorted(results)}"

    def test_control_distinct_variables_returns_all_edges(self, spark, graph):
        """Control: with two distinct variables, all 8 edges must come back.

        Guards the two tests above -- proves the fixture and the join work in
        general, so a failure there is specific to the repeated variable.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        RETURN a.node_id AS src, b.node_id AS dst
        """
        sql = graph.transpile(query)
        rows = spark.sql(sql).collect()
        results = {(r["src"], r["dst"]) for r in rows}
        expected = {
            ("A1", "A2"), ("A1", "A5"), ("A2", "A3"), ("A3", "A1"),
            ("A3", "A4"), ("A4", "A7"), ("A5", "A2"), ("A5", "A6"),
            ("A6", "A6"),
        }
        assert results == expected, f"Expected {expected}, got {results}"


class TestClosedTriangle:
    """A 3-cycle pattern must close back onto the first variable."""

    TRIANGLE_QUERY = """
    MATCH (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)
          -[:TRANSFER]->(a)
    RETURN DISTINCT a.node_id AS id
    """

    def test_triangle_cycle_closes(self, spark, graph):
        """The genuine triangle is found; non-cycling 3-paths are excluded.

        Genuine 3-cycle: A1->A2->A3->A1, matched from each rotation, so
        a in {A1, A2, A3}.  A6 also appears because the transpiler reuses
        the single self-loop edge A6->A6 for all three hops -- openCypher's
        relationship uniqueness forbids that, but the transpiler does not
        enforce it (documented limitation; strict test below is xfail).

        The load-bearing assertion is the *absence* of A4, A5, A7-A9:
        plenty of non-cycling 3-hop paths start there (A5->A2->A3->A4,
        A2->A3->A4->A7, ...), so before the closing-join fix this returned
        them all.
        """
        sql = graph.transpile(self.TRIANGLE_QUERY)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == {"A1", "A2", "A3", "A6"}, (
            f"Expected the A1->A2->A3->A1 triangle members plus the "
            f"self-loop reuse artefact A6, got {sorted(results)}. Any of "
            f"A4/A5/A7 present means the final hop was not joined back "
            f"to 'a'."
        )

    @pytest.mark.xfail(
        reason="Relationship uniqueness (edge isomorphism) is not enforced: "
        "the single self-loop edge A6->A6 is reused for all three pattern "
        "relationships, which openCypher forbids. Documented in "
        "docs_help_dev/limitations.md.",
        strict=True,
    )
    def test_triangle_strict_relationship_uniqueness(self, spark, graph):
        """Strict openCypher: each pattern relationship binds a distinct edge.

        Under edge isomorphism only the genuine triangle qualifies, so the
        expected set is exactly {A1, A2, A3} -- no A6.
        """
        sql = graph.transpile(self.TRIANGLE_QUERY)
        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == {"A1", "A2", "A3"}, sorted(results)

    def test_two_hop_return_to_origin(self, spark, graph):
        """``(a)->(b)->(a)`` -- a 2-cycle. Expected today: {A6}.

        No genuine 2-cycle exists (no pair of accounts transfers in both
        directions).  A6 matches only by reusing the self-loop edge for
        both hops -- the same relationship-uniqueness gap as above; strict
        openCypher would return {}.  Locked in as current behaviour so the
        closing join itself stays covered: before the fix this returned
        every 2-path origin {A1, A2, A3, A5, A6}.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(a)
        RETURN DISTINCT a.node_id AS id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == {"A6"}, (
            f"Expected {{A6}} (self-loop edge reused for both hops; no "
            f"genuine 2-cycle exists), got {sorted(results)}"
        )
