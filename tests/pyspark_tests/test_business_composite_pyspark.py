"""Business-shaped composite queries: pattern + filter + traversal + aggregation.

These are the queries a fraud/credit analyst actually writes.  Each combines
several features whose *interaction* is the risk -- single-feature coverage
already exists for all of them individually.

Fixture A -- ``payments``::

                   (frozen)
    A1 ──────► A2 ──────► A3 ──────► A4
    │          ▲                      │
    │          │                      ▼
    └────────► A5 ──────► A6         A7  (leaf, no outgoing edges)
                           │
                           └──► A6   (SELF-LOOP)

    A8  (isolated -- no edges at all)
    A9  (NULL risk_score, no edges)

    id  risk_score  status   city
    A1      90      active   Lisboa
    A2      20      frozen   Lisboa      <- barrier node
    A3      55      active   Porto
    A4      70      active   Porto
    A5      40      active   Lisboa
    A6      85      active   Porto       <- self-loop
    A7      30      active   Braga       <- leaf
    A8      10      active   Braga       <- isolated
    A9    NULL      active   Lisboa      <- NULL property

    edges (TRANSFER, amount):
    A1->A2 1000, A1->A5 500, A2->A3 2000, A3->A4 300,
    A4->A7 750,  A5->A2 600, A5->A6 900, A6->A6 250

Every node and edge is load-bearing: A2 is the frozen barrier, A6 the
self-loop, A7 the leaf, A8 the isolated node (zero-count under LEFT JOIN),
A9 the NULL property, and A1's two routes to A2 (direct and via A5) make
route-counting non-trivial.
"""

import pytest

from tests.utils.spark_session import new_test_session

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    """Isolated child session. Never call spark.stop() -- see tests/utils."""
    session = new_test_session()
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW bc_nodes AS
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
        CREATE OR REPLACE TEMPORARY VIEW bc_edges AS
        SELECT * FROM VALUES
            ('A1', 'A2', 'TRANSFER', 1000),
            ('A1', 'A5', 'TRANSFER',  500),
            ('A2', 'A3', 'TRANSFER', 2000),
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
        nodes_table="bc_nodes",
        edges_table="bc_edges",
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


class TestLaunderingRouteAnalysis:
    """VLP + node barrier + sink filter + aggregation + ordering."""

    def test_risky_accounts_reachable_avoiding_frozen(self, spark, graph):
        """"Which risky accounts are reachable from A1 within 3 hops without
        routing through a frozen account, and by how many routes?"

        Traversal from A1 with every path node required to be non-frozen:
          * A1 -> A2  blocked immediately (A2 is frozen)
          * A1 -> A5              (1 hop)
          * A1 -> A5 -> A2        blocked (A2 frozen)
          * A1 -> A5 -> A6        (2 hops)
          * A1 -> A5 -> A6 -> A6  (3 hops, traversing the self-loop once)

        Reachable, non-frozen: {A5, A6}.
        Sink filter risk_score > 50: A5 is 40 (fails), A6 is 85 (passes).

        A6 is therefore reached by TWO distinct paths -- the self-loop is a
        legitimate third hop, used once, so relationship uniqueness does not
        exclude it and the cycle guard (which tracks the edge *source*) does
        not either.  Verified by projecting nodes(p):
        ``['A1','A5','A6']`` and ``['A1','A5','A6','A6']``.

        Expected: exactly one row, (A6, 2 routes).
        """
        query = """
        MATCH p = (src:Account)-[:TRANSFER*1..3]->(dst:Account)
        WHERE src.node_id = 'A1'
          AND dst.risk_score > 50
          AND ALL(n IN nodes(p) WHERE n.status <> 'frozen')
        RETURN dst.node_id AS id, COUNT(*) AS routes
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["id"], r["routes"]) for r in rows]
        assert results == [("A6", 2)], (
            f"Expected [('A6', 2)], got {results}. A6 is the only account "
            f"with risk_score > 50 reachable from A1 without passing through "
            f"the frozen account A2, and it is reached by two paths."
        )

    def test_without_barrier_frozen_route_opens_up(self, spark, graph):
        """Control: drop the barrier and the A2 route becomes available.

        Without the ``status <> 'frozen'`` guard, from A1 within 3 hops:
          A2 (1), A5 (1), A3 (2 via A2), A6 (2 via A5), A4 (3 via A2->A3),
          A2 again (2, via A5).
        With risk_score > 50: A3 (55), A4 (70), A6 (85).

        Proves the barrier in the previous test does real work rather than
        the result being empty for an unrelated reason.
        """
        query = """
        MATCH p = (src:Account)-[:TRANSFER*1..3]->(dst:Account)
        WHERE src.node_id = 'A1' AND dst.risk_score > 50
        RETURN DISTINCT dst.node_id AS id
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [r["id"] for r in rows]
        assert results == ["A3", "A4", "A6"], (
            f"Expected ['A3', 'A4', 'A6'], got {results}"
        )


class TestCityRiskConcentration:
    """Aggregation + HAVING + ordering + expression in the projection."""

    def test_cities_by_average_risk_of_senders(self, spark, graph):
        """"Which cities host >= 2 accounts that send money, ranked by the
        average risk carried per outgoing transfer?"

        One row is produced per outgoing TRANSFER, so an account that sends
        twice contributes twice to ``AVG`` while ``COUNT(DISTINCT)`` counts it
        once.  That asymmetry is correct openCypher and is the point of the
        test: ``DISTINCT`` inside one aggregate must not silently dedupe the
        rows feeding the others.

        Outgoing transfers by city:
          Lisboa: A1->A2 (90), A1->A5 (90), A2->A3 (20), A5->A2 (40),
                  A5->A6 (40)                      -> 5 rows, 3 distinct senders
          Porto:  A3->A4 (55), A4->A7 (70), A6->A6 (85)
                                                   -> 3 rows, 3 distinct senders
          Braga:  none (A7 is a leaf, A8 isolated)  -> absent

        avg_risk: Lisboa (90+90+20+40+40)/5 = 56.0, Porto (55+70+85)/3 = 70.0.
        Both cities pass ``senders >= 2``.  Ordered DESC: Porto, Lisboa.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(:Account)
        WITH a.city AS city, COUNT(DISTINCT a.node_id) AS senders,
             AVG(a.risk_score) AS avg_risk
        WHERE senders >= 2
        RETURN city, senders, avg_risk
        ORDER BY avg_risk DESC
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [
            (r["city"], r["senders"], round(float(r["avg_risk"]), 1))
            for r in rows
        ]
        assert results == [("Porto", 3, 70.0), ("Lisboa", 3, 56.0)], (
            f"Expected [('Porto', 3, 70.0), ('Lisboa', 3, 56.0)], got "
            f"{results}. Lisboa's 56.0 (not 50.0) is correct: A1 and A5 each "
            f"send twice, so they weight AVG twice while COUNT(DISTINCT) "
            f"counts them once."
        )


class TestSharedCounterparty:
    """Self-join on one label + cross-variable inequality + COUNT(DISTINCT)."""

    def test_accounts_receiving_from_multiple_senders(self, spark, graph):
        """"Which accounts receive transfers from more than one sender?"

        In-edges by target:
          A2 <- {A1, A5}     two distinct senders
          A3 <- {A2}
          A4 <- {A3}
          A5 <- {A1}
          A6 <- {A5, A6}     two distinct senders (A6 via its self-loop)
          A7 <- {A4}

        The ``x.node_id < y.node_id`` guard keeps each unordered sender pair
        once and excludes ``x = y``.  Pairs that survive:
          A2: (A1, A5) -> 1 pair
          A6: (A5, A6) -> 1 pair

        So both A2 and A6 have exactly one qualifying sender pair.  A6
        qualifies *because* of its self-loop: A6 is a genuine second sender
        into A6, distinct from A5.

        Expected: [('A2', 1), ('A6', 1)].
        """
        query = """
        MATCH (x:Account)-[:TRANSFER]->(mid:Account)
        MATCH (y:Account)-[:TRANSFER]->(mid)
        WHERE x.node_id < y.node_id
        RETURN mid.node_id AS shared, COUNT(*) AS sender_pairs
        ORDER BY shared
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["shared"], r["sender_pairs"]) for r in rows]
        assert results == [("A2", 1), ("A6", 1)], (
            f"Expected [('A2', 1), ('A6', 1)], got {results}. A2 receives "
            f"from A1 and A5; A6 receives from A5 and from itself."
        )


class TestUnboundedTraversalReachability:
    """Unbounded ``*`` on a graph whose diameter is well under the cap."""

    def test_full_reachability_from_a1(self, spark, graph):
        """Every account reachable from A1, with no explicit depth bound.

        A1 -> {A2, A5} -> {A3, A6} -> {A4} -> {A7}.  A6's self-loop adds
        nothing new.  A8 and A9 have no edges and are unreachable.

        Expected: {A2, A3, A4, A5, A6, A7}.

        The fixture's longest path from A1 is 4 hops, comfortably below the
        transpiler's default ceiling of 10 for unbounded VLP -- so this test
        measures reachability, not the cap.  The cap itself is asserted in
        ``tests/test_unbounded_vlp_depth_cap.py``.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER*]->(b:Account)
        WHERE a.node_id = 'A1'
        RETURN DISTINCT b.node_id AS id
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [r["id"] for r in rows]
        assert results == ["A2", "A3", "A4", "A5", "A6", "A7"], (
            f"Expected all six reachable accounts, got {results}"
        )
