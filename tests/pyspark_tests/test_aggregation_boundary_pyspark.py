"""Execution coverage for the aggregation-boundary code path.

``WITH <entity>, <aggregate> ... MATCH (<entity>)-[...]->(...)`` creates an
``AggregationBoundaryOperator`` plus a CTE that is joined back on the grouped
entity's id.  It is the densest bug area in this project's history --
``155da2f`` (preserve full column names for entity projections after
aggregation), ``6388679`` (use the AggregationBoundaryOperator itself),
``7ec0add`` (multi-WITH entity continuation) -- and
``docs_help_dev/limitations.md`` still lists "Aggregation After Aggregation" as
*Under Investigation*.

Existing coverage (``test_44_match_after_aggregating_with.py``,
``test_41_column_projection_through_aggregation.py``,
``test_aggregation_entity_projection.py``) is **S-level only**: substring
assertions on the SQL.  Nothing executes this path.  These tests do.

Fixture A -- ``payments`` (see test_business_composite_pyspark.py for the full
diagram)::

                   (frozen)
    A1 ──────► A2 ──────► A3 ──────► A4
    │          ▲                      │
    │          │                      ▼
    └────────► A5 ──────► A6         A7  (leaf)
                           │
                           └──► A6   (self-loop)

    A8 (isolated)   A9 (NULL risk_score)

Out-degrees: A1=2, A2=1, A3=1, A4=1, A5=2, A6=1, A7=0, A8=0, A9=0.
"""

import pytest

from tests.utils.spark_session import new_test_session

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    """Isolated child session. Never call spark.stop() -- see tests/utils."""
    session = new_test_session()
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW ab_nodes AS
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
        CREATE OR REPLACE TEMPORARY VIEW ab_edges AS
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
        nodes_table="ab_nodes",
        edges_table="ab_edges",
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


class TestMatchAfterAggregatingWith:
    """Aggregate, filter on the aggregate, then traverse from the entity."""

    def test_rebind_entity_and_traverse_again(self, spark, graph):
        """Out-degree >= 2, then re-traverse from the surviving accounts.

        Out-degrees: A1=2, A2=1, A3=1, A4=1, A5=2, A6=1.
        ``out_degree >= 2`` keeps {A1, A5}.
        Re-matching their outgoing transfers counts 2 each.

        ``city`` in the projection proves entity *properties* still resolve
        after the aggregation boundary -- the exact failure mode of ``155da2f``.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS out_degree
        WHERE out_degree >= 2
        MATCH (a)-[:TRANSFER]->(x:Account)
        RETURN a.node_id AS id, a.city AS city, COUNT(x) AS transfers
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["id"], r["city"], r["transfers"]) for r in rows]
        assert results == [("A1", "Lisboa", 2), ("A5", "Lisboa", 2)], (
            f"Expected [('A1','Lisboa',2), ('A5','Lisboa',2)], got {results}"
        )

    def test_aggregate_value_survives_the_boundary(self, spark, graph):
        """The aggregate computed *before* the boundary is still readable.

        ``out_degree`` is projected in the final RETURN, after a second MATCH
        has joined new rows in.  It must keep its pre-boundary value (2), not
        be recomputed against the new row count.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS out_degree
        WHERE out_degree >= 2
        MATCH (a)-[:TRANSFER]->(x:Account)
        RETURN a.node_id AS id, out_degree, x.node_id AS target
        ORDER BY id, target
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["id"], r["out_degree"], r["target"]) for r in rows]
        assert results == [
            ("A1", 2, "A2"),
            ("A1", 2, "A5"),
            ("A5", 2, "A2"),
            ("A5", 2, "A6"),
        ], f"Expected 4 rows with out_degree 2 throughout, got {results}"


class TestAggregationAfterAggregation:
    """The pattern ``limitations.md`` lists as 'Under Investigation'."""

    def test_group_then_regroup(self, spark, graph):
        """Count per account, then sum those counts per city.

        Per-sender out-degrees: A1=2, A2=1, A3=1, A4=1, A5=2, A6=1.
        Grouped by city:
          Lisboa {A1:2, A2:1, A5:2} -> 5
          Porto  {A3:1, A4:1, A6:1} -> 3
        Braga has no senders and is absent.
        """
        query = """
        MATCH (a:Account)-[:TRANSFER]->(b:Account)
        WITH a, COUNT(b) AS out_degree
        WITH a.city AS city, SUM(out_degree) AS total_transfers
        RETURN city, total_transfers
        ORDER BY city
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["city"], r["total_transfers"]) for r in rows]
        assert results == [("Lisboa", 5), ("Porto", 3)], (
            f"Expected [('Lisboa', 5), ('Porto', 3)], got {results}"
        )


class TestOptionalMatchCounting:
    """``COUNT(x)`` vs ``COUNT(*)`` across a LEFT JOIN."""

    def test_isolated_nodes_count_zero_not_one(self, spark, graph):
        """Every account appears; senders count their edges, others count 0.

        A7 (leaf), A8 (isolated) and A9 have no outgoing transfers.  They must
        appear with ``COUNT(b) = 0`` -- not be dropped (that would be an INNER
        JOIN) and not be 1 (that would be counting the NULL-padded row).
        """
        query = """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:TRANSFER]->(b:Account)
        RETURN a.node_id AS id, COUNT(b) AS out_deg
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["id"], r["out_deg"]) for r in rows]
        assert results == [
            ("A1", 2), ("A2", 1), ("A3", 1), ("A4", 1), ("A5", 2),
            ("A6", 1), ("A7", 0), ("A8", 0), ("A9", 0),
        ], f"Expected all 9 accounts with A7/A8/A9 at 0, got {results}"

    def test_count_star_counts_the_padded_row(self, spark, graph):
        """``COUNT(*)`` counts rows, so an unmatched left row still counts 1.

        This is the classic divergence: for A7/A8/A9, ``COUNT(b)`` is 0 but
        ``COUNT(*)`` is 1, because the LEFT JOIN produced one NULL-padded row.
        """
        query = """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:TRANSFER]->(b:Account)
        RETURN a.node_id AS id, COUNT(b) AS matched, COUNT(*) AS rows_out
        ORDER BY id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        by_id = {r["id"]: (r["matched"], r["rows_out"]) for r in rows}
        assert by_id["A7"] == (0, 1), f"A7: {by_id['A7']}"
        assert by_id["A8"] == (0, 1), f"A8: {by_id['A8']}"
        assert by_id["A1"] == (2, 2), f"A1: {by_id['A1']}"


class TestAggregateCoverage:
    """Aggregates with templates but no query-level test."""

    def test_min_max_over_nullable_column(self, spark, graph):
        """MIN/MAX skip NULL: A9's NULL risk_score must not become the MIN.

        risk_scores: 90, 20, 55, 70, 40, 85, 30, 10, NULL.
        MIN = 10 (A8), MAX = 90 (A1), COUNT = 8 non-NULL of 9 accounts.
        """
        query = """
        MATCH (a:Account)
        RETURN MIN(a.risk_score) AS lo, MAX(a.risk_score) AS hi,
               COUNT(a.risk_score) AS scored, COUNT(*) AS total
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert len(rows) == 1
        row = rows[0]
        assert (row["lo"], row["hi"]) == (10, 90), (row["lo"], row["hi"])
        assert row["scored"] == 8, row["scored"]
        assert row["total"] == 9, row["total"]

    def test_stdevp_over_known_population(self, spark, graph):
        """``stDevP`` renders to STDDEV_POP and computes over non-NULL values.

        Non-NULL scores: 90, 20, 55, 70, 40, 85, 30, 10 -- mean 50.0.
        Population standard deviation (``statistics.pstdev``) = 27.9508...,
        which distinguishes ``stDevP`` from ``stDev`` (the *sample* stdev of
        the same values is 29.8807).  Asserted to a tolerance rather than a
        decimal expansion.
        """
        query = """
        MATCH (a:Account)
        RETURN stDevP(a.risk_score) AS spread, AVG(a.risk_score) AS mean
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert len(rows) == 1
        # (90+20+55+70+40+85+30+10)/8 = 50.0
        assert float(rows[0]["mean"]) == pytest.approx(50.0, abs=0.01)
        assert float(rows[0]["spread"]) == pytest.approx(27.951, abs=0.01)

    def test_percentile_cont_median(self, spark, graph):
        """``percentileCont`` must aggregate, not return one row per account.

        Non-NULL scores sorted: 10, 20, 30, 40, 55, 70, 85, 90.
        Continuous median = (40 + 55) / 2 = 47.5.

        EXPECTED TO FAIL -- ``percentileCont`` has no entry in
        ``AGGREGATION_TEMPLATES``, so the renderer emits the bare operand and
        this returns 9 rows of raw scores.  See
        ``tests/test_unsupported_aggregations.py`` for the structural
        counterpart and the root cause.
        """
        query = """
        MATCH (a:Account)
        RETURN percentileCont(a.risk_score, 0.5) AS median
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert len(rows) == 1, (
            f"percentileCont must produce a single aggregate row, got "
            f"{len(rows)} rows: {[r.asDict() for r in rows]}"
        )
        assert float(rows[0]["median"]) == pytest.approx(47.5, abs=0.01)
