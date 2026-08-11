"""NULL propagation and deduplication semantics.

Two areas the suite does not cover:

* **Three-valued logic.**  ``NULL`` is neither TRUE nor FALSE, so a row with a
  NULL property must be excluded by both ``WHERE x > k`` *and*
  ``WHERE NOT (x > k)``.  The only NULL path currently tested anywhere is
  ``COALESCE`` inside a VLP node predicate
  (``test_nodes_path_predicates_pyspark.py``).
* **Deduplication beyond a single column.**  The catalog counts 373 queries
  mentioning ``DISTINCT``, but they are overwhelmingly
  ``RETURN DISTINCT <one column>``.  Multi-column dedup and
  ``COUNT(DISTINCT)`` over a nullable column are untested.

Fixture B -- ``people``::

    Alice ──KNOWS──► Bob ──KNOWS──► Carol
    Dave  ──KNOWS──► Dave    (self-loop)
    Erin                     (isolated)

    id  name   age  salary
    P1  Alice   30   100.0
    P2  Bob     40   NULL     <- NULL salary
    P3  Carol   30   200.0    <- duplicate age with Alice
    P4  Dave    50   NULL     <- NULL salary, self-loop
    P5  Erin    25   300.0    <- isolated

Two NULL salaries (Bob, Dave) and a duplicated age (30, shared by Alice and
Carol) are the load-bearing properties.
"""

import pytest

from tests.utils.spark_session import new_test_session

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    """Isolated child session. Never call spark.stop() -- see tests/utils."""
    session = new_test_session()
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW ns_nodes AS
        SELECT * FROM VALUES
            ('P1', 'Person', 'Alice', 30, CAST(100.0 AS DOUBLE)),
            ('P2', 'Person', 'Bob',   40, CAST(NULL  AS DOUBLE)),
            ('P3', 'Person', 'Carol', 30, CAST(200.0 AS DOUBLE)),
            ('P4', 'Person', 'Dave',  50, CAST(NULL  AS DOUBLE)),
            ('P5', 'Person', 'Erin',  25, CAST(300.0 AS DOUBLE))
        AS t(node_id, node_type, name, age, salary)
    """)
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW ns_edges AS
        SELECT * FROM VALUES
            ('P1', 'P2', 'KNOWS'),
            ('P2', 'P3', 'KNOWS'),
            ('P4', 'P4', 'KNOWS')
        AS t(src, dst, relationship_type)
    """)
    return session


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="ns_nodes",
        edges_table="ns_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"name": str, "age": int, "salary": float},
    )
    ctx.set_types(node_types=["Person"], edge_types=["KNOWS"])
    return ctx


class TestThreeValuedLogic:
    """A NULL operand makes a comparison NULL -- neither TRUE nor FALSE."""

    def test_greater_than_excludes_null(self, spark, graph):
        """``salary > 150``: Carol (200) and Erin (300).

        Bob and Dave have NULL salaries, so the comparison is NULL and the
        rows are dropped.  Alice (100) fails on value.
        """
        query = """
        MATCH (p:Person) WHERE p.salary > 150
        RETURN p.name AS name ORDER BY name
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert [r["name"] for r in rows] == ["Carol", "Erin"]

    def test_negated_comparison_also_excludes_null(self, spark, graph):
        """``NOT (salary > 150)``: Alice only.

        The discriminating case.  ``NOT NULL`` is NULL, so Bob and Dave must
        be excluded here *as well as* from the un-negated test above -- the
        two results do not partition the five people.  A transpiler that
        coalesced the inner comparison to FALSE before negating would wrongly
        return Alice, Bob and Dave.
        """
        query = """
        MATCH (p:Person) WHERE NOT (p.salary > 150)
        RETURN p.name AS name ORDER BY name
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        names = [r["name"] for r in rows]
        assert names == ["Alice"], (
            f"Expected ['Alice'], got {names}. Bob and Dave have NULL "
            f"salaries: NOT(NULL > 150) is NULL, not TRUE, so they must be "
            f"excluded from the negated predicate too."
        )

    def test_is_null_is_the_only_way_to_select_nulls(self, spark, graph):
        """``IS NULL`` returns exactly the two people the comparisons dropped."""
        query = """
        MATCH (p:Person) WHERE p.salary IS NULL
        RETURN p.name AS name ORDER BY name
        """
        sql = graph.transpile(query)
        rows = spark.sql(sql).collect()
        assert [r["name"] for r in rows] == ["Bob", "Dave"]

    def test_aggregates_skip_nulls(self, spark, graph):
        """AVG/SUM/COUNT ignore NULL; COUNT(*) does not.

        Salaries: 100, NULL, 200, NULL, 300.
        AVG = (100+200+300)/3 = 200.0, SUM = 600.0,
        COUNT(salary) = 3, COUNT(*) = 5.
        """
        query = """
        MATCH (p:Person)
        RETURN AVG(p.salary) AS avg_sal, SUM(p.salary) AS sum_sal,
               COUNT(p.salary) AS n_sal, COUNT(*) AS n_rows
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert len(rows) == 1
        row = rows[0]
        assert float(row["avg_sal"]) == pytest.approx(200.0, abs=0.01)
        assert float(row["sum_sal"]) == pytest.approx(600.0, abs=0.01)
        assert row["n_sal"] == 3, row["n_sal"]
        assert row["n_rows"] == 5, row["n_rows"]

    def test_avg_over_all_null_group_is_null_not_zero(self, spark, graph):
        """AVG over a group with no non-NULL values is NULL, not 0.

        Restricting to the NULL-salary people leaves nothing to average.
        """
        query = """
        MATCH (p:Person) WHERE p.salary IS NULL
        RETURN AVG(p.salary) AS avg_sal, COUNT(*) AS n_rows
        """
        sql = graph.transpile(query)
        rows = spark.sql(sql).collect()
        assert len(rows) == 1
        assert rows[0]["avg_sal"] is None, (
            f"AVG over an all-NULL group must be NULL, got {rows[0]['avg_sal']}"
        )
        assert rows[0]["n_rows"] == 2


class TestDeduplication:
    """DISTINCT across several columns, and over nullable input."""

    def test_distinct_multi_column(self, spark, graph):
        """``DISTINCT a.age, b.age`` over the two KNOWS edges.

        Edges (excluding the Dave self-loop, which pairs 50 with 50):
          Alice(30) -> Bob(40)    -> (30, 40)
          Bob(40)   -> Carol(30)  -> (40, 30)
          Dave(50)  -> Dave(50)   -> (50, 50)

        All three pairs are distinct, so dedup changes nothing here -- the
        assertion is that the pair *combination* is preserved rather than
        each column being deduplicated independently (which would give the
        cross-product of {30,40,50} x {30,40,50}).
        """
        query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN DISTINCT a.age AS a_age, b.age AS b_age
        ORDER BY a_age, b_age
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        results = [(r["a_age"], r["b_age"]) for r in rows]
        assert results == [(30, 40), (40, 30), (50, 50)], results

    def test_count_distinct_excludes_null(self, spark, graph):
        """``COUNT(DISTINCT salary)`` = 3; NULL is not a distinct value.

        Salaries 100, NULL, 200, NULL, 300 -> three distinct non-NULL values.
        A implementation counting NULL as a value would return 4.
        """
        query = """
        MATCH (p:Person)
        RETURN COUNT(DISTINCT p.salary) AS n
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()
        assert rows[0]["n"] == 3, (
            f"Expected 3 distinct non-NULL salaries, got {rows[0]['n']}"
        )

    def test_count_distinct_collapses_duplicates(self, spark, graph):
        """``COUNT(DISTINCT age)`` = 4; Alice and Carol share age 30.

        Ages 30, 40, 30, 50, 25 -> {25, 30, 40, 50} = 4 distinct.
        """
        query = "MATCH (p:Person) RETURN COUNT(DISTINCT p.age) AS n"
        sql = graph.transpile(query)
        rows = spark.sql(sql).collect()
        assert rows[0]["n"] == 4, rows[0]["n"]

    def test_distinct_single_column_collapses(self, spark, graph):
        """Control: single-column DISTINCT over the duplicated age."""
        query = """
        MATCH (p:Person) RETURN DISTINCT p.age AS age ORDER BY age
        """
        sql = graph.transpile(query)
        rows = spark.sql(sql).collect()
        assert [r["age"] for r in rows] == [25, 30, 40, 50]
