"""Scalar functions asserted by *value*, not by template substring.

``test_function_registry.py`` validates the registry data and
``test_30_math_functions.py`` asserts template substrings.  Neither proves the
mapping is *semantically* right -- and two of them are not:

* ``size(<string>)`` renders to Spark's ``SIZE()``, which accepts array/map
  only.  In Cypher ``size()`` on a string is its character count.
* ``list + list`` renders to SQL ``+``, which Spark has no array overload for.
  ``pyspark_tests/test_string_concat_pyspark.py`` already proves ``+`` is
  special-cased for *strings*; the array case is the missing sibling.

Fixture B -- ``people`` (with a ``tags`` array column added)::

    id  name     age  tags
    P1  Alice     30  ['vip', 'eu']
    P2  Bob       40  ['eu']
    P3  Carol     30  []
"""

import pytest

from tests.utils.spark_session import new_test_session

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    """Isolated child session. Never call spark.stop() -- see tests/utils."""
    session = new_test_session()
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW sf_nodes AS
        SELECT * FROM VALUES
            ('P1', 'Person', 'Alice', 30, ARRAY('vip', 'eu')),
            ('P2', 'Person', 'Bob',   40, ARRAY('eu')),
            ('P3', 'Person', 'Carol', 30, ARRAY())
        AS t(node_id, node_type, name, age, tags)
    """)
    session.sql("""
        CREATE OR REPLACE TEMPORARY VIEW sf_edges AS
        SELECT * FROM VALUES ('P1', 'P2', 'KNOWS')
        AS t(src, dst, relationship_type)
    """)
    return session


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="sf_nodes",
        edges_table="sf_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"name": str, "age": int, "tags": list},
    )
    ctx.set_types(node_types=["Person"], edge_types=["KNOWS"])
    return ctx


def _one(spark, graph, cypher: str):
    sql = graph.transpile(cypher)
    print(f"\n=== SQL ===\n{sql}")
    return spark.sql(sql).collect()


class TestStringFunctionValues:
    """String functions must return the values Cypher specifies."""

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("toUpper(p.name)", "ALICE"),
            ("toLower(p.name)", "alice"),
            ("trim(p.name)", "Alice"),
            ("toString(p.age)", "30"),
            ("left(p.name, 3)", "Ali"),
            ("right(p.name, 3)", "ice"),
        ],
    )
    def test_string_function_value(
        self, spark, graph, expr: str, expected: str
    ) -> None:
        rows = _one(
            spark,
            graph,
            f"MATCH (p:Person) WHERE p.name = 'Alice' RETURN {expr} AS v",
        )
        assert len(rows) == 1
        assert rows[0]["v"] == expected, f"{expr} -> {rows[0]['v']}"

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("toInteger(p.age)", 30),
            ("toFloat(p.age)", 30.0),
        ],
    )
    def test_cast_function_value(
        self, spark, graph, expr: str, expected: object
    ) -> None:
        rows = _one(
            spark,
            graph,
            f"MATCH (p:Person) WHERE p.name = 'Alice' RETURN {expr} AS v",
        )
        assert len(rows) == 1
        assert rows[0]["v"] == expected, f"{expr} -> {rows[0]['v']}"

    def test_size_of_string_is_character_count(self, spark, graph) -> None:
        """``size('Alice')`` is 5 in Cypher.

        EXPECTED TO FAIL -- the renderer emits Spark's ``SIZE()``, which
        accepts array/map only, so this raises a type error at execution
        rather than returning 5.  The authoritative type system already knows
        the operand is a string, so ``LENGTH()`` is selectable.
        """
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN size(p.name) AS v",
        )
        assert rows[0]["v"] == 5, (
            f"size('Alice') must be 5, got {rows[0]['v']}"
        )


class TestListFunctionValues:
    """List operations must produce arrays, not arithmetic."""

    def test_size_of_list(self, spark, graph) -> None:
        """Control: ``size()`` over an array is already correct."""
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN size(p.tags) AS v",
        )
        assert rows[0]["v"] == 2, rows[0]["v"]

    def test_list_concatenation(self, spark, graph) -> None:
        """``['vip','eu'] + ['extra']`` is ``['vip','eu','extra']``.

        EXPECTED TO FAIL -- renders as SQL ``(tags) + (('extra'))``.  Spark
        has no ``+`` operator for arrays, so this is a type error at
        execution.  ``CONCAT()`` is the correct Databricks form.
        """
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p.tags + ['extra'] AS v",
        )
        assert list(rows[0]["v"]) == ["vip", "eu", "extra"], rows[0]["v"]

    def test_list_plus_list_property(self, spark, graph) -> None:
        """Concatenating two array-typed properties.

        EXPECTED TO FAIL for the same reason as above.
        """
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p.tags + p.tags AS v",
        )
        assert list(rows[0]["v"]) == ["vip", "eu", "vip", "eu"], rows[0]["v"]


class TestStringConcatControl:
    """Control: string ``+`` is special-cased correctly and must stay so."""

    def test_string_plus_string(self, spark, graph) -> None:
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN 'Hi ' + p.name AS v",
        )
        assert rows[0]["v"] == "Hi Alice", rows[0]["v"]

    def test_numeric_plus_numeric(self, spark, graph) -> None:
        rows = _one(
            spark,
            graph,
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p.age + 5 AS v",
        )
        assert rows[0]["v"] == 35, rows[0]["v"]
