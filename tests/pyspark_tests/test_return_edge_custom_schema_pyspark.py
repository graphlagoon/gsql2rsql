"""PySpark tests: RETURN r must execute with custom edge src/dst column names.

Regression: the edge NAMED_STRUCT hardcoded 'src'/'dst' keys/columns instead of
deriving them from the schema. With a custom schema (source_node_id/target_node_id)
the struct referenced _gsql2rsql_r_src/_gsql2rsql_r_dst, which were never
materialized in the inner projection, failing at runtime with UNRESOLVED_COLUMN.

Test Graph:
    A ---OWNS---> B   (edge_id e1)
    B ---OWNS---> C   (edge_id e2)
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("ReturnEdgeCustomSchema_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW custom_nodes AS
        SELECT * FROM VALUES
            ('A', 'Person'),
            ('B', 'Person'),
            ('C', 'Person')
        AS t(node_id, node_type)
    """)
    # Edge columns deliberately NOT named src/dst
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW custom_edges AS
        SELECT * FROM VALUES
            ('A', 'B', 'OWNS', 'e1'),
            ('B', 'C', 'OWNS', 'e2')
        AS t(source_node_id, target_node_id, edge_type, edge_id)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    ctx = GraphContext(
        spark=spark,
        nodes_table="custom_nodes",
        edges_table="custom_edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="edge_type",
        edge_src_col="source_node_id",
        edge_dst_col="target_node_id",
        extra_edge_attrs={"edge_id": str},
    )
    ctx.set_types(
        node_types=["Person"],
        edge_types=["OWNS"],
    )
    return ctx


class TestReturnEdgeCustomSchema:
    """RETURN r with custom src/dst column names must execute on Spark."""

    def test_return_edge_executes_and_projects_struct(self, spark, graph):
        """MATCH (s)-[r]->(d) RETURN r must run without UNRESOLVED_COLUMN.

        The struct must expose the real source/target values under the real
        schema column names.
        """
        query = """
        MATCH (s)-[r]->(d)
        RETURN r
        """

        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()

        # Both edges returned, struct keyed by the real schema column names
        edges = {
            (row["r"]["source_node_id"], row["r"]["target_node_id"],
             row["r"]["edge_id"])
            for row in rows
        }
        expected = {("A", "B", "e1"), ("B", "C", "e2")}
        assert edges == expected, f"Expected {expected}, got {edges}"

    def test_collect_edge_executes(self, spark, graph):
        """collect(r) shares the same struct collector — must also execute."""
        query = """
        MATCH (s)-[r]->(d)
        RETURN s.node_id AS src_id, collect(r) AS rels
        """

        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()

        by_src = {row["src_id"]: row["rels"] for row in rows}
        assert set(by_src) == {"A", "B"}
        assert by_src["A"][0]["target_node_id"] == "B"
        assert by_src["A"][0]["edge_id"] == "e1"
        assert by_src["B"][0]["target_node_id"] == "C"
        assert by_src["B"][0]["edge_id"] == "e2"
