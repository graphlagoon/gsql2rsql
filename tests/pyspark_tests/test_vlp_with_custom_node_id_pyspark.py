"""PySpark tests: WITH node after VLP must work with custom node ID column.

Regression: the column resolver's fallback for nodes propagated through a
WITH clause after VLP hardcoded 'node_id' as the identifier column. With a
custom schema (node_id_col="vertex_key") it produced _gsql2rsql_b_node_id,
which was never materialized, failing at runtime with UNRESOLVED_COLUMN.

Test Graph:
    A ---OWNS---> B ---OWNS---> C
"""

import pytest
from pyspark.sql import SparkSession

from gsql2rsql import GraphContext


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("VlpWithCustomNodeId_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    # Node/edge columns deliberately NOT named node_id/src/dst
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW vk_nodes AS
        SELECT * FROM VALUES
            ('A', 'Person', 'Alice'),
            ('B', 'Person', 'Bob'),
            ('C', 'Person', 'Carol')
        AS t(vertex_key, kind, name)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW vk_edges AS
        SELECT * FROM VALUES
            ('A', 'B', 'OWNS'),
            ('B', 'C', 'OWNS')
        AS t(source_node_id, target_node_id, rel_kind)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    ctx = GraphContext(
        spark=spark,
        nodes_table="vk_nodes",
        edges_table="vk_edges",
        node_id_col="vertex_key",
        node_type_col="kind",
        edge_type_col="rel_kind",
        edge_src_col="source_node_id",
        edge_dst_col="target_node_id",
        extra_node_attrs={"name": str},
    )
    ctx.set_types(
        node_types=["Person"],
        edge_types=["OWNS"],
    )
    return ctx


class TestVlpWithCustomNodeId:
    """VLP endpoint propagated through WITH, custom node_id_col."""

    def test_vlp_with_return_node(self, spark, graph):
        """MATCH p=(a)-[*1..2]->(b) WITH b RETURN b must execute.

        From A: reaches B (depth 1) and C (depth 2).
        """
        query = """
        MATCH p = (a)-[*1..2]->(b)
        WHERE a.vertex_key = 'A'
        WITH b
        RETURN b
        """

        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()

        reached = {row["b"]["vertex_key"] for row in rows}
        expected = {"B", "C"}
        assert reached == expected, f"Expected {expected}, got {reached}"

    def test_vlp_with_return_node_property(self, spark, graph):
        """MATCH p=(a)-[*1..2]->(b) WITH b RETURN b.name must execute."""
        query = """
        MATCH p = (a)-[*1..2]->(b)
        WHERE a.vertex_key = 'A'
        WITH b
        RETURN b.name AS nm
        """

        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")

        rows = spark.sql(sql).collect()

        names = {row["nm"] for row in rows}
        expected = {"Bob", "Carol"}
        assert names == expected, f"Expected {expected}, got {names}"
