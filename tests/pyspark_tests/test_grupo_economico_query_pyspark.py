"""PySpark tests for the "grupo econômico" query shape.

Exercises three constructs reported by a user query:
1. ``type(r)`` inside ``ALL(r IN arestas WHERE ...)`` — edge type check.
2. ``nodes(path)[0..-1]`` — slice excluding the LAST path node from the
   node filter (the final entity may exceed the degree cap; intermediate
   nodes may not be "traversed through").
3. ``size((n)--())`` inside a lambda — pattern count, NOT supported:
   must raise a clear error suggesting a materialized degree column.

Test Graph:
    R(Entidade, grau 1)
      -[FILIAL_DE]->            A(PJ, ATIVA?, grau 2)
      -[SOCIO_DE pct 60]->      B(PJ, grau 40)   <- grau above PJ cap (30)
    A -[SOCIO_DE pct 40]->      C(PJ, grau 2)    <- pct below 50: edge fails
    A -[FILIAL_DE]->            D(PF, grau 2)
    B -[FILIAL_DE]->            E(PJ, grau 2)    <- reachable ONLY through B

Rules under test:
    edge rule:  type = FILIAL_DE OR pct >= 50
    node rule (all nodes EXCEPT the last): (PF and grau<=15) or (PJ and
    grau<=30) — Entidade root included via its own branch (grau<=30 as PJ?
    root is typed Entidade; rule uses tipo in {PF,PJ}, so the root would
    FAIL the node rule... the test root is given tipo-compatible values
    via the rule's PJ branch by typing it PJ in the data while labeling
    the query anchor by id only).
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
        .appName("GrupoEconomico_Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW ge_nodes AS
        SELECT * FROM VALUES
            ('R', 'PJ',  1),
            ('A', 'PJ',  2),
            ('B', 'PJ', 40),
            ('C', 'PJ',  2),
            ('D', 'PF',  2),
            ('E', 'PJ',  2)
        AS t(id, tipo, grau)
    """)
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW ge_edges AS
        SELECT * FROM VALUES
            ('R', 'A', 'FILIAL_DE', CAST(NULL AS DOUBLE)),
            ('A', 'B', 'SOCIO_DE',  60.0),
            ('A', 'C', 'SOCIO_DE',  40.0),
            ('A', 'D', 'FILIAL_DE', CAST(NULL AS DOUBLE)),
            ('B', 'E', 'FILIAL_DE', CAST(NULL AS DOUBLE))
        AS t(src, dst, relationship_type, percentual_participacao)
    """)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def graph(spark):
    from gsql2rsql import GraphContext

    ctx = GraphContext(
        spark=spark,
        nodes_table="ge_nodes",
        edges_table="ge_edges",
        node_id_col="id",
        node_type_col="tipo",
        edge_type_col="relationship_type",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"grau": int},
        extra_edge_attrs={"percentual_participacao": float},
    )
    ctx.set_types(
        node_types=["PF", "PJ"],
        edge_types=["FILIAL_DE", "SOCIO_DE"],
    )
    return ctx


GRUPO_QUERY = """
MATCH path = (raiz {id: 'R'})-[arestas*1..3]-(entidade)
WHERE
    ALL(r IN arestas WHERE
        type(r) = 'FILIAL_DE' OR r.percentual_participacao >= 50.0)
    AND ALL(n IN nodes(path)[0..-1] WHERE
        (n.tipo = 'PF' AND n.grau <= 15) OR
        (n.tipo = 'PJ' AND n.grau <= 30)
    )
RETURN DISTINCT entidade.id AS id
"""

# Expected reachable set from R (undirected, 1..3 hops):
#   Edge rule kills A-C (SOCIO_DE 40 < 50). All other edges pass.
#   Node rule applies to every path node EXCEPT the last:
#     R(PJ,1) ok; A(PJ,2) ok; B(PJ,40) FAILS when traversed THROUGH.
#   Paths:
#     R-A                -> A   (interior: R ok)
#     R-A-B              -> B   (interior: R,A ok; B is LAST -> exempt)
#     R-A-D              -> D
#     R-A-B-E            -> XX  (interior includes B -> path rejected)
EXPECTED = {"A", "B", "D"}


class TestGrupoEconomicoCTE:
    def test_full_query_cte(self, spark, graph):
        sql = graph.transpile(GRUPO_QUERY)
        print(f"\n=== SQL ===\n{sql}")
        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        assert results == EXPECTED, f"Expected {EXPECTED}, got {results}"

    def test_type_function_in_edge_all(self, spark, graph):
        """type(r) = 'X' inside ALL over relationships."""
        query = """
        MATCH path = (raiz {id: 'R'})-[arestas*1..2]-(e)
        WHERE ALL(r IN arestas WHERE type(r) = 'FILIAL_DE')
        RETURN DISTINCT e.id AS id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        # Only FILIAL_DE edges: R-A, A-D, B-E. From R: A (1 hop), D (2
        # hops). B unreachable (R-A-B needs SOCIO_DE).
        assert results == {"A", "D"}, f"got {results}"

    def test_slice_excludes_last_node(self, spark, graph):
        """nodes(path)[0..-1]: last node exempt from the node rule.

        B (grau 40 > 30) is a valid FINAL node but not a valid interior
        node: R-A-B returns B, but R-A-B-E must be pruned.
        """
        query = """
        MATCH path = (raiz {id: 'R'})-[*1..3]-(e)
        WHERE ALL(n IN nodes(path)[0..-1] WHERE
            (n.tipo = 'PF' AND n.grau <= 15) OR
            (n.tipo = 'PJ' AND n.grau <= 30))
        RETURN DISTINCT e.id AS id
        """
        sql = graph.transpile(query)
        print(f"\n=== SQL ===\n{sql}")
        rows = spark.sql(sql).collect()
        results = {row["id"] for row in rows}
        # C reachable (edge rule not applied here): R-A-C. E must NOT
        # appear (only path R-A-B-E traverses B as interior).
        assert results == {"A", "B", "C", "D"}, f"got {results}"

    def test_pattern_count_in_lambda_raises(self, graph):
        """size((n)--()) inside a lambda: clear error, not garbage SQL."""
        from gsql2rsql.common.exceptions import (
            TranspilerNotSupportedException,
        )

        query = """
        MATCH path = (raiz {id: 'R'})-[*1..2]-(e)
        WHERE ALL(n IN nodes(path) WHERE size((n)--()) <= 15)
        RETURN DISTINCT e.id AS id
        """
        with pytest.raises(TranspilerNotSupportedException):
            graph.transpile(query)


class TestGrupoEconomicoProcedural:
    def test_full_query_procedural_raises_for_slice(self, graph):
        """The slice form exempts the FINAL node — a frontier filter
        cannot express that (it would need to re-admit last-hop nodes),
        so procedural mode must refuse clearly."""
        from gsql2rsql.common.exceptions import (
            TranspilerNotSupportedException,
        )

        with pytest.raises(TranspilerNotSupportedException):
            graph.transpile(GRUPO_QUERY, vlp_rendering_mode="procedural")
