"""Guardrails for constructs the procedural BFS renderer cannot represent.

The procedural BFS architecture is single-source: one global visited set and
a ``CROSS JOIN`` against the seed frontier to attribute the start node.  Some
openCypher constructs silently produced WRONG results under it (empirically
verified against CTE mode on PySpark):

- ``length(p)`` / ``nodes(p)`` returned NULL (the ``path`` column is emitted
  as ``CAST(NULL AS ARRAY<STRING>)``);
- ``*0..N`` omitted the depth-0 row (the start node itself);
- an unfiltered VLP source seeded EVERY node, so the visited anti-join
  discarded all expansions (chained ``MATCH (a)-[*..]->(b) MATCH (b)-[*..]->``
  returned empty) — and with a few seeds the CROSS JOIN fabricated
  (start, end) pairs that are not connected at all.

A transpiler must never return wrong results silently: these now raise
``TranspilerNotSupportedException`` at transpile time (pointing to CTE mode),
plus a runtime single-seed guard inside the generated script for cardinality
only known at execution.  CTE mode keeps supporting all of these constructs.
"""

import pytest

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.graph_context import GraphContext

PROC = {"vlp_rendering_mode": "procedural",
        "materialization_strategy": "numbered_views"}


def _graph() -> GraphContext:
    graph = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="source_node_id",
        edge_dst_col="target_node_id",
        edge_type_col="relationship_type",
        extra_node_attrs={"origin_id": str},
    )
    graph.set_types(node_types=["CNPJ_RAIZ", "socio"], edge_types=["OWNS"])
    return graph


ROOT = "MATCH (root:CNPJ_RAIZ {origin_id: 'x'})\n"


class TestPathFunctionsProcedural:
    """nodes(p)/length(p) need the path array, which procedural never builds."""

    def test_length_p_raises_in_procedural(self) -> None:
        query = (ROOT + "MATCH p = (root)-[*1..2]-(d) "
                 "RETURN DISTINCT length(p) AS l")
        with pytest.raises(TranspilerNotSupportedException, match="length"):
            _graph().transpile(query, **PROC)

    def test_nodes_p_raises_in_procedural(self) -> None:
        query = (ROOT + "MATCH p = (root)-[*1..2]-(d) "
                 "RETURN DISTINCT size(nodes(p)) AS s")
        with pytest.raises(TranspilerNotSupportedException, match="nodes"):
            _graph().transpile(query, **PROC)

    def test_relationships_p_still_supported_in_procedural(self) -> None:
        """relationships(p) uses path_edges, which procedural DOES build."""
        query = (ROOT + "MATCH p = (root)-[*1..2]-(d) "
                 "UNWIND relationships(p) AS r "
                 "RETURN DISTINCT r.source_node_id AS a")
        sql = _graph().transpile(query, **PROC)
        assert "path_edges" in sql

    def test_length_and_nodes_work_in_cte_mode(self) -> None:
        for ret in ("length(p) AS l", "size(nodes(p)) AS s"):
            query = (ROOT + f"MATCH p = (root)-[*1..2]-(d) "
                     f"RETURN DISTINCT {ret}")
            assert _graph().transpile(query, vlp_rendering_mode="cte")


class TestZeroMinHopsProcedural:
    """*0..N includes the start node itself; procedural never emits depth 0."""

    def test_zero_min_hops_raises_in_procedural(self) -> None:
        query = ROOT + "MATCH (root)-[*0..2]-(d) RETURN DISTINCT d"
        with pytest.raises(TranspilerNotSupportedException, match="0"):
            _graph().transpile(query, **PROC)

    def test_zero_min_hops_works_in_cte(self) -> None:
        query = (ROOT + "MATCH (root)-[*0..2]-(d) "
                 "RETURN DISTINCT d.origin_id AS v")
        assert _graph().transpile(query, vlp_rendering_mode="cte")


class TestUnfilteredSourceProcedural:
    """Unfiltered source seeds every node — the single-source design breaks."""

    def test_chained_vlp_raises_in_procedural(self) -> None:
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})-[*1..1]->(b) "
                 "MATCH (b)-[*1..1]->(c) RETURN DISTINCT c.origin_id AS v")
        with pytest.raises(TranspilerNotSupportedException):
            _graph().transpile(query, **PROC)

    def test_chained_vlp_works_in_cte(self) -> None:
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})-[*1..1]->(b) "
                 "MATCH (b)-[*1..1]->(c) RETURN DISTINCT c.origin_id AS v")
        assert _graph().transpile(query, vlp_rendering_mode="cte")

    def test_filtered_source_still_supported(self) -> None:
        """The normal single-root case must keep working in procedural."""
        query = ROOT + "MATCH (root)-[*1..2]-(d) RETURN count(DISTINCT d)"
        assert _graph().transpile(query, **PROC)


class TestRuntimeSingleSeedGuard:
    """A filter can still match >1 node — only the script can check that."""

    def test_generated_script_contains_seed_guard(self) -> None:
        query = ROOT + "MATCH (root)-[*1..2]-(d) RETURN count(DISTINCT d)"
        sql = _graph().transpile(query, **PROC)
        assert "RAISE_ERROR" in sql.upper(), (
            "procedural script must guard against multi-node seeds at runtime"
        )
