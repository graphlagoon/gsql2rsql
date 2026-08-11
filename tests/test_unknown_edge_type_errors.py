"""Actionable errors for edge types that do not exist in the schema.

Mirror of the node-label diagnostics: referencing an unknown relationship
type must fail at BINDING time with the available verbs listed and a
case-insensitive near-match suggestion — never as a late
``TranspilerInternalErrorException`` from the enrichment pass ("No enriched
data for RecursiveTraversalOperator"), which reads as a transpiler bug when
it is really a schema/config mismatch.

Two failure situations must stay distinct:

- the VERB does not exist anywhere in the schema (a typo) → raise, both in
  single-type and OR syntax;
- the verb exists but not for this (source, sink) endpoint combination →
  silently dropped from the OR list, which is load-bearing for schemas
  registered with restricted ``edge_combinations``.
"""

import pytest

from gsql2rsql.common.exceptions import TranspilerBindingException
from gsql2rsql.graph_context import GraphContext

PROC = {"vlp_rendering_mode": "procedural",
        "materialization_strategy": "numbered_views"}


def _graph() -> GraphContext:
    graph = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="src",
        edge_dst_col="dst",
        edge_type_col="rel_type",
        extra_node_attrs={"origin_id": str},
    )
    graph.set_types(
        node_types=["CNPJ_RAIZ", "socio"],
        edge_types=["OWNS", "PARTICIPA"],
    )
    return graph


class TestSingleHopUnknownEdgeType:
    def test_unknown_verb_lists_available(self) -> None:
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(
                "MATCH (a:CNPJ_RAIZ)-[:KNOWS]->(b) RETURN a.origin_id AS x"
            )
        message = str(exc_info.value)
        assert "KNOWS" in message
        assert "OWNS" in message and "PARTICIPA" in message, (
            f"error should list available edge types, got: {message}"
        )

    def test_case_mismatch_suggests_near_match(self) -> None:
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(
                "MATCH (a:CNPJ_RAIZ)-[:owns]->(b) RETURN a.origin_id AS x"
            )
        assert "OWNS" in str(exc_info.value)


class TestVLPUnknownEdgeType:
    """VLP used to die late in enrichment as an 'Internal error'."""

    def test_unknown_verb_is_binding_error_not_internal(self) -> None:
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})-[:KNOWS*1..2]->(b) "
                 "RETURN count(DISTINCT b)")
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(query)
        message = str(exc_info.value)
        assert "KNOWS" in message
        assert "OWNS" in message and "PARTICIPA" in message

    def test_case_mismatch_suggests_near_match(self) -> None:
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})-[:owns*1..2]->(b) "
                 "RETURN count(DISTINCT b)")
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(query)
        assert "OWNS" in str(exc_info.value)

    def test_procedural_mode_same_binding_error(self) -> None:
        """Binding runs before the renderer fork — mode-independent."""
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})-[:KNOWS*1..2]->(b) "
                 "RETURN count(DISTINCT b)")
        with pytest.raises(TranspilerBindingException):
            _graph().transpile(query, **PROC)


class TestOrSyntaxTypoDetection:
    """A typo inside [:A|B] silently changed semantics before."""

    def test_or_with_unknown_verb_raises(self) -> None:
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(
                "MATCH (a:CNPJ_RAIZ)-[:OWNS|KNOWS]->(b) "
                "RETURN a.origin_id AS x"
            )
        assert "KNOWS" in str(exc_info.value)

    def test_vlp_or_with_unknown_verb_raises(self) -> None:
        query = ("MATCH (a:CNPJ_RAIZ {origin_id: 'x'})"
                 "-[:OWNS|KNOWS*1..2]->(b) RETURN count(DISTINCT b)")
        with pytest.raises(TranspilerBindingException) as exc_info:
            _graph().transpile(query)
        assert "KNOWS" in str(exc_info.value)


class TestLegitimateCasesUnaffected:
    def test_known_verbs_still_transpile(self) -> None:
        graph = _graph()
        assert graph.transpile(
            "MATCH (a:CNPJ_RAIZ)-[:OWNS]->(b) RETURN a.origin_id AS x"
        )
        assert graph.transpile(
            "MATCH (a:CNPJ_RAIZ)-[:OWNS|PARTICIPA]->(b) "
            "RETURN a.origin_id AS x"
        )

    def test_untyped_edge_still_transpiles(self) -> None:
        assert _graph().transpile(
            "MATCH (a:CNPJ_RAIZ)-[]->(b) RETURN a.origin_id AS x"
        )

    def test_verb_valid_only_for_other_endpoints_is_dropped_not_raised(
        self,
    ) -> None:
        """OR pruning by endpoint combination is load-bearing — keep it.

        ``Y`` exists in the schema but only as A→B; in an A→A pattern the
        OR list must silently reduce to ``X`` (no typo happened).
        """
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="node_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="rel_type",
            extra_node_attrs={"origin_id": str},
        )
        graph.set_types(
            node_types=["A", "B"],
            edge_types=["X", "Y"],
            edge_combinations=[("A", "X", "A"), ("A", "Y", "B")],
        )
        assert graph.transpile(
            "MATCH (a:A)-[:X|Y]->(b:A) RETURN a.origin_id AS v"
        )
