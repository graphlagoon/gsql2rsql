"""Tests for backtick-escaped names (openCypher EscapedSymbolicName).

openCypher allows any identifier to be escaped with backticks:

    MATCH (n:`My Label`)-[r:`MY REL`]->(`other node`)
    RETURN n.`first name`

The backticks are *delimiters*, not part of the name.  A label written as
``:`cnpj_raiz``` must bind to the node type ``cnpj_raiz``, exactly as the
unescaped ``:cnpj_raiz`` does.  Failing to strip them produces a schema
lookup for the literal string ```cnpj_raiz``` which can never match, and
surfaces as a confusing "Failed to bind entity" error even though the type
is correctly registered.
"""

import pytest

from gsql2rsql.graph_context import GraphContext
from gsql2rsql.parser.opencypher_parser import OpenCypherParser


def _collect_entity_names(ast: object) -> list[str]:
    """Walk an AST collecting every non-empty ``entity_name``."""
    found: list[str] = []
    seen: set[int] = set()

    def walk(node: object, depth: int = 0) -> None:
        if depth > 8 or id(node) in seen:
            return
        seen.add(id(node))
        name = getattr(node, "entity_name", None)
        if isinstance(name, str) and name:
            found.append(name)
        for attr in ("entities", "match_clauses", "clauses", "children"):
            value = getattr(node, attr, None)
            if isinstance(value, list):
                for child in value:
                    walk(child, depth + 1)

    walk(ast)
    return found


class TestBacktickStripping:
    """Backticks must be removed from parsed identifiers."""

    def test_node_label_backticks_stripped(self) -> None:
        """`:`cnpj_raiz`` must yield entity_name 'cnpj_raiz', not '`cnpj_raiz`'."""
        ast = OpenCypherParser().parse("MATCH (root:`cnpj_raiz`) RETURN root")
        names = _collect_entity_names(ast)
        assert names, "expected at least one entity_name in the AST"
        for name in names:
            assert "`" not in name, f"backticks leaked into entity_name: {name!r}"
        assert "cnpj_raiz" in names

    def test_node_label_with_spaces(self) -> None:
        """Backticks are what make spaces legal inside a label."""
        ast = OpenCypherParser().parse("MATCH (n:`My Label`) RETURN n")
        assert "My Label" in _collect_entity_names(ast)

    def test_relationship_type_backticks_stripped(self) -> None:
        """Relationship types are escaped the same way as node labels."""
        ast = OpenCypherParser().parse("MATCH (a)-[r:`MY REL`]->(b) RETURN r")
        names = _collect_entity_names(ast)
        for name in names:
            assert "`" not in name, f"backticks leaked into entity_name: {name!r}"
        assert "MY REL" in names

    def test_unescaped_label_unaffected(self) -> None:
        """The common unescaped path must not regress."""
        ast = OpenCypherParser().parse("MATCH (root:cnpj_raiz) RETURN root")
        assert "cnpj_raiz" in _collect_entity_names(ast)


class TestBacktickBinding:
    """An escaped label must bind against the registered node type."""

    @staticmethod
    def _graph() -> GraphContext:
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="origin_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="relationship_type",
        )
        graph.set_types(
            node_types=["cnpj_raiz", "socio"],
            edge_types=["OWNS"],
        )
        return graph

    def test_escaped_label_binds_in_vlp_query(self) -> None:
        """Regression: escaped label + multi-MATCH VLP must transpile.

        This is the exact shape that failed: the type IS registered, so the
        only reason binding could fail is the backticks reaching the schema
        lookup.
        """
        query = """
        MATCH (root:`cnpj_raiz` {origin_id: '04906728'})
        MATCH (root)-[*1..2]-(d)
        RETURN count(DISTINCT d) AS total
        """
        sql = self._graph().transpile(query)
        assert "`cnpj_raiz`" not in sql
        assert "node_type = 'cnpj_raiz'" in sql

    def test_escaped_and_unescaped_produce_same_sql(self) -> None:
        """Backticks are delimiters only — they must not change the output."""
        graph = self._graph()
        escaped = graph.transpile(
            "MATCH (root:`cnpj_raiz`) RETURN root.origin_id AS id"
        )
        plain = graph.transpile(
            "MATCH (root:cnpj_raiz) RETURN root.origin_id AS id"
        )
        assert escaped == plain


class TestBacktickAcrossConstructs:
    """Backticks must be stripped wherever an identifier can appear.

    Identifier extraction is scattered across many ``getText()`` sites in the
    visitor (properties, expression variables, map keys, aliases…).  Each of
    these queries failed with ``ColumnResolutionError`` before unescaping was
    applied at every site, because the leaked backticks made the name miss
    the schema/scope lookup.
    """

    @staticmethod
    def _graph() -> GraphContext:
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="node_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="rel_type",
            extra_node_attrs={"age": int, "name": str},
        )
        graph.set_types(node_types=["Person"], edge_types=["KNOWS", "OWNS"])
        return graph

    @pytest.mark.parametrize(
        "query",
        [
            # property access in RETURN
            "MATCH (n:Person) RETURN n.`age` AS a",
            # property access inside an arithmetic expression
            "MATCH (n:Person) RETURN n.`age` + 1 AS a",
            # property access in WHERE
            "MATCH (n:Person) WHERE n.`age` > 1 RETURN n.age AS a",
            # inline property (map literal) key
            "MATCH (n:Person {`age`: 30}) RETURN n.age AS a",
            # escaped WITH alias, then referenced escaped
            "MATCH (n:Person) WITH n.age AS `my age` RETURN `my age` AS a",
            # escaped variable declared in MATCH and referenced in RETURN
            "MATCH (`my n`:Person) RETURN `my n`.age AS a",
            # escaped OR relationship types
            "MATCH (a:Person)-[r:`KNOWS`|`OWNS`]->(b) RETURN a.age AS x",
            # escaped type inside a VLP quantifier
            "MATCH (a:Person)-[:`KNOWS`*1..2]->(b) RETURN b.age AS x",
            # escaped alias referenced in ORDER BY
            "MATCH (n:Person) RETURN n.age AS `my age` ORDER BY `my age`",
            # UNWIND with escaped alias (needs a leading MATCH: standalone
            # UNWIND is an unrelated pre-existing planner limitation)
            "MATCH (n:Person) UNWIND [1, 2] AS `my x` RETURN `my x` AS v",
        ],
    )
    def test_escaped_identifier_transpiles(self, query: str) -> None:
        # None of these schemas/tables contain a literal backtick, so any
        # backtick in the output is a leaked delimiter.
        sql = self._graph().transpile(query)
        assert "`" not in sql, f"backtick leaked into SQL:\n{sql}"

    def test_doubled_backtick_is_literal_backtick(self) -> None:
        """`` `Per``son` `` names the type ``Per`son`` (escaped backtick)."""
        ast = OpenCypherParser().parse("MATCH (n:`Per``son`) RETURN n")
        assert "Per`son" in _collect_entity_names(ast)


class TestBacktickedTableNamesPreserved:
    """Unescaping Cypher identifiers must NOT touch SQL table names.

    Table names come from schema configuration (``SQLTableDescriptor``) and
    never pass through the Cypher parser, so a catalog that requires
    backtick-quoting in Databricks SQL (e.g. a hyphenated catalog name) must
    reach the rendered SQL verbatim.
    """

    @staticmethod
    def _graph() -> GraphContext:
        graph = GraphContext(
            nodes_table="`catalogo-algo`.schema.nodes",
            edges_table="`catalogo-algo`.schema.edges",
            node_type_col="node_type",
            node_id_col="node_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="rel_type",
            extra_node_attrs={"origin_id": str},
        )
        graph.set_types(node_types=["CNPJ_RAIZ"], edge_types=["OWNS"])
        return graph

    def test_simple_match_keeps_table_backticks(self) -> None:
        sql = self._graph().transpile(
            "MATCH (n:CNPJ_RAIZ) RETURN n.origin_id AS a"
        )
        assert "`catalogo-algo`.schema.nodes" in sql
        assert "catalogo-algo.schema" not in sql.replace(
            "`catalogo-algo`.schema", ""
        ), "table name lost its backticks somewhere"

    def test_vlp_keeps_table_backticks_everywhere(self) -> None:
        """The recursive CTE references the tables many times — all quoted."""
        sql = self._graph().transpile(
            "MATCH (root:CNPJ_RAIZ {origin_id: 'x'}) "
            "MATCH (root)-[*1..2]-(d) RETURN count(DISTINCT d)"
        )
        assert sql.count("`catalogo-algo`.schema") >= 4
        assert "catalogo-algo.schema" not in sql.replace(
            "`catalogo-algo`.schema", ""
        )

    def test_escaped_label_and_backticked_table_coexist(self) -> None:
        """Label backticks are stripped; table backticks are kept."""
        sql = self._graph().transpile(
            "MATCH (n:`CNPJ_RAIZ`) RETURN n.origin_id AS a"
        )
        assert "`catalogo-algo`.schema.nodes" in sql
        assert "`CNPJ_RAIZ`" not in sql
        assert "node_type = 'CNPJ_RAIZ'" in sql

    def test_hyphenated_node_type_requires_and_survives_escaping(self) -> None:
        """A type like ``cnpj-raiz`` is only expressible via backticks.

        After unescaping, the lookup matches the registered name and the
        rendered filter is a plain string literal — no SQL quoting needed.
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
        graph.set_types(node_types=["cnpj-raiz"], edge_types=["OWNS"])
        sql = graph.transpile("MATCH (n:`cnpj-raiz`) RETURN n.origin_id AS a")
        assert "node_type = 'cnpj-raiz'" in sql


class TestUnknownLabelErrorMessage:
    """A genuinely unknown label must fail with an actionable message."""

    def test_error_lists_available_node_types(self) -> None:
        """The error should name the registered types, not just the failure."""
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="origin_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="relationship_type",
        )
        graph.set_types(node_types=["socio"], edge_types=["OWNS"])

        with pytest.raises(Exception) as exc_info:
            graph.transpile("MATCH (root:cnpj_raiz) RETURN root")

        message = str(exc_info.value)
        assert "cnpj_raiz" in message
        assert "socio" in message, (
            f"error should list available node types, got: {message}"
        )

    def test_error_suggests_case_insensitive_match(self) -> None:
        """A case-only mismatch is the most common cause — say so explicitly."""
        graph = GraphContext(
            nodes_table="nodes",
            edges_table="edges",
            node_type_col="node_type",
            node_id_col="origin_id",
            edge_src_col="src",
            edge_dst_col="dst",
            edge_type_col="relationship_type",
        )
        graph.set_types(node_types=["CNPJ_RAIZ"], edge_types=["OWNS"])

        with pytest.raises(Exception) as exc_info:
            graph.transpile("MATCH (root:cnpj_raiz) RETURN root")

        message = str(exc_info.value)
        assert "CNPJ_RAIZ" in message, (
            f"error should surface the near-match, got: {message}"
        )
