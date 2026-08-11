"""Tests for RETURN items that carry no explicit AS alias.

openCypher lets any expression be returned without an alias::

    RETURN count(DISTINCT d)
    RETURN count(*)
    RETURN n.age + 1

The column is then named after the expression text.  Emitting an empty
alias produces ``SELECT COUNT(DISTINCT x) AS  FROM ...`` — a hard SQL
syntax error ("Invalid expression / Unexpected token") that only surfaces
at execution time, well after transpilation appears to succeed.

Property access (``RETURN n.name``) already derived an alias; the gap was
every *other* unaliased expression, aggregations above all.
"""

import re

import pytest

from gsql2rsql.graph_context import GraphContext


def _graph() -> GraphContext:
    graph = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        node_type_col="node_type",
        node_id_col="node_id",
        edge_src_col="source_node_id",
        edge_dst_col="target_node_id",
        edge_type_col="relationship_type",
        extra_node_attrs={"origin_id": str, "age": int},
    )
    graph.set_types(
        node_types=["CNPJ_RAIZ", "socio"],
        edge_types=["OWNS"],
    )
    return graph


#: ``AS`` followed by end-of-line, a comma, or a closing/FROM keyword.
_EMPTY_ALIAS = re.compile(r"\bAS\s*(?=$|\n|,|\)|FROM\b)", re.MULTILINE)


def assert_no_empty_alias(sql: str) -> None:
    """Fail if the SQL contains a dangling ``AS`` with no alias name."""
    match = _EMPTY_ALIAS.search(sql)
    assert match is None, (
        "SQL contains an empty alias (`AS` with no name) at offset "
        f"{match.start() if match else -1}:\n"
        f"...{sql[max(0, (match.start() if match else 0) - 120):][:240]}..."
    )


class TestUnaliasedAggregations:
    """Aggregations without AS must still get a column name."""

    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:CNPJ_RAIZ) RETURN count(DISTINCT n)",
            "MATCH (n:CNPJ_RAIZ) RETURN count(n)",
            "MATCH (n:CNPJ_RAIZ) RETURN count(*)",
            "MATCH (n:CNPJ_RAIZ) RETURN max(n.age)",
            "MATCH (n:CNPJ_RAIZ) RETURN count(DISTINCT n.origin_id)",
        ],
    )
    def test_aggregation_without_alias(self, query: str) -> None:
        """Each aggregation form must render a non-empty alias."""
        assert_no_empty_alias(_graph().transpile(query))

    def test_user_reported_vlp_count(self) -> None:
        """Regression: the exact reported query.

        ``MATCH ... MATCH (root)-[*1..2]-(d) RETURN count(DISTINCT d)``
        transpiled to ``COUNT(DISTINCT ...) AS  FROM`` and failed at
        execution with a syntax error.
        """
        query = """
        MATCH (root:CNPJ_RAIZ {origin_id: 'xx'})
        MATCH (root)-[*1..2]-(d)
        RETURN count(DISTINCT d)
        """
        assert_no_empty_alias(_graph().transpile(query))


class TestUnaliasedExpressions:
    """Non-aggregate expressions without AS must also get a name."""

    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:CNPJ_RAIZ) RETURN n.age + 1",
            "MATCH (n:CNPJ_RAIZ) RETURN 42",
            "MATCH (n:CNPJ_RAIZ) RETURN toUpper(n.origin_id)",
        ],
    )
    def test_expression_without_alias(self, query: str) -> None:
        assert_no_empty_alias(_graph().transpile(query))


class TestAliasedProjectionsUnaffected:
    """The explicit-alias and property paths must not regress."""

    def test_explicit_alias_preserved(self) -> None:
        sql = _graph().transpile(
            "MATCH (n:CNPJ_RAIZ) RETURN count(DISTINCT n) AS total"
        )
        assert_no_empty_alias(sql)
        assert "AS total" in sql

    def test_property_alias_still_derived(self) -> None:
        """RETURN n.origin_id keeps naming the column after the property."""
        sql = _graph().transpile("MATCH (n:CNPJ_RAIZ) RETURN n.origin_id")
        assert_no_empty_alias(sql)
        assert "origin_id" in sql
