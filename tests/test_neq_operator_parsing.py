"""Parser tests for the inequality operator.

Cypher's inequality operator is `<>` (openCypher standard). `!=` is NOT
part of the grammar — users coming from SQL must rewrite `!=` as `<>`.
These tests lock in both behaviors: `<>` parses to BinaryOperator.NEQ
(and renders to SQL `!=` downstream), while `!=` fails with a syntax
error at the lexer.
"""

from __future__ import annotations

import pytest

from gsql2rsql.common.exceptions import TranspilerSyntaxErrorException
from gsql2rsql.parser.ast import QueryExpressionBinary
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.parser.operators import BinaryOperator


@pytest.fixture
def parser():
    return OpenCypherParser()


def _where_expr(parser: OpenCypherParser, cypher: str):
    ast = parser.parse(cypher)
    return ast.parts[0].match_clauses[0].where_expression


def _find_neq(expr):
    """Recursively find a NEQ binary expression in the tree."""
    if (
        isinstance(expr, QueryExpressionBinary)
        and expr.operator is not None
        and expr.operator.name == BinaryOperator.NEQ
    ):
        return expr
    if hasattr(expr, "children"):
        for child in expr.children:
            found = _find_neq(child)
            if found is not None:
                return found
    return None


class TestNeqOperatorParsing:
    def test_angle_brackets_neq(self, parser):
        """a.x <> 1 parses to BinaryOperator.NEQ."""
        where = _where_expr(parser, "MATCH (a) WHERE a.x <> 1 RETURN a")
        assert _find_neq(where) is not None

    def test_angle_brackets_with_coalesce(self, parser):
        """coalesce(a.x, 'Y') <> 'Y' — the user-reported shape with <>."""
        where = _where_expr(
            parser,
            "MATCH (a) WHERE coalesce(a.x, 'Y') <> 'Y' RETURN a",
        )
        assert _find_neq(where) is not None

    def test_angle_brackets_in_is_terminator(self, parser):
        """<> inside is_terminator() predicate parses (user case 1 rewritten)."""
        cypher = """
        MATCH p = (root)-[*1..3]-(d)
        WHERE is_terminator(
            (d.node_type = 'PF' AND d.degree > 15) OR
            (d.node_type = 'PJ' AND d.degree > 30) OR
            coalesce(d.something, 'ATIVA') <> 'ATIVA'
        )
        RETURN d
        """
        where = _where_expr(parser, cypher)
        assert _find_neq(where) is not None

    def test_bang_equals_is_a_syntax_error(self, parser):
        """`!=` is not valid Cypher — must fail with a syntax error.

        The lexer has no `!` token; users must use `<>` instead.
        """
        with pytest.raises(TranspilerSyntaxErrorException, match="'!'"):
            parser.parse("MATCH (a) WHERE a.x != 1 RETURN a")
