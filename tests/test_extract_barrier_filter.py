"""Unit tests for extract_barrier_filter().

Tests extraction of is_terminator() directive from WHERE clause,
verifying that:
- The inner predicate is extracted (unwrapped from is_terminator)
- The barrier filter references only the target alias
- Remaining WHERE conjuncts are preserved
- Without is_terminator, barrier_filter is None
"""

from __future__ import annotations

import pytest

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionProperty,
    QueryExpressionValue,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.recursive_traversal import (
    _flatten_and_tree,
    extract_barrier_filter,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

AND_OP = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
EQ_OP = BinaryOperatorInfo(BinaryOperator.EQ, BinaryOperatorType.COMPARISON)
GT_OP = BinaryOperatorInfo(BinaryOperator.GT, BinaryOperatorType.COMPARISON)


def prop(var: str, name: str) -> QueryExpressionProperty:
    return QueryExpressionProperty(variable_name=var, property_name=name)


def lit_str(val: str) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=str)


def lit_bool(val: bool) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=bool)


def lit_int(val: int) -> QueryExpressionValue:
    return QueryExpressionValue(value=val, value_type=int)


def eq(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=EQ_OP, left_expression=left, right_expression=right)


def gt(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=GT_OP, left_expression=left, right_expression=right)


def and_(left: QueryExpression, right: QueryExpression) -> QueryExpressionBinary:
    return QueryExpressionBinary(operator=AND_OP, left_expression=left, right_expression=right)


def is_terminator(inner: QueryExpression) -> QueryExpressionFunction:
    return QueryExpressionFunction(
        function=Function.IS_TERMINATOR,
        parameters=[inner],
    )


def not_(inner: QueryExpression) -> QueryExpressionFunction:
    return QueryExpressionFunction(
        function=Function.NOT,
        parameters=[inner],
        data_type=bool,
    )


# Reusable leaf expressions
A_ID = eq(prop("a", "node_id"), lit_str("N1"))
B_HUB = eq(prop("b", "is_hub"), lit_bool(True))
B_SCORE = gt(prop("b", "score"), lit_int(50))


class TestExtractBarrierFilter:
    def test_no_terminator_returns_none(self):
        """Without is_terminator(), barrier_filter is None."""
        expr = and_(A_ID, B_HUB)
        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is None
        assert remaining is expr

    def test_none_input(self):
        """None -> (None, None)."""
        barrier, remaining = extract_barrier_filter(None, "b")
        assert barrier is None
        assert remaining is None

    def test_only_terminator(self):
        """is_terminator(b.is_hub = true) alone -> barrier extracted, remaining None."""
        inner = eq(prop("b", "is_hub"), lit_bool(True))
        expr = is_terminator(inner)
        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is inner
        assert remaining is None

    def test_extract_barrier_from_and(self):
        """a.id AND is_terminator(b.hub) AND b.score -> extracts barrier, keeps rest."""
        inner_pred = eq(prop("b", "is_hub"), lit_bool(True))
        terminator = is_terminator(inner_pred)
        expr = and_(and_(A_ID, terminator), B_SCORE)

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is inner_pred
        assert remaining is not None
        parts = _flatten_and_tree(remaining)
        assert len(parts) == 2

    def test_terminator_wrong_alias_left_in_where(self):
        """is_terminator(a.x = 1) referencing source alias should be left in WHERE."""
        inner = eq(prop("a", "x"), lit_int(1))
        terminator = is_terminator(inner)
        expr = and_(A_ID, terminator)

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is None
        # The is_terminator call remains in remaining
        assert remaining is not None

    def test_not_wrapped_terminator_alone(self):
        """NOT is_terminator(P) alone -> barrier is NOT(P), remaining None.

        Semantics: NOT is_terminator(P) == is_terminator(NOT P) — the barrier
        fires where P is false.
        """
        inner = eq(prop("b", "is_hub"), lit_bool(True))
        expr = not_(is_terminator(inner))

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is not None
        assert isinstance(barrier, QueryExpressionFunction)
        assert barrier.function == Function.NOT
        assert barrier.parameters == [inner]
        assert remaining is None

    def test_not_wrapped_terminator_in_and(self):
        """a.id AND NOT is_terminator(b.hub) -> barrier NOT(b.hub), a.id kept."""
        inner = eq(prop("b", "is_hub"), lit_bool(True))
        expr = and_(A_ID, not_(is_terminator(inner)))

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is not None
        assert isinstance(barrier, QueryExpressionFunction)
        assert barrier.function == Function.NOT
        assert barrier.parameters == [inner]
        assert remaining is A_ID

    def test_not_wrapped_terminator_wrong_alias_left_in_where(self):
        """NOT is_terminator(a.x = 1) with wrong alias stays in WHERE."""
        inner = eq(prop("a", "x"), lit_int(1))
        expr = and_(A_ID, not_(is_terminator(inner)))

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is None
        assert remaining is not None

    def test_double_not_is_not_unwrapped(self):
        """NOT NOT is_terminator(P) is not treated as a barrier directive."""
        inner = eq(prop("b", "is_hub"), lit_bool(True))
        expr = not_(not_(is_terminator(inner)))

        barrier, remaining = extract_barrier_filter(expr, "b")
        assert barrier is None
        assert remaining is expr

    def test_extract_barrier_with_parsed_cypher(self):
        """Integration: real parser produces correct extraction."""
        from gsql2rsql.parser.opencypher_parser import OpenCypherParser
        parser = OpenCypherParser()
        cypher = """
        MATCH path = (a:Station)-[:LINK*1..5]->(b:Station)
        WHERE a.node_id = 'N1' AND is_terminator(b.is_hub = true) AND b.score > 0.5
        RETURN DISTINCT b.node_id AS dst
        """
        ast = parser.parse(cypher)
        where = ast.parts[0].match_clauses[0].where_expression

        barrier, remaining = extract_barrier_filter(where, "b")
        assert barrier is not None, "barrier_filter must be extracted"
        assert "is_hub" in str(barrier)
        assert remaining is not None
        parts = _flatten_and_tree(remaining)
        assert len(parts) == 2  # a.node_id and b.score
