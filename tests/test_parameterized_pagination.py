"""Parameters in ``SKIP`` / ``LIMIT`` must not be silently discarded.

``test_22_parameterized_in.py`` covers ``$param`` inside ``WHERE ... IN``.
Nothing covers it in a pagination clause, and there the parameter is dropped:
``RETURN p.name LIMIT $n`` produces SQL with **no ``LIMIT`` at all**, so the
query returns the entire result set.

A literal ``LIMIT 10`` works, which is what makes this dangerous -- pagination
appears to work until someone parameterises it.

Either outcome is acceptable:

* emit a parameter marker (``LIMIT :n``), or
* reject the query with a transpiler error.

Silently returning every row is not.
"""

import re

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.exceptions import TranspilerException
from gsql2rsql.common.schema import NodeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)


def _schema() -> SimpleSQLSchemaProvider:
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("id", int),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("id", int),
        ),
        SQLTableDescriptor(table_name="g.Person", node_id_columns=["id"]),
    )
    return schema


def _render(cypher: str) -> str:
    schema = _schema()
    ast = OpenCypherParser().parse(cypher)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=cypher)
    return SQLRenderer(db_schema_provider=schema).render_plan(plan)


class TestLiteralPaginationControl:
    """Control group: literal SKIP/LIMIT work and must keep working."""

    def test_literal_limit_renders(self) -> None:
        sql = _render("MATCH (p:Person) RETURN p.name AS name LIMIT 10")
        assert re.search(r"\bLIMIT\b", sql, re.IGNORECASE), sql

    def test_literal_skip_and_limit_render(self) -> None:
        sql = _render(
            "MATCH (p:Person) RETURN p.name AS name SKIP 5 LIMIT 10"
        )
        assert re.search(r"\bLIMIT\b", sql, re.IGNORECASE), sql
        assert re.search(r"\bOFFSET\b|\bSKIP\b", sql, re.IGNORECASE), sql


class TestParameterizedPagination:
    """``LIMIT $n`` / ``SKIP $s`` must bind or be rejected -- never dropped."""

    def test_parameterized_limit_is_not_dropped(self) -> None:
        """``LIMIT $n`` must not vanish from the generated SQL.

        Without it the query returns every row, which for a pagination query
        against a large table is both a wrong answer and a performance
        incident.
        """
        cypher = "MATCH (p:Person) RETURN p.name AS name LIMIT $n"
        try:
            sql = _render(cypher)
        except TranspilerException:
            return  # Acceptable: rejected clearly.

        print(f"\n=== SQL ===\n{sql}")
        assert re.search(r"\bLIMIT\b", sql, re.IGNORECASE), (
            f"LIMIT $n was silently discarded -- the generated SQL has no "
            f"LIMIT clause and returns every row:\n{sql}"
        )

    def test_parameterized_skip_and_limit_are_not_dropped(self) -> None:
        """Both pagination parameters must survive."""
        cypher = "MATCH (p:Person) RETURN p.name AS name SKIP $s LIMIT $n"
        try:
            sql = _render(cypher)
        except TranspilerException:
            return

        print(f"\n=== SQL ===\n{sql}")
        assert re.search(r"\bLIMIT\b", sql, re.IGNORECASE), (
            f"LIMIT $n was silently discarded:\n{sql}"
        )
        assert re.search(r"\bOFFSET\b|\bSKIP\b", sql, re.IGNORECASE), (
            f"SKIP $s was silently discarded:\n{sql}"
        )

    def test_parameterized_limit_with_order_by(self) -> None:
        """The top-N idiom: ``ORDER BY ... LIMIT $n``.

        Dropping the LIMIT here turns a top-N query into a full sort of the
        entire table.
        """
        cypher = (
            "MATCH (p:Person) RETURN p.name AS name "
            "ORDER BY p.age DESC LIMIT $n"
        )
        try:
            sql = _render(cypher)
        except TranspilerException:
            return

        print(f"\n=== SQL ===\n{sql}")
        assert re.search(r"\bORDER BY\b", sql, re.IGNORECASE), sql
        assert re.search(r"\bLIMIT\b", sql, re.IGNORECASE), (
            f"ORDER BY survived but LIMIT $n was dropped:\n{sql}"
        )
