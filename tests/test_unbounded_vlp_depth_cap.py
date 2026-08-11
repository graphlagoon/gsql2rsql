"""Unbounded variable-length paths are capped at a default maximum depth.

``-[:KNOWS*]->`` and ``-[:KNOWS*2..]->`` have no upper bound in openCypher.
The transpiler applies a default ceiling of 10 hops instead, so a path of
length 11 is **silently omitted** from the result -- no warning, no error.

These tests deliberately assert **current behaviour**, not openCypher
semantics.  The cap is defensible engineering (Spark's recursive-CTE row and
iteration limits are real, and ``limitations.md`` recommends bounding depth
explicitly), so removing it is not obviously right.

What *is* wrong is that the cap is **undocumented**:
``docs_help_dev/limitations.md`` describes recursion limits and recommends
``max depth <= 10``, but nowhere states that an unbounded ``*`` is silently
rewritten to ``*1..10``.  A user writing ``*`` believes they asked for a full
traversal.

These tests exist so the cap is visible and cannot change unnoticed.  The
documentation gap is reported in ``test-failures-report.md``.
"""

import re

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)

#: The default ceiling the renderer applies to an unbounded upper bound.
DEFAULT_MAX_DEPTH = 10


def _schema() -> SimpleSQLSchemaProvider:
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[EntityProperty("id", int), EntityProperty("name", str)],
            node_id_property=EntityProperty("id", int),
        ),
        SQLTableDescriptor(table_name="g.Person", node_id_columns=["id"]),
    )
    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", int),
            sink_id_property=EntityProperty("dst", int),
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="g.Knows",
            node_id_columns=["src", "dst"],
        ),
    )
    return schema


def _render(cypher: str) -> str:
    schema = _schema()
    ast = OpenCypherParser().parse(cypher)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=cypher)
    return SQLRenderer(db_schema_provider=schema).render_plan(plan)


def _recursion_ceiling(sql: str) -> int | None:
    """Read the ``p.depth < N`` guard out of the recursive case."""
    match = re.search(r"depth\s*<\s*(\d+)", sql)
    return int(match.group(1)) if match else None


UNBOUNDED_FORMS = [
    ("bare_star", "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN b.name AS n"),
    (
        "open_upper_bound",
        "MATCH (a:Person)-[:KNOWS*2..]->(b:Person) RETURN b.name AS n",
    ),
    (
        "zero_min_open_upper",
        "MATCH (a:Person)-[:KNOWS*0..]->(b:Person) RETURN b.name AS n",
    ),
]


class TestUnboundedVLPDepthCap:
    """The cap is applied, and it is the value documented here."""

    @pytest.mark.parametrize(
        "label,cypher", UNBOUNDED_FORMS, ids=[n for n, _ in UNBOUNDED_FORMS]
    )
    def test_unbounded_upper_bound_gets_default_ceiling(
        self, label: str, cypher: str
    ) -> None:
        """An open upper bound becomes ``depth < 10`` in the recursive case.

        LOCKS IN CURRENT BEHAVIOUR.  openCypher's ``*`` is unbounded; this
        asserts the transpiler's deliberate ceiling so a change to it is a
        visible, reviewed decision rather than a silent one.
        """
        sql = _render(cypher)
        print(f"\n=== SQL ({label}) ===\n{sql}")

        ceiling = _recursion_ceiling(sql)
        assert ceiling == DEFAULT_MAX_DEPTH, (
            f"Expected the default ceiling {DEFAULT_MAX_DEPTH} for an "
            f"unbounded VLP, found {ceiling}.\n{sql}"
        )

    def test_explicit_bound_is_not_overridden(self) -> None:
        """Control: an explicit bound below the cap must be respected.

        Guards the tests above -- if the ceiling were applied unconditionally
        this would also read 10.
        """
        sql = _render(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.name AS n"
        )
        assert _recursion_ceiling(sql) == 3, sql

    def test_explicit_bound_above_cap_is_respected(self) -> None:
        """An explicit ``*1..15`` must not be silently clamped to 10.

        The user has stated a depth; honouring it is the whole point of
        making the bound explicit.
        """
        sql = _render(
            "MATCH (a:Person)-[:KNOWS*1..15]->(b:Person) RETURN b.name AS n"
        )
        print(f"\n=== SQL ===\n{sql}")
        assert _recursion_ceiling(sql) == 15, (
            f"An explicit upper bound of 15 was clamped to "
            f"{_recursion_ceiling(sql)}.\n{sql}"
        )
