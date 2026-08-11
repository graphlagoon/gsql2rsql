"""Aggregate functions must not silently degrade to their bare operand.

``percentileCont`` and ``percentileDisc`` are declared in
``AggregationFunction`` (the parser recognises the names) but have no entry in
``AGGREGATION_TEMPLATES``.  The renderer falls through to emitting the operand,
so::

    MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS median

becomes ``SELECT _gsql2rsql_p_age AS median`` -- one row **per person**
instead of one aggregate value, with no grouping and no warning.

An *unknown* function (``totallyMadeUpFn(x)``) raises correctly, so this is
specifically the "declared in the enum, absent from the template map" path:
recognised, then discarded.

The parser also drops the percentile argument: the AST for the query above
prints as ``PERCENTILE_CONT(p.age)``, with ``0.5`` gone.
"""

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


PERCENTILE_FUNCTIONS = [
    ("percentileCont", "PERCENTILE_CONT"),
    ("percentileDisc", "PERCENTILE_DISC"),
]


class TestPercentileAggregates:
    """Either aggregate properly, or reject -- never pass the operand through."""

    @pytest.mark.parametrize(
        "cypher_fn,sql_fn",
        PERCENTILE_FUNCTIONS,
        ids=[n for n, _ in PERCENTILE_FUNCTIONS],
    )
    def test_percentile_is_not_silently_dropped(
        self, cypher_fn: str, sql_fn: str
    ) -> None:
        """The call must survive into the SQL, or the query must be rejected.

        A projection that is neither an aggregate nor a grouping key means the
        query returns one row per input row -- a silent wrong answer, and the
        worst possible failure mode.
        """
        cypher = f"MATCH (p:Person) RETURN {cypher_fn}(p.age, 0.5) AS median"
        try:
            sql = _render(cypher)
        except TranspilerException:
            # Acceptable: a clear rejection of an unimplemented aggregate.
            return

        print(f"\n=== SQL ({cypher_fn}) ===\n{sql}")
        assert sql_fn in sql.upper(), (
            f"{cypher_fn}() vanished from the generated SQL, which now "
            f"returns one row per Person instead of a single aggregate "
            f"value:\n{sql}"
        )

    @pytest.mark.parametrize(
        "cypher_fn,sql_fn",
        PERCENTILE_FUNCTIONS,
        ids=[n for n, _ in PERCENTILE_FUNCTIONS],
    )
    def test_percentile_argument_survives_parsing(
        self, cypher_fn: str, sql_fn: str
    ) -> None:
        """The percentile itself (``0.5``) must reach the AST.

        Localises the defect: even a correct renderer template could not
        produce the right SQL if the parser has already discarded the
        argument.
        """
        cypher = f"MATCH (p:Person) RETURN {cypher_fn}(p.age, 0.5) AS median"
        ast = OpenCypherParser().parse(cypher)
        rendered = str(ast)
        print(f"\n=== AST ({cypher_fn}) ===\n{rendered}")

        assert "0.5" in rendered, (
            f"The percentile argument 0.5 was dropped during parsing; the "
            f"AST is: {rendered}"
        )

    def test_unknown_aggregate_still_raises(self) -> None:
        """Control: a name the parser does not know must be rejected.

        Confirms the pass-through above is specific to enum-declared,
        template-less aggregates -- not a general failure to validate.
        """
        cypher = "MATCH (p:Person) RETURN totallyMadeUpFn(p.age) AS x"
        with pytest.raises(Exception):
            sql = _render(cypher)
            print(f"\n=== SQL (should not exist) ===\n{sql}")


class TestSupportedAggregatesStillRender:
    """Control group: the templated aggregates must keep working."""

    @pytest.mark.parametrize(
        "cypher_fn,sql_fn",
        [
            ("stDevP", "STDDEV_POP"),
            ("stDev", "STDDEV"),
            ("first", "FIRST"),
            ("last", "LAST"),
            ("min", "MIN"),
            ("max", "MAX"),
        ],
    )
    def test_templated_aggregate_renders(
        self, cypher_fn: str, sql_fn: str
    ) -> None:
        """These have ``AGGREGATION_TEMPLATES`` entries and must appear."""
        cypher = f"MATCH (p:Person) RETURN {cypher_fn}(p.age) AS v"
        sql = _render(cypher)
        assert sql_fn in sql.upper(), f"{cypher_fn} did not render:\n{sql}"
