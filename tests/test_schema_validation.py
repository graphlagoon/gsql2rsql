"""Under-specified edge schemas should be rejected at construction time.

An ``EdgeSchema`` with no ``source_id_property`` / ``sink_id_property`` is
silently accepted.  The renderer then prunes the (non-existent) join-key
columns out of the edge subquery while still naming them in the ``ON``
clause, producing SQL that references a column that was never selected::

    INNER JOIN (
      SELECT 1 AS _dummy FROM Knows        -- join keys pruned away
    ) AS _right_1 ON
      _left_1._gsql2rsql_p_id = _right_1._gsql2rsql__anon1_source_id

No user of the public API can reach this state -- ``GraphContext`` always
populates both properties.  It is reachable only by hand-building an
``EdgeSchema``, which several ``transpile_tests`` fixtures do.

.. note::
   ``tests/transpile_tests/test_06_single_hop_relationship.py`` builds exactly
   this shape and passes because it asserts only ``"JOIN" in sql``.  This
   module does not modify that test; the conflict is reported in
   ``test-failures-report.md``.
"""

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

CYPHER = "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name"


def _provider_with_person() -> SimpleSQLSchemaProvider:
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[EntityProperty("id", int), EntityProperty("name", str)],
            node_id_property=EntityProperty("id", int),
        ),
        SQLTableDescriptor(table_name="dbo.Person", node_id_columns=["id"]),
    )
    return schema


def _add_knows(
    schema: SimpleSQLSchemaProvider, *, with_id_properties: bool
) -> None:
    kwargs = {}
    if with_id_properties:
        kwargs = {
            "source_id_property": EntityProperty("person1_id", int),
            "sink_id_property": EntityProperty("person2_id", int),
        }
    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            **kwargs,
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="dbo.Knows",
            node_id_columns=["person1_id", "person2_id"],
        ),
    )


def _render(schema: SimpleSQLSchemaProvider) -> str:
    ast = OpenCypherParser().parse(CYPHER)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=CYPHER)
    return SQLRenderer(db_schema_provider=schema).render_plan(plan)


class TestEdgeSchemaIdPropertyValidation:
    """Omitting the id properties must not produce un-runnable SQL.

    ``add_edge`` repairs the omission when it can: the descriptor's
    ``node_id_columns`` are ``[source, sink]`` by convention, so the missing
    id properties are derived from them. When even that is impossible
    (fewer than two columns), it rejects at construction time -- with the
    schema author present -- instead of failing at query time with an
    ``UNRESOLVED_COLUMN`` error from Databricks.
    """

    def test_missing_id_properties_derived_from_descriptor(self) -> None:
        """Both id properties are populated from ``node_id_columns``."""
        schema = _provider_with_person()
        _add_knows(schema, with_id_properties=False)

        edge = schema.get_edge_definition("KNOWS", "Person", "Person")
        assert edge is not None
        assert edge.source_id_property is not None
        assert edge.source_id_property.property_name == "person1_id"
        assert edge.sink_id_property is not None
        assert edge.sink_id_property.property_name == "person2_id"

    def test_underivable_id_properties_rejected_at_add_edge(self) -> None:
        """No id properties AND no usable descriptor columns must raise."""
        schema = _provider_with_person()
        with pytest.raises(Exception) as exc_info:
            schema.add_edge(
                EdgeSchema(
                    name="KNOWS",
                    source_node_id="Person",
                    sink_node_id="Person",
                ),
                SQLTableDescriptor(
                    entity_id="Person@KNOWS@Person",
                    table_name="dbo.Knows",
                    node_id_columns=[],
                ),
            )

        message = str(exc_info.value).lower()
        assert "id" in message or "propert" in message, (
            f"Rejection message should name the missing id properties: "
            f"{exc_info.value}"
        )

    def test_rendered_join_keys_exist_in_the_edge_subquery(self) -> None:
        """Every column named in an ``ON`` clause must be selected somewhere.

        This is the *observable* consequence of the missing validation, and it
        is asserted without pinning any alias-naming scheme: it simply checks
        that each identifier used as a join key also appears as an output
        column of a subquery.
        """
        schema = _provider_with_person()
        _add_knows(schema, with_id_properties=False)
        sql = _render(schema)
        print(f"\n=== SQL ===\n{sql}")

        import re

        on_columns = set()
        for left, right in re.findall(
            r"ON\s+([\w.]+)\s*=\s*([\w.]+)", sql, flags=re.IGNORECASE
        ):
            for ref in (left, right):
                on_columns.add(ref.split(".")[-1])

        missing = [
            col
            for col in on_columns
            # A join key must be produced by some SELECT item, i.e. appear
            # as "<expr> AS <col>" somewhere in the query.
            if not re.search(rf"AS\s+{re.escape(col)}\b", sql)
        ]
        assert not missing, (
            f"These join-key columns are referenced in an ON clause but never "
            f"produced by any subquery: {missing}. The generated SQL cannot "
            f"execute.\n{sql}"
        )

    def test_control_fully_specified_schema_renders_valid_join(self) -> None:
        """Control: with the id properties set, the same query is fine.

        Confirms the assertion above discriminates the defect rather than
        rejecting all generated SQL.
        """
        schema = _provider_with_person()
        _add_knows(schema, with_id_properties=True)
        sql = _render(schema)
        print(f"\n=== SQL ===\n{sql}")

        import re

        on_columns = set()
        for left, right in re.findall(
            r"ON\s+([\w.]+)\s*=\s*([\w.]+)", sql, flags=re.IGNORECASE
        ):
            for ref in (left, right):
                on_columns.add(ref.split(".")[-1])

        missing = [
            col
            for col in on_columns
            if not re.search(rf"AS\s+{re.escape(col)}\b", sql)
        ]
        assert not missing, f"Unexpectedly missing: {missing}\n{sql}"
