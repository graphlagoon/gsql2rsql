"""Output column names are a user-facing contract.

Two defects covered here:

* ``RETURN a.name, b.name`` emits two columns both named ``name``.  In Cypher
  they are ``a.name`` and ``b.name``.  Anything selecting by name downstream
  (a ``WITH``, ``df.select("name")``, a BI tool) gets the wrong column or an
  ambiguity error.
* ``RETURN *`` exposes the transpiler's internal ``_gsql2rsql_*`` names.

Both are asserted on the *set of output column names*, never on the specific
naming scheme, so a legitimate rename of the internal prefix does not break
these tests -- only losing the contract does.

``test_unaliased_projection_alias.py`` covers unaliased *aggregations* getting
a name at all; it does not cover collisions between two variables' same-named
properties, nor ``RETURN *``.
"""

import re

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

INTERNAL_PREFIX = "_gsql2rsql_"


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


def _outermost_select_aliases(sql: str) -> list[str]:
    """Extract the ``AS <name>`` aliases of the outermost SELECT list.

    The outermost SELECT is everything before the first top-level ``FROM``
    (nesting depth 0).  Only the alias names are returned, so the expressions
    feeding them may change freely.
    """
    depth = 0
    end = len(sql)
    for i, ch in enumerate(sql):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and sql[i : i + 4].upper() == "FROM":
            # Guard against matching "FROM" inside an identifier.
            before = sql[i - 1] if i else " "
            if not before.isalnum() and before != "_":
                end = i
                break
    head = sql[:end]
    return re.findall(r"\bAS\s+([A-Za-z_`][\w`.]*)", head)


class TestAliasCollisions:
    """Two variables' same-named properties must not collide."""

    def test_two_variables_same_property_get_distinct_names(self) -> None:
        """``RETURN a.name, b.name`` must produce two distinguishable columns.

        openCypher names them ``a.name`` and ``b.name``.  Any scheme that keeps
        them distinct (``a_name``/``b_name``) is acceptable; two columns both
        called ``name`` is not.
        """
        cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
        sql = _render(cypher)
        print(f"\n=== SQL ===\n{sql}")

        aliases = _outermost_select_aliases(sql)
        assert len(aliases) == 2, f"Expected 2 output columns, got {aliases}"
        assert len(set(aliases)) == 2, (
            f"Both output columns are named the same thing: {aliases}. "
            f"In Cypher these are 'a.name' and 'b.name'; anything selecting "
            f"by name downstream cannot tell them apart.\n{sql}"
        )

    def test_three_way_collision(self) -> None:
        """Three variables projecting ``age`` must yield three distinct names."""
        cypher = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.age, b.age, c.age"
        )
        sql = _render(cypher)
        print(f"\n=== SQL ===\n{sql}")

        aliases = _outermost_select_aliases(sql)
        assert len(set(aliases)) == 3, (
            f"Expected 3 distinct output names, got {aliases}\n{sql}"
        )

    def test_explicit_aliases_are_respected(self) -> None:
        """Control: explicit ``AS`` must survive untouched.

        Guards the two tests above -- if this one broke too, the problem would
        be alias handling in general rather than collision resolution.
        """
        cypher = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS a_name, b.name AS b_name"
        )
        sql = _render(cypher)
        aliases = _outermost_select_aliases(sql)
        assert set(aliases) == {"a_name", "b_name"}, aliases


class TestReturnStarNaming:
    """``RETURN *`` must not leak internal column names."""

    def test_return_star_does_not_leak_internal_prefix(self) -> None:
        """The user asked for ``*``; they must not get ``_gsql2rsql_p_name``."""
        cypher = "MATCH (p:Person) RETURN *"
        sql = _render(cypher)
        print(f"\n=== SQL ===\n{sql}")

        aliases = _outermost_select_aliases(sql)
        leaked = [a for a in aliases if a.startswith(INTERNAL_PREFIX)]
        assert not leaked, (
            f"RETURN * exposes internal column names to the user: {leaked}. "
            f"Expected user-facing names (e.g. 'id', 'name', 'age' or "
            f"'p.id', 'p.name', 'p.age').\n{sql}"
        )

    def test_return_star_after_join_does_not_leak(self) -> None:
        """Same contract once a join has introduced a second variable."""
        cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN *"
        sql = _render(cypher)
        print(f"\n=== SQL ===\n{sql}")

        aliases = _outermost_select_aliases(sql)
        leaked = [a for a in aliases if a.startswith(INTERNAL_PREFIX)]
        assert not leaked, (
            f"RETURN * after a join exposes internal names: {leaked}\n{sql}"
        )

    def test_with_star_then_return_does_not_leak(self) -> None:
        """``WITH *`` passthrough must not change the final naming contract."""
        cypher = "MATCH (p:Person) WITH * RETURN p.name AS name"
        sql = _render(cypher)
        print(f"\n=== SQL ===\n{sql}")

        aliases = _outermost_select_aliases(sql)
        assert aliases == ["name"], aliases
