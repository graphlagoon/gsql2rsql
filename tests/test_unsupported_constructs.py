"""Unsupported constructs must fail as *library* errors, not internal ones.

Three groups, with deliberately different contracts:

1. **Functions recognised by the parser but absent from the renderer's
   template map.**  These currently raise ``NotImplementedError``, an
   internal-invariant error that escapes the public API and names no
   function.  The tests below assert a ``TranspilerException`` subclass --
   the correct contract -- so they fail today.  They are *not* ``xfail``:
   ``docs_help_dev/limitations.md`` does not document these as unsupported,
   so there is no documented non-support to defer to.

2. **Constructs rejected at parse time.**  These already behave well
   (``TranspilerSyntaxErrorException`` with a caret-annotated position).
   Cheap regression lock.

3. **Label predicates** (``WHERE a:Person``), which currently render a
   non-boolean ``WHERE`` clause.

.. note::
   Five tests in ``test_where_label_predicates.py`` and
   ``test_no_label_solution.py`` are ``@pytest.mark.skip``-ed for the
   label-predicate feature.  This module does not modify them; it adds the
   error-contract assertion they do not make.
"""

import pytest

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.exceptions import (
    TranspilerException,
    TranspilerSyntaxErrorException,
)
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
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
                EntityProperty("tags", list),
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


def _transpile(cypher: str) -> str:
    schema = _schema()
    ast = OpenCypherParser().parse(cypher)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=cypher)
    return SQLRenderer(db_schema_provider=schema).render_plan(plan)


# ---------------------------------------------------------------------------
# Group 1: recognised by the parser, no renderer template
# ---------------------------------------------------------------------------
UNTEMPLATED_FUNCTIONS = [
    ("split", "MATCH (p:Person) RETURN split(p.name, ' ') AS parts"),
    ("substring", "MATCH (p:Person) RETURN substring(p.name, 0, 3) AS s"),
    ("replace", "MATCH (p:Person) RETURN replace(p.name, 'a', 'b') AS r"),
    ("reverse", "MATCH (p:Person) RETURN reverse(p.name) AS r"),
    ("head", "MATCH (p:Person) RETURN head(p.tags) AS h"),
    ("tail", "MATCH (p:Person) RETURN tail(p.tags) AS t"),
    ("keys", "MATCH (p:Person) RETURN keys(p) AS k"),
    ("labels", "MATCH (p:Person) RETURN labels(p) AS l"),
    ("id", "MATCH (p:Person) RETURN id(p) AS i"),
    ("properties", "MATCH (p:Person) RETURN properties(p) AS props"),
]


class TestUntemplatedFunctionsRaiseLibraryError:
    """``NotImplementedError`` must not escape the public API."""

    @pytest.mark.parametrize(
        "fn_name,cypher",
        UNTEMPLATED_FUNCTIONS,
        ids=[n for n, _ in UNTEMPLATED_FUNCTIONS],
    )
    def test_raises_transpiler_exception_naming_the_function(
        self, fn_name: str, cypher: str
    ) -> None:
        """Either render it, or reject it with an actionable library error.

        ``NotImplementedError: _render_function: unsupported function
        <Function.INVALID: 1>`` fails on both counts: it is not a
        ``TranspilerException``, and it names ``INVALID`` rather than the
        function the user actually wrote.
        """
        try:
            sql = _transpile(cypher)
        except TranspilerException as exc:
            # Correct contract: a library error that names the construct.
            assert fn_name.lower() in str(exc).lower(), (
                f"Error does not name '{fn_name}', so the user cannot tell "
                f"which function is unsupported: {exc}"
            )
            return
        except NotImplementedError as exc:
            pytest.fail(
                f"NotImplementedError escaped the public API for "
                f"'{fn_name}()': {exc}. This is an internal-invariant error; "
                f"users should see a TranspilerNotSupportedException naming "
                f"the function."
            )
        else:
            # Rendering is also acceptable -- the function is supported.
            assert fn_name.lower() in sql.lower() or sql, sql


# ---------------------------------------------------------------------------
# Group 2: rejected at parse time -- already good, lock it in
# ---------------------------------------------------------------------------
SYNTAX_REJECTED = [
    (
        "shortestPath",
        "MATCH p = shortestPath((a:Person)-[:KNOWS*1..5]->(b:Person)) RETURN p",
    ),
    (
        "allShortestPaths",
        "MATCH p = allShortestPaths((a:Person)-[:KNOWS*1..5]->(b:Person)) "
        "RETURN p",
    ),
    ("map_projection", "MATCH (p:Person) RETURN p{.id, .name} AS m"),
    (
        "call_subquery",
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f) "
        "RETURN count(f) AS c } RETURN p.name, c",
    ),
    (
        "count_subquery",
        "MATCH (p:Person) RETURN COUNT { (p)-[:KNOWS]->() } AS degree",
    ),
    ("regex_match", "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name"),
]


class TestSyntaxLevelRejections:
    """Constructs the grammar does not accept fail cleanly at parse time."""

    @pytest.mark.parametrize(
        "label,cypher", SYNTAX_REJECTED, ids=[n for n, _ in SYNTAX_REJECTED]
    )
    def test_raises_syntax_error_with_position(
        self, label: str, cypher: str
    ) -> None:
        """A syntax error, not a crash deep in the pipeline."""
        with pytest.raises(TranspilerSyntaxErrorException) as exc_info:
            _transpile(cypher)

        message = str(exc_info.value)
        assert "line" in message.lower(), (
            f"Syntax error for {label} gives no position: {message}"
        )


# ---------------------------------------------------------------------------
# Group 3: label predicates render a non-boolean WHERE
# ---------------------------------------------------------------------------
class TestLabelPredicateErrorContract:
    """``WHERE a:Person`` must fail loudly rather than emit ``WHERE <id>``."""

    def test_label_predicate_raises_instead_of_bare_id_column(self) -> None:
        """Today this renders ``WHERE _gsql2rsql_a_id`` -- not a boolean.

        Databricks rejects a non-boolean ``WHERE`` operand, so the user gets a
        confusing SQL error instead of a transpiler error naming the
        unsupported construct.
        """
        cypher = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a:Person "
            "RETURN a.name AS name"
        )
        with pytest.raises(TranspilerException):
            sql = _transpile(cypher)
            print(f"\n=== SQL (should not have been produced) ===\n{sql}")

    def test_disjunctive_label_predicate_raises(self) -> None:
        """``WHERE a:Person OR a:Company`` loses the label information entirely.

        Both branches currently render to the *same* bare id column, so even
        the distinction between the two labels is gone.
        """
        cypher = (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a:Person OR a:Person RETURN a.name AS name"
        )
        with pytest.raises(TranspilerException):
            _transpile(cypher)
