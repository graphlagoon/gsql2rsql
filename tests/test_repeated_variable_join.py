"""Repeated pattern variables must constrain BOTH join keys.

When a variable appears at both endpoints of a pattern, openCypher requires
the edge's source *and* sink to bind to the same node::

    MATCH (a:Person)-[:KNOWS]->(a)     -- self-loops only
    MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)  -- closed triangle

Structural counterpart to
``tests/pyspark_tests/test_repeated_variable_patterns_pyspark.py``, which
asserts the resulting rows.  These tests inspect the *logical plan*, so they
localise the defect to a phase without needing a SparkSession -- and they run
in CI, which the PySpark suites do not.

The existing self-loop suites
(``pyspark_tests/test_self_loop_dedup_pyspark.py``,
``test_undirected_optimization.py::test_self_loops_deduplicated``) all use two
*distinct* variables ``(a)-[:KNOWS]-(b)``.  The repeated-variable form below is
a different pattern and is not covered by them.
"""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import (
    NodeSchema,
    EdgeSchema,
    EntityProperty,
)
from gsql2rsql.planner.operators import JoinOperator, LogicalOperator
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


def _plan(cypher: str) -> LogicalPlan:
    schema = _schema()
    ast = OpenCypherParser().parse(cypher)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=cypher)
    return plan


def _render(cypher: str) -> str:
    schema = _schema()
    ast = OpenCypherParser().parse(cypher)
    plan = LogicalPlan.process_query_tree(ast, schema)
    plan.resolve(original_query=cypher)
    return SQLRenderer(db_schema_provider=schema).render_plan(plan)


def _join_pairs_for(plan: LogicalPlan, variable: str) -> list[str]:
    """Collect the join-pair *types* that bind ``variable`` to an edge."""
    types: list[str] = []
    seen: set[int] = set()
    for start_op in plan.starting_operators:
        for op in start_op.get_all_downstream_operators(LogicalOperator):
            # A join is reachable from both of its inputs; count it once.
            if not isinstance(op, JoinOperator) or id(op) in seen:
                continue
            seen.add(id(op))
            for pair in op.join_pairs:
                if pair.node_alias == variable:
                    types.append(pair.pair_type.name)
    return types


class TestSelfLoopRepeatedVariable:
    """``(a)-[:KNOWS]->(a)`` must pin both edge endpoints to ``a``."""

    CYPHER = "MATCH (a:Person)-[:KNOWS]->(a) RETURN a.name AS name"

    def test_parser_keeps_both_endpoints(self) -> None:
        """The AST must carry both endpoints -- proves this is not a parser bug."""
        ast = OpenCypherParser().parse(self.CYPHER)
        rendered = str(ast)
        # Both the source and the sink node reference the same variable "a".
        assert rendered.count("a") >= 2, rendered

    def test_plan_binds_variable_as_both_source_and_sink(self) -> None:
        """The join must carry a SOURCE *and* a SINK pair for ``a``.

        With only the SOURCE pair the SQL constrains ``a.id = edge.src`` and
        leaves ``edge.dst`` free, so every node with any outgoing edge matches
        -- not just the self-loops openCypher asks for.
        """
        plan = _plan(self.CYPHER)
        pair_types = _join_pairs_for(plan, "a")

        assert len(pair_types) == 2, (
            f"Expected 'a' to be joined to the edge twice (as SOURCE and as "
            f"SINK); got {len(pair_types)} pair(s): {pair_types}. "
            f"A single pair leaves the other edge endpoint unconstrained.\n"
            f"Plan:\n{plan.dump_graph()}"
        )
        assert any("SOURCE" in t for t in pair_types), pair_types
        assert any("SINK" in t for t in pair_types), pair_types

    def test_sql_constrains_both_edge_columns(self) -> None:
        """Both ``src`` and ``dst`` of the edge must appear in a join condition."""
        sql = _render(self.CYPHER)
        print(f"\n=== SQL ===\n{sql}")

        # The projected id column for `a` must be equated against BOTH edge
        # endpoint columns somewhere in the query.
        assert "_anon1_src" in sql, sql
        assert "_anon1_dst" in sql, (
            "The edge's sink column never appears in the SQL, so the "
            f"self-loop condition cannot be expressed.\n{sql}"
        )


class TestClosedTriangleRepeatedVariable:
    """``(a)->(b)->(c)->(a)`` must close the cycle back onto ``a``."""

    CYPHER = (
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)"
        "-[:KNOWS]->(a) RETURN a.name AS name"
    )

    def test_final_hop_binds_back_to_first_variable(self) -> None:
        """The third edge's sink must be joined to ``a``.

        Without it the query returns every 3-hop path rather than every
        triangle -- a silent wrong answer, since 3-paths vastly outnumber
        triangles in any real graph.
        """
        plan = _plan(self.CYPHER)
        pair_types = _join_pairs_for(plan, "a")

        assert len(pair_types) == 2, (
            f"Expected 'a' to be bound twice (SOURCE of the first edge, SINK "
            f"of the third); got {len(pair_types)}: {pair_types}. The cycle "
            f"never closes.\nPlan:\n{plan.dump_graph()}"
        )
        assert any("SINK" in t for t in pair_types), (
            f"'a' is never bound as the SINK of the closing edge: {pair_types}"
        )
