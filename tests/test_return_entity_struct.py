"""TDD Tests: RETURN entity should generate NAMED_STRUCT.

When returning an entire entity (node or edge) in Cypher, the SQL output
should be a STRUCT containing all properties, not just the entity ID.

Expected behavior:
- RETURN a (node) -> NAMED_STRUCT('node_id', ..., 'name', ..., ...)
- RETURN r (edge) -> NAMED_STRUCT('src', ..., 'dst', ..., ...)
- collect(a) -> COLLECT_LIST(NAMED_STRUCT(...))

Run with: uv run pytest tests/test_return_entity_struct.py -v
"""

import pytest

from gsql2rsql import GraphContext


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def graph_context():
    """GraphContext with nodes and edges for testing."""
    g = GraphContext(
        nodes_table="catalog.graph.nodes",
        edges_table="catalog.graph.edges",
        extra_node_attrs={
            "name": str,
            "age": int,
            "active": bool,
        },
        extra_edge_attrs={
            "since": int,
            "weight": float,
        },
    )
    g.set_types(
        node_types=["Person", "Company"],
        edge_types=["KNOWS", "WORKS_AT"],
    )
    return g


@pytest.fixture
def graph_context_minimal():
    """Minimal GraphContext for simple tests."""
    g = GraphContext(
        nodes_table="nodes",
        edges_table="edges",
        extra_node_attrs={"name": str},
        extra_edge_attrs={"weight": float},
    )
    g.set_types(
        node_types=["Person"],
        edge_types=["KNOWS"],
    )
    return g


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def has_named_struct(sql: str, alias: str) -> bool:
    """Check if SQL has NAMED_STRUCT for given alias.

    Looks for pattern: NAMED_STRUCT(...) AS alias
    """
    # Simple check: NAMED_STRUCT should appear before AS alias
    import re
    pattern = rf"NAMED_STRUCT\s*\([^)]+\)\s+AS\s+{re.escape(alias)}"
    return bool(re.search(pattern, sql, re.IGNORECASE | re.DOTALL))


def struct_contains_field(sql: str, field_name: str) -> bool:
    """Check if any NAMED_STRUCT in SQL contains the given field name."""
    # Look for 'field_name' inside NAMED_STRUCT
    return f"'{field_name}'" in sql and "NAMED_STRUCT" in sql


def no_extra_columns(sql: str, expected_aliases: list[str]) -> bool:
    """Check that the final SELECT only has expected aliases (no extras).

    This is a simplified check - looks at first SELECT line.
    """
    import re
    # Find the first SELECT clause
    match = re.search(r"SELECT\s+(DISTINCT\s+)?(.*?)FROM", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return False

    select_clause = match.group(2)

    # Count AS clauses - should match expected count
    as_count = len(re.findall(r"\bAS\s+\w+", select_clause, re.IGNORECASE))
    return as_count == len(expected_aliases)


# =============================================================================
# TEST CLASS: RETURN node entity -> NAMED_STRUCT
# =============================================================================


class TestReturnNodeEntity:
    """Test that RETURN node generates NAMED_STRUCT."""

    def test_return_single_node_generates_struct(self, graph_context):
        """RETURN a should generate NAMED_STRUCT with all node properties.

        Query: MATCH (a:Person) RETURN a
        Expected: SELECT NAMED_STRUCT('node_id', ..., 'name', ..., 'age', ..., 'active', ...) AS a
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)
            RETURN a
        """)

        print(f"\n=== SQL ===\n{sql}")

        # Should have NAMED_STRUCT for 'a'
        assert has_named_struct(sql, "a"), (
            "RETURN a should generate NAMED_STRUCT(...) AS a"
        )

        # STRUCT should contain node properties
        assert struct_contains_field(sql, "node_id"), "STRUCT should contain 'node_id'"
        assert struct_contains_field(sql, "name"), "STRUCT should contain 'name'"
        assert struct_contains_field(sql, "age"), "STRUCT should contain 'age'"
        assert struct_contains_field(sql, "active"), "STRUCT should contain 'active'"

    def test_return_node_with_alias_generates_struct(self, graph_context):
        """RETURN a AS person should generate NAMED_STRUCT with alias.

        Query: MATCH (a:Person) RETURN a AS person
        Expected: SELECT NAMED_STRUCT(...) AS person
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)
            RETURN a AS person
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "person"), (
            "RETURN a AS person should generate NAMED_STRUCT(...) AS person"
        )

    def test_return_multiple_nodes_generates_structs(self, graph_context):
        """RETURN a, b should generate NAMED_STRUCT for each.

        Query: MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            RETURN a, b
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "a"), "Should have NAMED_STRUCT for a"
        assert has_named_struct(sql, "b"), "Should have NAMED_STRUCT for b"

    def test_return_node_plus_property_no_duplicate(self, graph_context):
        """RETURN a, a.name should have STRUCT for a and separate column for name.

        Query: MATCH (a:Person) RETURN a, a.name AS name
        Expected:
          - NAMED_STRUCT(...) AS a
          - _gsql2rsql_a_name AS name
          - NO extra columns
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)
            RETURN a, a.name AS name
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "a"), "Should have NAMED_STRUCT for a"
        assert "AS name" in sql, "Should have explicit name column"

        # Should NOT have extra unpacked columns
        assert "_gsql2rsql_a_age AS _gsql2rsql_a_age" not in sql, (
            "Should not have extra unpacked columns"
        )


# =============================================================================
# TEST CLASS: RETURN edge entity -> NAMED_STRUCT
# =============================================================================


class TestReturnEdgeEntity:
    """Test that RETURN edge generates NAMED_STRUCT."""

    def test_return_single_edge_generates_struct(self, graph_context):
        """RETURN r should generate NAMED_STRUCT with all edge properties.

        Query: MATCH ()-[r:KNOWS]->() RETURN r
        Expected: SELECT NAMED_STRUCT('src', ..., 'dst', ..., 'since', ..., 'weight', ...) AS r
        """
        sql = graph_context.transpile("""
            MATCH ()-[r:KNOWS]->()
            RETURN r
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "r"), (
            "RETURN r should generate NAMED_STRUCT(...) AS r"
        )

        # STRUCT should contain edge properties
        assert struct_contains_field(sql, "src"), "STRUCT should contain 'src'"
        assert struct_contains_field(sql, "dst"), "STRUCT should contain 'dst'"
        assert struct_contains_field(sql, "since"), "STRUCT should contain 'since'"
        assert struct_contains_field(sql, "weight"), "STRUCT should contain 'weight'"

    def test_return_untyped_edge_generates_struct(self, graph_context):
        """RETURN r (untyped edge) should generate NAMED_STRUCT.

        Query: MATCH ()-[r]->() RETURN r
        """
        sql = graph_context.transpile("""
            MATCH ()-[r]->()
            RETURN r
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "r"), (
            "RETURN r (untyped) should generate NAMED_STRUCT(...) AS r"
        )

    def test_return_edge_with_alias_generates_struct(self, graph_context):
        """RETURN r AS rel should generate NAMED_STRUCT with alias.

        Query: MATCH ()-[r:KNOWS]->() RETURN r AS rel
        """
        sql = graph_context.transpile("""
            MATCH ()-[r:KNOWS]->()
            RETURN r AS rel
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "rel"), (
            "RETURN r AS rel should generate NAMED_STRUCT(...) AS rel"
        )

    def test_return_all_entities_generates_structs(self, graph_context):
        """RETURN a, r, b should generate NAMED_STRUCT for all.

        Query: MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)-[r:KNOWS]->(b:Person)
            RETURN a, r, b
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "a"), "Should have NAMED_STRUCT for a"
        assert has_named_struct(sql, "r"), "Should have NAMED_STRUCT for r"
        assert has_named_struct(sql, "b"), "Should have NAMED_STRUCT for b"


# =============================================================================
# TEST CLASS: collect(entity) -> COLLECT_LIST(NAMED_STRUCT)
# =============================================================================


class TestCollectEntityStruct:
    """Test that collect(entity) generates COLLECT_LIST(NAMED_STRUCT(...))."""

    def test_collect_node_generates_struct_list(self, graph_context):
        """collect(b) should generate COLLECT_LIST(NAMED_STRUCT(...)).

        Query: MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, collect(b) AS friends
        Expected: COLLECT_LIST(NAMED_STRUCT('node_id', ..., 'name', ..., ...)) AS friends
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            RETURN a.name, collect(b) AS friends
        """)

        print(f"\n=== SQL ===\n{sql}")

        # Should have COLLECT_LIST with NAMED_STRUCT inside
        assert "COLLECT_LIST" in sql, "Should use COLLECT_LIST"
        assert "NAMED_STRUCT" in sql, "Should have NAMED_STRUCT inside COLLECT_LIST"

        # The NAMED_STRUCT should contain node properties
        assert struct_contains_field(sql, "name"), "STRUCT should contain 'name'"

    def test_collect_edge_generates_struct_list(self, graph_context):
        """collect(r) should generate COLLECT_LIST(NAMED_STRUCT(...)).

        Query: MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, collect(r) AS relationships
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)-[r:KNOWS]->(b:Person)
            RETURN a.name, collect(r) AS relationships
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert "COLLECT_LIST" in sql, "Should use COLLECT_LIST"
        assert "NAMED_STRUCT" in sql, "Should have NAMED_STRUCT inside COLLECT_LIST"

        # The NAMED_STRUCT should contain edge properties
        assert struct_contains_field(sql, "src"), "STRUCT should contain 'src'"
        assert struct_contains_field(sql, "dst"), "STRUCT should contain 'dst'"


# =============================================================================
# TEST CLASS: No extra columns in output
# =============================================================================


class TestNoExtraColumns:
    """Test that RETURN entity doesn't add extra unpacked columns."""

    def test_return_node_no_extra_columns(self, graph_context_minimal):
        """RETURN a should only have one output column (the STRUCT), no extras.

        Query: MATCH (a:Person) RETURN a
        Expected: SELECT NAMED_STRUCT(...) AS a  (only one column)
        """
        sql = graph_context_minimal.transpile("""
            MATCH (a:Person)
            RETURN a
        """)

        print(f"\n=== SQL ===\n{sql}")

        # Should NOT have extra columns like _gsql2rsql_a_name AS _gsql2rsql_a_name
        assert "_gsql2rsql_a_name AS _gsql2rsql_a_name" not in sql, (
            "Should not have extra unpacked columns"
        )
        assert "_gsql2rsql_a_node_id AS _gsql2rsql_a_node_id" not in sql, (
            "Should not have extra unpacked columns"
        )

    def test_return_edge_no_extra_columns(self, graph_context_minimal):
        """RETURN r should only have one output column (the STRUCT), no extras.

        Query: MATCH ()-[r:KNOWS]->() RETURN r
        Expected: Outer SELECT has only NAMED_STRUCT(...) AS r, not extra columns
        Note: Inner subqueries may have the columns (needed for STRUCT), that's OK
        """
        sql = graph_context_minimal.transpile("""
            MATCH ()-[r:KNOWS]->()
            RETURN r
        """)

        print(f"\n=== SQL ===\n{sql}")

        # The outer SELECT should have NAMED_STRUCT as the only output
        import re
        # Find the outer SELECT clause (first SELECT in the SQL)
        outer_select_match = re.search(
            r"SELECT\s+(DISTINCT\s+)?(.*?)FROM",
            sql,
            re.IGNORECASE | re.DOTALL
        )
        assert outer_select_match, "Should have a SELECT clause"
        outer_select = outer_select_match.group(2)

        # The outer select should contain NAMED_STRUCT for r
        assert "NAMED_STRUCT" in outer_select, (
            "Outer SELECT should have NAMED_STRUCT"
        )
        assert "AS r" in outer_select, (
            "Outer SELECT should have AS r"
        )

        # The outer select should NOT have extra entity columns as separate outputs
        # (Note: The columns CAN exist in inner queries, just not as separate outer outputs)
        assert "_gsql2rsql_r_src AS r" not in outer_select, (
            "Outer SELECT should not have src as separate output"
        )
        assert "_gsql2rsql_r_dst AS r" not in outer_select, (
            "Outer SELECT should not have dst as separate output"
        )


# =============================================================================
# TEST CLASS: Edge cases
# =============================================================================


class TestEntityStructEdgeCases:
    """Edge cases for entity STRUCT generation."""

    def test_return_entity_through_with(self, graph_context):
        """Entity passed through WITH should still generate STRUCT in final RETURN.

        Query: MATCH (a:Person) WITH a RETURN a
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)
            WITH a
            RETURN a
        """)

        print(f"\n=== SQL ===\n{sql}")

        # Final output should have NAMED_STRUCT
        # Note: intermediate WITH may or may not use STRUCT
        assert "NAMED_STRUCT" in sql, (
            "RETURN a (after WITH) should eventually generate NAMED_STRUCT"
        )

    def test_distinct_entity_generates_struct(self, graph_context):
        """RETURN DISTINCT a should generate STRUCT.

        Query: MATCH (a:Person)-[:KNOWS]->() RETURN DISTINCT a
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)-[:KNOWS]->()
            RETURN DISTINCT a
        """)

        print(f"\n=== SQL ===\n{sql}")

        # DISTINCT on structs uses GROUP BY TO_JSON workaround for Spark MAP type compat
        assert "NAMED_STRUCT" in sql, (
            "RETURN DISTINCT a should generate NAMED_STRUCT"
        )
        assert "GROUP BY" in sql.upper(), "Should have GROUP BY (DISTINCT workaround)"
        assert "TO_JSON" in sql.upper(), "Should use TO_JSON for grouping"
        assert "FIRST(" in sql, "Should wrap in FIRST() for GROUP BY"

    def test_return_entity_with_order_by(self, graph_context):
        """RETURN a ORDER BY a.name should generate STRUCT.

        Query: MATCH (a:Person) RETURN a ORDER BY a.name
        """
        sql = graph_context.transpile("""
            MATCH (a:Person)
            RETURN a
            ORDER BY a.name
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "a"), (
            "RETURN a ORDER BY should generate NAMED_STRUCT(...) AS a"
        )
        assert "ORDER BY" in sql.upper(), "Should have ORDER BY clause"


# =============================================================================
# TEST CLASS: Edge struct with custom src/dst column names (regression)
# =============================================================================


@pytest.fixture
def graph_context_custom_cols():
    """GraphContext with NON-default edge src/dst column names.

    Regression fixture: the edge NAMED_STRUCT used to hardcode 'src'/'dst'
    keys/columns, producing dangling references (_gsql2rsql_r_src) when the
    schema uses custom column names like 'source_node_id'.
    """
    g = GraphContext(
        nodes_table="cat.sch.nodes",
        edges_table="cat.sch.edges",
        node_id_col="node_id",
        node_type_col="node_type",
        edge_type_col="edge_type",
        edge_src_col="source_node_id",
        edge_dst_col="target_node_id",
        extra_edge_attrs={"edge_id": str},
    )
    g.set_types(node_types=["Person"], edge_types=["OWNS"])
    return g


def dangling_struct_refs(sql: str, entity_var: str) -> set[str]:
    """Return prefixed columns referenced in the SQL but never materialized.

    Every `_gsql2rsql_<var>_*` column referenced anywhere must also appear
    as an `AS _gsql2rsql_<var>_*` alias somewhere in the SQL; otherwise it is
    a dangling reference (UNRESOLVED_COLUMN at runtime).
    """
    import re
    prefix = rf"_gsql2rsql_{re.escape(entity_var)}_\w+"
    referenced = set(re.findall(prefix, sql))
    materialized = set(re.findall(rf"AS\s+({prefix})", sql))
    return referenced - materialized


class TestReturnEdgeCustomColumns:
    """RETURN r must derive struct keys from the schema's real src/dst columns."""

    def test_return_edge_custom_src_dst_no_dangling_refs(
        self, graph_context_custom_cols
    ):
        """Struct must reference only materialized columns with custom schema.

        Query: MATCH (s)-[r]->(d) RETURN r
        Bug: NAMED_STRUCT referenced _gsql2rsql_r_src/_gsql2rsql_r_dst which
        were never materialized (schema columns are source_node_id/target_node_id).
        """
        sql = graph_context_custom_cols.transpile("""
            MATCH (s)-[r]->(d)
            RETURN r
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert has_named_struct(sql, "r"), "RETURN r should generate NAMED_STRUCT"

        dangling = dangling_struct_refs(sql, "r")
        assert not dangling, (
            f"Struct references columns never materialized: {dangling}"
        )

        # Keys must be the REAL schema column names, not hardcoded src/dst
        assert struct_contains_field(sql, "source_node_id"), (
            "STRUCT should use real source column name 'source_node_id'"
        )
        assert struct_contains_field(sql, "target_node_id"), (
            "STRUCT should use real sink column name 'target_node_id'"
        )
        assert not struct_contains_field(sql, "src"), (
            "STRUCT must not contain hardcoded 'src' key with custom schema"
        )
        assert not struct_contains_field(sql, "dst"), (
            "STRUCT must not contain hardcoded 'dst' key with custom schema"
        )

    def test_return_edge_custom_src_dst_collect(self, graph_context_custom_cols):
        """collect(r) shares the same struct collector — must also be fixed.

        Query: MATCH (s)-[r]->(d) RETURN collect(r) AS rels
        """
        sql = graph_context_custom_cols.transpile("""
            MATCH (s)-[r]->(d)
            RETURN collect(r) AS rels
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert "COLLECT_LIST" in sql, "Should use COLLECT_LIST"
        assert "NAMED_STRUCT" in sql, "Should have NAMED_STRUCT inside COLLECT_LIST"

        dangling = dangling_struct_refs(sql, "r")
        assert not dangling, (
            f"Struct references columns never materialized: {dangling}"
        )
        assert struct_contains_field(sql, "source_node_id")
        assert struct_contains_field(sql, "target_node_id")

    def test_return_edge_default_schema_unchanged(self, graph_context_minimal):
        """Default schema (src/dst) must keep 'src'/'dst' struct keys.

        Locks in no-regression: consumers reading r.src / r.dst on the default
        schema must not break.
        """
        sql = graph_context_minimal.transpile("""
            MATCH (s)-[r:KNOWS]->(d)
            RETURN r
        """)

        print(f"\n=== SQL ===\n{sql}")

        assert struct_contains_field(sql, "src"), "Default STRUCT keeps 'src' key"
        assert struct_contains_field(sql, "dst"), "Default STRUCT keeps 'dst' key"
        dangling = dangling_struct_refs(sql, "r")
        assert not dangling, (
            f"Struct references columns never materialized: {dangling}"
        )
