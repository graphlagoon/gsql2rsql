"""Unit tests for procedural BFS rendering mode.

Tests verify SQL text output (structure, keywords, patterns) without PySpark.
Uses SimpleSQLSchemaProvider pattern from existing tests.

Two test classes:
- TestNumberedViewsRendering: EXECUTE IMMEDIATE + numbered views (PySpark 4.2)
- TestTempTablesRendering: CREATE TEMPORARY TABLE + INSERT INTO (Databricks)
"""

import re

import pytest

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.common.schema import (
    EdgeSchema,
    EntityProperty,
    NodeSchema,
)
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.bidirectional_optimizer import (
    apply_bidirectional_optimization,
)
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.renderer.procedural_bfs_renderer import (
    ProceduralBFSOptimizations,
)
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.renderer.sql_renderer import SQLRenderer


# ------------------------------------------------------------------
# Shared schema setup
# ------------------------------------------------------------------

def _make_schema() -> SimpleSQLSchemaProvider:
    """Create a schema with Person nodes and KNOWS edges."""
    schema = SimpleSQLSchemaProvider()

    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[
                EntityProperty("node_id", str),
                EntityProperty("name", str),
                EntityProperty("age", int),
            ],
            node_id_property=EntityProperty("node_id", str),
        ),
        SQLTableDescriptor(
            table_name="nodes",
            node_id_columns=["node_id"],
            filter="node_type = 'Person'",
        ),
    )

    schema.add_edge(
        EdgeSchema(
            name="KNOWS",
            source_node_id="Person",
            sink_node_id="Person",
            source_id_property=EntityProperty("src", str),
            sink_id_property=EntityProperty("dst", str),
            properties=[
                EntityProperty("src", str),
                EntityProperty("dst", str),
                EntityProperty("amount", int),
            ],
        ),
        SQLTableDescriptor(
            entity_id="Person@KNOWS@Person",
            table_name="edges",
            node_id_columns=["src", "dst"],
            filter="relationship_type = 'KNOWS'",
        ),
    )

    return schema


def _make_multi_edge_schema() -> SimpleSQLSchemaProvider:
    """Schema with two physical edge tables (forces multi-table UNION ALL)."""
    schema = SimpleSQLSchemaProvider()
    schema.add_node(
        NodeSchema(
            name="Person",
            properties=[EntityProperty("node_id", str)],
            node_id_property=EntityProperty("node_id", str),
        ),
        SQLTableDescriptor(
            table_name="nodes",
            node_id_columns=["node_id"],
            filter="node_type = 'Person'",
        ),
    )
    for etype, table in (("KNOWS", "knows_edges"), ("OWNS", "owns_edges")):
        schema.add_edge(
            EdgeSchema(
                name=etype,
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("src", str),
                sink_id_property=EntityProperty("dst", str),
                properties=[
                    EntityProperty("src", str),
                    EntityProperty("dst", str),
                ],
            ),
            SQLTableDescriptor(
                entity_id=f"Person@{etype}@Person",
                table_name=table,
                node_id_columns=["src", "dst"],
                filter=f"relationship_type = '{etype}'",
            ),
        )
    return schema


def _transpile(
    query: str,
    schema: SimpleSQLSchemaProvider,
    *,
    materialization: str = "temp_tables",
    vlp_mode: str = "procedural",
    bidirectional_mode: str = "off",
    procedural_optimizations: "ProceduralBFSOptimizations | None" = None,
) -> str:
    """Transpile a Cypher query with the given strategy.

    ``bidirectional_mode`` (other than ``"off"``) applies the bidirectional BFS
    optimization, mirroring ``GraphContext.transpile``; required to reach the
    bidirectional procedural renderer paths.
    """
    parser = OpenCypherParser()
    renderer = SQLRenderer(
        db_schema_provider=schema,
        vlp_rendering_mode=vlp_mode,
        materialization_strategy=materialization,
        procedural_optimizations=procedural_optimizations,
    )
    ast = parser.parse(query)
    plan = LogicalPlan.process_query_tree(ast, schema)
    optimize_plan(plan)
    if bidirectional_mode != "off":
        apply_bidirectional_optimization(
            plan, graph_schema=schema, mode=bidirectional_mode,
        )
    plan.resolve(original_query=query)
    return renderer.render_plan(plan)


BASIC_QUERY = """
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.node_id = 'Alice'
RETURN b.node_id
"""

MIN_HOPS_QUERY = """
MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
WHERE a.node_id = 'Alice'
RETURN b.node_id
"""

# Undirected + untyped over multiple physical edge tables → multi-table UNION ALL.
MULTI_TABLE_QUERY = """
MATCH (a:Person)-[*1..2]-(b:Person)
WHERE a.node_id = 'Alice'
RETURN b.node_id
"""

# Both endpoints filtered by id → eligible for bidirectional BFS.
BIDIR_QUERY = """
MATCH (a:Person)-[:KNOWS*1..4]->(b:Person)
WHERE a.node_id = 'Alice' AND b.node_id = 'Carol'
RETURN b.node_id
"""


# A local variable referenced inside a ``CREATE TEMPORARY TABLE/VIEW ... AS``
# *definition* is rejected by Databricks with
# ``LOCAL_VARIABLE_IN_TEMP_OBJECT_DEFINITION`` (SQLSTATE 42K0M). Local variables
# are only legal in DML (INSERT ... SELECT). This helper extracts every
# temp-object definition body and reports any declared local variable found in
# one, so tests can assert the transpiler never emits the illegal construct.
_TEMP_DEF_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TEMPORARY\s+(?:TABLE|VIEW)\s+(\S+)\s+AS\b(.*?);",
    re.IGNORECASE | re.DOTALL,
)
_DECLARE_RE = re.compile(r"\bDECLARE\s+(\w+)", re.IGNORECASE)


def _local_vars_in_temp_definitions(sql: str) -> list[tuple[str, str]]:
    """Return (object_name, local_var) pairs for each declared local variable
    referenced inside a CREATE TEMPORARY TABLE/VIEW ... AS definition."""
    local_vars = set(_DECLARE_RE.findall(sql))
    hits: list[tuple[str, str]] = []
    for name, body in _TEMP_DEF_RE.findall(sql):
        for var in local_vars:
            if re.search(rf"\b{re.escape(var)}\b", body):
                hits.append((name, var))
    return hits


# ======================================================================
# numbered_views strategy (PySpark 4.2)
# ======================================================================


class TestNumberedViewsRendering:
    """Verify SQL text for materialization_strategy='numbered_views'."""

    MATERIALIZATION = "numbered_views"

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def _sql(self, query: str = BASIC_QUERY) -> str:
        return _transpile(
            query, self.schema, materialization=self.MATERIALIZATION,
        )

    # Structure
    def test_produces_begin_end(self) -> None:
        sql = self._sql()
        assert sql.strip().startswith("BEGIN")
        assert sql.strip().endswith("END")

    def test_contains_while_loop(self) -> None:
        sql = self._sql()
        assert "WHILE" in sql
        assert "END WHILE" in sql

    def test_contains_execute_immediate(self) -> None:
        sql = self._sql()
        assert "EXECUTE IMMEDIATE" in sql

    def test_contains_declare_statements(self) -> None:
        sql = self._sql()
        assert "DECLARE bfs_depth_" in sql
        assert "DECLARE bfs_frontier_count_" in sql
        assert "DECLARE bfs_union_sql_" in sql

    def test_frontier_count_updated_via_into_not_set(self) -> None:
        """The loop-control variable must be assigned with
        ``EXECUTE IMMEDIATE '<query>' INTO bfs_frontier_count_N`` (which writes
        the script-local variable), NOT ``EXECUTE IMMEDIATE 'SET
        bfs_frontier_count_N = (...)'``.

        On Spark 4.2 the ``SET`` form inside ``EXECUTE IMMEDIATE`` is a silent
        no-op for the local variable — it writes a session *conf* key instead —
        so ``WHILE bfs_frontier_count_N > 0`` never terminates early and the
        loop always runs to ``max_hops``, exploding the lazy-view lineage
        (~2^max_hops). See docs_help_dev/analysis_canonical_query_bfs_memory.md.
        """
        sql = self._sql()
        assert "INTO bfs_frontier_count_" in sql
        assert "SET bfs_frontier_count_" not in sql

    # Frontier / visited init
    def test_frontier_init_has_start_filter(self) -> None:
        sql = self._sql()
        assert "bfs_frontier_" in sql
        assert "_0 AS" in sql
        assert "node_type = 'Person'" in sql

    def test_visited_init_from_frontier(self) -> None:
        sql = self._sql()
        assert "bfs_visited_" in sql
        assert "SELECT node FROM bfs_frontier_" in sql

    def test_visited_exclusion_uses_not_exists(self) -> None:
        """Visited exclusion must use NOT EXISTS, not `NOT IN (subquery)`.

        `NOT IN` is a null-aware anti-join that Spark can only execute as an
        unconditional broadcast of the whole visited set (no shuffle fallback),
        which OOMs the driver once visited grows large in deep BFS. NOT EXISTS
        degrades gracefully to SortMergeJoin. Result-identical given non-null
        node ids. See docs_help_dev/analysis_canonical_query_bfs_memory.md (O5).
        """
        sql = self._sql()
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql
        assert "NOT IN (SELECT node FROM bfs_visited_" not in sql

    # Direction
    def test_directed_forward_join_on_src(self) -> None:
        assert "e.src = f.node" in self._sql()

    def test_directed_backward_join_on_dst(self) -> None:
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        assert "e.dst = f.node" in self._sql(query)

    def test_undirected_default_uses_doubled_adjacency(self) -> None:
        """By DEFAULT undirected traversal uses the doubled-adjacency
        rewrite (O8): a single equi-join against a pre-built ``bfs_adj_``
        table, no OR-join and no CASE next-node.

        Both alternative rewrites of the non-equi OR-join
        (``undirected_union_all`` and ``undirected_doubled_adjacency``)
        eliminate the same BroadcastNestedLoopJoin; adjacency is the
        default because it references the frontier/visited views ONCE per
        level (unlike per-level UNION ALL, which doubles the lazy-lineage
        fan-out on numbered_views — measured 2–7.7× regression there).
        """
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "bfs_adj_" in sql
        assert "ON e._jk = f.node" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "CASE WHEN f.node = e.src" not in sql

    def test_undirected_legacy_or_join_via_all_off(self) -> None:
        """``ProceduralBFSOptimizations.all_off()`` reproduces the legacy
        single non-equi OR-join + CASE next-node (pre-O8/O7 SQL)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = _transpile(
            query, self.schema,
            materialization=self.MATERIALIZATION,
            procedural_optimizations=ProceduralBFSOptimizations.all_off(),
        )
        assert "e.src = f.node OR e.dst = f.node" in sql
        assert "CASE WHEN f.node = e.src" in sql

    def test_undirected_union_all_opt_in(self) -> None:
        """With ``undirected_union_all=True`` (and the default
        ``undirected_doubled_adjacency`` explicitly disabled — the two
        undirected-expansion rewrites are mutually exclusive), undirected
        traversal expands as a UNION ALL of two *equi-join* branches
        (e.src=f.node → next=e.dst; e.dst=f.node → next=e.src) instead of
        the OR-join + CASE.

        An OR join predicate is non-equi, so Spark can only run it as a
        BroadcastNestedLoopJoin (O(frontier*edges)); two equi-joins use
        hash/sort-merge joins. Row-for-row identical. Opt-in for materialized
        execution over large graphs (see analysis doc §O7).
        """
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = _transpile(
            query, self.schema,
            materialization=self.MATERIALIZATION,
            procedural_optimizations=ProceduralBFSOptimizations(
                undirected_union_all=True,
                undirected_doubled_adjacency=False,
            ),
        )
        # both equi-join directions present
        assert "e.src = f.node" in sql
        assert "e.dst = f.node" in sql
        # the slow OR-join and its CASE next-node are gone
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "CASE WHEN f.node = e.src" not in sql
        # the (default) adjacency rewrite is not the one active here
        assert "bfs_adj_" not in sql

    # Edge filter
    def test_edge_type_filter_in_expansion(self) -> None:
        assert "relationship_type" in self._sql()

    # min_hops
    def test_min_hops_skips_early_levels(self) -> None:
        query = """
        MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "bfs_depth_" in sql
        assert ">= 2" in sql

    # Final view
    def test_final_view_cross_join_frontier(self) -> None:
        assert "CROSS JOIN bfs_frontier_" in self._sql()

    def test_final_view_has_start_node_end_node(self) -> None:
        sql = self._sql()
        assert "start_node" in sql
        assert "end_node" in sql

    # No numbered suffix uses depth-based naming
    def test_numbered_suffix_in_views(self) -> None:
        """Views use depth suffix like bfs_frontier_N_0."""
        sql = self._sql()
        assert "_0 AS" in sql  # frontier_N_0, visited_N_0

    # collect_edges support
    def test_collect_edges_produces_path_edges(self) -> None:
        """relationships(path) should produce path_edges column."""
        query = """
        MATCH path = (a:Person)-[r:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN relationships(path)
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "NAMED_STRUCT" in sql

    def test_unwind_relationships_produces_path_edges(self) -> None:
        """UNWIND relationships(path) should work with procedural BFS."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src, r.dst
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "EXPLODE" in sql

    # CTE mode unchanged
    def test_cte_mode_unchanged(self) -> None:
        sql = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="numbered_views",
        )
        assert "WITH RECURSIVE" in sql
        assert "BEGIN" not in sql


# ======================================================================
# temp_tables strategy (Databricks)
# ======================================================================


class TestTempTablesRendering:
    """Verify SQL text for materialization_strategy='temp_tables'."""

    MATERIALIZATION = "temp_tables"

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def _sql(self, query: str = BASIC_QUERY) -> str:
        return _transpile(
            query, self.schema, materialization=self.MATERIALIZATION,
        )

    # Structure
    def test_produces_begin_end(self) -> None:
        sql = self._sql()
        assert sql.strip().startswith("BEGIN")
        assert sql.strip().endswith("END")

    def test_contains_while_loop(self) -> None:
        sql = self._sql()
        assert "WHILE" in sql
        assert "END WHILE" in sql

    def test_contains_create_temporary_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE" in sql

    def test_contains_insert_into(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "INSERT INTO bfs_result_" in sql

    def test_contains_drop_temporary_table(self) -> None:
        sql = self._sql()
        assert "DROP TEMPORARY TABLE" in sql

    def test_no_execute_immediate(self) -> None:
        """temp_tables should NOT use EXECUTE IMMEDIATE."""
        sql = self._sql()
        assert "EXECUTE IMMEDIATE" not in sql

    def test_no_union_sql_variable(self) -> None:
        """temp_tables should NOT need bfs_union_sql string variable."""
        sql = self._sql()
        assert "bfs_union_sql_" not in sql

    def test_declare_current_depth(self) -> None:
        sql = self._sql()
        assert "DECLARE current_depth_" in sql
        assert "DECLARE rows_in_frontier_" in sql

    # Setup
    def test_setup_creates_visited_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_visited_" in sql

    def test_setup_creates_frontier_from_node_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_frontier_" in sql
        assert "FROM nodes n" in sql

    def test_setup_seeds_visited_from_frontier(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "SELECT node FROM bfs_frontier_" in sql

    def test_visited_exclusion_uses_not_exists(self) -> None:
        """Visited exclusion must use NOT EXISTS, not `NOT IN (subquery)`.

        See TestNumberedViewsRendering.test_visited_exclusion_uses_not_exists
        and docs_help_dev/analysis_canonical_query_bfs_memory.md (O5).
        """
        sql = self._sql()
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql
        assert "NOT IN (SELECT node FROM bfs_visited_" not in sql

    def test_setup_creates_empty_result_table(self) -> None:
        sql = self._sql()
        assert "CREATE TEMPORARY TABLE bfs_result_" in sql

    def test_setup_saves_frontier_init(self) -> None:
        sql = self._sql()
        assert "bfs_frontier_" in sql
        assert "_init" in sql

    def test_start_filter_in_setup(self) -> None:
        sql = self._sql()
        assert "node_type = 'Person'" in sql

    # Direction
    def test_directed_forward_join_on_src(self) -> None:
        assert "e.src = f.node" in self._sql()

    def test_directed_backward_join_on_dst(self) -> None:
        query = """
        MATCH (a:Person)<-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        assert "e.dst = f.node" in self._sql(query)

    def test_undirected_default_uses_doubled_adjacency(self) -> None:
        """By DEFAULT undirected traversal uses the doubled-adjacency
        rewrite (O8): a single equi-join against a pre-built ``bfs_adj_``
        table, no OR-join and no CASE next-node.

        Both alternative rewrites of the non-equi OR-join
        (``undirected_union_all`` and ``undirected_doubled_adjacency``)
        eliminate the same BroadcastNestedLoopJoin; adjacency is the
        default because it references the frontier/visited views ONCE per
        level (unlike per-level UNION ALL, which doubles the lazy-lineage
        fan-out on numbered_views — measured 2–7.7× regression there).
        """
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "bfs_adj_" in sql
        assert "ON e._jk = f.node" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "CASE WHEN f.node = e.src" not in sql

    def test_undirected_legacy_or_join_via_all_off(self) -> None:
        """``ProceduralBFSOptimizations.all_off()`` reproduces the legacy
        single non-equi OR-join + CASE next-node (pre-O8/O7 SQL)."""
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = _transpile(
            query, self.schema,
            materialization=self.MATERIALIZATION,
            procedural_optimizations=ProceduralBFSOptimizations.all_off(),
        )
        assert "e.src = f.node OR e.dst = f.node" in sql
        assert "CASE WHEN f.node = e.src" in sql

    def test_undirected_union_all_opt_in(self) -> None:
        """With ``undirected_union_all=True`` (and the default
        ``undirected_doubled_adjacency`` explicitly disabled — the two
        undirected-expansion rewrites are mutually exclusive), undirected
        traversal expands as a UNION ALL of two *equi-join* branches
        (e.src=f.node → next=e.dst; e.dst=f.node → next=e.src) instead of
        the OR-join + CASE.

        An OR join predicate is non-equi, so Spark can only run it as a
        BroadcastNestedLoopJoin (O(frontier*edges)); two equi-joins use
        hash/sort-merge joins. Row-for-row identical. Opt-in for materialized
        execution over large graphs (see analysis doc §O7).
        """
        query = """
        MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = _transpile(
            query, self.schema,
            materialization=self.MATERIALIZATION,
            procedural_optimizations=ProceduralBFSOptimizations(
                undirected_union_all=True,
                undirected_doubled_adjacency=False,
            ),
        )
        # both equi-join directions present
        assert "e.src = f.node" in sql
        assert "e.dst = f.node" in sql
        # the slow OR-join and its CASE next-node are gone
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "CASE WHEN f.node = e.src" not in sql
        # the (default) adjacency rewrite is not the one active here
        assert "bfs_adj_" not in sql

    # Edge filter
    def test_edge_type_filter(self) -> None:
        assert "relationship_type" in self._sql()

    # WHILE loop internals
    def test_while_drops_and_creates_edges(self) -> None:
        sql = self._sql()
        assert "DROP TEMPORARY TABLE IF EXISTS bfs_edges_" in sql
        assert "CREATE TEMPORARY TABLE bfs_edges_" in sql

    def test_while_counts_edges_with_set(self) -> None:
        """SET rows_in_frontier = (SELECT COUNT...)."""
        sql = self._sql()
        assert "SET rows_in_frontier_" in sql
        assert "SELECT COUNT(1) FROM bfs_edges_" in sql

    def test_while_inserts_into_visited(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_visited_" in sql
        assert "SELECT DISTINCT _next_node FROM bfs_edges_" in sql

    def test_while_replaces_frontier(self) -> None:
        """Frontier replaced via DROP + CREATE TABLE AS."""
        sql = self._sql()
        assert "DROP TEMPORARY TABLE bfs_frontier_" in sql
        assert "SELECT DISTINCT _next_node AS node FROM bfs_edges_" in sql

    def test_while_inserts_into_result(self) -> None:
        sql = self._sql()
        assert "INSERT INTO bfs_result_" in sql

    # min_hops
    def test_min_hops_conditional_insert(self) -> None:
        """*2..4 should guard INSERT INTO result with IF depth >= 2."""
        query = """
        MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = self._sql(query)
        assert "current_depth_" in sql
        assert ">= 2" in sql

    # 42K0M: no local variable inside a temp-object definition
    def test_no_local_variable_in_temp_object_definition(self) -> None:
        """Databricks rejects local vars (e.g. current_depth_N) referenced
        inside a ``CREATE TEMPORARY TABLE/VIEW ... AS`` definition with
        LOCAL_VARIABLE_IN_TEMP_OBJECT_DEFINITION (42K0M). The BFS depth must be
        injected in the result INSERT (DML) instead, never in the bfs_edges
        definition. Covers single-table, min_hops>1, and multi-table paths."""
        for query, schema in (
            (BASIC_QUERY, self.schema),
            (MIN_HOPS_QUERY, self.schema),
            (MULTI_TABLE_QUERY, _make_multi_edge_schema()),
        ):
            sql = _transpile(query, schema, materialization="temp_tables")
            hits = _local_vars_in_temp_definitions(sql)
            assert not hits, (
                f"local variable(s) leaked into temp-object definition(s): "
                f"{hits}\n\n{sql}"
            )

    def test_bidir_no_local_variable_in_temp_object_definition(self) -> None:
        """Same 42K0M invariant for the bidirectional temp_tables renderer."""
        sql = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables", bidirectional_mode="auto",
        )
        # Sanity: ensure the bidirectional path was actually exercised.
        assert "bfs_bwd_visited_" in sql, "bidirectional path not triggered"
        hits = _local_vars_in_temp_definitions(sql)
        assert not hits, (
            f"local variable(s) leaked into temp-object definition(s): "
            f"{hits}\n\n{sql}"
        )

    def test_depth_injected_in_result_insert(self) -> None:
        """_bfs_depth is supplied by current_depth_N in the result INSERT
        (DML), and the bfs_edges definition no longer carries the column."""
        sql = self._sql()
        assert (
            "SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1" in sql
        )
        edge_def = next(
            body for name, body in _TEMP_DEF_RE.findall(sql)
            if name.startswith("bfs_edges_")
        )
        assert "_bfs_depth" not in edge_def

    # Final view
    def test_final_view_cross_join_frontier_init(self) -> None:
        sql = self._sql()
        assert "CROSS JOIN bfs_frontier_" in sql
        assert "_init f0" in sql

    def test_final_view_has_start_node_end_node(self) -> None:
        sql = self._sql()
        assert "start_node" in sql
        assert "end_node" in sql

    def test_final_view_selects_from_result(self) -> None:
        sql = self._sql()
        assert "FROM bfs_result_" in sql

    # No numbered suffixes (fixed table names)
    def test_no_numbered_suffix(self) -> None:
        """Table names are bfs_visited_N, NOT bfs_visited_N_0."""
        sql = self._sql()
        # Should NOT contain the numbered_views pattern of _N_0
        # (except for bfs_frontier_N_init which is different)
        lines = sql.split("\n")
        for line in lines:
            if "bfs_visited_" in line and "DECLARE" not in line:
                # visited tables should NOT have depth suffix
                assert "_0 AS" not in line or "bfs_frontier_" in line

    # collect_edges support
    def test_collect_edges_produces_path_edges(self) -> None:
        """relationships(path) should produce path_edges column."""
        query = """
        MATCH path = (a:Person)-[r:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN relationships(path)
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "NAMED_STRUCT" in sql
        # temp_tables uses direct SQL, no EXECUTE IMMEDIATE
        assert "ARRAY(NAMED_STRUCT(" in sql

    def test_unwind_relationships_produces_path_edges(self) -> None:
        """UNWIND relationships(path) should work with procedural BFS."""
        query = """
        MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
        WHERE a.node_id = 'Alice'
        UNWIND relationships(path) AS r
        RETURN r.src, r.dst
        """
        sql = self._sql(query)
        assert "path_edges" in sql
        assert "EXPLODE" in sql


# ======================================================================
# Cross-strategy equivalence
# ======================================================================


class TestStrategyEquivalence:
    """Verify both strategies produce the same final view schema."""

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def test_both_strategies_produce_same_final_columns(self) -> None:
        """Both should have start_node, end_node, depth in final view."""
        sql_tt = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema, materialization="numbered_views",
        )
        for sql, name in [(sql_tt, "temp_tables"), (sql_nv, "numbered_views")]:
            assert "start_node" in sql, f"{name} missing start_node"
            assert "end_node" in sql, f"{name} missing end_node"
            assert "depth" in sql, f"{name} missing depth"

    def test_both_strategies_use_cross_join(self) -> None:
        sql_tt = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema, materialization="numbered_views",
        )
        assert "CROSS JOIN" in sql_tt
        assert "CROSS JOIN" in sql_nv

    def test_cte_mode_ignores_materialization(self) -> None:
        """When vlp_mode='cte', materialization_strategy is irrelevant."""
        sql_tt = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="temp_tables",
        )
        sql_nv = _transpile(
            BASIC_QUERY, self.schema,
            vlp_mode="cte", materialization="numbered_views",
        )
        assert "WITH RECURSIVE" in sql_tt
        assert "WITH RECURSIVE" in sql_nv
        assert "BEGIN" not in sql_tt
        assert "BEGIN" not in sql_nv


# ======================================================================
# Feature flags: ProceduralBFSOptimizations (default ON, legacy escape hatch)
# ======================================================================

UNDIRECTED_QUERY = """
MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
WHERE a.node_id = 'Alice'
RETURN b.node_id
"""


class TestProceduralOptimizationFlags:
    """The three memory-fix optimizations are individually flag-gated
    (``ProceduralBFSOptimizations``), all ON by default, with
    ``all_off()`` reproducing the legacy (pre-optimization) SQL as an
    escape hatch for engines we cannot test locally (e.g. Databricks
    SQL Warehouse). See docs_help_dev/analysis_canonical_query_bfs_memory.md.
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()
        self.legacy = ProceduralBFSOptimizations.all_off()

    # --- default == explicit all-on (flags truly default to ON) ---

    def test_default_equals_explicit_all_on(self) -> None:
        for materialization in ("temp_tables", "numbered_views"):
            sql_default = _transpile(
                UNDIRECTED_QUERY, self.schema,
                materialization=materialization,
            )
            sql_all_on = _transpile(
                UNDIRECTED_QUERY, self.schema,
                materialization=materialization,
                procedural_optimizations=ProceduralBFSOptimizations(),
            )
            assert sql_default == sql_all_on

    # --- all_off() reproduces every legacy form ---

    def test_legacy_temp_tables_uses_not_in_and_or_join(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.legacy,
        )
        # O5 off -> NOT IN form, no visited NOT EXISTS
        assert "NOT IN (SELECT node FROM bfs_visited_" in sql
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" not in sql
        # O7 off -> single OR-join with CASE next-node
        assert "e.src = f.node OR e.dst = f.node" in sql
        assert "CASE WHEN f.node = e.src" in sql

    def test_legacy_numbered_views_uses_not_in_or_join_and_set(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="numbered_views",
            procedural_optimizations=self.legacy,
        )
        # O5 off
        assert "NOT IN (" in sql
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" not in sql
        # O7 off
        assert "e.src = f.node OR e.dst = f.node" in sql
        assert "CASE WHEN f.node = e.src" in sql
        # loop_control_into off -> legacy SET form
        assert "SET bfs_frontier_count_" in sql
        assert "INTO bfs_frontier_count_" not in sql

    # --- flags are individual (bisectable) ---

    def test_only_visited_not_exists_off(self) -> None:
        opts = ProceduralBFSOptimizations(visited_not_exists=False)
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=opts,
        )
        # O5 off -> NOT IN
        assert "NOT IN (SELECT node FROM bfs_visited_" in sql
        # O8 keeps its (ON) default -> doubled adjacency, no OR-join
        assert "bfs_adj_" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql

    def test_visited_not_exists_composes_with_union_all(self) -> None:
        # undirected_union_all and the default undirected_doubled_adjacency
        # are mutually exclusive; disable adjacency to opt into union_all.
        opts = ProceduralBFSOptimizations(
            undirected_union_all=True,
            undirected_doubled_adjacency=False,
        )
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=opts,
        )
        # O7 opted in -> equi-join branches, no OR-join
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "bfs_adj_" not in sql
        # O5 default ON -> NOT EXISTS
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql

    def test_only_undirected_union_all_off(self) -> None:
        # undirected_union_all=False is already the default; the (ON by
        # default) doubled adjacency remains in effect -> no OR-join.
        opts = ProceduralBFSOptimizations(undirected_union_all=False)
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=opts,
        )
        assert "bfs_adj_" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql
        # O5 still on -> NOT EXISTS (applied to the carried next-node col)
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql

    def test_legacy_or_join_requires_disabling_both_rewrites(self) -> None:
        """The legacy single OR-join only reappears when BOTH
        undirected-expansion rewrites (O7 union_all, O8 doubled adjacency)
        are explicitly disabled."""
        opts = ProceduralBFSOptimizations(
            undirected_union_all=False,
            undirected_doubled_adjacency=False,
        )
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=opts,
        )
        assert "bfs_adj_" not in sql
        assert "e.src = f.node OR e.dst = f.node" in sql

    def test_only_loop_control_into_off(self) -> None:
        opts = ProceduralBFSOptimizations(loop_control_into=False)
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="numbered_views",
            procedural_optimizations=opts,
        )
        assert "SET bfs_frontier_count_" in sql
        assert "INTO bfs_frontier_count_" not in sql
        # O5 keeps its (ON) default; O8 keeps its (ON) default
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql
        assert "bfs_adj_" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql

    def test_legacy_bidir_uses_or_join_and_not_in(self) -> None:
        sql = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables",
            bidirectional_mode="auto",
            procedural_optimizations=self.legacy,
        )
        assert "NOT IN (SELECT node FROM bfs_visited_" in sql
        assert "NOT EXISTS (SELECT 1 FROM bfs_visited_" not in sql


# ======================================================================
# Visited NOT EXISTS: IS NOT NULL guard (correctness, not flag-gated)
# ======================================================================


class TestVisitedNotExistsNullGuard:
    """The NOT EXISTS visited probe must carry an ``IS NOT NULL`` guard.

    ``x NOT IN (subquery)`` is null-aware: a NULL probe yields NULL and the
    row is DROPPED. ``NOT EXISTS (... WHERE v.node = x)`` with a NULL ``x``
    never matches, so the row is KEPT — without a guard, switching to
    NOT EXISTS (``visited_not_exists=True``, the default) silently changes
    semantics for edges with a NULL endpoint: the NULL leaks into
    visited/frontier/result and passes the barrier check. The guard restores
    exact NOT IN semantics (visited is root-seeded, hence never empty) and
    matches Cypher (an edge must connect two nodes).
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def test_temp_tables_guard_present(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
        )
        assert "IS NOT NULL AND NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql

    def test_numbered_views_guard_present(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="numbered_views",
        )
        assert "IS NOT NULL AND NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql

    def test_directed_guard_on_next_node_column(self) -> None:
        sql = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        assert (
            "e.dst IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM bfs_visited_" in sql
        )

    def test_undirected_guard_on_adjacency_next_column(self) -> None:
        """Default (doubled adjacency): the guard wraps the carried
        ``e._next`` column."""
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
        )
        assert "e._next IS NOT NULL AND NOT EXISTS" in sql

    def test_undirected_guard_on_case_expression(self) -> None:
        """Legacy OR-join (both undirected rewrites disabled): the guard
        wraps the CASE next-node."""
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
            procedural_optimizations=ProceduralBFSOptimizations(
                undirected_doubled_adjacency=False,
            ),
        )
        assert (
            "CASE WHEN f.node = e.src THEN e.dst ELSE e.src END "
            "IS NOT NULL AND NOT EXISTS" in sql
        )

    def test_legacy_not_in_has_no_guard(self) -> None:
        """all_off() must keep reproducing the legacy SQL byte-exactly."""
        for materialization in ("temp_tables", "numbered_views"):
            sql = _transpile(
                UNDIRECTED_QUERY, self.schema,
                materialization=materialization,
                procedural_optimizations=ProceduralBFSOptimizations.all_off(),
            )
            assert "IS NOT NULL AND NOT" not in sql
            assert "NOT IN (" in sql


# ======================================================================
# Loop depth bounds: lock the exact numeric mapping for *1..N
# ======================================================================


class TestLoopDepthBounds:
    """``*1..3`` must expand depths 1..3 exactly.

    The loop is increment-then-work starting at 0, so the correct bound is
    ``< 3`` (work at depths 1, 2, 3) with the final safety filter
    ``depth <= 3``. This locks the invariant against a plausible-looking
    "off-by-one fix" (e.g. changing to ``< 2`` on the false premise that
    the loop is work-then-increment), which would silently drop all
    max-depth paths.
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()

    def test_temp_tables_bounds_exact(self) -> None:
        sql = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        assert "current_depth_1 < 3" in sql
        assert "depth <= 3" in sql
        assert "current_depth_1 < 2" not in sql
        assert "current_depth_1 < 4" not in sql
        assert "depth <= 4" not in sql

    def test_numbered_views_bounds_exact(self) -> None:
        sql = _transpile(
            BASIC_QUERY, self.schema, materialization="numbered_views",
        )
        assert "bfs_depth_1 < 3" in sql
        assert "depth <= 3" in sql
        assert "bfs_depth_1 < 2" not in sql
        assert "bfs_depth_1 < 4" not in sql
        assert "depth <= 4" not in sql

    def test_min_hops_filter_exact(self) -> None:
        sql = _transpile(
            MIN_HOPS_QUERY, self.schema, materialization="temp_tables",
        )
        # *2..4 → loop bound < 4, result filter depth >= 2 and <= 4
        assert "current_depth_1 < 4" in sql
        assert "depth >= 2" in sql
        assert "depth <= 4" in sql


# ======================================================================
# O(adjacency): undirected_doubled_adjacency (default OFF, opt-in)
# ======================================================================


def _loop_body(sql: str) -> str:
    """Extract the WHILE loop body from a procedural block."""
    start = sql.index("WHILE ")
    end = sql.index("END WHILE")
    return sql[start:end]


class TestDoubledAdjacency:
    """``undirected_doubled_adjacency=True`` materializes the (filtered)
    edge set once in BOTH orientations (``bfs_adj_{n}``) before the loop;
    each level is then a single equi-join against it, eliminating the
    non-equi OR-join (BroadcastNestedLoopJoin) without the per-level
    UNION ALL's doubled frontier references.
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()
        # deferred_edge_payload defaults True too; disable it here to test
        # O8 in isolation (composition with O9 is covered separately in
        # TestDeferredEdgePayload.test_composes_with_doubled_adjacency).
        self.adj = ProceduralBFSOptimizations(
            undirected_doubled_adjacency=True,
            deferred_edge_payload=False,
        )

    def test_mutually_exclusive_with_union_all(self) -> None:
        with pytest.raises(ValueError):
            ProceduralBFSOptimizations(
                undirected_union_all=True,
                undirected_doubled_adjacency=True,
            )

    def test_tt_adjacency_built_once_before_loop(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.adj,
        )
        assert sql.count("CREATE TEMPORARY TABLE bfs_adj_1 AS") == 1
        assert (
            sql.index("CREATE TEMPORARY TABLE bfs_adj_1 AS")
            < sql.index("WHILE ")
        )

    def test_tt_adjacency_has_both_orientations(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.adj,
        )
        assert "e.src AS _jk, e.dst AS _next" in sql
        assert "e.dst AS _jk, e.src AS _next" in sql

    def test_tt_loop_is_single_equijoin(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.adj,
        )
        loop = _loop_body(sql)
        assert "ON e._jk = f.node" in loop
        assert "FROM bfs_adj_1 e" in loop
        assert "e.src = f.node OR e.dst = f.node" not in sql
        assert "CASE WHEN f.node = e.src" not in sql
        # visited probe (with null guard) on the carried next-node column
        assert "e._next IS NOT NULL AND NOT EXISTS" in loop

    def test_tt_edge_filter_applied_at_build_not_per_level(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.adj,
        )
        loop = _loop_body(sql)
        assert "relationship_type = 'KNOWS'" not in loop
        adj_ddl = sql[
            sql.index("CREATE TEMPORARY TABLE bfs_adj_1"):sql.index("WHILE ")
        ]
        assert "relationship_type = 'KNOWS'" in adj_ddl

    def test_tt_directed_flag_is_noop(self) -> None:
        # Only toggle adjacency here; deferred_edge_payload keeps its
        # (also ON) default on both sides so the comparison isolates O8.
        sql_flag = _transpile(
            BASIC_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=ProceduralBFSOptimizations(
                undirected_doubled_adjacency=True,
            ),
        )
        sql_default = _transpile(
            BASIC_QUERY, self.schema, materialization="temp_tables",
        )
        assert sql_flag == sql_default
        assert "bfs_adj_" not in sql_flag

    def test_nv_adjacency_static_view_single_frontier_reference(self) -> None:
        """NV: adj is a STATIC pre-loop view; each level references the
        previous frontier view exactly once (single-branch recursion, no
        lazy-lineage doubling — the reason per-level UNION ALL regressed
        2–7.7× on numbered_views)."""
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema,
            materialization="numbered_views",
            procedural_optimizations=self.adj,
        )
        assert "CREATE OR REPLACE TEMPORARY VIEW bfs_adj_1 AS" in sql
        assert (
            sql.index("CREATE OR REPLACE TEMPORARY VIEW bfs_adj_1 AS")
            < sql.index("WHILE ")
        )
        assert "FROM bfs_adj_1 e" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql
        # exactly one frontier reference in the per-level expansion
        assert sql.count("INNER JOIN bfs_frontier_1_'") == 1

    def test_multi_table_adjacency_collapses_loop_to_one_join(self) -> None:
        """OR-typed relationships over two physical edge tables: the adj
        build contains all (table × orientation) branches; the loop is
        still one equi-join. (Untyped patterns resolve via the wildcard
        edge table descriptor, not the multi-table path.)"""
        schema = _make_multi_edge_schema()
        or_typed_query = """
        MATCH (a:Person)-[:KNOWS|OWNS*1..2]-(b:Person)
        WHERE a.node_id = 'Alice'
        RETURN b.node_id
        """
        sql = _transpile(
            or_typed_query, schema,
            materialization="temp_tables",
            procedural_optimizations=self.adj,
        )
        adj_ddl = sql[
            sql.index("CREATE TEMPORARY TABLE bfs_adj_1"):sql.index("WHILE ")
        ]
        assert "knows_edges" in adj_ddl
        assert "owns_edges" in adj_ddl
        loop = _loop_body(sql)
        assert "knows_edges" not in loop
        assert "owns_edges" not in loop
        assert loop.count("ON e._jk = f.node") == 1

    def test_bidir_ignores_adjacency_flag(self) -> None:
        sql_flag = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables",
            bidirectional_mode="auto",
            procedural_optimizations=self.adj,
        )
        sql_default = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables",
            bidirectional_mode="auto",
        )
        assert sql_flag == sql_default
        assert "bfs_adj_" not in sql_flag

    def test_active_by_default(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
        )
        assert "bfs_adj_" in sql
        assert "e.src = f.node OR e.dst = f.node" not in sql

    def test_all_off_keeps_or_join(self) -> None:
        sql = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
            procedural_optimizations=ProceduralBFSOptimizations.all_off(),
        )
        assert "bfs_adj_" not in sql
        assert "e.src = f.node OR e.dst = f.node" in sql


# ======================================================================
# O(payload): deferred_edge_payload (default OFF, temp_tables only)
# ======================================================================

# Path variable + UNWIND relationships(p) → edge property columns are
# carried (collect_edges), which is what the deferral targets.
PAYLOAD_QUERY = """
MATCH path = (a:Person)-[:KNOWS*1..3]-(b:Person)
WHERE a.node_id = 'Alice'
UNWIND relationships(path) AS r
RETURN r.src, r.dst
"""


class TestDeferredEdgePayload:
    """``deferred_edge_payload=True``: per-level tables carry only
    ``(_row_id, _next_node)``; the edge payload is written once into
    ``bfs_edges_keyed_{n}`` (materialized MONOTONICALLY_INCREASING_ID)
    and re-attached once in the final view.
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()
        self.deferred = ProceduralBFSOptimizations(
            deferred_edge_payload=True,
        )

    def _sql(self, query: str = PAYLOAD_QUERY, **kw: object) -> str:
        return _transpile(
            query, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.deferred,
            **kw,  # type: ignore[arg-type]
        )

    def test_keyed_table_built_once_before_loop(self) -> None:
        sql = self._sql()
        assert sql.count("CREATE TEMPORARY TABLE bfs_edges_keyed_1 AS") == 1
        assert (
            sql.index("CREATE TEMPORARY TABLE bfs_edges_keyed_1 AS")
            < sql.index("WHILE ")
        )
        assert "MONOTONICALLY_INCREASING_ID() AS _row_id" in sql

    def test_loop_carries_only_key_and_next(self) -> None:
        sql = self._sql()
        loop = _loop_body(sql)
        assert "e._row_id" in loop
        # the payload column never travels through the loop
        assert "amount" not in loop
        # slim result accumulator
        assert (
            "CREATE TEMPORARY TABLE bfs_result_1 "
            "(_row_id BIGINT, _next_node STRING, _bfs_depth INT);" in sql
        )

    def test_edge_filter_applied_at_keyed_build_not_per_level(self) -> None:
        sql = self._sql()
        loop = _loop_body(sql)
        assert "relationship_type = 'KNOWS'" not in loop
        keyed_ddl = sql[
            sql.index("CREATE TEMPORARY TABLE bfs_edges_keyed_1"):
            sql.index("WHILE ")
        ]
        assert "relationship_type = 'KNOWS'" in keyed_ddl

    def test_final_view_reattaches_payload_by_row_id(self) -> None:
        sql = self._sql()
        final = sql[sql.index("END WHILE"):]
        assert "JOIN bfs_edges_keyed_1 e ON e._row_id = r._row_id" in final
        assert "e.amount" in final
        # path_edges struct is built from the keyed table alias
        assert "'amount', e.amount" in final

    def test_no_edge_props_flag_is_noop(self) -> None:
        """Without carried edge properties there is no payload to defer.

        Note: ``edge_prop_cols`` holds ALL schema edge properties whenever
        the edge has any (they are carried through every level even if the
        query never reads them), so a props-free schema is needed to hit
        the no-payload path.
        """
        schema = SimpleSQLSchemaProvider()
        schema.add_node(
            NodeSchema(
                name="Person",
                properties=[EntityProperty("node_id", str)],
                node_id_property=EntityProperty("node_id", str),
            ),
            SQLTableDescriptor(
                table_name="nodes",
                node_id_columns=["node_id"],
                filter="node_type = 'Person'",
            ),
        )
        schema.add_edge(
            EdgeSchema(
                name="KNOWS",
                source_node_id="Person",
                sink_node_id="Person",
                source_id_property=EntityProperty("src", str),
                sink_id_property=EntityProperty("dst", str),
                properties=[
                    EntityProperty("src", str),
                    EntityProperty("dst", str),
                ],
            ),
            SQLTableDescriptor(
                entity_id="Person@KNOWS@Person",
                table_name="edges",
                node_id_columns=["src", "dst"],
                filter="relationship_type = 'KNOWS'",
            ),
        )
        sql_flag = _transpile(
            BASIC_QUERY, schema,
            materialization="temp_tables",
            procedural_optimizations=self.deferred,
        )
        sql_default = _transpile(
            BASIC_QUERY, schema, materialization="temp_tables",
        )
        assert sql_flag == sql_default
        assert "bfs_edges_keyed_" not in sql_flag

    def test_numbered_views_ignores_flag(self) -> None:
        sql_flag = _transpile(
            PAYLOAD_QUERY, self.schema,
            materialization="numbered_views",
            procedural_optimizations=self.deferred,
        )
        sql_default = _transpile(
            PAYLOAD_QUERY, self.schema, materialization="numbered_views",
        )
        assert sql_flag == sql_default
        assert "bfs_edges_keyed_" not in sql_flag

    def test_composes_with_doubled_adjacency(self) -> None:
        """Both flags: adj is built narrow from the keyed table."""
        opts = ProceduralBFSOptimizations(
            deferred_edge_payload=True,
            undirected_doubled_adjacency=True,
        )
        sql = _transpile(
            PAYLOAD_QUERY, self.schema,
            materialization="temp_tables",
            procedural_optimizations=opts,
        )
        adj_start = sql.index("CREATE TEMPORARY TABLE bfs_adj_1")
        adj_ddl = sql[adj_start:sql.index("WHILE ")]
        assert "FROM bfs_edges_keyed_1 e" in adj_ddl
        assert "SELECT e._row_id, e.src AS _jk, e.dst AS _next" in adj_ddl
        # keyed table exists and precedes adj
        assert (
            sql.index("CREATE TEMPORARY TABLE bfs_edges_keyed_1") < adj_start
        )
        loop = _loop_body(sql)
        assert "SELECT e._row_id, e._next AS _next_node" in loop
        assert "amount" not in loop

    def test_bidir_ignores_flag(self) -> None:
        sql_flag = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables",
            bidirectional_mode="auto",
            procedural_optimizations=self.deferred,
        )
        sql_default = _transpile(
            BIDIR_QUERY, self.schema,
            materialization="temp_tables",
            bidirectional_mode="auto",
        )
        assert sql_flag == sql_default
        assert "bfs_edges_keyed_" not in sql_flag


# ======================================================================
# O(barrier): barrier_precompute (default OFF, temp_tables only)
# ======================================================================

BARRIER_QUERY = """
MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
WHERE a.node_id = 'Alice' AND is_terminator(b.age > 30)
RETURN b.node_id
"""

VOLATILE_BARRIER_QUERY = """
MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
WHERE a.node_id = 'Alice' AND is_terminator(b.age > rand())
RETURN b.node_id
"""


class TestBarrierPrecompute:
    """``barrier_precompute=True``: the is_terminator barrier decision is
    materialized once before the loop (``bfs_barrier_{n}`` = DISTINCT node
    ids satisfying the predicate); each level then anti-joins this small id
    table instead of re-scanning the full node table. Predicate-first +
    DISTINCT is exactly equivalent to the per-level correlated NOT EXISTS
    for deterministic predicates (never use MAX-style aggregation — unsound
    for non-monotone predicates).
    """

    def setup_method(self) -> None:
        self.schema = _make_schema()
        self.pre = ProceduralBFSOptimizations(barrier_precompute=True)

    def _sql(self, query: str = BARRIER_QUERY, **kw: object) -> str:
        return _transpile(
            query, self.schema,
            materialization="temp_tables",
            procedural_optimizations=self.pre,
            **kw,  # type: ignore[arg-type]
        )

    def test_barrier_table_built_once_before_loop(self) -> None:
        sql = self._sql()
        assert sql.count("CREATE TEMPORARY TABLE bfs_barrier_1 AS") == 1
        assert (
            sql.index("CREATE TEMPORARY TABLE bfs_barrier_1 AS")
            < sql.index("WHILE ")
        )
        barrier_ddl = sql[
            sql.index("CREATE TEMPORARY TABLE bfs_barrier_1"):
            sql.index("WHILE ")
        ]
        assert "SELECT DISTINCT" in barrier_ddl
        assert "(barrier.age) > (30)" in barrier_ddl
        assert "node_type = 'Person'" in barrier_ddl

    def test_loop_antijoins_precomputed_table(self) -> None:
        sql = self._sql()
        loop = _loop_body(sql)
        assert (
            "NOT EXISTS (SELECT 1 FROM bfs_barrier_1 b "
            "WHERE b.node = _next_node)" in loop
        )
        # the full node table is no longer probed inside the loop
        assert "FROM nodes barrier" not in loop

    def test_active_by_default(self) -> None:
        sql = _transpile(
            BARRIER_QUERY, self.schema, materialization="temp_tables",
        )
        assert "bfs_barrier_" in sql
        assert "FROM nodes barrier" not in _loop_body(sql)

    def test_all_off_scans_node_table_per_level(self) -> None:
        sql = _transpile(
            BARRIER_QUERY, self.schema, materialization="temp_tables",
            procedural_optimizations=ProceduralBFSOptimizations.all_off(),
        )
        assert "bfs_barrier_" not in sql
        assert "FROM nodes barrier" in _loop_body(sql)

    def test_no_barrier_flag_is_noop(self) -> None:
        sql_flag = self._sql(UNDIRECTED_QUERY)
        sql_default = _transpile(
            UNDIRECTED_QUERY, self.schema, materialization="temp_tables",
        )
        assert sql_flag == sql_default
        assert "bfs_barrier_" not in sql_flag

    def test_volatile_predicate_falls_back_to_per_level(self) -> None:
        """A volatile barrier predicate would be frozen at build time —
        fall back to the legacy per-level probe (conservative check; false
        positives only cost the optimization, never correctness)."""
        sql = self._sql(VOLATILE_BARRIER_QUERY)
        assert "bfs_barrier_" not in sql
        assert "FROM nodes barrier" in _loop_body(sql)

    def test_numbered_views_ignores_flag(self) -> None:
        sql_flag = _transpile(
            BARRIER_QUERY, self.schema,
            materialization="numbered_views",
            procedural_optimizations=self.pre,
        )
        sql_default = _transpile(
            BARRIER_QUERY, self.schema, materialization="numbered_views",
        )
        assert sql_flag == sql_default
        assert "bfs_barrier_" not in sql_flag
