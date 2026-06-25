"""Procedural BFS renderer — frontier-based BFS with global visited set.

Alternative to WITH RECURSIVE CTE for variable-length path queries.
Uses SQL scripting (BEGIN...END, WHILE) with two materialization strategies:

- ``temp_tables`` (Databricks): CREATE TEMPORARY TABLE + INSERT INTO.
  Fixed table names, O(1) visited reads per level.
- ``numbered_views`` (PySpark 4.2+): EXECUTE IMMEDIATE + numbered views.
  Dynamic view names, UNION chain for visited (degrades with depth).

Semantic differences from CTE mode:
- Global visited set: each node discovered once (shortest-path semantics)
- ``relationships(path)`` supported via ARRAY(NAMED_STRUCT(...)) wrapping:
  each BFS result row is one edge, so path_edges is always a 1-element array.
  UNWIND/EXPLODE produces the same number of rows (one per edge).
- ``nodes(path)`` NOT supported (requires full path reconstruction)
- ``length(path)`` = depth is available

TODO(path_collection): The current ``relationships(path)`` support is partial.
  Each BFS result row stores ONE edge (the edge that discovered the node at that
  depth level). The ``path_edges`` column wraps it in a 1-element ARRAY for
  compatibility with the CTE pipeline (EXPLODE produces one row per edge).
  This means ``UNWIND relationships(path) AS r`` works correctly — each row
  becomes one edge — but ``COLLECT(relationships(path))`` or grouping edges
  into full source-to-target paths would require backtracking through BFS levels,
  which is not implemented. For full path reconstruction, use ``vlp_rendering_mode='cte'``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.parser.ast import RelationshipDirection
from gsql2rsql.planner.operators import (
    LogicalOperator,
    RecursiveTraversalOperator,
)

if TYPE_CHECKING:
    from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
    from gsql2rsql.renderer.render_context import RenderContext
    from gsql2rsql.renderer.sql_enrichment import EnrichedRecursiveOp


def _build_bfs_barrier_where(p: "_BFSParams", node_col: str) -> str:
    """Build NOT EXISTS clause for barrier nodes in BFS frontier.

    Returns SQL like:
        NOT EXISTS (SELECT 1 FROM nodes barrier
                    WHERE barrier.node_id = _next_node
                      AND barrier.node_type = 'Station'
                      AND barrier.is_hub = true)
    """
    parts = [f"barrier.{p.barrier_node_id_col} = {node_col}"]
    if p.barrier_node_type_filter:
        parts.append(f"barrier.{p.barrier_node_type_filter}")
    parts.append(f"({p.barrier_predicate})")
    inner_where = " AND ".join(parts)
    return (
        f"NOT EXISTS (SELECT 1 FROM {p.barrier_node_table} barrier "
        f"WHERE {inner_where})"
    )


def _build_bfs_barrier_where_escaped(
    p: "_BFSParams", node_col: str,
) -> str:
    """Same as _build_bfs_barrier_where but with single-quote escaping.

    For use inside EXECUTE IMMEDIATE strings in numbered_views strategy.
    """
    raw = _build_bfs_barrier_where(p, node_col)
    return raw.replace("'", "''")


@dataclass
class _BFSParams:
    """Common parameters extracted once and shared between strategies."""

    n: int
    cte_name: str
    is_backward: bool
    is_undirected: bool
    src_col: str
    dst_col: str
    edge_prop_cols: list[str]
    min_hops: int
    max_hops: int
    edge_table_sql: str
    edge_type_filter: str | None
    edge_predicate: str | None
    node_table: str
    node_id_col: str
    start_filter: str | None
    node_type_filter: str | None
    enriched: EnrichedRecursiveOp
    collect_edges: bool = False
    collect_nodes: bool = False
    # Bidirectional optimization fields
    bidir_mode: str = "off"  # "off", "recursive", "unrolling"
    bidir_depth_forward: int = 0
    bidir_depth_backward: int = 0
    bidir_target_table: str = ""
    bidir_target_id_col: str = ""
    bidir_target_filter: str | None = None
    bidir_target_type_filter: str | None = None
    # Barrier filter (is_terminator directive)
    barrier_predicate: str | None = None
    barrier_node_table: str | None = None
    barrier_node_id_col: str | None = None
    barrier_node_type_filter: str | None = None


class ProceduralBFSRenderer:
    """Renders RecursiveTraversalOperator as procedural BFS blocks.

    Supports two materialization strategies (selected via
    ``ctx.materialization_strategy``):

    - ``temp_tables``: Databricks — CREATE TEMPORARY TABLE + INSERT INTO
    - ``numbered_views``: PySpark 4.2 — EXECUTE IMMEDIATE + numbered views

    The final result view has the same schema as the CTE output
    (start_node, end_node, depth) so the join renderer works unchanged.
    """

    def __init__(
        self,
        ctx: RenderContext,
        expr_renderer: ExpressionRenderer,
        render_operator_fn: Callable[[LogicalOperator, int], str],
    ) -> None:
        self._ctx = ctx
        self._expr = expr_renderer
        self._render_operator = render_operator_fn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render_procedural_block(
        self, op: RecursiveTraversalOperator
    ) -> tuple[str, str]:
        """Render the procedural BFS block for a RecursiveTraversalOperator.

        Dispatches to ``_render_temp_tables`` or ``_render_numbered_views``
        based on ``ctx.materialization_strategy``.

        Returns:
            (declarations, body) — DECLARE statements and the WHILE loop body.
            Both are plain SQL strings to be placed inside a BEGIN...END block.
        """
        params = self._resolve_common_params(op)

        if params.bidir_mode != "off":
            if self._ctx.materialization_strategy == "temp_tables":
                return self._render_bidir_temp_tables(params)
            else:
                return self._render_bidir_numbered_views(params)

        if self._ctx.materialization_strategy == "temp_tables":
            return self._render_temp_tables(params)
        else:
            return self._render_numbered_views(params)

    def render_procedural_reference(
        self, op: RecursiveTraversalOperator, depth: int
    ) -> str:
        """Render a SELECT from the procedural BFS result view.

        Same interface as RecursiveCTERenderer._render_recursive_reference().
        Returns SQL like: SELECT start_node, end_node, depth[, path_edges] FROM {cte_name}
        """
        indent = self._ctx.indent(depth)
        cte_name = getattr(op, "cte_name", "paths")
        min_depth = op.min_hops if op.min_hops is not None else 1

        lines: list[str] = []
        lines.append(f"{indent}SELECT")
        cols = ["start_node", "end_node", "depth"]
        if op.collect_edges:
            cols.append("path_edges")
        if op.collect_nodes:
            cols.append("path")
        for i, col in enumerate(cols):
            comma = "," if i < len(cols) - 1 else ""
            lines.append(f"{indent}   {col}{comma}")
        lines.append(f"{indent}FROM {cte_name}")

        # Depth bounds (already enforced by WHILE loop, but kept for safety
        # and to match CTE interface expectations)
        where_parts: list[str] = []
        if min_depth > 0:
            where_parts.append(f"depth >= {min_depth}")
        if op.max_hops is not None:
            where_parts.append(f"depth <= {op.max_hops}")
        if where_parts:
            lines.append(f"{indent}WHERE {' AND '.join(where_parts)}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Common parameter resolution
    # ------------------------------------------------------------------

    def _resolve_common_params(
        self, op: RecursiveTraversalOperator
    ) -> _BFSParams:
        """Validate op and extract all parameters shared by both strategies."""
        self._validate(op)
        enriched = self._get_enriched(op)

        # Assign CTE name (reused as final view name)
        self._ctx.cte_counter += 1
        n = self._ctx.cte_counter
        cte_name = f"paths_{n}"
        op.cte_name = cte_name

        # Direction
        is_backward = op.swap_source_sink
        is_undirected = (
            op.direction == RelationshipDirection.BOTH
            or op.use_internal_union_for_bidirectional
        )

        # Columns
        src_col = enriched.source_id_col
        dst_col = enriched.target_id_col
        edge_prop_cols = [
            p for p in enriched.edge_property_names
            if p not in (enriched.source_id_col, enriched.target_id_col)
        ]
        min_hops = op.min_hops if op.min_hops is not None else 1
        assert op.max_hops is not None  # guaranteed by _validate
        max_hops: int = op.max_hops

        # Edge table info
        edge_table_sql = self._build_edge_table_sql(enriched)
        edge_type_filter = self._build_edge_type_filter(enriched)

        # Edge predicate filter
        edge_predicate = None
        if enriched.edge_filter_as_e:
            edge_predicate = self._expr.render_edge_filter_expression(
                enriched.edge_filter_as_e
            )

        # Source node info
        assert enriched.source_node is not None  # guaranteed by _validate
        node_table = enriched.source_node.table_descriptor.full_table_name
        node_id_col = enriched.source_node.id_column
        start_filter = None
        if enriched.start_filter_as_n:
            start_filter = self._expr.render_edge_filter_expression(
                enriched.start_filter_as_n
            )
        node_type_filter = enriched.source_node.table_descriptor.filter

        # Bidirectional optimization fields
        bidir_mode = op.bidirectional_bfs_mode
        bidir_depth_forward = op.bidirectional_depth_forward or 0
        bidir_depth_backward = op.bidirectional_depth_backward or 0
        bidir_target_table = ""
        bidir_target_id_col = ""
        bidir_target_filter: str | None = None
        bidir_target_type_filter: str | None = None

        if bidir_mode != "off" and enriched.target_node is not None:
            td = enriched.target_node.table_descriptor
            bidir_target_table = td.full_table_name
            bidir_target_id_col = enriched.target_node.id_column
            bidir_target_type_filter = td.filter
            if enriched.sink_filter_as_tgt:
                bidir_target_filter = (
                    self._expr.render_edge_filter_expression(
                        enriched.sink_filter_as_tgt
                    )
                )

        # Barrier filter (is_terminator directive)
        barrier_predicate = None
        barrier_node_table = None
        barrier_node_id_col = None
        barrier_node_type_filter = None
        if enriched.barrier_filter_as_barrier:
            barrier_predicate = (
                self._expr.render_edge_filter_expression(
                    enriched.barrier_filter_as_barrier
                )
            )
            if enriched.target_node:
                td = enriched.target_node.table_descriptor
                barrier_node_table = td.full_table_name
                barrier_node_id_col = enriched.target_node.id_column
                barrier_node_type_filter = td.filter

        return _BFSParams(
            n=n,
            cte_name=cte_name,
            is_backward=is_backward,
            is_undirected=is_undirected,
            src_col=src_col,
            dst_col=dst_col,
            edge_prop_cols=edge_prop_cols,
            min_hops=min_hops,
            max_hops=max_hops,
            edge_table_sql=edge_table_sql,
            edge_type_filter=edge_type_filter,
            edge_predicate=edge_predicate,
            node_table=node_table,
            node_id_col=node_id_col,
            start_filter=start_filter,
            node_type_filter=node_type_filter,
            enriched=enriched,
            collect_edges=op.collect_edges,
            collect_nodes=op.collect_nodes,
            bidir_mode=bidir_mode,
            bidir_depth_forward=bidir_depth_forward,
            bidir_depth_backward=bidir_depth_backward,
            bidir_target_table=bidir_target_table,
            bidir_target_id_col=bidir_target_id_col,
            bidir_target_filter=bidir_target_filter,
            bidir_target_type_filter=bidir_target_type_filter,
            barrier_predicate=barrier_predicate,
            barrier_node_table=barrier_node_table,
            barrier_node_id_col=barrier_node_id_col,
            barrier_node_type_filter=barrier_node_type_filter,
        )

    # ------------------------------------------------------------------
    # Validation & enrichment
    # ------------------------------------------------------------------

    def _validate(self, op: RecursiveTraversalOperator) -> None:
        """Validate that the operator is compatible with procedural BFS.

        collect_nodes/collect_edges flags: both are force-set True when a
        path variable exists (line 156-157 of recursive.py). We allow them
        because:
        - collect_edges: supported via ARRAY(NAMED_STRUCT(...)) wrapping.
        - collect_nodes: the `path` column (node ID array) is omitted from
          the final view. The `nodes(path)` Cypher function will fail at
          expression rendering (no `path` column to reference), which is
          the correct behavior. We don't block it here because path
          variables that only use `relationships(path)` also set
          collect_nodes=True.
        """
        if op.max_hops is None:
            raise TranspilerNotSupportedException(
                "Procedural BFS requires a finite max_hops bound."
            )

        enriched = self._get_enriched(op)
        if enriched.source_node is None:
            raise TranspilerNotSupportedException(
                "Procedural BFS requires a resolvable start node."
            )
        if op.bidirectional_bfs_mode != "off":
            if enriched.target_node is None:
                raise TranspilerNotSupportedException(
                    "Bidirectional procedural BFS requires a "
                    "resolvable target node."
                )

    def _get_enriched(self, op: RecursiveTraversalOperator) -> EnrichedRecursiveOp:
        """Get enriched data, raising if not found."""
        if self._ctx.enriched:
            enriched = self._ctx.enriched.recursive_ops.get(op.operator_debug_id)
            if enriched:
                return enriched
        raise TranspilerNotSupportedException(
            "No enriched data for RecursiveTraversalOperator "
            f"(edge_types={op.edge_types})"
        )

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _build_edge_table_sql(self, enriched: EnrichedRecursiveOp) -> str:
        """Build the edge table reference for single-table case."""
        if enriched.single_table and enriched.single_table_name:
            return enriched.single_table_name
        return ""  # Multi-table: handled per-strategy

    def _build_edge_type_filter(self, enriched: EnrichedRecursiveOp) -> str | None:
        """Build the edge type filter clause.

        Wraps in parentheses when the filter contains OR to prevent
        precedence issues when combined with AND in WHERE clauses.
        """
        if enriched.single_table and enriched.single_table_filter:
            f = enriched.single_table_filter
            if " OR " in f:
                return f"({f})"
            return f
        return None

    @staticmethod
    def _resolve_direction(
        src_col: str,
        dst_col: str,
        is_backward: bool,
        is_undirected: bool,
    ) -> tuple[str, str, str]:
        """Return (join_cond, next_node_expr, visited_exclusion_col) for direction."""
        if is_undirected:
            join_cond = f"(e.{src_col} = f.node OR e.{dst_col} = f.node)"
            next_node_expr = (
                f"CASE WHEN f.node = e.{src_col} "
                f"THEN e.{dst_col} ELSE e.{src_col} END"
            )
            return join_cond, next_node_expr, next_node_expr
        elif is_backward:
            join_cond = f"e.{dst_col} = f.node"
            return join_cond, f"e.{src_col}", f"e.{src_col}"
        else:
            join_cond = f"e.{src_col} = f.node"
            return join_cond, f"e.{dst_col}", f"e.{dst_col}"

    @staticmethod
    def _build_where_clause(where_parts: list[str]) -> str:
        """Build WHERE clause from parts."""
        return f"\nWHERE {' AND '.join(where_parts)}" if where_parts else ""

    @staticmethod
    def _build_path_edges_expr(p: _BFSParams, alias: str = "r") -> str:
        """Build ARRAY(NAMED_STRUCT(...)) expression for path_edges column.

        Each BFS result row is one edge. We wrap its columns in a 1-element
        array of NAMED_STRUCT so that EXPLODE (from UNWIND) works unchanged.
        """
        struct_parts = [
            f"'{p.src_col}', {alias}.{p.src_col}",
            f"'{p.dst_col}', {alias}.{p.dst_col}",
        ]
        for prop in p.edge_prop_cols:
            struct_parts.append(f"'{prop}', {alias}.{prop}")
        return f"ARRAY(NAMED_STRUCT({', '.join(struct_parts)}))"

    @staticmethod
    def _build_start_where(
        node_type_filter: str | None, start_filter: str | None,
    ) -> str:
        """Build WHERE clause for start node selection."""
        parts: list[str] = []
        if node_type_filter:
            parts.append(node_type_filter)
        if start_filter:
            parts.append(start_filter)
        if parts:
            return f"\nWHERE {' AND '.join(parts)}"
        return ""

    # ======================================================================
    # STRATEGY: temp_tables (Databricks)
    # ======================================================================

    def _render_temp_tables(self, p: _BFSParams) -> tuple[str, str]:
        """Render procedural BFS using CREATE TEMPORARY TABLE + INSERT INTO."""
        declarations = self._tt_declarations(p)

        body_parts: list[str] = []
        body_parts.append(self._tt_setup(p))
        body_parts.append(self._tt_while_loop(p))
        body_parts.append(self._tt_final_view(p))

        return declarations, "\n\n".join(body_parts)

    def _tt_declarations(self, p: _BFSParams) -> str:
        """DECLARE statements for temp_tables strategy."""
        return (
            f"DECLARE current_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE rows_in_frontier_{p.n} BIGINT DEFAULT 1;"
        )

    def _tt_setup(self, p: _BFSParams) -> str:
        """Create initial temp tables: visited, frontier, result, frontier_init."""
        prop_cols_def = "".join(
            f", {c} STRING" for c in p.edge_prop_cols
        )
        where = self._build_start_where(p.node_type_filter, p.start_filter)

        lines: list[str] = []

        # Drop pre-existing tables
        for name in [
            f"bfs_visited_{p.n}", f"bfs_frontier_{p.n}",
            f"bfs_result_{p.n}", f"bfs_frontier_{p.n}_init",
        ]:
            lines.append(f"DROP TEMPORARY TABLE IF EXISTS {name};")

        # Visited (accumulator)
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_visited_{p.n} (node STRING);"
        )
        # Frontier (current level)
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_frontier_{p.n} AS\n"
            f"SELECT n.{p.node_id_col} AS node\n"
            f"FROM {p.node_table} n{where};"
        )
        # Seed visited from frontier
        lines.append(
            f"INSERT INTO bfs_visited_{p.n}\n"
            f"SELECT node FROM bfs_frontier_{p.n};"
        )
        # Result accumulator (empty, matching edge schema)
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_result_{p.n} "
            f"({p.src_col} STRING, {p.dst_col} STRING{prop_cols_def}, "
            f"_next_node STRING, _bfs_depth INT);"
        )
        # Save frontier_0 for CROSS JOIN in final view
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_frontier_{p.n}_init AS\n"
            f"SELECT node FROM bfs_frontier_{p.n};"
        )

        return "\n".join(lines)

    def _tt_edge_expansion_sql(self, p: _BFSParams) -> str:
        """Build edge expansion SELECT for temp_tables (no quote escaping needed)."""
        join_cond, next_node_expr, visited_excl = self._resolve_direction(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)

        if not p.enriched.single_table:
            return self._tt_multi_table_edge_expansion(
                p, join_cond, next_node_expr, visited_excl,
            )

        where_parts: list[str] = []
        where_parts.append(
            f"{visited_excl} NOT IN (SELECT node FROM bfs_visited_{p.n})"
        )
        if p.edge_type_filter:
            where_parts.append(p.edge_type_filter)
        if p.edge_predicate:
            where_parts.append(p.edge_predicate)

        return (
            f"SELECT "
            f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
            f"{next_node_expr} AS _next_node\n"
            f"FROM {p.edge_table_sql} e\n"
            f"INNER JOIN bfs_frontier_{p.n} f ON {join_cond}"
            + self._build_where_clause(where_parts)
        )

    def _tt_multi_table_edge_expansion(
        self,
        p: _BFSParams,
        join_cond: str,
        next_node_expr: str,
        visited_excl: str,
    ) -> str:
        """UNION ALL edge expansion for multiple edge tables (temp_tables)."""
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)
        parts: list[str] = []

        for edge_info in p.enriched.edge_tables:
            table_name = edge_info.table_descriptor.full_table_name
            where_parts: list[str] = []
            where_parts.append(
                f"{visited_excl} NOT IN (SELECT node FROM bfs_visited_{p.n})"
            )
            if edge_info.filter_clause:
                where_parts.append(edge_info.filter_clause)
            if p.edge_predicate:
                where_parts.append(p.edge_predicate)

            parts.append(
                f"SELECT "
                f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                f"{next_node_expr} AS _next_node\n"
                f"FROM {table_name} e\n"
                f"INNER JOIN bfs_frontier_{p.n} f ON {join_cond}"
                + self._build_where_clause(where_parts)
            )

        return "\nUNION ALL\n".join(parts)

    def _tt_while_loop(self, p: _BFSParams) -> str:
        """Build WHILE loop for temp_tables strategy."""
        lines: list[str] = []
        lines.append(
            f"WHILE rows_in_frontier_{p.n} > 0 "
            f"AND current_depth_{p.n} < {p.max_hops} DO"
        )
        lines.append(f"  SET current_depth_{p.n} = current_depth_{p.n} + 1;")
        lines.append("")

        # A. Edge expansion — CREATE TEMPORARY TABLE bfs_edges
        edge_sql = self._tt_edge_expansion_sql(p)
        lines.append(f"  DROP TEMPORARY TABLE IF EXISTS bfs_edges_{p.n};")
        lines.append(
            f"  CREATE TEMPORARY TABLE bfs_edges_{p.n} AS\n"
            f"  {edge_sql};"
        )
        lines.append("")

        # B. Count new edges
        lines.append(
            f"  SET rows_in_frontier_{p.n} = "
            f"(SELECT COUNT(1) FROM bfs_edges_{p.n});"
        )
        lines.append("")

        # C. If edges found, update visited, frontier, result
        lines.append(f"  IF rows_in_frontier_{p.n} > 0 THEN")

        # Update visited
        lines.append(
            f"    INSERT INTO bfs_visited_{p.n}\n"
            f"    SELECT DISTINCT _next_node FROM bfs_edges_{p.n};"
        )

        # Replace frontier: DROP + CREATE TABLE AS
        lines.append(f"    DROP TEMPORARY TABLE bfs_frontier_{p.n};")
        if p.barrier_predicate and p.barrier_node_table:
            barrier_where = _build_bfs_barrier_where(p, "_next_node")
            lines.append(
                f"    CREATE TEMPORARY TABLE bfs_frontier_{p.n} AS\n"
                f"    SELECT DISTINCT _next_node AS node "
                f"FROM bfs_edges_{p.n}\n"
                f"    WHERE {barrier_where};"
            )
        else:
            lines.append(
                f"    CREATE TEMPORARY TABLE bfs_frontier_{p.n} AS\n"
                f"    SELECT DISTINCT _next_node AS node "
                f"FROM bfs_edges_{p.n};"
            )

        # Accumulate result (only for levels >= min_hops)
        if p.min_hops > 1:
            lines.append(
                f"    IF current_depth_{p.n} >= {p.min_hops} THEN"
            )
            lines.append(
                f"      INSERT INTO bfs_result_{p.n}\n"
                f"      SELECT *, current_depth_{p.n} AS _bfs_depth "
                f"FROM bfs_edges_{p.n};"
            )
            lines.append("    END IF;")
        else:
            lines.append(
                f"    INSERT INTO bfs_result_{p.n}\n"
                f"    SELECT *, current_depth_{p.n} AS _bfs_depth "
                f"FROM bfs_edges_{p.n};"
            )

        lines.append("  END IF;")
        lines.append("END WHILE;")
        return "\n".join(lines)

    def _tt_final_view(self, p: _BFSParams) -> str:
        """Build final result view for temp_tables strategy."""
        prop_cols = "".join(f", r.{c}" for c in p.edge_prop_cols)

        # path_edges: ARRAY(NAMED_STRUCT(...)) wrapping one edge per row
        path_edges_col = ""
        if p.collect_edges:
            path_edges_expr = self._build_path_edges_expr(p, alias="r")
            path_edges_col = f",\n       {path_edges_expr} AS path_edges"

        # Only emit path column when collect_nodes is True
        path_col = ""
        if p.collect_nodes:
            path_col = (
                ",\n       CAST(NULL AS ARRAY<STRING>) AS path"
            )

        return (
            f"CREATE OR REPLACE TEMPORARY VIEW {p.cte_name} AS\n"
            f"SELECT f0.node AS start_node, r._next_node AS end_node, "
            f"r._bfs_depth AS depth,\n"
            f"       r.{p.src_col}, r.{p.dst_col}{prop_cols}"
            f"{path_edges_col}"
            f"{path_col}\n"
            f"FROM bfs_result_{p.n} r\n"
            f"CROSS JOIN bfs_frontier_{p.n}_init f0;"
        )

    # ======================================================================
    # STRATEGY: temp_tables — BIDIRECTIONAL
    # ======================================================================

    def _render_bidir_temp_tables(
        self, p: _BFSParams
    ) -> tuple[str, str]:
        """Render bidirectional procedural BFS using temp tables.

        Phase 1: Backward BFS from target → reachable set
        Phase 2: Forward BFS from source with pruning after depth_forward
        Phase 3: Final view (same schema as unidirectional)
        """
        decl_parts: list[str] = []
        # Forward declarations (same as unidirectional)
        decl_parts.append(
            f"DECLARE current_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE rows_in_frontier_{p.n} BIGINT DEFAULT 1;"
        )
        # Backward declarations
        decl_parts.append(
            f"DECLARE bwd_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE bwd_frontier_count_{p.n} BIGINT DEFAULT 1;"
        )
        declarations = "\n".join(decl_parts)

        body_parts: list[str] = []
        body_parts.append(self._tt_bidir_backward_phase(p))
        body_parts.append(self._tt_setup(p))
        body_parts.append(self._tt_bidir_forward_loop(p))
        body_parts.append(self._tt_final_view(p))

        return declarations, "\n\n".join(body_parts)

    def _tt_bidir_backward_phase(self, p: _BFSParams) -> str:
        """Phase 1: Backward BFS from target to build reachable set."""
        bwd_is_backward = not p.is_backward
        if p.is_undirected:
            bwd_is_backward = False  # undirected stays undirected

        bwd_join, bwd_next, bwd_excl = self._resolve_direction(
            p.src_col, p.dst_col, bwd_is_backward, p.is_undirected,
        )

        # Build target WHERE clause
        tgt_where = self._build_start_where(
            p.bidir_target_type_filter, p.bidir_target_filter,
        )

        lines: list[str] = []

        # Drop pre-existing tables
        for name in [
            f"bfs_bwd_visited_{p.n}",
            f"bfs_bwd_frontier_{p.n}",
            f"bfs_bwd_edges_{p.n}",
        ]:
            lines.append(f"DROP TEMPORARY TABLE IF EXISTS {name};")

        # Create backward visited and frontier from target
        # Table alias must be 'tgt' to match sink_filter_as_tgt rewrite
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_bwd_visited_{p.n} "
            f"(node STRING);"
        )
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_bwd_frontier_{p.n} AS\n"
            f"SELECT tgt.{p.bidir_target_id_col} AS node\n"
            f"FROM {p.bidir_target_table} tgt{tgt_where};"
        )
        lines.append(
            f"INSERT INTO bfs_bwd_visited_{p.n}\n"
            f"SELECT node FROM bfs_bwd_frontier_{p.n};"
        )
        lines.append("")

        # Backward WHILE loop
        lines.append(
            f"WHILE bwd_frontier_count_{p.n} > 0 "
            f"AND bwd_depth_{p.n} < {p.bidir_depth_backward} DO"
        )
        lines.append(
            f"  SET bwd_depth_{p.n} = bwd_depth_{p.n} + 1;"
        )
        lines.append("")

        # Edge expansion (backward direction, no result cols)
        bwd_edge_sql = self._tt_bidir_backward_edge_sql(
            p, bwd_join, bwd_next, bwd_excl,
        )

        lines.append(
            f"  DROP TEMPORARY TABLE IF EXISTS "
            f"bfs_bwd_edges_{p.n};"
        )
        lines.append(
            f"  CREATE TEMPORARY TABLE bfs_bwd_edges_{p.n} AS\n"
            f"  {bwd_edge_sql};"
        )
        lines.append("")

        lines.append(
            f"  SET bwd_frontier_count_{p.n} = "
            f"(SELECT COUNT(1) FROM bfs_bwd_edges_{p.n});"
        )
        lines.append("")

        lines.append(f"  IF bwd_frontier_count_{p.n} > 0 THEN")
        lines.append(
            f"    INSERT INTO bfs_bwd_visited_{p.n}\n"
            f"    SELECT DISTINCT _next_node "
            f"FROM bfs_bwd_edges_{p.n};"
        )
        lines.append(
            f"    DROP TEMPORARY TABLE bfs_bwd_frontier_{p.n};"
        )
        lines.append(
            f"    CREATE TEMPORARY TABLE bfs_bwd_frontier_{p.n} AS\n"
            f"    SELECT DISTINCT _next_node AS node "
            f"FROM bfs_bwd_edges_{p.n};"
        )
        lines.append("  END IF;")
        lines.append("END WHILE;")

        return "\n".join(lines)

    def _tt_bidir_forward_loop(self, p: _BFSParams) -> str:
        """Phase 2: Forward BFS with pruning after depth_forward.

        Same as _tt_while_loop but with an additional pruning
        condition after depth_forward: _next_node must be in
        bfs_bwd_visited_{n} (the backward reachable set).
        """
        lines: list[str] = []
        lines.append(
            f"WHILE rows_in_frontier_{p.n} > 0 "
            f"AND current_depth_{p.n} < {p.max_hops} DO"
        )
        lines.append(
            f"  SET current_depth_{p.n} = "
            f"current_depth_{p.n} + 1;"
        )
        lines.append("")

        # Pruning condition: _next_node in backward reachable set
        join_cond, next_node_expr, _ = self._resolve_direction(
            p.src_col, p.dst_col,
            p.is_backward, p.is_undirected,
        )
        prune_cond = (
            f"{next_node_expr} IN "
            f"(SELECT node FROM bfs_bwd_visited_{p.n})"
        )

        # Build edge expansion SQL for pruned and unpruned
        pruned_sql = self._tt_bidir_forward_edge_sql(
            p, extra_where=[prune_cond],
        )
        unpruned_sql = self._tt_bidir_forward_edge_sql(p)

        # IF/ELSE: prune after depth_forward, normal before
        lines.append(
            f"  IF current_depth_{p.n} > "
            f"{p.bidir_depth_forward} THEN"
        )
        lines.append(
            f"    DROP TEMPORARY TABLE IF EXISTS "
            f"bfs_edges_{p.n};"
        )
        lines.append(
            f"    CREATE TEMPORARY TABLE bfs_edges_{p.n} AS\n"
            f"    {pruned_sql};"
        )
        lines.append("  ELSE")
        lines.append(
            f"    DROP TEMPORARY TABLE IF EXISTS "
            f"bfs_edges_{p.n};"
        )
        lines.append(
            f"    CREATE TEMPORARY TABLE bfs_edges_{p.n} AS\n"
            f"    {unpruned_sql};"
        )
        lines.append("  END IF;")
        lines.append("")

        # Count, update visited/frontier/result (same as unidirectional)
        lines.append(
            f"  SET rows_in_frontier_{p.n} = "
            f"(SELECT COUNT(1) FROM bfs_edges_{p.n});"
        )
        lines.append("")

        lines.append(f"  IF rows_in_frontier_{p.n} > 0 THEN")
        lines.append(
            f"    INSERT INTO bfs_visited_{p.n}\n"
            f"    SELECT DISTINCT _next_node "
            f"FROM bfs_edges_{p.n};"
        )
        lines.append(
            f"    DROP TEMPORARY TABLE bfs_frontier_{p.n};"
        )
        if p.barrier_predicate and p.barrier_node_table:
            barrier_where = _build_bfs_barrier_where(
                p, "_next_node",
            )
            lines.append(
                f"    CREATE TEMPORARY TABLE bfs_frontier_{p.n} AS\n"
                f"    SELECT DISTINCT _next_node AS node "
                f"FROM bfs_edges_{p.n}\n"
                f"    WHERE {barrier_where};"
            )
        else:
            lines.append(
                f"    CREATE TEMPORARY TABLE bfs_frontier_{p.n} AS\n"
                f"    SELECT DISTINCT _next_node AS node "
                f"FROM bfs_edges_{p.n};"
        )

        if p.min_hops > 1:
            lines.append(
                f"    IF current_depth_{p.n} >= "
                f"{p.min_hops} THEN"
            )
            lines.append(
                f"      INSERT INTO bfs_result_{p.n}\n"
                f"      SELECT *, current_depth_{p.n} AS _bfs_depth "
                f"FROM bfs_edges_{p.n};"
            )
            lines.append("    END IF;")
        else:
            lines.append(
                f"    INSERT INTO bfs_result_{p.n}\n"
                f"    SELECT *, current_depth_{p.n} AS _bfs_depth "
                f"FROM bfs_edges_{p.n};"
            )

        lines.append("  END IF;")
        lines.append("END WHILE;")
        return "\n".join(lines)

    def _tt_bidir_backward_edge_sql(
        self,
        p: _BFSParams,
        bwd_join: str,
        bwd_next: str,
        bwd_excl: str,
    ) -> str:
        """Build backward edge expansion SQL for temp_tables bidir.

        Handles both single-table and multi-table edge schemas.
        Returns only _next_node (no result columns needed).
        """
        if not p.enriched.single_table:
            parts: list[str] = []
            for edge_info in p.enriched.edge_tables:
                table_name = (
                    edge_info.table_descriptor.full_table_name
                )
                where_parts: list[str] = [
                    f"{bwd_excl} NOT IN "
                    f"(SELECT node FROM bfs_bwd_visited_{p.n})"
                ]
                if edge_info.filter_clause:
                    where_parts.append(edge_info.filter_clause)
                if p.edge_predicate:
                    where_parts.append(p.edge_predicate)
                parts.append(
                    f"SELECT DISTINCT {bwd_next} AS _next_node\n"
                    f"FROM {table_name} e\n"
                    f"INNER JOIN bfs_bwd_frontier_{p.n} f "
                    f"ON {bwd_join}"
                    + self._build_where_clause(where_parts)
                )
            return "\nUNION ALL\n".join(parts)

        where_parts = [
            f"{bwd_excl} NOT IN "
            f"(SELECT node FROM bfs_bwd_visited_{p.n})"
        ]
        if p.edge_type_filter:
            where_parts.append(p.edge_type_filter)
        if p.edge_predicate:
            where_parts.append(p.edge_predicate)
        return (
            f"SELECT DISTINCT {bwd_next} AS _next_node\n"
            f"FROM {p.edge_table_sql} e\n"
            f"INNER JOIN bfs_bwd_frontier_{p.n} f "
            f"ON {bwd_join}"
            + self._build_where_clause(where_parts)
        )

    def _tt_bidir_forward_edge_sql(
        self,
        p: _BFSParams,
        extra_where: list[str] | None = None,
    ) -> str:
        """Build forward edge expansion SQL for temp_tables bidir.

        Handles both single-table and multi-table edge schemas.
        Optionally adds extra_where conditions (e.g. pruning).
        """
        join_cond, next_node_expr, visited_excl = (
            self._resolve_direction(
                p.src_col, p.dst_col,
                p.is_backward, p.is_undirected,
            )
        )
        prop_select = "".join(
            f", e.{c}" for c in p.edge_prop_cols
        )
        extra = extra_where or []

        if not p.enriched.single_table:
            parts: list[str] = []
            for edge_info in p.enriched.edge_tables:
                table_name = (
                    edge_info.table_descriptor.full_table_name
                )
                where_parts: list[str] = [
                    f"{visited_excl} NOT IN "
                    f"(SELECT node FROM bfs_visited_{p.n})"
                ]
                if edge_info.filter_clause:
                    where_parts.append(edge_info.filter_clause)
                if p.edge_predicate:
                    where_parts.append(p.edge_predicate)
                where_parts.extend(extra)
                parts.append(
                    f"SELECT "
                    f"e.{p.src_col}, e.{p.dst_col}"
                    f"{prop_select}, "
                    f"{next_node_expr} AS _next_node\n"
                    f"FROM {table_name} e\n"
                    f"INNER JOIN bfs_frontier_{p.n} f "
                    f"ON {join_cond}"
                    + self._build_where_clause(where_parts)
                )
            return "\nUNION ALL\n".join(parts)

        where_parts = [
            f"{visited_excl} NOT IN "
            f"(SELECT node FROM bfs_visited_{p.n})"
        ]
        if p.edge_type_filter:
            where_parts.append(p.edge_type_filter)
        if p.edge_predicate:
            where_parts.append(p.edge_predicate)
        where_parts.extend(extra)
        return (
            f"SELECT "
            f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
            f"{next_node_expr} AS _next_node\n"
            f"FROM {p.edge_table_sql} e\n"
            f"INNER JOIN bfs_frontier_{p.n} f "
            f"ON {join_cond}"
            + self._build_where_clause(where_parts)
        )

    # ======================================================================
    # STRATEGY: numbered_views (PySpark 4.2)
    # ======================================================================

    def _render_numbered_views(self, p: _BFSParams) -> tuple[str, str]:
        """Render procedural BFS using EXECUTE IMMEDIATE + numbered views."""
        declarations = self._nv_declarations(p)

        body_parts: list[str] = []
        body_parts.append(self._nv_frontier_init(p))
        body_parts.append(self._nv_visited_init(p))
        body_parts.append(self._nv_while_loop(p))
        body_parts.append(self._nv_final_view(p))

        return declarations, "\n\n".join(body_parts)

    def _nv_declarations(self, p: _BFSParams) -> str:
        """DECLARE statements for numbered_views strategy."""
        return (
            f"DECLARE bfs_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE bfs_frontier_count_{p.n} BIGINT DEFAULT 1;\n"
            f"DECLARE bfs_union_sql_{p.n} STRING DEFAULT '';"
        )

    def _nv_frontier_init(self, p: _BFSParams) -> str:
        """Create frontier_0 view."""
        where = self._build_start_where(p.node_type_filter, p.start_filter)

        return (
            f"CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_{p.n}_0 AS\n"
            f"SELECT n.{p.node_id_col} AS node\n"
            f"FROM {p.node_table} n{where};"
        )

    def _nv_visited_init(self, p: _BFSParams) -> str:
        """Create visited_0 view from frontier_0."""
        return (
            f"CREATE OR REPLACE TEMPORARY VIEW bfs_visited_{p.n}_0 AS\n"
            f"SELECT node FROM bfs_frontier_{p.n}_0;"
        )

    def _nv_edge_expansion_sql(self, p: _BFSParams) -> str:
        """Build edge expansion SELECT for numbered_views (quotes doubled for EXECUTE IMMEDIATE)."""
        join_cond, next_node_expr, visited_excl = self._resolve_direction(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)

        if not p.enriched.single_table:
            return self._nv_multi_table_edge_expansion(
                p, join_cond, next_node_expr, visited_excl,
            )

        # WHERE conditions (inside EXECUTE IMMEDIATE — quotes must be doubled)
        where_parts: list[str] = []
        where_parts.append(
            f"{visited_excl} NOT IN ("
            f"SELECT node FROM bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || ')"
        )
        if p.edge_type_filter:
            escaped_filter = p.edge_type_filter.replace("'", "''")
            where_parts.append(escaped_filter)
        if p.edge_predicate:
            escaped_pred = p.edge_predicate.replace("'", "''")
            where_parts.append(escaped_pred)

        where_clause = " AND ".join(where_parts)

        return (
            f"SELECT "
            f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
            f"{next_node_expr} AS _next_node, "
            f"' || CAST(bfs_depth_{p.n} AS STRING) || ' AS _bfs_depth "
            f"FROM {p.edge_table_sql} e "
            f"INNER JOIN bfs_frontier_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || ' f "
            f"ON {join_cond} "
            f"WHERE {where_clause}"
        )

    def _nv_multi_table_edge_expansion(
        self,
        p: _BFSParams,
        join_cond: str,
        next_node_expr: str,
        visited_excl: str,
    ) -> str:
        """UNION ALL edge expansion for multiple edge tables (numbered_views)."""
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)
        parts: list[str] = []

        base_visited = (
            f"{visited_excl} NOT IN ("
            f"SELECT node FROM bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || ')"
        )

        for edge_info in p.enriched.edge_tables:
            table_name = edge_info.table_descriptor.full_table_name
            where_parts: list[str] = [base_visited]
            if edge_info.filter_clause:
                escaped = edge_info.filter_clause.replace("'", "''")
                where_parts.append(escaped)
            if p.edge_predicate:
                escaped_pred = p.edge_predicate.replace("'", "''")
                where_parts.append(escaped_pred)

            where_clause = " AND ".join(where_parts)

            parts.append(
                f"SELECT "
                f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                f"{next_node_expr} AS _next_node, "
                f"' || CAST(bfs_depth_{p.n} AS STRING) || ' AS _bfs_depth "
                f"FROM {table_name} e "
                f"INNER JOIN bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || ' f "
                f"ON {join_cond} "
                f"WHERE {where_clause}"
            )

        return " UNION ALL ".join(parts)

    def _nv_while_loop(self, p: _BFSParams) -> str:
        """Build WHILE loop for numbered_views strategy."""
        lines: list[str] = []
        lines.append(
            f"WHILE bfs_frontier_count_{p.n} > 0 "
            f"AND bfs_depth_{p.n} < {p.max_hops} DO"
        )
        lines.append(f"  SET bfs_depth_{p.n} = bfs_depth_{p.n} + 1;")
        lines.append("")

        # A. Edge expansion (inside EXECUTE IMMEDIATE)
        edge_sql = self._nv_edge_expansion_sql(p)
        lines.append(
            f"  EXECUTE IMMEDIATE\n"
            f"    'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
            f"     {edge_sql}';"
        )
        lines.append("")

        # B. Check frontier size (SET via EXECUTE IMMEDIATE)
        lines.append(
            f"  EXECUTE IMMEDIATE\n"
            f"    'SET bfs_frontier_count_{p.n} = (SELECT COUNT(1)"
            f" FROM bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ')';"
        )
        lines.append("")

        # C. Update visited, frontier, union (only if edges found)
        lines.append(f"  IF bfs_frontier_count_{p.n} > 0 THEN")
        lines.append("")

        # Update visited
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
            f"       SELECT node FROM bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || '\n"
            f"       UNION\n"
            f"       SELECT DISTINCT _next_node AS node\n"
            f"       FROM bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("")

        # Update frontier
        if p.barrier_predicate and p.barrier_node_table:
            escaped_barrier = _build_bfs_barrier_where_escaped(
                p, "_next_node",
            )
            lines.append(
                f"    EXECUTE IMMEDIATE\n"
                f"      'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
                f"       SELECT DISTINCT _next_node AS node\n"
                f"       FROM bfs_edges_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || '\n"
                f"       WHERE {escaped_barrier}';"
            )
        else:
            lines.append(
                f"    EXECUTE IMMEDIATE\n"
                f"      'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
                f"       SELECT DISTINCT _next_node AS node\n"
                f"       FROM bfs_edges_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING);"
            )
        lines.append("")

        # D. Build UNION ALL incrementally (only for levels >= min_hops)
        prop_cols = "".join(f", {c}" for c in p.edge_prop_cols)
        union_select = (
            f"SELECT {p.src_col}, {p.dst_col}{prop_cols}, "
            f"_next_node AS end_node, _bfs_depth AS depth "
            f"FROM bfs_edges_{p.n}_"
        )

        lines.append(f"    IF bfs_depth_{p.n} >= {p.min_hops} THEN")
        lines.append(f"      IF bfs_union_sql_{p.n} = '' THEN")
        lines.append(
            f"        SET bfs_union_sql_{p.n} =\n"
            f"          '{union_select}'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("      ELSE")
        lines.append(
            f"        SET bfs_union_sql_{p.n} = bfs_union_sql_{p.n}\n"
            f"          || ' UNION ALL "
            f"{union_select}'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("      END IF;")
        lines.append("    END IF;")
        lines.append("")
        lines.append("  END IF;")
        lines.append("END WHILE;")

        return "\n".join(lines)

    def _nv_final_view(self, p: _BFSParams) -> str:
        """Build final result view for numbered_views strategy."""
        prop_cols = "".join(f", r.{c}" for c in p.edge_prop_cols)

        # path_edges: ARRAY(NAMED_STRUCT(...)) wrapping one edge per row
        # Inside EXECUTE IMMEDIATE, single quotes must be doubled.
        path_edges_col = ""
        path_edges_null = ""
        if p.collect_edges:
            struct_parts = [
                f"''{p.src_col}'', r.{p.src_col}",
                f"''{p.dst_col}'', r.{p.dst_col}",
            ]
            for prop in p.edge_prop_cols:
                struct_parts.append(f"''{prop}'', r.{prop}")
            nv_struct = f"NAMED_STRUCT({', '.join(struct_parts)})"
            path_edges_col = (
                f",\n            ARRAY({nv_struct})"
                " AS path_edges"
            )
            path_edges_null = (
                ",\n    CAST(NULL AS ARRAY<STRUCT<"
                f"{p.src_col}: STRING, {p.dst_col}: STRING"
                + "".join(f", {c}: STRING" for c in p.edge_prop_cols)
                + ">>) AS path_edges"
            )

        # Only emit path column when collect_nodes is True
        path_col = ""
        path_null = ""
        if p.collect_nodes:
            path_col = (
                ",\n            CAST(NULL AS ARRAY<STRING>)"
                " AS path"
            )
            path_null = (
                ",\n    CAST(NULL AS ARRAY<STRING>) AS path"
            )

        return (
            f"IF bfs_union_sql_{p.n} != '' THEN\n"
            f"  EXECUTE IMMEDIATE\n"
            f"    'CREATE OR REPLACE TEMPORARY VIEW {p.cte_name} AS\n"
            f"     SELECT f0.node AS start_node, r.end_node, r.depth,\n"
            f"            r.{p.src_col}, r.{p.dst_col}{prop_cols}"
            f"{path_edges_col}"
            f"{path_col}\n"
            f"     FROM (' || bfs_union_sql_{p.n} || ') r\n"
            f"     CROSS JOIN bfs_frontier_{p.n}_0 f0';\n"
            f"ELSE\n"
            f"  CREATE OR REPLACE TEMPORARY VIEW {p.cte_name} AS\n"
            f"  SELECT\n"
            f"    CAST(NULL AS STRING) AS start_node,\n"
            f"    CAST(NULL AS STRING) AS end_node,\n"
            f"    CAST(NULL AS INT) AS depth,\n"
            f"    CAST(NULL AS STRING) AS {p.src_col},\n"
            f"    CAST(NULL AS STRING) AS {p.dst_col}"
            + "".join(
                f",\n    CAST(NULL AS STRING) AS {c}" for c in p.edge_prop_cols
            )
            + path_edges_null
            + path_null
            + "\n  WHERE 1 = 0;\n"
            "END IF;"
        )

    # ======================================================================
    # STRATEGY: numbered_views — BIDIRECTIONAL
    # ======================================================================

    def _render_bidir_numbered_views(
        self, p: _BFSParams
    ) -> tuple[str, str]:
        """Render bidirectional procedural BFS using numbered views.

        Phase 1: Backward BFS from target → reachable set view
        Phase 2: Forward BFS from source with pruning after depth_forward
        Phase 3: Final view (same schema as unidirectional)
        """
        decl_parts: list[str] = []
        # Forward declarations (same as unidirectional)
        decl_parts.append(
            f"DECLARE bfs_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE bfs_frontier_count_{p.n} BIGINT DEFAULT 1;\n"
            f"DECLARE bfs_union_sql_{p.n} STRING DEFAULT '';"
        )
        # Backward declarations
        decl_parts.append(
            f"DECLARE bwd_depth_{p.n} INT DEFAULT 0;\n"
            f"DECLARE bwd_frontier_count_{p.n} BIGINT DEFAULT 1;\n"
            f"DECLARE bwd_union_sql_{p.n} STRING DEFAULT '';"
        )
        declarations = "\n".join(decl_parts)

        body_parts: list[str] = []
        body_parts.append(self._nv_bidir_backward_phase(p))
        body_parts.append(self._nv_frontier_init(p))
        body_parts.append(self._nv_visited_init(p))
        body_parts.append(self._nv_bidir_forward_loop(p))
        body_parts.append(self._nv_final_view(p))

        return declarations, "\n\n".join(body_parts)

    def _nv_bidir_backward_phase(self, p: _BFSParams) -> str:
        """Phase 1: Backward BFS from target using numbered views."""
        bwd_is_backward = not p.is_backward
        if p.is_undirected:
            bwd_is_backward = False

        bwd_join, bwd_next, bwd_excl = self._resolve_direction(
            p.src_col, p.dst_col, bwd_is_backward, p.is_undirected,
        )

        tgt_where = self._build_start_where(
            p.bidir_target_type_filter, p.bidir_target_filter,
        )

        lines: list[str] = []

        # Initial backward frontier and visited
        # Table alias must be 'tgt' to match sink_filter_as_tgt rewrite
        lines.append(
            f"CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_frontier_{p.n}_0 AS\n"
            f"SELECT tgt.{p.bidir_target_id_col} AS node\n"
            f"FROM {p.bidir_target_table} tgt{tgt_where};"
        )
        lines.append(
            f"CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_visited_{p.n}_0 AS\n"
            f"SELECT node FROM bfs_bwd_frontier_{p.n}_0;"
        )
        lines.append("")

        # Backward WHILE loop
        lines.append(
            f"WHILE bwd_frontier_count_{p.n} > 0 "
            f"AND bwd_depth_{p.n} < "
            f"{p.bidir_depth_backward} DO"
        )
        lines.append(
            f"  SET bwd_depth_{p.n} = bwd_depth_{p.n} + 1;"
        )
        lines.append("")

        # Edge expansion via EXECUTE IMMEDIATE
        bwd_edge_sql = self._nv_bidir_backward_edge_sql(
            p, bwd_join, bwd_next, bwd_excl,
        )
        lines.append(
            f"  EXECUTE IMMEDIATE\n"
            f"    'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_edges_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING) || ' AS\n"
            f"     {bwd_edge_sql}';"
        )
        lines.append("")

        # Count
        lines.append(
            f"  EXECUTE IMMEDIATE\n"
            f"    'SET bwd_frontier_count_{p.n} = "
            f"(SELECT COUNT(1)"
            f" FROM bfs_bwd_edges_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING) || ')';"
        )
        lines.append("")

        # Update visited, frontier
        lines.append(f"  IF bwd_frontier_count_{p.n} > 0 THEN")
        lines.append("")

        # Visited = prev_visited UNION new edges
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_visited_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING) || ' AS\n"
            f"       SELECT node FROM bfs_bwd_visited_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} - 1 AS STRING) || '\n"
            f"       UNION\n"
            f"       SELECT DISTINCT _next_node AS node\n"
            f"       FROM bfs_bwd_edges_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING);"
        )
        lines.append("")

        # Frontier
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_frontier_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING) || ' AS\n"
            f"       SELECT DISTINCT _next_node AS node\n"
            f"       FROM bfs_bwd_edges_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} AS STRING);"
        )
        lines.append("")

        # Build UNION for reachable set
        # bfs_bwd_edges has _next_node, alias to node for UNION compat
        bwd_select = (
            f"SELECT _next_node AS node FROM bfs_bwd_edges_{p.n}_"
        )
        lines.append(
            f"    IF bwd_union_sql_{p.n} = '' THEN"
        )
        lines.append(
            f"      SET bwd_union_sql_{p.n} =\n"
            f"        '{bwd_select}'"
            f" || CAST(bwd_depth_{p.n} AS STRING);"
        )
        lines.append("    ELSE")
        lines.append(
            f"      SET bwd_union_sql_{p.n} = "
            f"bwd_union_sql_{p.n}\n"
            f"        || ' UNION "
            f"{bwd_select}'"
            f" || CAST(bwd_depth_{p.n} AS STRING);"
        )
        lines.append("    END IF;")
        lines.append("")

        lines.append("  END IF;")
        lines.append("END WHILE;")
        lines.append("")

        # Create reachable set view: initial target + all backward edges
        lines.append(
            f"IF bwd_union_sql_{p.n} != '' THEN\n"
            f"  EXECUTE IMMEDIATE\n"
            f"    'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_reachable_{p.n} AS\n"
            f"     SELECT node FROM bfs_bwd_frontier_{p.n}_0\n"
            f"     UNION ' || bwd_union_sql_{p.n};\n"
            f"ELSE\n"
            f"  CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_bwd_reachable_{p.n} AS\n"
            f"  SELECT node FROM bfs_bwd_frontier_{p.n}_0;\n"
            f"END IF;"
        )

        return "\n".join(lines)

    def _nv_bidir_forward_loop(self, p: _BFSParams) -> str:
        """Phase 2: Forward BFS with pruning via numbered views.

        After depth_forward, adds pruning condition:
        _next_node IN (SELECT node FROM bfs_bwd_reachable_{n})
        """
        join_cond, next_node_expr, _ = self._resolve_direction(
            p.src_col, p.dst_col,
            p.is_backward, p.is_undirected,
        )

        prune_cond = (
            f"{next_node_expr} IN "
            f"(SELECT node FROM bfs_bwd_reachable_{p.n})"
        )

        pruned_sql = self._nv_bidir_forward_edge_sql(
            p, extra_where=[prune_cond],
        )
        unpruned_sql = self._nv_bidir_forward_edge_sql(p)

        lines: list[str] = []
        lines.append(
            f"WHILE bfs_frontier_count_{p.n} > 0 "
            f"AND bfs_depth_{p.n} < {p.max_hops} DO"
        )
        lines.append(
            f"  SET bfs_depth_{p.n} = bfs_depth_{p.n} + 1;"
        )
        lines.append("")

        # IF/ELSE: prune after depth_forward
        lines.append(
            f"  IF bfs_depth_{p.n} > "
            f"{p.bidir_depth_forward} THEN"
        )
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
            f"       {pruned_sql}';"
        )
        lines.append("  ELSE")
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
            f"       {unpruned_sql}';"
        )
        lines.append("  END IF;")
        lines.append("")

        # Count
        lines.append(
            f"  EXECUTE IMMEDIATE\n"
            f"    'SET bfs_frontier_count_{p.n} = "
            f"(SELECT COUNT(1)"
            f" FROM bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ')';"
        )
        lines.append("")

        # Update visited, frontier, union (same as unidirectional)
        lines.append(
            f"  IF bfs_frontier_count_{p.n} > 0 THEN"
        )
        lines.append("")

        # Visited
        lines.append(
            f"    EXECUTE IMMEDIATE\n"
            f"      'CREATE OR REPLACE TEMPORARY VIEW "
            f"bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
            f"       SELECT node FROM bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || '\n"
            f"       UNION\n"
            f"       SELECT DISTINCT _next_node AS node\n"
            f"       FROM bfs_edges_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("")

        # Frontier
        if p.barrier_predicate and p.barrier_node_table:
            escaped_barrier = _build_bfs_barrier_where_escaped(
                p, "_next_node",
            )
            lines.append(
                f"    EXECUTE IMMEDIATE\n"
                f"      'CREATE OR REPLACE TEMPORARY VIEW "
                f"bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
                f"       SELECT DISTINCT _next_node AS node\n"
                f"       FROM bfs_edges_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || '\n"
                f"       WHERE {escaped_barrier}';"
            )
        else:
            lines.append(
                f"    EXECUTE IMMEDIATE\n"
                f"      'CREATE OR REPLACE TEMPORARY VIEW "
                f"bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING) || ' AS\n"
                f"       SELECT DISTINCT _next_node AS node\n"
                f"       FROM bfs_edges_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING);"
            )
        lines.append("")

        # Build UNION ALL incrementally
        prop_cols = "".join(f", {c}" for c in p.edge_prop_cols)
        union_select = (
            f"SELECT {p.src_col}, {p.dst_col}{prop_cols}, "
            f"_next_node AS end_node, _bfs_depth AS depth "
            f"FROM bfs_edges_{p.n}_"
        )

        lines.append(
            f"    IF bfs_depth_{p.n} >= {p.min_hops} THEN"
        )
        lines.append(
            f"      IF bfs_union_sql_{p.n} = '' THEN"
        )
        lines.append(
            f"        SET bfs_union_sql_{p.n} =\n"
            f"          '{union_select}'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("      ELSE")
        lines.append(
            f"        SET bfs_union_sql_{p.n} = "
            f"bfs_union_sql_{p.n}\n"
            f"          || ' UNION ALL "
            f"{union_select}'"
            f" || CAST(bfs_depth_{p.n} AS STRING);"
        )
        lines.append("      END IF;")
        lines.append("    END IF;")
        lines.append("")
        lines.append("  END IF;")
        lines.append("END WHILE;")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # numbered_views bidir helpers (multi-table safe)
    # ------------------------------------------------------------------

    def _nv_bidir_backward_edge_sql(
        self,
        p: _BFSParams,
        bwd_join: str,
        bwd_next: str,
        bwd_excl: str,
    ) -> str:
        """Build backward edge expansion SQL for nv bidir.

        Handles single-table and multi-table. Quotes doubled
        for EXECUTE IMMEDIATE.
        """
        bwd_visited_ref = (
            f"{bwd_excl} NOT IN ("
            f"SELECT node FROM bfs_bwd_visited_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} - 1"
            f" AS STRING) || ')"
        )
        frontier_ref = (
            f"bfs_bwd_frontier_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} - 1"
            f" AS STRING) || '"
        )

        if not p.enriched.single_table:
            parts: list[str] = []
            for ei in p.enriched.edge_tables:
                tbl = ei.table_descriptor.full_table_name
                wp: list[str] = [bwd_visited_ref]
                if ei.filter_clause:
                    wp.append(
                        ei.filter_clause.replace("'", "''")
                    )
                if p.edge_predicate:
                    wp.append(
                        p.edge_predicate.replace("'", "''")
                    )
                parts.append(
                    f"SELECT DISTINCT "
                    f"{bwd_next} AS _next_node "
                    f"FROM {tbl} e "
                    f"INNER JOIN {frontier_ref} f "
                    f"ON {bwd_join} "
                    f"WHERE {' AND '.join(wp)}"
                )
            return " UNION ALL ".join(parts)

        wp = [bwd_visited_ref]
        if p.edge_type_filter:
            wp.append(
                p.edge_type_filter.replace("'", "''")
            )
        if p.edge_predicate:
            wp.append(
                p.edge_predicate.replace("'", "''")
            )
        return (
            f"SELECT DISTINCT {bwd_next} AS _next_node "
            f"FROM {p.edge_table_sql} e "
            f"INNER JOIN {frontier_ref} f "
            f"ON {bwd_join} "
            f"WHERE {' AND '.join(wp)}"
        )

    def _nv_bidir_forward_edge_sql(
        self,
        p: _BFSParams,
        extra_where: list[str] | None = None,
    ) -> str:
        """Build forward edge expansion SQL for nv bidir.

        Handles single-table and multi-table. Quotes doubled
        for EXECUTE IMMEDIATE.
        """
        join_cond, next_node_expr, visited_excl = (
            self._resolve_direction(
                p.src_col, p.dst_col,
                p.is_backward, p.is_undirected,
            )
        )
        prop_select = "".join(
            f", e.{c}" for c in p.edge_prop_cols
        )
        extra = extra_where or []

        base_visited = (
            f"{visited_excl} NOT IN ("
            f"SELECT node FROM bfs_visited_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1"
            f" AS STRING) || ')"
        )
        depth_expr = (
            f"' || CAST(bfs_depth_{p.n} AS STRING) || '"
        )
        frontier_ref = (
            f"bfs_frontier_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1"
            f" AS STRING) || '"
        )

        if not p.enriched.single_table:
            parts: list[str] = []
            for ei in p.enriched.edge_tables:
                tbl = ei.table_descriptor.full_table_name
                wp: list[str] = [base_visited]
                if ei.filter_clause:
                    wp.append(
                        ei.filter_clause.replace("'", "''")
                    )
                if p.edge_predicate:
                    wp.append(
                        p.edge_predicate.replace("'", "''")
                    )
                wp.extend(extra)
                parts.append(
                    f"SELECT "
                    f"e.{p.src_col}, e.{p.dst_col}"
                    f"{prop_select}, "
                    f"{next_node_expr} AS _next_node, "
                    f"{depth_expr} AS _bfs_depth "
                    f"FROM {tbl} e "
                    f"INNER JOIN {frontier_ref} f "
                    f"ON {join_cond} "
                    f"WHERE {' AND '.join(wp)}"
                )
            return " UNION ALL ".join(parts)

        wp = [base_visited]
        if p.edge_type_filter:
            wp.append(
                p.edge_type_filter.replace("'", "''")
            )
        if p.edge_predicate:
            wp.append(
                p.edge_predicate.replace("'", "''")
            )
        wp.extend(extra)
        return (
            f"SELECT "
            f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
            f"{next_node_expr} AS _next_node, "
            f"{depth_expr} AS _bfs_depth "
            f"FROM {p.edge_table_sql} e "
            f"INNER JOIN {frontier_ref} f "
            f"ON {join_cond} "
            f"WHERE {' AND '.join(wp)}"
        )
