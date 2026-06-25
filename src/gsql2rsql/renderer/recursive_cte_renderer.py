"""Recursive CTE renderer — WITH RECURSIVE generation for variable-length paths.

Handles rendering of recursive CTEs for BFS/DFS variable-length path
traversals, bidirectional BFS (both recursive and unrolled variants),
edge table lookups, filter clause generation, and aggregation boundary CTEs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
)
from gsql2rsql.parser.ast import RelationshipDirection
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    RecursiveTraversalOperator,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
    from gsql2rsql.renderer.render_context import RenderContext
    from gsql2rsql.renderer.schema_provider import SQLTableDescriptor
    from gsql2rsql.renderer.sql_enrichment import EnrichedRecursiveOp


def _build_barrier_not_exists(
    barrier_filter_sql: str,
    barrier_node_table: str,
    barrier_node_id_col: str,
    barrier_node_type_filter: str | None,
    node_col: str,
) -> str:
    """Build NOT EXISTS subquery for barrier nodes.

    Returns SQL like:
        NOT EXISTS (SELECT 1 FROM nodes barrier
                    WHERE barrier.node_id = <node_col>
                      AND barrier.node_type = 'Station'
                      AND barrier.is_hub = true)
    """
    parts = [f"barrier.{barrier_node_id_col} = {node_col}"]
    if barrier_node_type_filter:
        parts.append(f"barrier.{barrier_node_type_filter}")
    parts.append(f"({barrier_filter_sql})")
    inner_where = " AND ".join(parts)
    return (
        f"NOT EXISTS (SELECT 1 FROM {barrier_node_table} barrier "
        f"WHERE {inner_where})"
    )


@dataclass
class EdgeInfo:
    """Parameter object replacing closure-captured variables in CTE generation.

    Holds all resolved edge table information, column names, filters, and
    directional flags needed by the base-case and recursive-case builders.
    """

    cte_name: str
    edge_tables: list[tuple[str, "SQLTableDescriptor"]]
    source_id_col: str
    target_id_col: str
    edge_props: list[str]
    single_table: bool
    single_table_name: str | None
    single_table_filter: str | None
    min_depth: int
    max_depth: int
    is_backward: bool
    needs_union_for_undirected: bool
    edge_filter_sql: str | None = None
    source_node_filter_sql: str | None = None
    source_node_table: "SQLTableDescriptor | None" = None
    barrier_filter_sql: str | None = None
    barrier_node_table: str | None = None
    barrier_node_id_col: str | None = None
    barrier_node_type_filter: str | None = None


@dataclass
class BidirectionalConfig:
    """Direction-specific parameters for one half of a bidirectional BFS.

    Captures all the values that differ between the forward and backward CTEs,
    so that shared builder methods can emit either direction without duplication.
    """

    cte_prefix: str  # "forward" or "backward"
    cte_short: str  # "fwd" or "bwd" (for unrolling CTE names)
    cte_var: str  # "f" or "b"
    traverse_col: str  # Column to JOIN on (edge_src for fwd, edge_dst for bwd)
    arrive_col: str  # Column arrived at (edge_dst for fwd, edge_src for bwd)
    node_table_name: str | None  # Full table name for source/target node
    node_alias: str  # "src" or "tgt"
    node_id_col: str
    filter_sql: str | None  # Pre-rendered node filter SQL
    depth_bound: int
    prepend_path: bool  # False for forward (append), True for backward (prepend)


class RecursiveCTERenderer:
    """Renders WITH RECURSIVE CTEs for variable-length path traversal.

    Receives a ``RenderContext`` for shared state and an ``ExpressionRenderer``
    for rendering edge filter expressions within CTE bodies.
    """

    def __init__(
        self,
        ctx: "RenderContext",
        expr: "ExpressionRenderer",
        render_operator_fn: Callable[..., str] | None = None,
    ) -> None:
        self._ctx = ctx
        self._expr = expr
        self._render_operator: Callable[..., str] | None = render_operator_fn

    @staticmethod
    def _build_where_clause(
        *conditions: str | None,
        indent: str = "    ",
    ) -> str | None:
        """Build a single-line WHERE clause from non-None conditions.

        Returns None when all conditions are None/empty, so callers can
        conditionally append the result.
        """
        parts = [c for c in conditions if c]
        if not parts:
            return None
        return f"{indent}WHERE {' AND '.join(parts)}"

    def _get_enriched_recursive(
        self, op: RecursiveTraversalOperator
    ) -> EnrichedRecursiveOp | None:
        """Get enriched data for a RecursiveTraversalOperator."""
        if self._ctx.enriched:
            return self._ctx.enriched.recursive_ops.get(
                op.operator_debug_id
            )
        return None

    def render_recursive_cte(self, op: RecursiveTraversalOperator) -> str:
        """Render a recursive CTE for variable-length path traversal.

        Generates Databricks SQL WITH RECURSIVE for BFS/DFS traversal.
        Supports multiple edge types, predicate pushdown (edge and source
        node filters), and undirected traversal via internal UNION ALL.
        """
        # Bidirectional BFS dispatch
        if op.bidirectional_bfs_mode == "recursive":
            return self._render_bidirectional_recursive_cte(op)
        elif op.bidirectional_bfs_mode == "unrolling":
            return self._render_bidirectional_unrolling_cte(op)

        # Resolve edge tables and build EdgeInfo parameter object
        ei = self._resolve_edge_tables(op)

        # Resolve filter clauses (edge predicate pushdown + source node filter)
        self._resolve_filter_clauses(op, ei)

        # Assemble CTE
        lines: list[str] = []
        lines.append(f"  {ei.cte_name} AS (")

        # Zero-length path base case (min_depth == 0)
        if ei.min_depth == 0:
            self._append_zero_length_base_case(op, ei, lines)

        # Base case: direct edges (depth = 1)
        lines.append("    -- Base case: direct edges (depth = 1)")
        self._append_base_cases(op, ei, lines)

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        lines.append("    -- Recursive case: extend paths")

        # Recursive case: extend paths
        self._append_recursive_cases(op, ei, lines)

        lines.append("  )")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # _render_recursive_cte sub-methods
    # ------------------------------------------------------------------

    def _resolve_edge_tables(
        self, op: RecursiveTraversalOperator
    ) -> EdgeInfo:
        """Build EdgeInfo from pre-resolved enrichment data.

        CTE naming (cte_counter, op.cte_name) remains a rendering concern.
        All db_schema lookups are replaced by reads from enriched.recursive_ops.
        """
        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        enriched_rec = self._get_enriched_recursive(op)
        if not enriched_rec:
            raise TranspilerInternalErrorException(
                "No enriched data for RecursiveTraversalOperator "
                f"(edge_types={op.edge_types})"
            )

        edge_tables: list[tuple[str, SQLTableDescriptor]] = [
            (et.edge_type, et.table_descriptor)
            for et in enriched_rec.edge_tables
        ]

        if not edge_tables:
            edge_type_str = "|".join(op.edge_types or [])
            raise TranspilerInternalErrorException(
                f"No table descriptor for edges: {edge_type_str}"
            )

        return EdgeInfo(
            cte_name=cte_name,
            edge_tables=edge_tables,
            source_id_col=enriched_rec.source_id_col,
            target_id_col=enriched_rec.target_id_col,
            edge_props=[
                p for p in enriched_rec.edge_property_names
                if p not in (enriched_rec.source_id_col, enriched_rec.target_id_col)
            ],
            single_table=enriched_rec.single_table,
            single_table_name=enriched_rec.single_table_name,
            single_table_filter=enriched_rec.single_table_filter,
            min_depth=op.min_hops if op.min_hops is not None else 1,
            max_depth=op.max_hops or 10,
            is_backward=op.swap_source_sink,
            needs_union_for_undirected=op.use_internal_union_for_bidirectional,
        )

    def _resolve_filter_clauses(
        self, op: RecursiveTraversalOperator, ei: EdgeInfo
    ) -> None:
        """Resolve edge and source-node filter clauses into *ei*."""
        enriched_rec = self._get_enriched_recursive(op)
        if not enriched_rec:
            return

        # Edge predicate pushdown
        if enriched_rec.edge_filter_as_e:
            ei.edge_filter_sql = self._expr.render_edge_filter_expression(
                enriched_rec.edge_filter_as_e
            )

        # Source node filter pushdown
        if enriched_rec.start_filter_as_src and enriched_rec.source_node:
            ei.source_node_filter_sql = (
                self._expr.render_edge_filter_expression(
                    enriched_rec.start_filter_as_src
                )
            )
            ei.source_node_table = enriched_rec.source_node.table_descriptor

        # Barrier filter (is_terminator directive)
        if enriched_rec.barrier_filter_as_barrier:
            ei.barrier_filter_sql = (
                self._expr.render_edge_filter_expression(
                    enriched_rec.barrier_filter_as_barrier
                )
            )
            if enriched_rec.target_node:
                td = enriched_rec.target_node.table_descriptor
                ei.barrier_node_table = td.full_table_name
                ei.barrier_node_id_col = enriched_rec.target_node.id_column
                ei.barrier_node_type_filter = td.filter

    def _build_edge_struct(self, ei: EdgeInfo, alias: str = "e") -> str:
        """Build NAMED_STRUCT expression for an edge with its properties."""
        struct_parts = [
            f"'{ei.source_id_col}', {alias}.{ei.source_id_col}",
            f"'{ei.target_id_col}', {alias}.{ei.target_id_col}",
        ]
        for prop in ei.edge_props:
            struct_parts.append(f"'{prop}', {alias}.{prop}")
        return f"NAMED_STRUCT({', '.join(struct_parts)})"

    def _append_zero_length_base_case(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append zero-length path base case (depth = 0) to *lines*."""
        enriched_rec = self._get_enriched_recursive(op)
        source_node = (
            enriched_rec.source_node if enriched_rec else None
        )
        if not source_node:
            raise TranspilerInternalErrorException(
                f"No enriched source node for: {op.source_node_type}"
            )
        source_table = source_node.table_descriptor

        lines.append("    -- Base case: Zero-length paths (depth = 0)")
        lines.append("    SELECT")
        lines.append(f"      n.{op.source_id_column} AS start_node,")
        lines.append(f"      n.{op.source_id_column} AS end_node,")
        lines.append("      0 AS depth,")
        if op.collect_nodes:
            lines.append(
                f"      ARRAY(n.{op.source_id_column}) AS path,"
            )
        if op.collect_edges:
            lines.append("      ARRAY() AS path_edges,")
        lines.append("      ARRAY() AS visited")
        lines.append(f"    FROM {source_table.full_table_name} n")

        enriched_rec = self._get_enriched_recursive(op)
        if enriched_rec and enriched_rec.start_filter_as_n:
            filter_sql = self._expr.render_edge_filter_expression(
                enriched_rec.start_filter_as_n
            )
            lines.append(f"    WHERE {filter_sql}")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")

    def _build_base_case_select(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        table_name: str,
        src_col: str,
        dst_col: str,
        filter_clause: str | None,
        direction_label: str = "",
    ) -> str:
        """Build a single base case SELECT for one direction."""
        base_sql: list[str] = []
        if direction_label:
            base_sql.append(f"    -- {direction_label}")
        base_sql.append("    SELECT")
        base_sql.append(f"      e.{src_col} AS start_node,")
        base_sql.append(f"      e.{dst_col} AS end_node,")
        base_sql.append("      1 AS depth,")
        if op.collect_nodes:
            base_sql.append(
                f"      ARRAY(e.{src_col}, e.{dst_col}) AS path,"
            )
        if op.collect_edges:
            base_sql.append(f"      ARRAY({self._build_edge_struct(ei)}) AS path_edges,")
        base_sql.append(f"      ARRAY(e.{src_col}) AS visited")
        base_sql.append(f"    FROM {table_name} e")

        if ei.source_node_filter_sql and ei.source_node_table:
            base_sql.append(
                f"    JOIN {ei.source_node_table.full_table_name} src "
                f"ON src.{op.source_id_column} = e.{src_col}"
            )

        where = self._build_where_clause(
            f"({filter_clause})" if filter_clause else None,
            ei.source_node_filter_sql,
            ei.edge_filter_sql,
        )
        if where:
            base_sql.append(where)

        return "\n".join(base_sql)

    def _append_base_cases(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append base case SELECT(s) to *lines*."""
        src = ei.source_id_col
        dst = ei.target_id_col

        if ei.single_table:
            assert ei.single_table_name is not None
            if ei.needs_union_for_undirected:
                fwd = self._build_base_case_select(
                    op, ei, ei.single_table_name, src, dst,
                    ei.single_table_filter, "Forward direction",
                )
                bwd = self._build_base_case_select(
                    op, ei, ei.single_table_name, dst, src,
                    ei.single_table_filter, "Backward direction",
                )
                lines.append("    SELECT * FROM (")
                lines.append(fwd)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(bwd)
                lines.append("    )")
            elif ei.is_backward:
                lines.append(self._build_base_case_select(
                    op, ei, ei.single_table_name, dst, src, ei.single_table_filter,
                ))
            else:
                lines.append(self._build_base_case_select(
                    op, ei, ei.single_table_name, src, dst, ei.single_table_filter,
                ))
        else:
            base_cases: list[str] = []
            for edge_type, edge_table in ei.edge_tables:
                fc = edge_table.filter
                if ei.needs_union_for_undirected:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, src, dst,
                        fc, f"Forward: {edge_type}",
                    ))
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, dst, src,
                        fc, f"Backward: {edge_type}",
                    ))
                elif ei.is_backward:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, dst, src, fc,
                    ))
                else:
                    base_cases.append(self._build_base_case_select(
                        op, ei, edge_table.full_table_name, src, dst, fc,
                    ))
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(base_cases))
            lines.append("    )")

    def _build_recursive_case_select(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        table_name: str,
        join_col: str,
        end_col: str,
        visited_col: str,
        filter_clause: str | None,
        direction_label: str = "",
    ) -> str:
        """Build a single recursive case SELECT for one direction."""
        rec: list[str] = []
        if direction_label:
            rec.append(f"    -- {direction_label}")
        rec.append("    SELECT")
        rec.append("      p.start_node,")
        rec.append(f"      e.{end_col} AS end_node,")
        rec.append("      p.depth + 1 AS depth,")
        if op.collect_nodes:
            rec.append(
                f"      CONCAT(p.path, ARRAY(e.{end_col})) AS path,"
            )
        if op.collect_edges:
            rec.append(
                f"      ARRAY_APPEND(p.path_edges, {self._build_edge_struct(ei)}) AS path_edges,"
            )
        rec.append(f"      CONCAT(p.visited, ARRAY(e.{visited_col})) AS visited")
        rec.append(f"    FROM {ei.cte_name} p")
        rec.append(f"    JOIN {table_name} e")
        rec.append(f"      ON p.end_node = e.{join_col}")

        where_parts = [
            f"p.depth < {ei.max_depth}",
            f"NOT ARRAY_CONTAINS(p.visited, e.{end_col})",
        ]
        if filter_clause:
            where_parts.append(f"({filter_clause})")
        if ei.edge_filter_sql:
            where_parts.append(ei.edge_filter_sql)
        if ei.barrier_filter_sql and ei.barrier_node_table:
            where_parts.append(
                _build_barrier_not_exists(
                    ei.barrier_filter_sql,
                    ei.barrier_node_table,
                    ei.barrier_node_id_col or "id",
                    ei.barrier_node_type_filter,
                    "p.end_node",
                )
            )
        rec.append(f"    WHERE {where_parts[0]}")
        for wp in where_parts[1:]:
            rec.append(f"      AND {wp}")

        return "\n".join(rec)

    def _append_recursive_cases(
        self,
        op: RecursiveTraversalOperator,
        ei: EdgeInfo,
        lines: list[str],
    ) -> None:
        """Append recursive case SELECT(s) to *lines*."""
        src = ei.source_id_col
        dst = ei.target_id_col

        if ei.single_table:
            assert ei.single_table_name is not None
            if ei.needs_union_for_undirected:
                fwd = self._build_recursive_case_select(
                    op, ei, ei.single_table_name, src, dst, src,
                    ei.single_table_filter, "Forward direction",
                )
                bwd = self._build_recursive_case_select(
                    op, ei, ei.single_table_name, dst, src, dst,
                    ei.single_table_filter, "Backward direction",
                )
                lines.append("    SELECT * FROM (")
                lines.append(fwd)
                lines.append("")
                lines.append("      UNION ALL")
                lines.append("")
                lines.append(bwd)
                lines.append("    )")
            elif ei.is_backward:
                lines.append(self._build_recursive_case_select(
                    op, ei, ei.single_table_name, dst, src, dst,
                    ei.single_table_filter,
                ))
            else:
                lines.append(self._build_recursive_case_select(
                    op, ei, ei.single_table_name, src, dst, src,
                    ei.single_table_filter,
                ))
        else:
            recursive_cases: list[str] = []
            for edge_type, edge_table in ei.edge_tables:
                fc = edge_table.filter
                if ei.needs_union_for_undirected:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, src, dst, src,
                        fc, f"Forward: {edge_type}",
                    ))
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, dst, src, dst,
                        fc, f"Backward: {edge_type}",
                    ))
                elif ei.is_backward:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, dst, src, dst, fc,
                    ))
                else:
                    recursive_cases.append(self._build_recursive_case_select(
                        op, ei, edge_table.full_table_name, src, dst, src, fc,
                    ))
            lines.append("    SELECT * FROM (")
            lines.append("\n      UNION ALL\n".join(recursive_cases))
            lines.append("    )")

    # ------------------------------------------------------------------
    # Bidirectional builder helpers
    # ------------------------------------------------------------------

    def _build_bidir_configs(
        self,
        op: RecursiveTraversalOperator,
        edge_src_col: str,
        edge_dst_col: str,
    ) -> tuple[BidirectionalConfig, BidirectionalConfig]:
        """Build forward and backward configs for a bidirectional traversal."""
        enriched_rec = self._get_enriched_recursive(op)
        source_node_table = (
            enriched_rec.source_node.table_descriptor
            if enriched_rec and enriched_rec.source_node
            else None
        )
        target_node_table = (
            enriched_rec.target_node.table_descriptor
            if enriched_rec and enriched_rec.target_node
            else None
        )
        source_id_col = op.source_id_column or "id"
        target_id_col = op.target_id_column or "id"
        source_filter_sql = self._render_source_filter_for_bidirectional(op)
        target_filter_sql = self._render_target_filter_for_bidirectional(op)
        fwd = BidirectionalConfig(
            cte_prefix="forward",
            cte_short="fwd",
            cte_var="f",
            traverse_col=edge_src_col,
            arrive_col=edge_dst_col,
            node_table_name=(
                source_node_table.full_table_name if source_node_table else None
            ),
            node_alias="src",
            node_id_col=source_id_col,
            filter_sql=source_filter_sql,
            depth_bound=op.bidirectional_depth_forward or 5,
            prepend_path=False,
        )
        bwd = BidirectionalConfig(
            cte_prefix="backward",
            cte_short="bwd",
            cte_var="b",
            traverse_col=edge_dst_col,
            arrive_col=edge_src_col,
            node_table_name=(
                target_node_table.full_table_name if target_node_table else None
            ),
            node_alias="tgt",
            node_id_col=target_id_col,
            filter_sql=target_filter_sql,
            depth_bound=op.bidirectional_depth_backward or 5,
            prepend_path=True,
        )
        return fwd, bwd

    def _build_bidir_depth0(
        self,
        cfg: BidirectionalConfig,
        cte_name: str,
        edge_table_name: str,
    ) -> list[str]:
        """Emit depth-0 base case for one direction of a bidirectional CTE."""
        lines: list[str] = []
        lines.append(f"  {cfg.cte_prefix}_{cte_name} AS (")
        lines.append(
            f"    -- Depth 0: {cfg.node_alias} node itself "
            f"(for meeting with {'backward' if not cfg.prepend_path else 'forward'})"
        )
        lines.append("    SELECT")
        lines.append(f"      {cfg.node_alias}.{cfg.node_id_col} AS current_node,")
        lines.append("      0 AS depth,")
        lines.append(f"      ARRAY({cfg.node_alias}.{cfg.node_id_col}) AS path,")
        lines.append(
            "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
        )
        if cfg.node_table_name:
            lines.append(f"    FROM {cfg.node_table_name} {cfg.node_alias}")
        else:
            lines.append(f"    FROM {edge_table_name} e")
            lines.append(
                f"    JOIN (SELECT DISTINCT {cfg.traverse_col} AS {cfg.node_id_col} "
                f"FROM {edge_table_name}) {cfg.node_alias} ON 1=1"
            )
        if cfg.filter_sql:
            lines.append(f"    WHERE {cfg.filter_sql}")
        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        return lines

    def _build_bidir_depth1_block(
        self,
        cfg: BidirectionalConfig,
        edge_table_name: str,
        edge_src_col: str,
        edge_dst_col: str,
        edge_filter_clause: str | None,
        edge_struct: str,
        is_undirected: bool,
    ) -> list[str]:
        """Emit depth-1 block for one direction, with optional undirected UNION.

        The depth-1 path always stores the actual edge direction
        (``ARRAY(edge_src_col, edge_dst_col)``), NOT direction-swapped columns.
        """
        lines: list[str] = []
        lines.append(f"    -- Depth 1+: explore edges from {cfg.node_alias}")
        if is_undirected:
            lines.append("    SELECT * FROM (")

        # Primary direction
        lines.append("    SELECT")
        lines.append(f"      e.{cfg.arrive_col} AS current_node,")
        lines.append("      1 AS depth,")
        lines.append(f"      ARRAY(e.{edge_src_col}, e.{edge_dst_col}) AS path,")
        lines.append(f"      ARRAY({edge_struct}) AS path_edges")
        lines.append(f"    FROM {edge_table_name} e")
        if cfg.node_table_name:
            lines.append(
                f"    JOIN {cfg.node_table_name} {cfg.node_alias} "
                f"ON {cfg.node_alias}.{cfg.node_id_col} = e.{cfg.traverse_col}"
            )
        where = self._build_where_clause(
            f"({edge_filter_clause})" if edge_filter_clause else None,
            cfg.filter_sql,
        )
        if where:
            lines.append(where)

        # Undirected reverse
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{cfg.traverse_col} AS current_node,")
            lines.append("      1 AS depth,")
            lines.append(f"      ARRAY(e.{edge_dst_col}, e.{edge_src_col}) AS path,")
            lines.append(f"      ARRAY({edge_struct}) AS path_edges")
            lines.append(f"    FROM {edge_table_name} e")
            if cfg.node_table_name:
                lines.append(
                    f"    JOIN {cfg.node_table_name} {cfg.node_alias} "
                    f"ON {cfg.node_alias}.{cfg.node_id_col} = e.{cfg.arrive_col}"
                )
            where = self._build_where_clause(
                f"({edge_filter_clause})" if edge_filter_clause else None,
                cfg.filter_sql,
            )
            if where:
                lines.append(where)
            lines.append("    )")

        lines.append("")
        lines.append("    UNION ALL")
        lines.append("")
        return lines

    def _build_bidir_recursive_block(
        self,
        cfg: BidirectionalConfig,
        cte_name: str,
        edge_table_name: str,
        edge_filter_clause: str | None,
        edge_struct: str,
        is_undirected: bool,
        barrier_not_exists: str | None = None,
    ) -> list[str]:
        """Emit the recursive-case block for one direction."""
        cte_full = f"{cfg.cte_prefix}_{cte_name}"
        v = cfg.cte_var
        lines: list[str] = []
        lines.append(f"    -- Recursive case: extend {cfg.cte_prefix}")
        if is_undirected:
            lines.append("    SELECT * FROM (")

        # Primary direction
        lines.append("    SELECT")
        lines.append(f"      e.{cfg.arrive_col} AS current_node,")
        lines.append(f"      {v}.depth + 1 AS depth,")
        if cfg.prepend_path:
            lines.append(
                f"      CONCAT(ARRAY(e.{cfg.arrive_col}), {v}.path) AS path,"
            )
            lines.append(
                f"      CONCAT(ARRAY({edge_struct}), {v}.path_edges) AS path_edges"
            )
        else:
            lines.append(
                f"      CONCAT({v}.path, ARRAY(e.{cfg.arrive_col})) AS path,"
            )
            lines.append(
                f"      CONCAT({v}.path_edges, ARRAY({edge_struct})) AS path_edges"
            )
        lines.append(f"    FROM {cte_full} {v}")
        lines.append(f"    JOIN {edge_table_name} e")
        lines.append(f"      ON {v}.current_node = e.{cfg.traverse_col}")
        lines.append(f"    WHERE {v}.depth < {cfg.depth_bound}")
        lines.append(
            f"      AND NOT ARRAY_CONTAINS({v}.path, e.{cfg.arrive_col})"
        )
        if edge_filter_clause:
            lines.append(f"      AND ({edge_filter_clause})")
        if barrier_not_exists:
            lines.append(f"      AND {barrier_not_exists}")

        # Undirected reverse
        if is_undirected:
            lines.append("")
            lines.append("      UNION ALL")
            lines.append("")
            lines.append("    -- Reverse direction for undirected")
            lines.append("    SELECT")
            lines.append(f"      e.{cfg.traverse_col} AS current_node,")
            lines.append(f"      {v}.depth + 1 AS depth,")
            if cfg.prepend_path:
                lines.append(
                    f"      CONCAT(ARRAY(e.{cfg.traverse_col}), {v}.path) AS path,"
                )
                lines.append(
                    f"      CONCAT(ARRAY({edge_struct}), {v}.path_edges) AS path_edges"
                )
            else:
                lines.append(
                    f"      CONCAT({v}.path, ARRAY(e.{cfg.traverse_col})) AS path,"
                )
                lines.append(
                    f"      CONCAT({v}.path_edges, ARRAY({edge_struct})) AS path_edges"
                )
            lines.append(f"    FROM {cte_full} {v}")
            lines.append(f"    JOIN {edge_table_name} e")
            lines.append(f"      ON {v}.current_node = e.{cfg.arrive_col}")
            lines.append(f"    WHERE {v}.depth < {cfg.depth_bound}")
            lines.append(
                f"      AND NOT ARRAY_CONTAINS({v}.path, e.{cfg.traverse_col})"
            )
            if edge_filter_clause:
                lines.append(f"      AND ({edge_filter_clause})")
            if barrier_not_exists:
                lines.append(f"      AND {barrier_not_exists}")
            lines.append("    )")

        lines.append("  ),")
        lines.append("")
        return lines

    def _build_unrolling_level_cte(
        self,
        cfg: BidirectionalConfig,
        level: int,
        cte_name: str,
        edge_table_name: str,
        edge_filter_clause: str | None,
        edge_struct: str,
        id_col_attr: str,
    ) -> list[str]:
        """Emit one unrolled CTE level (base or recursive) for one direction."""
        v = cfg.cte_var
        lines: list[str] = []
        if level == 0:
            lines.append(f"  {cfg.cte_short}_{level}_{cte_name} AS (")
            lines.append("    SELECT")
            lines.append(f"      {cfg.node_alias}.{id_col_attr} AS current_node,")
            lines.append(f"      ARRAY({cfg.node_alias}.{id_col_attr}) AS path,")
            lines.append(
                "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
            )
            if cfg.node_table_name:
                lines.append(
                    f"    FROM {cfg.node_table_name} {cfg.node_alias}"
                )
                if cfg.filter_sql:
                    lines.append(f"    WHERE {cfg.filter_sql}")
            lines.append("  ),")
        else:
            prev = f"{cfg.cte_short}_{level - 1}_{cte_name}"
            lines.append(f"  {cfg.cte_short}_{level}_{cte_name} AS (")
            lines.append("    SELECT")
            lines.append(f"      e.{cfg.arrive_col} AS current_node,")
            if cfg.prepend_path:
                lines.append(
                    f"      CONCAT(ARRAY(e.{cfg.arrive_col}), {v}.path) AS path,"
                )
                lines.append(
                    f"      CONCAT(ARRAY({edge_struct}), {v}.path_edges) AS path_edges"
                )
            else:
                lines.append(
                    f"      CONCAT({v}.path, ARRAY(e.{cfg.arrive_col})) AS path,"
                )
                lines.append(
                    f"      CONCAT({v}.path_edges, ARRAY({edge_struct})) AS path_edges"
                )
            lines.append(f"    FROM {prev} {v}")
            lines.append(f"    JOIN {edge_table_name} e")
            lines.append(f"      ON {v}.current_node = e.{cfg.traverse_col}")
            where = self._build_where_clause(
                f"NOT ARRAY_CONTAINS({v}.path, e.{cfg.arrive_col})",
                f"({edge_filter_clause})" if edge_filter_clause else None,
            )
            if where:
                lines.append(where)
            lines.append("  ),")
        lines.append("")
        return lines

    def _render_bidirectional_recursive_cte(
        self, op: RecursiveTraversalOperator
    ) -> str:
        """Render bidirectional BFS using WITH RECURSIVE forward/backward CTEs.

        This implements the recursive CTE approach for bidirectional BFS:
        - forward CTE: explores from source toward target
        - backward CTE: explores from target toward source
        - final: JOINs forward and backward where they meet
        """
        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        min_depth = op.min_hops if op.min_hops is not None else 1
        max_depth = op.max_hops or 10

        edge_table_name = self._get_edge_table_name(op)
        edge_filter_clause = self._get_edge_filter_clause(op)
        edge_src_col, edge_dst_col = self._get_edge_column_names(op)
        edge_struct = (
            f"STRUCT(e.{edge_src_col} AS src, "
            f"e.{edge_dst_col} AS dst)"
        )
        is_undirected = (
            op.direction == RelationshipDirection.BOTH
        )

        fwd, bwd = self._build_bidir_configs(
            op, edge_src_col, edge_dst_col,
        )

        # Resolve barrier NOT EXISTS for forward expansion
        barrier_sql: str | None = None
        enriched_rec = self._get_enriched_recursive(op)
        if enriched_rec and enriched_rec.barrier_filter_as_barrier:
            bf_sql = self._expr.render_edge_filter_expression(
                enriched_rec.barrier_filter_as_barrier
            )
            if enriched_rec.target_node:
                td = enriched_rec.target_node.table_descriptor
                barrier_sql = _build_barrier_not_exists(
                    bf_sql,
                    td.full_table_name,
                    enriched_rec.target_node.id_column,
                    td.filter,
                    "f.current_node",
                )

        lines: list[str] = []
        for cfg in (fwd, bwd):
            lines.extend(self._build_bidir_depth0(
                cfg, cte_name, edge_table_name,
            ))
            lines.extend(self._build_bidir_depth1_block(
                cfg, edge_table_name, edge_src_col,
                edge_dst_col, edge_filter_clause,
                edge_struct, is_undirected,
            ))
            # Barrier only on forward expansion
            cfg_barrier = (
                barrier_sql if cfg.cte_prefix == "forward"
                else None
            )
            lines.extend(self._build_bidir_recursive_block(
                cfg, cte_name, edge_table_name,
                edge_filter_clause, edge_struct,
                is_undirected,
                barrier_not_exists=cfg_barrier,
            ))

        # Final CTE: join forward and backward where they meet
        lines.append(f"  {cte_name} AS (")
        lines.append(
            "    -- Intersection: paths that meet in the middle"
        )
        lines.append(
            "    -- Use DISTINCT to deduplicate paths found "
            "via different meeting points"
        )
        lines.append("    SELECT DISTINCT")
        lines.append("      f.path[0] AS start_node,")
        lines.append(
            "      b.path[SIZE(b.path) - 1] AS end_node,"
        )
        lines.append("      f.depth + b.depth AS depth,")
        lines.append(
            "      CONCAT(SLICE(f.path, 1, SIZE(f.path) - 1)"
            ", b.path) AS path,"
        )
        lines.append(
            "      CONCAT(f.path_edges, b.path_edges)"
            " AS path_edges"
        )
        lines.append(f"    FROM forward_{cte_name} f")
        lines.append(f"    JOIN backward_{cte_name} b")
        lines.append("      ON f.current_node = b.current_node")
        lines.append(
            f"    WHERE f.depth + b.depth >= {min_depth}"
        )
        lines.append(
            f"      AND f.depth + b.depth <= {max_depth}"
        )
        lines.append(
            "      AND SIZE(ARRAY_INTERSECT("
            "SLICE(f.path, 1, SIZE(f.path) - 1), b.path)) = 0"
        )
        lines.append("  )")

        return "\n".join(lines)

    def _render_bidirectional_unrolling_cte(
        self, op: RecursiveTraversalOperator
    ) -> str:
        """Render bidirectional BFS using unrolled CTEs (one per level).

        This implements the unrolling approach for bidirectional BFS:
        - fwd_0, fwd_1, ...: forward CTEs, one per depth level
        - bwd_0, bwd_1, ...: backward CTEs, one per depth level
        - final: UNION of all valid (fwd_i, bwd_j) combinations
        """
        self._ctx.cte_counter += 1
        cte_name = f"paths_{self._ctx.cte_counter}"
        op.cte_name = cte_name

        forward_depth = op.bidirectional_depth_forward or 3
        backward_depth = op.bidirectional_depth_backward or 3
        min_depth = op.min_hops if op.min_hops is not None else 1
        max_depth = op.max_hops or 6

        edge_src_col, edge_dst_col = self._get_edge_column_names(op)
        edge_table_name = self._get_edge_table_name(op)
        edge_filter_clause = self._get_edge_filter_clause(op)
        edge_struct = (
            f"STRUCT(e.{edge_src_col} AS src, "
            f"e.{edge_dst_col} AS dst)"
        )

        fwd, bwd = self._build_bidir_configs(
            op, edge_src_col, edge_dst_col,
        )
        # Override depth bounds for unrolling defaults
        fwd = BidirectionalConfig(
            **{
                **fwd.__dict__,
                "depth_bound": forward_depth,
            }
        )
        bwd = BidirectionalConfig(
            **{
                **bwd.__dict__,
                "depth_bound": backward_depth,
            }
        )

        lines: list[str] = []
        for cfg in (fwd, bwd):
            id_col = (
                op.source_id_column
                if cfg.cte_short == "fwd"
                else op.target_id_column
            )
            for level in range(cfg.depth_bound + 1):
                lines.extend(self._build_unrolling_level_cte(
                    cfg, level, cte_name,
                    edge_table_name, edge_filter_clause,
                    edge_struct, id_col,
                ))

        # Generate final CTE: UNION of all valid combinations
        lines.append(f"  {cte_name} AS (")
        union_parts: list[str] = []

        for fwd_level in range(forward_depth + 1):
            for bwd_level in range(backward_depth + 1):
                # Total path length = fwd_level + bwd_level
                # (fwd_0 has 1 node, fwd_1 has 2 nodes, etc.)
                total_length = fwd_level + bwd_level
                if total_length < min_depth or total_length > max_depth:
                    continue
                if fwd_level == 0 and bwd_level == 0:
                    # Both at base = direct source=target (skip if min > 0)
                    if min_depth > 0:
                        continue

                union_sql = []
                union_sql.append("    SELECT")
                union_sql.append("      f.path[0] AS start_node,")
                union_sql.append("      b.path[SIZE(b.path) - 1] AS end_node,")
                union_sql.append(f"      {total_length} AS depth,")
                if fwd_level == 0:
                    # Only backward path and path_edges
                    union_sql.append("      b.path AS path,")
                    union_sql.append("      b.path_edges AS path_edges")
                elif bwd_level == 0:
                    # Only forward path and path_edges
                    union_sql.append("      f.path AS path,")
                    union_sql.append("      f.path_edges AS path_edges")
                else:
                    # Combine: forward (except meeting node) + backward
                    union_sql.append(
                        "      CONCAT(SLICE(f.path, 1, SIZE(f.path) - 1), b.path) AS path,"
                    )
                    union_sql.append(
                        "      CONCAT(f.path_edges, b.path_edges) AS path_edges"
                    )
                union_sql.append(f"    FROM fwd_{fwd_level}_{cte_name} f")
                union_sql.append(f"    JOIN bwd_{bwd_level}_{cte_name} b")
                union_sql.append("      ON f.current_node = b.current_node")
                # Prevent duplicate nodes in combined path
                if fwd_level > 0 and bwd_level > 0:
                    union_sql.append(
                        "    WHERE SIZE(ARRAY_INTERSECT("
                        "SLICE(f.path, 1, SIZE(f.path) - 1), b.path)) = 0"
                    )

                union_parts.append("\n".join(union_sql))

        if union_parts:
            # Use UNION (not UNION ALL) to deduplicate paths found via different meeting points
            # E.g., path A→B→C→D can be found with fwd=1/bwd=2 or fwd=2/bwd=1
            lines.append("\n    UNION\n".join(union_parts))
        else:
            # Fallback: empty result if no valid combinations
            lines.append("    SELECT")
            lines.append("      NULL AS start_node,")
            lines.append("      NULL AS end_node,")
            lines.append("      0 AS depth,")
            lines.append("      ARRAY() AS path,")
            lines.append(
                "      CAST(ARRAY() AS ARRAY<STRUCT<src: STRING, dst: STRING>>) AS path_edges"
            )
            lines.append("    WHERE FALSE")

        lines.append("  )")

        return "\n".join(lines)

    def _get_edge_column_names(
        self, op: RecursiveTraversalOperator
    ) -> tuple[str, str]:
        """Get the edge source and destination column names.

        Reads from enriched data (source_id_col, target_id_col).

        Returns:
            Tuple of (source_col, dest_col) for the edges table
        """
        enriched_rec = self._get_enriched_recursive(op)
        if enriched_rec:
            return (
                enriched_rec.source_id_col,
                enriched_rec.target_id_col,
            )
        return ("src", "dst")

    def _get_edge_table_name(
        self, op: RecursiveTraversalOperator
    ) -> str:
        """Get the edge table name from enriched data."""
        enriched_rec = self._get_enriched_recursive(op)
        if enriched_rec and enriched_rec.edge_tables:
            # Use first edge table (single-table case) or
            # single_table_name (merged multi-edge case)
            if enriched_rec.single_table_name:
                return enriched_rec.single_table_name
            return (
                enriched_rec.edge_tables[0]
                .table_descriptor.full_table_name
            )

        raise TranspilerInternalErrorException(
            "No edge table found for bidirectional traversal"
        )

    def _get_edge_filter_clause(
        self, op: RecursiveTraversalOperator
    ) -> str | None:
        """Get the edge type filter clause from enriched data."""
        enriched_rec = self._get_enriched_recursive(op)
        if not enriched_rec:
            return None

        # Single-table filter already combined by enrichment
        if enriched_rec.single_table_filter:
            return enriched_rec.single_table_filter

        # Collect filters from individual edge tables
        filters = [
            f"({et.filter_clause})"
            for et in enriched_rec.edge_tables
            if et.filter_clause
        ]
        if filters:
            return " OR ".join(filters)
        return None

    def _render_source_filter_for_bidirectional(
        self, op: RecursiveTraversalOperator
    ) -> str | None:
        """Render source node filter for bidirectional base case."""
        enriched_rec = self._get_enriched_recursive(op)
        if not enriched_rec or not enriched_rec.start_filter_as_src:
            return None
        return self._expr.render_edge_filter_expression(
            enriched_rec.start_filter_as_src
        )

    def _render_target_filter_for_bidirectional(
        self, op: RecursiveTraversalOperator
    ) -> str | None:
        """Render target node filter for bidirectional backward base case."""
        enriched_rec = self._get_enriched_recursive(op)
        if not enriched_rec or not enriched_rec.sink_filter_as_tgt:
            return None
        return self._expr.render_edge_filter_expression(
            enriched_rec.sink_filter_as_tgt
        )

    def render_recursive_reference(
        self, op: RecursiveTraversalOperator, depth: int
    ) -> str:
        """Render a reference to a recursive CTE."""
        indent = self._ctx.indent(depth)
        cte_name = getattr(op, "cte_name", "paths")
        min_depth = op.min_hops if op.min_hops is not None else 1

        lines: list[str] = []
        lines.append(f"{indent}SELECT")
        lines.append(f"{indent}   start_node,")
        lines.append(f"{indent}   end_node,")
        lines.append(f"{indent}   depth,")
        if op.collect_edges:
            lines.append(f"{indent}   path,")
            lines.append(f"{indent}   path_edges")
        else:
            lines.append(f"{indent}   path")
        lines.append(f"{indent}FROM {cte_name}")

        # Add WHERE clause for depth bounds
        where = self._build_where_clause(
            f"depth >= {min_depth}",
            f"depth <= {op.max_hops}" if op.max_hops is not None else None,
            indent=indent,
        )
        if where:
            lines.append(where)

        return "\n".join(lines)

    def render_aggregation_boundary_cte(
        self, op: AggregationBoundaryOperator
    ) -> str:
        """Render an aggregation boundary operator as a CTE definition.

        This generates a CTE that materializes the aggregated result, allowing
        subsequent MATCH clauses to join with it.

        Example output for:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS population
            WHERE population > 100

        Generates:
            agg_boundary_1 AS (
              SELECT
                `c`.`id` AS `c_id`,
                COUNT(`p`.`id`) AS `population`
              FROM ... (rendered input)
              GROUP BY `c`.`id`
              HAVING COUNT(`p`.`id`) > 100
            )
        """
        cte_name = op.cte_name
        lines: list[str] = []

        # Use the AggregationBoundaryOperator itself as context for expression rendering
        # The expressions in group_keys and aggregates were resolved against this operator
        context_op = op

        lines.append(f"{cte_name} AS (")

        # Render the SELECT clause
        lines.append("  SELECT")

        # Render group keys and aggregates
        select_items: list[str] = []

        # Group keys - these become both SELECT columns and GROUP BY columns
        for alias, expr in op.group_keys:
            rendered_expr = self._expr.render_expression(expr, context_op)
            select_items.append(f"    {rendered_expr} AS `{alias}`")

        # Aggregates
        for alias, expr in op.aggregates:
            rendered_expr = self._expr.render_expression(expr, context_op)
            select_items.append(f"    {rendered_expr} AS `{alias}`")

        lines.append(",\n".join(select_items))

        # Render FROM clause (the input operator)
        if op.in_operator and self._render_operator:
            input_sql = self._render_operator(op.in_operator, depth=1)
            lines.append("  FROM (")
            lines.append(input_sql)
            lines.append("  ) AS _agg_input")

        # Render GROUP BY clause
        if op.group_keys:
            group_by_exprs = []
            for alias, expr in op.group_keys:
                rendered_expr = self._expr.render_expression(expr, context_op)
                group_by_exprs.append(rendered_expr)
            lines.append(f"  GROUP BY {', '.join(group_by_exprs)}")

        # Render HAVING clause
        if op.having_filter:
            having_sql = self._expr.render_expression(op.having_filter, context_op)
            lines.append(f"  HAVING {having_sql}")

        # Render ORDER BY clause
        if op.order_by:
            order_parts = []
            for expr, is_desc in op.order_by:
                rendered_expr = self._expr.render_expression(expr, context_op)
                direction = "DESC" if is_desc else "ASC"
                order_parts.append(f"{rendered_expr} {direction}")
            lines.append(f"  ORDER BY {', '.join(order_parts)}")

        # Render LIMIT clause
        if op.limit is not None:
            lines.append(f"  LIMIT {op.limit}")

        # Render OFFSET clause
        if op.skip is not None:
            lines.append(f"  OFFSET {op.skip}")

        lines.append(")")

        return "\n".join(lines)

    def render_aggregation_boundary_reference(
        self, op: AggregationBoundaryOperator, depth: int
    ) -> str:
        """Render a reference to an aggregation boundary CTE.

        When the aggregation boundary is used as input to a join or other
        operator, this generates a SELECT from the CTE.

        Example output:
            SELECT
              `c_id`,
              `population`
            FROM agg_boundary_1
        """
        indent = self._ctx.indent(depth)
        cte_name = op.cte_name
        lines: list[str] = []

        lines.append(f"{indent}SELECT")

        # Project all columns from the CTE
        select_items: list[str] = []
        for alias, _ in op.all_projections:
            # Map entity variable to its ID column for joins
            # e.g., if 'c' was projected, we need 'c_id' for joining
            select_items.append(f"`{alias}`")

        for i, item in enumerate(select_items):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{item}")

        lines.append(f"{indent}FROM {cte_name}")

        return "\n".join(lines)
