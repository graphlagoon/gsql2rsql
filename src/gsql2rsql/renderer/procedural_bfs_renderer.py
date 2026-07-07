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


@dataclass(frozen=True)
class ProceduralBFSOptimizations:
    """Feature flags for procedural-BFS SQL generation.

    Each flag guards one independently-verified, result-preserving rewrite so
    a production regression on an untested engine (e.g. Databricks SQL
    Warehouse, which we cannot exercise locally) can be bisected by toggling a
    single flag. Defaults follow the measured A/B evidence in
    docs_help_dev/analysis_canonical_query_bfs_memory.md: the two *safe* flags
    (asymmetric protection, ~zero cost) default ON; the one that regressed in
    the numbered_views lazy-lineage regime defaults OFF.

    Attributes:
        visited_not_exists: (default ON) O5 — emit the visited-set exclusion
            as ``NOT EXISTS (SELECT 1 FROM bfs_visited... v WHERE v.node =
            ...)`` instead of ``... NOT IN (SELECT node FROM bfs_visited...)``.
            NOT IN is a null-aware anti-join that Spark can only execute as an
            unconditional broadcast of the whole visited set (no shuffle
            fallback; ignores autoBroadcastJoinThreshold), OOMing the driver
            once visited grows in deep BFS. Measured: −39% peak execution
            memory on the real transpiled SQL, time-neutral at small scale.
            The NOT EXISTS probe carries an ``IS NOT NULL`` guard to preserve
            exact NOT IN null semantics (NULL-endpoint edges are dropped, as
            in legacy and per Cypher).
        undirected_union_all: (default OFF) O7 — expand undirected traversal
            as a UNION ALL of two equi-join branches instead of a single
            non-equi ``ON (e.src = f.node OR e.dst = f.node)`` join (which
            forces a BroadcastNestedLoopJoin, O(frontier·edges)).
            Row-identical under the frontier ⊆ visited invariant. Default OFF
            because on numbered_views the UNION ALL doubles the lazy-lineage
            fan-out (measured 2–7.7× wall-time regression), and on local-scale
            temp_tables it was neutral; enable explicitly for materialized
            execution over large graphs where the BNLJ dominates (isolated
            benchmark: 4.5s → 0.9s at frontier 1k; legacy does not finish at
            frontier 50k).
        loop_control_into: (default ON) numbered_views only — update the WHILE
            loop-control variable via ``EXECUTE IMMEDIATE '<count query>' INTO
            var``. The legacy ``EXECUTE IMMEDIATE 'SET var = (...)'`` form is a
            silent no-op for script locals on OSS Spark 4.2 (writes a conf key
            instead), so the loop never terminates early and always runs to
            max_hops, exploding the lazy-view lineage. Measured: 62.8s → 5.3s
            (11.9×) on a loose bound (*1..8 exhausting at depth 3), costing
            ~+0.5s absolute on tight bounds.
        undirected_doubled_adjacency: (default OFF) O8 — materialize the
            (type/predicate-filtered) edge set once in BOTH orientations
            (``bfs_adj_{n}``: ``_jk`` = endpoint matched against the
            frontier, ``_next`` = the other endpoint, plus the ORIGINAL
            src/dst/property columns unswapped so edge identity in
            path_edges is preserved); each level then expands with a single
            equi-join ``e._jk = f.node``. Eliminates the non-equi OR-join
            (BroadcastNestedLoopJoin, O(frontier·edges)) like
            ``undirected_union_all``, but references the frontier ONCE per
            level: on numbered_views the lazy-lineage recursion stays
            single-branch (~2^depth, parity with legacy — avoiding the
            measured 2–7.7× per-level-UNION-ALL regression), and on
            temp_tables it trades a one-time 2|E| write for one hash join
            per level with the edge filter applied once at build. Mutually
            exclusive with ``undirected_union_all``. No-op for directed
            traversal; bidirectional BFS has its own expansion paths and
            ignores this flag.
        deferred_edge_payload: (default OFF) O9 — temp_tables only. Copy the
            (filtered) edge set once into ``bfs_edges_keyed_{n}`` with a
            materialized ``MONOTONICALLY_INCREASING_ID() AS _row_id``; the
            per-level ``bfs_edges_{n}``/``bfs_result_{n}`` tables then carry
            only ``(_row_id, _next_node[, _bfs_depth])`` and the fat edge
            payload (e.g. serialized-JSON property columns, ~93–99% of row
            bytes at 1–10KB) is re-attached ONCE in the final view by
            joining back on ``_row_id``. Only active when the query carries
            edge property columns. Ignored on numbered_views:
            MONOTONICALLY_INCREASING_ID over a lazy view is
            non-deterministic across the multiple re-evaluations
            (visited/frontier/result), which would silently corrupt
            results; NV already benefits from lazy column pruning. Side
            effect (intentional): property columns keep their ORIGINAL
            types in the final view instead of the legacy all-STRING result
            table, matching CTE mode. Bidirectional BFS ignores this flag.
        barrier_precompute: (default OFF) O10 — temp_tables only. Decide the
            is_terminator barrier once before the loop:
            ``bfs_barrier_{n}`` = DISTINCT node ids satisfying the barrier
            predicate (predicate-first + DISTINCT — exactly equivalent to
            the per-level correlated NOT EXISTS for deterministic
            predicates); the per-level frontier then anti-joins this small
            id table instead of re-scanning the full node table every
            iteration. Skipped (falls back to the per-level form) when the
            barrier predicate contains a volatile function, which would be
            frozen at build time. Ignored on numbered_views (a lazy
            precompute view gains nothing). Bidirectional BFS ignores this
            flag.
        barrier_on_adjacency: (default OFF) O11 — temp_tables only; only
            effective when BOTH ``undirected_doubled_adjacency`` (the
            column rides on the adjacency) and ``barrier_precompute``
            (``bfs_barrier_{n}`` is the join source) are active. Stamps the
            barrier VERDICT on each adjacency row at build time —
            ``LEFT JOIN bfs_barrier_{n} b ON b.node = e.<next>`` plus
            ``(b.node IS NOT NULL) AS _next_is_barrier`` — so the per-level
            frontier reads a carried boolean (``WHERE NOT
            _next_is_barrier``, zero joins, merges into the scan stage)
            instead of anti-joining the barrier table every level. Carrying
            the verdict (not a value like ``to_degree``) keeps it sound for
            ANY deterministic predicate, with no manual NULL/negation
            reasoning: the boolean is total (never NULL) and the barrier
            build's ``SELECT DISTINCT`` is load-bearing (the LEFT JOIN
            matches at most one row per edge — no fanout). Trade-off vs
            plain ``barrier_precompute``: pays one LEFT JOIN of 2|E|×|B| at
            the adjacency build instead of ``max_hops`` per-level
            anti-joins (each level is a separate statement — no
            broadcast/exchange reuse); wins when |B| exceeds the broadcast
            threshold, ~ties otherwise. Ignored on numbered_views (a lazy
            adjacency view would re-run the LEFT JOIN on every lineage
            reference). No-op for directed traversal and bidirectional BFS.
        prune_barrier_adjacency: (default OFF) O12 — temp_tables only;
            only effective when both ``undirected_doubled_adjacency`` and
            ``barrier_precompute`` are active. Drops adjacency rows whose
            join key (``_jk`` — the node expanded FROM) is a barrier:
            barrier nodes never enter the frontier, so those rows are
            scanned every level and never match — dead weight that can be
            most of the table on hub-heavy graphs (hubs have high degree).
            Rows where the barrier is the DESTINATION (``_next``) are
            KEPT: edges into barriers are still discovered and returned.
            Root exception (load-bearing): the root expands at depth 1
            even if it is itself a barrier, so rows whose ``_jk`` is a
            start node are kept via ``OR EXISTS (...
            bfs_frontier_{n}_init ...)``. Emitted as EXISTS/NOT EXISTS
            (semi/anti-join) — no fanout risk by construction.
            Barrier-to-barrier edges (neither endpoint the root) were
            never discoverable in legacy either (neither endpoint is ever
            expanded), so pruning them is behavior-preserving. Ignored on
            numbered_views; no-op for directed traversal and
            bidirectional BFS.
    """

    # TODO(databricks-validation): these flags exist ONLY because the target
    # production engine (Databricks SQL Warehouse) could not be tested locally.
    # After running benchmarks/vlp_memory_investigation/bench_flags_ab*.py on a
    # real Databricks workspace, resolve each flag and DELETE it together with
    # its legacy branch:
    #   - visited_not_exists: if Databricks/Photon confirms no regression,
    #     remove the flag and the legacy NOT IN branches in
    #     _tt_visited_not_exists/_nv_visited_not_exists. Local evidence for
    #     keeping ON: -39% peak execution memory (1.88GB->1.14GB, Q3 directed
    #     *1..3 numbered_views); with a 20M-row/718MB visited set, NOT IN never
    #     finishes (unconditional >3.4GiB broadcast, ignores
    #     autoBroadcastJoinThreshold) vs 7.5s with NOT EXISTS; time-neutral at
    #     small scale (0.30s vs 0.36s).
    #   - undirected_union_all: decide per engine. Local evidence: isolated
    #     materialized benchmark 4.5s->0.9s (5x) at frontier 1k and legacy
    #     OR-join does not finish at frontier 50k x 2M edges; BUT inside the
    #     real numbered_views lazy-view pipeline it REGRESSED 2-7.7x
    #     (ON 11.5s vs OFF 1.49s at *1..4) because two branches double the
    #     lineage fan-out, and on local temp_tables it was neutral
    #     (0.84-0.97x) with 2x peak memory. If Databricks (materialized
    #     temp tables, big frontiers) confirms the win, flip the default to
    #     True for temp_tables or remove the OR-join branch entirely.
    #   - loop_control_into: numbered_views only. Local evidence for keeping
    #     ON: 62.8s->5.3s (11.9x) at *1..8 exhausting at depth 3 (and
    #     69.9s->2.1s = 33x in the isolated sweep); costs ~+0.5s absolute on
    #     tight bounds (one real COUNT per level re-evaluates the lazy
    #     lineage). Verify Databricks' EXECUTE IMMEDIATE 'SET var = ...'
    #     semantics: if it also fails to assign script locals there, the
    #     legacy branch is simply a bug and must be deleted.
    # TODO(databricks-validation): the three flags below (O8/O9/O10) are the
    # unmeasured-on-Databricks rewrites from the external BFS perf analysis
    # (2026-07). Local evidence says they are result-preserving; the perf win
    # can only be confirmed on the real temp_tables target (Databricks SQL
    # Warehouse). After A/B there (bench_flags_ab_tt.py pattern):
    #   - undirected_doubled_adjacency: if it beats both the OR-join and
    #     per-level UNION ALL on materialized temp tables, flip the default
    #     for temp_tables and delete undirected_union_all; verify with
    #     EXPLAIN that no BroadcastNestedLoopJoin remains and the frontier
    #     subtree appears once.
    #   - deferred_edge_payload: confirm MONOTONICALLY_INCREASING_ID is
    #     stable on warehouse temp tables, then default ON for wide-payload
    #     schemas (the keyed copy is O(|E|) — may lose on huge edge tables
    #     with tiny traversals).
    #   - barrier_precompute: expected win = replaces up-to-max_hops filtered
    #     node-table scans with 1 scan + cheap anti-joins; default ON for
    #     temp_tables if confirmed.
    #   - barrier_on_adjacency: A/B vs plain barrier_precompute with the
    #     production |B| (barrier-id count): if |B| exceeds the broadcast
    #     threshold the carried column should win (1 build join vs max_hops
    #     per-level shuffles of |B|); if |B| is broadcastable expect ~tie.
    #     Decision variable is |B| vs autoBroadcastJoinThreshold.
    #   - prune_barrier_adjacency: A/B with the production barrier density:
    #     the win scales with the fraction of edges INCIDENT-FROM barrier
    #     nodes (hub-heavy graphs: potentially most of the table — every
    #     per-level scan shrinks by that fraction); costs one anti-join +
    #     one semi-join per orientation at the adjacency build. Verify the
    #     root-is-barrier case on real data (kept via the frontier_init
    #     exception).
    visited_not_exists: bool = True
    undirected_union_all: bool = False
    loop_control_into: bool = True
    undirected_doubled_adjacency: bool = True
    deferred_edge_payload: bool = True
    barrier_precompute: bool = True
    barrier_on_adjacency: bool = False
    prune_barrier_adjacency: bool = False

    def __post_init__(self) -> None:
        if self.undirected_union_all and self.undirected_doubled_adjacency:
            raise ValueError(
                "undirected_union_all and undirected_doubled_adjacency are "
                "mutually exclusive undirected-expansion rewrites; enable "
                "at most one."
            )

    @classmethod
    def all_off(cls) -> "ProceduralBFSOptimizations":
        """Legacy escape hatch: reproduce the pre-optimization SQL."""
        return cls(
            visited_not_exists=False,
            undirected_union_all=False,
            loop_control_into=False,
            undirected_doubled_adjacency=False,
            deferred_edge_payload=False,
            barrier_precompute=False,
            barrier_on_adjacency=False,
            prune_barrier_adjacency=False,
        )


# Conservative markers for volatile SQL functions in a rendered predicate.
# Used to skip the barrier precompute (which would freeze one evaluation).
# A false positive only costs the optimization, never correctness.
_VOLATILE_SQL_MARKERS: tuple[str, ...] = (
    "rand(", "randn(", "uuid(", "now(",
    "current_timestamp", "current_date", "unix_timestamp(",
)


def _sql_contains_volatile_function(predicate: str) -> bool:
    """Substring check for volatile functions in a rendered SQL predicate."""
    lowered = predicate.lower()
    return any(marker in lowered for marker in _VOLATILE_SQL_MARKERS)


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
    def _build_where_clause(where_parts: list[str]) -> str:
        """Build WHERE clause from parts."""
        return f"\nWHERE {' AND '.join(where_parts)}" if where_parts else ""

    def _direction_branches(
        self,
        src_col: str,
        dst_col: str,
        is_backward: bool,
        is_undirected: bool,
    ) -> list[tuple[str, str]]:
        """Return ``[(join_cond, next_node_expr), ...]`` for the edge expansion.

        Undirected traversal is emitted as a UNION ALL of **two equi-join
        branches** (forward: ``e.src = f.node`` → next ``e.dst``; backward:
        ``e.dst = f.node`` → next ``e.src``) instead of a single non-equi
        ``ON (e.src = f.node OR e.dst = f.node)`` join. An OR join predicate
        forces Spark into a BroadcastNestedLoopJoin (O(frontier·edges), which
        does not scale with frontier size); two equi-joins use hash/sort-merge
        joins. The two forms are row-for-row identical — for an edge with both
        endpoints in the frontier the OR-join also matches twice (once per
        frontier node), yielding the same two rows the UNION ALL produces, and
        self-loops are filtered by the visited check in both. See
        docs_help_dev/analysis_canonical_query_bfs_memory.md (O7).

        Gated by ``ProceduralBFSOptimizations.undirected_union_all``; when off,
        undirected reverts to the legacy single OR-join branch with a CASE
        next-node expression.
        """
        if is_undirected:
            # TODO(databricks-validation): resolve this flag after measuring
            # on Databricks (materialized temp tables, production-size
            # frontiers). Local numbers: UNION ALL wins 5x isolated on
            # materialized tables (4.5s->0.9s at frontier 1k; OR-join never
            # finishes at frontier 50k x 2M edges) but REGRESSES 2-7.7x inside
            # numbered_views (lazy lineage doubles per branch) and is neutral
            # (0.84-0.97x, 2x peak mem) on small local temp_tables. If
            # Databricks confirms the win, default True for temp_tables (or
            # delete the OR-join branch); if not, delete the UNION ALL branch.
            if self._ctx.procedural_optimizations.undirected_union_all:
                return [
                    (f"e.{src_col} = f.node", f"e.{dst_col}"),
                    (f"e.{dst_col} = f.node", f"e.{src_col}"),
                ]
            # Legacy: single non-equi OR-join with CASE-based next node.
            join_cond = f"(e.{src_col} = f.node OR e.{dst_col} = f.node)"
            next_node_expr = (
                f"CASE WHEN f.node = e.{src_col} "
                f"THEN e.{dst_col} ELSE e.{src_col} END"
            )
            return [(join_cond, next_node_expr)]
        if is_backward:
            return [(f"e.{dst_col} = f.node", f"e.{src_col}")]
        return [(f"e.{src_col} = f.node", f"e.{dst_col}")]

    @staticmethod
    def _edge_targets(p: _BFSParams) -> list[tuple[str, str | None]]:
        """Return ``[(table_sql, filter_clause), ...]`` edge-table targets."""
        if p.enriched.single_table:
            return [(p.edge_table_sql, p.edge_type_filter)]
        return [
            (ei.table_descriptor.full_table_name, ei.filter_clause)
            for ei in p.enriched.edge_tables
        ]

    def _adjacency_active(self, p: _BFSParams) -> bool:
        """Whether the doubled-adjacency expansion applies to this operator.

        Only for undirected, non-bidirectional traversal; the bidirectional
        renderers have their own expansion paths and ignore the flag.
        """
        return (
            p.is_undirected
            and p.bidir_mode == "off"
            and self._ctx.procedural_optimizations.undirected_doubled_adjacency
        )

    def _deferral_active(self, p: _BFSParams) -> bool:
        """Whether the deferred-edge-payload rewrite applies (O9).

        temp_tables only — the ``_row_id`` is stable ONLY because the keyed
        table is materialized; numbered_views' lazy views would re-evaluate
        MONOTONICALLY_INCREASING_ID non-deterministically across the
        visited/frontier/result consumers. Checked explicitly (not just
        structurally by caller) because ``_adjacency_select_body`` is
        shared with the numbered_views static-adjacency path (O8+O9
        compose only under temp_tables). Requires carried edge property
        columns (otherwise there is no payload to defer); the bidirectional
        renderers have their own paths and ignore the flag.
        """
        return (
            self._ctx.materialization_strategy == "temp_tables"
            and p.bidir_mode == "off"
            and bool(p.edge_prop_cols)
            and self._ctx.procedural_optimizations.deferred_edge_payload
        )

    def _barrier_precompute_active(self, p: _BFSParams) -> bool:
        """Whether the barrier-precompute rewrite applies (O10).

        temp_tables only (a lazy numbered_views precompute gains nothing) —
        enforced structurally: only the ``_tt_*`` builders consult this.
        Requires a barrier and a non-volatile predicate (a volatile one
        would be frozen at build time instead of re-evaluated per level);
        the bidirectional renderers have their own paths and ignore the
        flag.
        """
        return (
            p.bidir_mode == "off"
            and p.barrier_predicate is not None
            and p.barrier_node_table is not None
            and p.barrier_node_id_col is not None
            and self._ctx.procedural_optimizations.barrier_precompute
            and not _sql_contains_volatile_function(p.barrier_predicate)
        )

    def _barrier_on_adjacency_active(self, p: _BFSParams) -> bool:
        """Whether the barrier verdict is carried on the adjacency (O11).

        Requires BOTH the doubled adjacency (the column rides on it) and
        the barrier precompute (``bfs_barrier_{n}`` is the join source,
        including its volatile-predicate gate). The temp_tables check is
        EXPLICIT — not just structural via callers — because
        ``_adjacency_select_body`` is shared with the numbered_views
        static-view path, where a lazy adjacency would re-run the LEFT
        JOIN on every lineage reference (same trap as O9's row ids).
        """
        return (
            self._ctx.materialization_strategy == "temp_tables"
            and self._adjacency_active(p)
            and self._barrier_precompute_active(p)
            and self._ctx.procedural_optimizations.barrier_on_adjacency
        )

    def _prune_barrier_adjacency_active(self, p: _BFSParams) -> bool:
        """Whether expand-FROM-barrier adjacency rows are pruned (O12).

        Same activation conjunction as O11 (temp_tables EXPLICIT — the
        shared ``_adjacency_select_body`` trap — plus adjacency and
        barrier precompute with its volatile gate), under its own flag.
        """
        return (
            self._ctx.materialization_strategy == "temp_tables"
            and self._adjacency_active(p)
            and self._barrier_precompute_active(p)
            and self._ctx.procedural_optimizations.prune_barrier_adjacency
        )

    def _adjacency_select_body(self, p: _BFSParams) -> str:
        """UNION ALL SELECT doubling each (filtered) edge into both
        orientations.

        Each branch keeps the ORIGINAL src/dst/property columns unswapped
        (edge identity for path_edges) and adds ``_jk`` (the endpoint to
        match against the frontier) and ``_next`` (the other endpoint). Edge
        type filters and the edge predicate are applied here, once, at build
        time — the per-level expansion then carries no edge filters. Must
        not reference any script-local variable (42K0M) — and does not.

        With ``deferred_edge_payload`` the adjacency is built narrow from
        the keyed copy (``_row_id, _jk, _next`` only; filters were already
        applied at the keyed build). With ``barrier_on_adjacency`` each
        orientation additionally stamps the barrier VERDICT of its next
        endpoint — ``LEFT JOIN bfs_barrier_{n}`` + ``(b.node IS NOT NULL)
        AS _next_is_barrier`` (total boolean, never NULL; no fanout because
        the barrier build is DISTINCT).
        """
        carried = self._barrier_on_adjacency_active(p)
        barrier_col = (
            ", (b.node IS NOT NULL) AS _next_is_barrier" if carried else ""
        )

        def barrier_join(next_col: str) -> str:
            if not carried:
                return ""
            return (
                f"\nLEFT JOIN bfs_barrier_{p.n} b ON b.node = e.{next_col}"
            )

        def prune_predicate(jk_col: str) -> str | None:
            """O12: drop expand-FROM-barrier rows, keeping start nodes
            (the root expands at depth 1 even if it is a barrier)."""
            if not self._prune_barrier_adjacency_active(p):
                return None
            return (
                f"(NOT EXISTS (SELECT 1 FROM bfs_barrier_{p.n} jb "
                f"WHERE jb.node = e.{jk_col}) "
                f"OR EXISTS (SELECT 1 FROM bfs_frontier_{p.n}_init s "
                f"WHERE s.node = e.{jk_col}))"
            )

        orientations = [(p.src_col, p.dst_col), (p.dst_col, p.src_col)]

        if self._deferral_active(p):
            keyed = f"bfs_edges_keyed_{p.n}"
            parts = []
            for jk, nxt in orientations:
                where_parts = [w for w in (prune_predicate(jk),) if w]
                parts.append(
                    f"SELECT e._row_id, "
                    f"e.{jk} AS _jk, e.{nxt} AS _next{barrier_col}\n"
                    f"FROM {keyed} e"
                    + barrier_join(nxt)
                    + self._build_where_clause(where_parts)
                )
            return "\nUNION ALL\n".join(parts)

        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)
        parts = []
        for jk, nxt in orientations:
            for table_sql, table_filter in self._edge_targets(p):
                where_parts = [
                    w for w in (
                        table_filter, p.edge_predicate, prune_predicate(jk),
                    ) if w
                ]
                parts.append(
                    f"SELECT e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                    f"e.{jk} AS _jk, e.{nxt} AS _next{barrier_col}\n"
                    f"FROM {table_sql} e"
                    + barrier_join(nxt)
                    + self._build_where_clause(where_parts)
                )
        return "\nUNION ALL\n".join(parts)

    def _tt_visited_not_exists(
        self, visited_table: str, next_expr: str,
    ) -> str:
        """Visited-exclusion predicate for temp_tables (fixed table name).

        Emits ``<expr> IS NOT NULL AND NOT EXISTS (SELECT 1 FROM <visited> v
        WHERE v.node = <expr>)`` instead of ``<expr> NOT IN (SELECT node FROM
        <visited>)``. ``NOT IN`` is a *null-aware* anti-join that Spark can
        only run as an unconditional broadcast of the entire visited set
        (there is no shuffle fallback for a single-column null-aware
        anti-join, and it ignores ``autoBroadcastJoinThreshold``), so it OOMs
        the driver once visited grows large in deep BFS. ``NOT EXISTS``
        degrades to SortMergeJoin.

        The ``IS NOT NULL`` guard is required for exact NOT IN semantics:
        ``NULL NOT IN (non-empty set)`` yields NULL (row dropped), while
        ``NOT EXISTS (... v.node = NULL)`` never matches (row kept). Visited
        is root-seeded and therefore never empty, so legacy NOT IN always
        drops NULL-endpoint edges; without the guard those edges would leak
        into visited/frontier/result. Matches Cypher semantics (an edge
        connects two nodes) and is a free null-filter when endpoints are
        non-null.

        Gated by ``ProceduralBFSOptimizations.visited_not_exists``; when off,
        reverts to the legacy ``NOT IN`` form.
        """
        # TODO(databricks-validation): delete this legacy NOT IN branch (and
        # the flag) once Databricks confirms NOT EXISTS is safe there. Local
        # numbers: NOT EXISTS = -39% peak execution memory; NOT IN broadcasts
        # the whole visited set unconditionally (>3.4GiB for 20M rows, never
        # finishes) while NOT EXISTS degrades to SortMergeJoin (7.5s).
        if not self._ctx.procedural_optimizations.visited_not_exists:
            return f"{next_expr} NOT IN (SELECT node FROM {visited_table})"
        return (
            f"{next_expr} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {visited_table} v "
            f"WHERE v.node = {next_expr})"
        )

    def _nv_visited_not_exists(
        self, table_base: str, depth_var: str, next_expr: str,
    ) -> str:
        """Visited-exclusion predicate for numbered_views.

        Same rewrite as :meth:`_tt_visited_not_exists`, but the visited view is
        depth-numbered so the suffix is injected via the EXECUTE IMMEDIATE
        string-concatenation idiom (break out of the quoted literal, concat the
        depth, resume). See that method for why NOT EXISTS beats NOT IN and
        why the ``IS NOT NULL`` guard is required (exact NOT IN null
        semantics). The guard contains no quotes, so escaping is unaffected.

        Gated by ``ProceduralBFSOptimizations.visited_not_exists``; when off,
        reverts to the legacy ``NOT IN`` form.
        """
        # TODO(databricks-validation): delete this legacy NOT IN branch (and
        # the flag) once Databricks confirms NOT EXISTS is safe there. Same
        # evidence as _tt_visited_not_exists (-39% peak memory; anti-OOM).
        if not self._ctx.procedural_optimizations.visited_not_exists:
            return (
                f"{next_expr} NOT IN ("
                f"SELECT node FROM {table_base}_'"
                f" || CAST({depth_var} - 1 AS STRING) || ')"
            )
        return (
            f"{next_expr} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {table_base}_'"
            f" || CAST({depth_var} - 1 AS STRING) || ' v "
            f"WHERE v.node = {next_expr})"
        )

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

    def _tt_keyed_edges_select(self, p: _BFSParams) -> str:
        """SELECT body for the keyed edge copy (payload deferral, O9).

        ``MONOTONICALLY_INCREASING_ID()`` is assigned once and frozen by the
        temp-table materialization; every per-level table then references
        rows by ``_row_id`` and the payload is re-attached once in the final
        view. Do NOT key by a schema edge-id property instead: EdgeSchema
        carries no uniqueness metadata, and a duplicated id would fan out
        the final re-attach join. Edge type filters and the edge predicate
        are applied here, once.
        """
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)
        parts: list[str] = []
        for table_sql, table_filter in self._edge_targets(p):
            where_parts = [w for w in (table_filter, p.edge_predicate) if w]
            parts.append(
                f"SELECT e.{p.src_col}, e.{p.dst_col}{prop_select}\n"
                f"FROM {table_sql} e"
                + self._build_where_clause(where_parts)
            )
        union = "\nUNION ALL\n".join(parts)
        return (
            f"SELECT e.*, MONOTONICALLY_INCREASING_ID() AS _row_id\n"
            f"FROM (\n{union}\n) e"
        )

    def _tt_setup(self, p: _BFSParams) -> str:
        """Create initial temp tables: visited, frontier, result, frontier_init."""
        prop_cols_def = "".join(
            f", {c} STRING" for c in p.edge_prop_cols
        )
        where = self._build_start_where(p.node_type_filter, p.start_filter)

        lines: list[str] = []

        # Drop pre-existing tables
        drop_names = [
            f"bfs_visited_{p.n}", f"bfs_frontier_{p.n}",
            f"bfs_result_{p.n}", f"bfs_frontier_{p.n}_init",
        ]
        if self._deferral_active(p):
            drop_names.append(f"bfs_edges_keyed_{p.n}")
        if self._adjacency_active(p):
            drop_names.append(f"bfs_adj_{p.n}")
        if self._barrier_precompute_active(p):
            drop_names.append(f"bfs_barrier_{p.n}")
        for name in drop_names:
            lines.append(f"DROP TEMPORARY TABLE IF EXISTS {name};")

        # Barrier decision: materialized once, before the loop (O10).
        # Predicate-first + DISTINCT — exactly the per-level correlated
        # NOT EXISTS semantics ("node is a barrier iff ANY node-table row
        # with that id satisfies type filter + predicate"). Never rewrite
        # this as aggregate-then-predicate (e.g. MAX(col) > x): that is
        # only equivalent for monotone predicates. The DISTINCT is also
        # load-bearing for barrier_on_adjacency (O11): it guarantees the
        # adjacency-build LEFT JOIN matches at most one row per edge (no
        # fanout even on out-of-contract duplicate node rows).
        if self._barrier_precompute_active(p):
            barrier_parts: list[str] = []
            if p.barrier_node_type_filter:
                barrier_parts.append(p.barrier_node_type_filter)
            barrier_parts.append(f"({p.barrier_predicate})")
            lines.append(
                f"CREATE TEMPORARY TABLE bfs_barrier_{p.n} AS\n"
                f"SELECT DISTINCT barrier.{p.barrier_node_id_col} AS node\n"
                f"FROM {p.barrier_node_table} barrier\n"
                f"WHERE {' AND '.join(barrier_parts)};"
            )

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
        # Result accumulator (empty). With payload deferral it carries only
        # the row key + next node; otherwise it matches the edge schema
        # (legacy: all property columns as STRING).
        if self._deferral_active(p):
            lines.append(
                f"CREATE TEMPORARY TABLE bfs_result_{p.n} "
                f"(_row_id BIGINT, _next_node STRING, _bfs_depth INT);"
            )
        else:
            lines.append(
                f"CREATE TEMPORARY TABLE bfs_result_{p.n} "
                f"({p.src_col} STRING, {p.dst_col} STRING{prop_cols_def}, "
                f"_next_node STRING, _bfs_depth INT);"
            )
        # Save frontier_0 (CROSS JOIN in the final view; also the start-node
        # set for the O12 prune's root exception — must precede the adjacency)
        lines.append(
            f"CREATE TEMPORARY TABLE bfs_frontier_{p.n}_init AS\n"
            f"SELECT node FROM bfs_frontier_{p.n};"
        )

        # Keyed edge copy: materialized once, before the loop (O9). Must
        # precede the adjacency, which is built from it when both are on.
        if self._deferral_active(p):
            lines.append(
                f"CREATE TEMPORARY TABLE bfs_edges_keyed_{p.n} AS\n"
                f"{self._tt_keyed_edges_select(p)};"
            )

        # Doubled adjacency: materialized once, before the loop (O8).
        # Emitted last: it may reference bfs_edges_keyed (O9),
        # bfs_barrier (O11) and bfs_frontier_init (O12).
        if self._adjacency_active(p):
            lines.append(
                f"CREATE TEMPORARY TABLE bfs_adj_{p.n} AS\n"
                f"{self._adjacency_select_body(p)};"
            )

        return "\n".join(lines)

    def _tt_edge_expansion_sql(self, p: _BFSParams) -> str:
        """Build edge expansion SELECT for temp_tables (no quote escaping).

        Emits a ``UNION ALL`` over (direction branch × edge table): undirected
        traversal contributes two equi-join branches when
        ``undirected_union_all`` is on (see :meth:`_direction_branches`), and
        a multi-table edge schema contributes one part per physical table.
        With ``undirected_doubled_adjacency`` the whole expansion collapses
        to a single equi-join against the pre-built ``bfs_adj_{n}`` (filters
        were applied at build time).
        """
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)
        deferred = self._deferral_active(p)

        if self._adjacency_active(p):
            visited = self._tt_visited_not_exists(
                f"bfs_visited_{p.n}", "e._next",
            )
            select_cols = (
                "e._row_id" if deferred
                else f"e.{p.src_col}, e.{p.dst_col}{prop_select}"
            )
            barrier_col = (
                ", e._next_is_barrier"
                if self._barrier_on_adjacency_active(p) else ""
            )
            return (
                f"SELECT {select_cols}, e._next AS _next_node{barrier_col}\n"
                f"FROM bfs_adj_{p.n} e\n"
                f"INNER JOIN bfs_frontier_{p.n} f ON e._jk = f.node\n"
                f"WHERE {visited}"
            )

        branches = self._direction_branches(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        if deferred:
            # Expansion reads the keyed copy: filters/predicate were
            # applied at its build; only the row key travels per level.
            targets: list[tuple[str, str | None]] = [
                (f"bfs_edges_keyed_{p.n}", None),
            ]
            select_prefix = "e._row_id"
            edge_predicate = None
        else:
            targets = self._edge_targets(p)
            select_prefix = f"e.{p.src_col}, e.{p.dst_col}{prop_select}"
            edge_predicate = p.edge_predicate

        parts: list[str] = []
        for join_cond, next_expr in branches:
            for table_sql, table_filter in targets:
                where_parts: list[str] = [
                    self._tt_visited_not_exists(
                        f"bfs_visited_{p.n}", next_expr,
                    )
                ]
                if table_filter:
                    where_parts.append(table_filter)
                if edge_predicate:
                    where_parts.append(edge_predicate)
                parts.append(
                    f"SELECT "
                    f"{select_prefix}, "
                    f"{next_expr} AS _next_node\n"
                    f"FROM {table_sql} e\n"
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
            if self._barrier_on_adjacency_active(p):
                # The verdict was stamped on the adjacency at build time
                # (O11) — a plain column read, zero joins per level.
                barrier_where = "NOT _next_is_barrier"
            elif self._barrier_precompute_active(p):
                # Anti-join the small precomputed id table (O10) instead
                # of probing the full node table every level.
                barrier_where = (
                    f"NOT EXISTS (SELECT 1 FROM bfs_barrier_{p.n} b "
                    f"WHERE b.node = _next_node)"
                )
            else:
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

        # Accumulate result (only for levels >= min_hops). With the
        # carried barrier verdict (O11) bfs_edges has an extra
        # _next_is_barrier column that must NOT leak into bfs_result
        # (its DDL is unchanged) — project explicitly instead of *.
        if self._barrier_on_adjacency_active(p):
            if self._deferral_active(p):
                result_cols = "_row_id, _next_node"
            else:
                props = "".join(f", {c}" for c in p.edge_prop_cols)
                result_cols = f"{p.src_col}, {p.dst_col}{props}, _next_node"
            result_select = (
                f"SELECT {result_cols}, "
                f"current_depth_{p.n} AS _bfs_depth "
            )
        else:
            result_select = f"SELECT *, current_depth_{p.n} AS _bfs_depth "
        if p.min_hops > 1:
            lines.append(
                f"    IF current_depth_{p.n} >= {p.min_hops} THEN"
            )
            lines.append(
                f"      INSERT INTO bfs_result_{p.n}\n"
                f"      {result_select}"
                f"FROM bfs_edges_{p.n};"
            )
            lines.append("    END IF;")
        else:
            lines.append(
                f"    INSERT INTO bfs_result_{p.n}\n"
                f"    {result_select}"
                f"FROM bfs_edges_{p.n};"
            )

        lines.append("  END IF;")
        lines.append("END WHILE;")
        return "\n".join(lines)

    def _tt_final_view(self, p: _BFSParams) -> str:
        """Build final result view for temp_tables strategy.

        With ``deferred_edge_payload`` the edge columns come from the keyed
        copy via a single re-attach join on ``_row_id`` (and keep their
        ORIGINAL schema types); otherwise from the result accumulator
        (legacy: all-STRING columns).
        """
        deferred = self._deferral_active(p)
        edge_alias = "e" if deferred else "r"
        prop_cols = "".join(f", {edge_alias}.{c}" for c in p.edge_prop_cols)

        # path_edges: ARRAY(NAMED_STRUCT(...)) wrapping one edge per row
        path_edges_col = ""
        if p.collect_edges:
            path_edges_expr = self._build_path_edges_expr(p, alias=edge_alias)
            path_edges_col = f",\n       {path_edges_expr} AS path_edges"

        # Only emit path column when collect_nodes is True
        path_col = ""
        if p.collect_nodes:
            path_col = (
                ",\n       CAST(NULL AS ARRAY<STRING>) AS path"
            )

        if deferred:
            from_clause = (
                f"FROM bfs_result_{p.n} r\n"
                f"JOIN bfs_edges_keyed_{p.n} e ON e._row_id = r._row_id\n"
                f"CROSS JOIN bfs_frontier_{p.n}_init f0;"
            )
        else:
            from_clause = (
                f"FROM bfs_result_{p.n} r\n"
                f"CROSS JOIN bfs_frontier_{p.n}_init f0;"
            )

        return (
            f"CREATE OR REPLACE TEMPORARY VIEW {p.cte_name} AS\n"
            f"SELECT f0.node AS start_node, r._next_node AS end_node, "
            f"r._bfs_depth AS depth,\n"
            f"       {edge_alias}.{p.src_col}, "
            f"{edge_alias}.{p.dst_col}{prop_cols}"
            f"{path_edges_col}"
            f"{path_col}\n"
            f"{from_clause}"
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

        bwd_branches = self._direction_branches(
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
            p, bwd_branches,
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

        # Build edge expansion SQL for pruned and unpruned. Pruning restricts
        # next-nodes to the backward-reachable set (per branch inside the
        # forward edge builder).
        pruned_sql = self._tt_bidir_forward_edge_sql(
            p, prune_against=f"bfs_bwd_visited_{p.n}",
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
        branches: list[tuple[str, str]],
    ) -> str:
        """Build backward edge expansion SQL for temp_tables bidir.

        UNION ALL over (direction branch × edge table); undirected backward
        traversal contributes two equi-join branches (see
        :meth:`_direction_branches`). Returns only _next_node (no result cols).
        """
        if p.enriched.single_table:
            targets = [(p.edge_table_sql, p.edge_type_filter)]
        else:
            targets = [
                (ei.table_descriptor.full_table_name, ei.filter_clause)
                for ei in p.enriched.edge_tables
            ]

        parts: list[str] = []
        for bwd_join, bwd_next in branches:
            for table_sql, table_filter in targets:
                where_parts: list[str] = [
                    self._tt_visited_not_exists(
                        f"bfs_bwd_visited_{p.n}", bwd_next,
                    )
                ]
                if table_filter:
                    where_parts.append(table_filter)
                if p.edge_predicate:
                    where_parts.append(p.edge_predicate)
                parts.append(
                    f"SELECT DISTINCT {bwd_next} AS _next_node\n"
                    f"FROM {table_sql} e\n"
                    f"INNER JOIN bfs_bwd_frontier_{p.n} f ON {bwd_join}"
                    + self._build_where_clause(where_parts)
                )

        return "\nUNION ALL\n".join(parts)

    def _tt_bidir_forward_edge_sql(
        self,
        p: _BFSParams,
        prune_against: str | None = None,
    ) -> str:
        """Build forward edge expansion SQL for temp_tables bidir.

        UNION ALL over (direction branch × edge table); undirected traversal
        contributes two equi-join branches (see :meth:`_direction_branches`).
        When ``prune_against`` is a table name, each branch is restricted to
        next-nodes in that backward-reachable set — built per branch from the
        branch's own next-node column so no CASE expression is needed.
        """
        branches = self._direction_branches(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)

        if p.enriched.single_table:
            targets = [(p.edge_table_sql, p.edge_type_filter)]
        else:
            targets = [
                (ei.table_descriptor.full_table_name, ei.filter_clause)
                for ei in p.enriched.edge_tables
            ]

        parts: list[str] = []
        for join_cond, next_expr in branches:
            for table_sql, table_filter in targets:
                where_parts: list[str] = [
                    self._tt_visited_not_exists(
                        f"bfs_visited_{p.n}", next_expr,
                    )
                ]
                if table_filter:
                    where_parts.append(table_filter)
                if p.edge_predicate:
                    where_parts.append(p.edge_predicate)
                if prune_against:
                    where_parts.append(
                        f"{next_expr} IN "
                        f"(SELECT node FROM {prune_against})"
                    )
                parts.append(
                    f"SELECT "
                    f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                    f"{next_expr} AS _next_node\n"
                    f"FROM {table_sql} e\n"
                    f"INNER JOIN bfs_frontier_{p.n} f ON {join_cond}"
                    + self._build_where_clause(where_parts)
                )

        return "\nUNION ALL\n".join(parts)

    # ======================================================================
    # STRATEGY: numbered_views (PySpark 4.2)
    # ======================================================================

    def _render_numbered_views(self, p: _BFSParams) -> tuple[str, str]:
        """Render procedural BFS using EXECUTE IMMEDIATE + numbered views."""
        declarations = self._nv_declarations(p)

        body_parts: list[str] = []
        body_parts.append(self._nv_frontier_init(p))
        body_parts.append(self._nv_visited_init(p))
        if self._adjacency_active(p):
            body_parts.append(self._nv_adjacency_init(p))
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

    def _nv_adjacency_init(self, p: _BFSParams) -> str:
        """Create the static doubled-adjacency view (pre-loop, O8).

        The view name is fixed (not depth-numbered), so this is a plain
        statement — no EXECUTE IMMEDIATE, no quote escaping, no locals
        (42K0M-safe). The view is lazy but constant-depth: each per-level
        expansion references it once, keeping the recursive lineage
        single-branch (unlike per-level UNION ALL, which doubles it).
        """
        return (
            f"CREATE OR REPLACE TEMPORARY VIEW bfs_adj_{p.n} AS\n"
            f"{self._adjacency_select_body(p)};"
        )

    def _nv_edge_expansion_sql(self, p: _BFSParams) -> str:
        """Build edge expansion SELECT for numbered_views (quotes doubled).

        Emits a ``UNION ALL`` over (direction branch × edge table), same as the
        temp_tables path (see :meth:`_tt_edge_expansion_sql`), but with the
        depth-numbered frontier/visited view names injected via the EXECUTE
        IMMEDIATE string-concatenation idiom. With
        ``undirected_doubled_adjacency`` the expansion is a single equi-join
        against the static ``bfs_adj_{n}`` view (filters applied there).
        """
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)

        if self._adjacency_active(p):
            visited = self._nv_visited_not_exists(
                f"bfs_visited_{p.n}", f"bfs_depth_{p.n}", "e._next",
            )
            return (
                f"SELECT e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                f"e._next AS _next_node, "
                f"' || CAST(bfs_depth_{p.n} AS STRING) || ' AS _bfs_depth "
                f"FROM bfs_adj_{p.n} e "
                f"INNER JOIN bfs_frontier_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} - 1 AS STRING) || ' f "
                f"ON e._jk = f.node "
                f"WHERE {visited}"
            )

        branches = self._direction_branches(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        targets = self._edge_targets(p)

        parts: list[str] = []
        for join_cond, next_expr in branches:
            for table_sql, table_filter in targets:
                where_parts: list[str] = [
                    self._nv_visited_not_exists(
                        f"bfs_visited_{p.n}", f"bfs_depth_{p.n}", next_expr,
                    )
                ]
                if table_filter:
                    where_parts.append(table_filter.replace("'", "''"))
                if p.edge_predicate:
                    where_parts.append(p.edge_predicate.replace("'", "''"))

                where_clause = " AND ".join(where_parts)
                parts.append(
                    f"SELECT "
                    f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                    f"{next_expr} AS _next_node, "
                    f"' || CAST(bfs_depth_{p.n} AS STRING) || ' AS _bfs_depth "
                    f"FROM {table_sql} e "
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

        # B. Check frontier size (EXECUTE IMMEDIATE ... INTO local variable).
        # NOTE: `EXECUTE IMMEDIATE 'SET var = (...)'` does NOT assign the
        # script-local variable on Spark 4.2 — it silently writes a session
        # conf key of the same name, leaving `var` at its DECLARE default. That
        # makes `WHILE bfs_frontier_count > 0` never terminate early, so the
        # loop always runs to max_hops and the lazy-view lineage explodes
        # (~2^max_hops). The `... INTO var` form assigns the local correctly.
        # Gated by ProceduralBFSOptimizations.loop_control_into.
        # TODO(databricks-validation): check whether Databricks' EXECUTE
        # IMMEDIATE 'SET var = ...' assigns script locals. If it is a no-op
        # there too (as on OSS Spark 4.2), the legacy branch below is simply a
        # bug — delete it and the flag. Local numbers for INTO: 62.8s->5.3s
        # (11.9x) at *1..8 exhausting at depth 3; costs ~+0.5s on tight
        # bounds (one real COUNT per level re-evaluates the lazy lineage).
        if self._ctx.procedural_optimizations.loop_control_into:
            lines.append(
                f"  EXECUTE IMMEDIATE\n"
                f"    'SELECT COUNT(1) FROM bfs_edges_{p.n}_'"
                f" || CAST(bfs_depth_{p.n} AS STRING)"
                f" INTO bfs_frontier_count_{p.n};"
            )
        else:
            # Legacy form (silent no-op for the local on OSS Spark 4.2).
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

        bwd_branches = self._direction_branches(
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
            p, bwd_branches,
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
        pruned_sql = self._nv_bidir_forward_edge_sql(
            p, prune_against=f"bfs_bwd_reachable_{p.n}",
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
        branches: list[tuple[str, str]],
    ) -> str:
        """Build backward edge expansion SQL for nv bidir.

        UNION ALL over (direction branch × edge table); undirected backward
        traversal contributes two equi-join branches. Quotes doubled for
        EXECUTE IMMEDIATE.
        """
        frontier_ref = (
            f"bfs_bwd_frontier_{p.n}_'"
            f" || CAST(bwd_depth_{p.n} - 1"
            f" AS STRING) || '"
        )

        if p.enriched.single_table:
            targets = [(p.edge_table_sql, p.edge_type_filter)]
        else:
            targets = [
                (ei.table_descriptor.full_table_name, ei.filter_clause)
                for ei in p.enriched.edge_tables
            ]

        parts: list[str] = []
        for bwd_join, bwd_next in branches:
            bwd_visited_ref = self._nv_visited_not_exists(
                f"bfs_bwd_visited_{p.n}", f"bwd_depth_{p.n}", bwd_next,
            )
            for tbl, table_filter in targets:
                wp: list[str] = [bwd_visited_ref]
                if table_filter:
                    wp.append(table_filter.replace("'", "''"))
                if p.edge_predicate:
                    wp.append(p.edge_predicate.replace("'", "''"))
                parts.append(
                    f"SELECT DISTINCT "
                    f"{bwd_next} AS _next_node "
                    f"FROM {tbl} e "
                    f"INNER JOIN {frontier_ref} f "
                    f"ON {bwd_join} "
                    f"WHERE {' AND '.join(wp)}"
                )

        return " UNION ALL ".join(parts)

    def _nv_bidir_forward_edge_sql(
        self,
        p: _BFSParams,
        prune_against: str | None = None,
    ) -> str:
        """Build forward edge expansion SQL for nv bidir.

        UNION ALL over (direction branch × edge table); undirected traversal
        contributes two equi-join branches. When ``prune_against`` is set, each
        branch is restricted to next-nodes in that backward-reachable set
        (built per branch, no CASE needed). Quotes doubled for EXECUTE
        IMMEDIATE.
        """
        branches = self._direction_branches(
            p.src_col, p.dst_col, p.is_backward, p.is_undirected,
        )
        prop_select = "".join(f", e.{c}" for c in p.edge_prop_cols)

        depth_expr = (
            f"' || CAST(bfs_depth_{p.n} AS STRING) || '"
        )
        frontier_ref = (
            f"bfs_frontier_{p.n}_'"
            f" || CAST(bfs_depth_{p.n} - 1"
            f" AS STRING) || '"
        )

        if p.enriched.single_table:
            targets = [(p.edge_table_sql, p.edge_type_filter)]
        else:
            targets = [
                (ei.table_descriptor.full_table_name, ei.filter_clause)
                for ei in p.enriched.edge_tables
            ]

        parts: list[str] = []
        for join_cond, next_expr in branches:
            base_visited = self._nv_visited_not_exists(
                f"bfs_visited_{p.n}", f"bfs_depth_{p.n}", next_expr,
            )
            for tbl, table_filter in targets:
                wp: list[str] = [base_visited]
                if table_filter:
                    wp.append(table_filter.replace("'", "''"))
                if p.edge_predicate:
                    wp.append(p.edge_predicate.replace("'", "''"))
                if prune_against:
                    wp.append(
                        f"{next_expr} IN "
                        f"(SELECT node FROM {prune_against})"
                    )
                parts.append(
                    f"SELECT "
                    f"e.{p.src_col}, e.{p.dst_col}{prop_select}, "
                    f"{next_expr} AS _next_node, "
                    f"{depth_expr} AS _bfs_depth "
                    f"FROM {tbl} e "
                    f"INNER JOIN {frontier_ref} f "
                    f"ON {join_cond} "
                    f"WHERE {' AND '.join(wp)}"
                )

        return " UNION ALL ".join(parts)
