"""SQL Enrichment — pre-rendering resolution of SQL-specific metadata.

This module sits between the planner/resolver and the renderer emission phase.
It walks the resolved logical plan once, resolving all SQL-specific metadata
(table names, join columns, edge info) into an immutable side-channel
(``EnrichedPlanData``).  The renderer reads from this side-channel instead of
calling ``db_schema.*`` directly, making emission mechanical.

Architecture (see ADR-001, ADR-004 in decisions_log.md)::

    Planner (IGraphSchemaProvider)
        ↓  LogicalPlan with semantic info
    Optimizer (pass_manager.py)
        ↓  Optimized LogicalPlan
    Column Resolver
        ↓  Resolved LogicalPlan
    **SQL Enrichment** (this module)        ← runs inside render_plan()
        ↓  EnrichedPlanData (SQL table names, join columns, edge info)
    Renderer (reads EnrichedPlanData, emits SQL)

Design decisions:
    - **Side-channel, not mutation**: Operators stay semantic; SQL metadata is
      stored separately in frozen dataclasses keyed by operator_debug_id.
    - **Incremental migration**: Renderer falls back to ``db_schema`` for operator
      types not yet covered by enrichment.  Each migration adds an ``_enrich_*``
      method here and removes the corresponding ``db_schema.*`` call from the
      renderer.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from gsql2rsql.common.schema import (
    WILDCARD_EDGE_TYPE,
    WILDCARD_NODE_TYPE,
    EdgeSchema,
)
from gsql2rsql.parser.ast import (
    NodeEntity,
    QueryExpression,
    QueryExpressionExists,
    QueryExpressionFunction,
    QueryExpressionListPredicate,
    QueryExpressionProperty,
    RelationshipEntity,
)
from gsql2rsql.parser.operators import Function
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
)
from gsql2rsql.planner.path_analyzer import (
    rewrite_predicate_for_edge_alias,
    rewrite_type_calls_to_property,
)
from gsql2rsql.planner.schema import EntityField
from gsql2rsql.renderer.schema_provider import ISQLDBSchemaProvider, SQLTableDescriptor

# ---------------------------------------------------------------------------
# Enriched metadata dataclasses (immutable, produced by SQLEnrichmentPass)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnrichedDataSource:
    """SQL-resolved metadata for a DataSourceOperator."""

    table_descriptor: SQLTableDescriptor
    type_filter_clause: str | None = None


@dataclass(frozen=True)
class EnrichedEdgeInfo:
    """SQL-resolved metadata for one edge type in a RecursiveTraversalOperator."""

    edge_type: str
    table_descriptor: SQLTableDescriptor
    source_column: str
    sink_column: str
    type_filter_clause: str | None = None
    properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class EnrichedJoinPair:
    """SQL-resolved column names for one join key pair."""

    left_column: str
    right_column: str
    original_pair_type: JoinKeyPairType


@dataclass(frozen=True)
class EnrichedNodeInfo:
    """SQL-resolved metadata for a node type (table + ID column + properties)."""

    table_descriptor: SQLTableDescriptor
    id_column: str
    property_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnrichedRecursiveEdge:
    """SQL-resolved metadata for one edge type in a RecursiveTraversalOperator."""

    edge_type: str
    table_descriptor: SQLTableDescriptor
    filter_clause: str | None = None


@dataclass(frozen=True)
class EnrichedRecursiveOp:
    """SQL-resolved metadata for a RecursiveTraversalOperator.

    Pre-resolves edge tables, column names, filters, and the single-table
    optimisation flag.  Also resolves source and target node info.
    """

    edge_tables: tuple[EnrichedRecursiveEdge, ...]
    source_id_col: str
    target_id_col: str
    edge_property_names: tuple[str, ...]
    single_table: bool
    single_table_name: str | None
    single_table_filter: str | None
    source_node: EnrichedNodeInfo | None = None
    target_node: EnrichedNodeInfo | None = None
    # Pre-rewritten filter ASTs (produced by rewrite_predicate_for_edge_alias)
    edge_filter_as_e: QueryExpression | None = None
    start_filter_as_src: QueryExpression | None = None
    start_filter_as_n: QueryExpression | None = None
    sink_filter_as_tgt: QueryExpression | None = None
    sink_filter_as_sink: QueryExpression | None = None
    # Barrier filter (is_terminator directive) rewritten for "barrier" alias
    barrier_filter_as_barrier: QueryExpression | None = None
    # Node predicate pushdown: from ALL(n IN nodes(path) WHERE pred).
    # When node_pushdown_node is set, the CTE renderer may traverse a
    # pre-filtered edge set (both edge endpoints satisfy the predicate)
    # instead of the raw edge table. OPTIMIZATION ONLY — the originating
    # ALL always stays in the final WHERE as a residual subquery, so a
    # renderer that ignores these fields is slower, never wrong.
    node_filter_as_nf: QueryExpression | None = None
    node_pushdown_node: EnrichedNodeInfo | None = None


@dataclass(frozen=True)
class EnrichedExistsExpr:
    """SQL-resolved metadata for a QueryExpressionExists.

    Pre-resolves all db_schema lookups needed by expression_renderer._render_exists():
    edge table, edge columns, target node table/ID, source node ID column.

    Keyed by ``id(expr)`` in ``EnrichedPlanData.exists_exprs``.
    """

    rel_table_name: str
    source_id_col: str
    target_id_col: str
    # Updated source entity name (may differ from AST when resolved via fallback)
    source_entity_name: str
    target_table_name: str | None = None
    target_node_id_col: str = "id"
    source_node_id_col: str = "id"


@dataclass(frozen=True)
class EnrichedNodesListPredicate:
    """SQL-resolved metadata for a list predicate over ``nodes(path)``.

    Covers ``ALL/ANY/NONE/SINGLE(n IN nodes(p) WHERE <property predicate>)``.
    The path array stores scalar node IDs, so a property-accessing lambda
    cannot be rendered as a HOF (``FORALL(path, n -> n.prop)`` is a type
    error in Spark). The expression renderer instead emits a correlated
    subquery against the node table; this dataclass carries the resolved
    table metadata so the renderer never touches ``db_schema``.

    Keyed by ``id(expr)`` in ``EnrichedPlanData.nodes_predicates``.
    """

    node_table_name: str
    node_id_column: str
    # Bare-column type filter (e.g. "node_type = 'Person'"); prefix with the
    # subquery alias when emitting. None when the traversal is untyped.
    node_type_filter: str | None = None
    # operator_debug_id of the RecursiveTraversalOperator whose node_filter
    # pushdown this expression originated (the ALL was extracted into the
    # op's node_filter). None for non-pushable predicates (ANY/NONE/SINGLE,
    # under OR, outer-variable lambdas). When set AND the pushdown is
    # active (see vlp_node_pushdown_active), the procedural renderer
    # enforces the predicate via its frontier filter and the expression
    # renderer emits TRUE for the residual — the only mode-correct
    # rendering there, since procedural never collects the path array.
    enforced_by_traversal_op_id: int | None = None


@dataclass(frozen=True)
class EnrichedPlanData:
    """Immutable SQL-resolved metadata for the entire plan.

    Produced by ``SQLEnrichmentPass``, consumed by the renderer.
    Keyed by ``operator_debug_id`` (operators) or ``id(expr)`` (expressions).

    During incremental migration, a dict being empty for a given operator type
    means the renderer should fall back to its existing ``db_schema`` code path.
    """

    data_sources: dict[int, EnrichedDataSource] = field(default_factory=dict)
    join_pairs: dict[int, list[EnrichedJoinPair]] = field(default_factory=dict)
    edge_infos: dict[int, list[EnrichedEdgeInfo]] = field(default_factory=dict)
    recursive_ops: dict[int, EnrichedRecursiveOp] = field(default_factory=dict)
    exists_exprs: dict[int, EnrichedExistsExpr] = field(default_factory=dict)
    nodes_predicates: dict[int, EnrichedNodesListPredicate] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Enrichment pass
# ---------------------------------------------------------------------------


class SQLEnrichmentPass:
    """Single-pass tree walker that resolves SQL metadata.

    Call ``.enrich(plan)`` to produce an ``EnrichedPlanData``
    instance.  The pass walks every operator exactly once (bottom-up) and
    delegates to type-specific ``_enrich_*`` methods.

    Currently a no-op skeleton — each ``_enrich_*`` method will be implemented
    as we migrate the corresponding renderer logic here.
    """

    def __init__(self, db_schema: ISQLDBSchemaProvider) -> None:
        self._db_schema = db_schema

    def enrich(
        self,
        plan: LogicalPlan,
    ) -> EnrichedPlanData:
        """Walk plan once, produce SQL-resolved metadata."""
        data_sources: dict[int, EnrichedDataSource] = {}
        join_pairs: dict[int, list[EnrichedJoinPair]] = {}
        edge_infos: dict[int, list[EnrichedEdgeInfo]] = {}
        recursive_ops: dict[int, EnrichedRecursiveOp] = {}
        exists_exprs: dict[int, EnrichedExistsExpr] = {}
        nodes_predicates: dict[int, EnrichedNodesListPredicate] = {}
        # path variable name -> its RecursiveTraversalOperator. Filled while
        # walking (bottom-up), so it is populated before the Selection /
        # Projection operators above the traversal are enriched.
        path_var_map: dict[str, RecursiveTraversalOperator] = {}

        visited: set[int] = set()
        for terminal in plan.terminal_operators:
            self._walk(
                terminal, visited, data_sources, join_pairs, edge_infos,
                recursive_ops, exists_exprs, nodes_predicates, path_var_map,
            )

        return EnrichedPlanData(
            data_sources=data_sources,
            join_pairs=join_pairs,
            edge_infos=edge_infos,
            recursive_ops=recursive_ops,
            exists_exprs=exists_exprs,
            nodes_predicates=nodes_predicates,
        )

    def _walk(
        self,
        op: LogicalOperator,
        visited: set[int],
        data_sources: dict[int, EnrichedDataSource],
        join_pairs: dict[int, list[EnrichedJoinPair]],
        edge_infos: dict[int, list[EnrichedEdgeInfo]],
        recursive_ops: dict[int, EnrichedRecursiveOp],
        exists_exprs: dict[int, EnrichedExistsExpr],
        nodes_predicates: dict[int, EnrichedNodesListPredicate],
        path_var_map: dict[str, RecursiveTraversalOperator],
    ) -> None:
        """Bottom-up walk: enrich children first, then current operator."""
        op_id = op.operator_debug_id
        if op_id in visited:
            return
        visited.add(op_id)

        for child in op.in_operators:
            self._walk(
                child, visited, data_sources, join_pairs, edge_infos,
                recursive_ops, exists_exprs, nodes_predicates, path_var_map,
            )

        self._enrich_one(
            op, data_sources, join_pairs, edge_infos, recursive_ops,
            exists_exprs, nodes_predicates, path_var_map,
        )

    def _enrich_one(
        self,
        op: LogicalOperator,
        data_sources: dict[int, EnrichedDataSource],
        join_pairs: dict[int, list[EnrichedJoinPair]],
        edge_infos: dict[int, list[EnrichedEdgeInfo]],
        recursive_ops: dict[int, EnrichedRecursiveOp],
        exists_exprs: dict[int, EnrichedExistsExpr],
        nodes_predicates: dict[int, EnrichedNodesListPredicate],
        path_var_map: dict[str, RecursiveTraversalOperator],
    ) -> None:
        """Dispatch to type-specific enrichment."""
        if isinstance(op, DataSourceOperator):
            self._enrich_data_source(op, data_sources)
        elif isinstance(op, JoinOperator):
            self._enrich_join(op, join_pairs)
        elif isinstance(op, RecursiveTraversalOperator):
            self._enrich_recursive(op, recursive_ops)
            if op.path_variable:
                path_var_map[op.path_variable] = op

        # Walk expression trees for EXISTS sub-expressions (any operator type)
        self._enrich_exists_in_operator(op, exists_exprs)
        # Walk expression trees for list predicates over nodes(path)
        self._enrich_nodes_predicates_in_operator(
            op, nodes_predicates, path_var_map
        )

    # ------------------------------------------------------------------
    # Type-specific enrichment methods (stubs for incremental migration)
    # ------------------------------------------------------------------

    def _enrich_data_source(
        self,
        op: DataSourceOperator,
        data_sources: dict[int, EnrichedDataSource],
    ) -> None:
        """Resolve SQL table name and type filter for a DataSourceOperator.

        Migrated from sql_renderer._render_data_source() lines 760-878.
        Resolves:
          - table_descriptor: SQL table from entity name (with wildcard fallback)
          - type_filter_clause: edge type filter (single or multi-edge OR)
        """
        if not op.output_schema:
            return

        entity_field = op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return

        # Resolve table descriptor (supports no-label nodes via wildcard)
        table_desc = self._resolve_table_descriptor(entity_field.bound_entity_name)
        if table_desc is None:
            return

        # Resolve type filter clause from edge types
        type_filter = self._resolve_type_filter(entity_field, table_desc)

        data_sources[op.operator_debug_id] = EnrichedDataSource(
            table_descriptor=table_desc,
            type_filter_clause=type_filter,
        )

    def _resolve_table_descriptor(
        self, entity_name: str
    ) -> SQLTableDescriptor | None:
        """Get table descriptor, falling back to wildcard for no-label nodes/edges."""
        if not entity_name or entity_name == WILDCARD_NODE_TYPE:
            return self._db_schema.get_wildcard_table_descriptor()
        if WILDCARD_EDGE_TYPE in entity_name:
            return self._db_schema.get_wildcard_edge_table_descriptor()
        return self._db_schema.get_sql_table_descriptors(entity_name)

    def _resolve_type_filter(
        self, entity_field: EntityField, table_desc: SQLTableDescriptor
    ) -> str | None:
        """Compute type filter clause for edge types.

        For multi-edge ([:KNOWS|WORKS_AT]), combines per-type filters with OR.
        For single-edge, uses the table descriptor's filter directly.
        """
        if len(entity_field.bound_edge_types) > 1:
            edge_filters: list[str] = []
            parts = entity_field.bound_entity_name.split("@")
            source_type = parts[0] if len(parts) >= 1 else None

            for edge_type in entity_field.bound_edge_types:
                edges = self._db_schema.find_edges_by_verb(
                    edge_type,
                    from_node_name=source_type,
                    to_node_name=None,
                )
                if edges:
                    edge_id = edges[0].id
                    type_desc = self._db_schema.get_sql_table_descriptors(edge_id)
                    if type_desc and type_desc.filter:
                        edge_filters.append(type_desc.filter)

            if edge_filters:
                return " OR ".join(f"({f})" for f in edge_filters)
            return None

        if table_desc.filter:
            return table_desc.filter

        return None

    def _enrich_join(
        self,
        op: JoinOperator,
        join_pairs: dict[int, list[EnrichedJoinPair]],
    ) -> None:
        """Resolve SQL column names for join key pairs.

        TODO: Migrate logic from sql_renderer._collect_join_key_columns() lines 397-486.
        Note: _collect_join_key_columns is column-pruning logic, not SQL resolution.
        The actual join rendering uses entity schema fields directly.  This stub
        may be removed if there's no real SQL resolution to migrate here.
        """

    def _enrich_recursive(
        self,
        op: RecursiveTraversalOperator,
        recursive_ops: dict[int, EnrichedRecursiveOp],
    ) -> None:
        """Resolve edge tables, columns, filters, and node info for recursive traversal.

        Migrated from recursive_cte_renderer._resolve_edge_tables(),
        _get_edge_column_names(), _get_edge_table_name(), _get_edge_filter_clause().
        """
        edge_types = op.edge_types if op.edge_types else []
        edge_tables: list[EnrichedRecursiveEdge] = []
        source_id_col = op.source_id_column or "source_id"
        target_id_col = op.target_id_column or "target_id"
        edge_props: list[str] = list(op.edge_properties) if op.edge_properties else []
        # Column holding the relationship type name (for Cypher type(r)
        # rewriting in edge lambdas). None when the schema doesn't declare
        # one — type(r) then fails with a clear renderer error.
        edge_type_column: str | None = None

        if edge_types:
            for edge_type in edge_types:
                edge_table = None
                edge_schema = None

                if not op.source_node_type or not op.target_node_type:
                    edges = self._db_schema.find_edges_by_verb(
                        edge_type,
                        from_node_name=op.source_node_type or None,
                        to_node_name=op.target_node_type or None,
                    )
                    if edges:
                        edge_schema = edges[0]
                        edge_table = self._db_schema.get_sql_table_descriptors(
                            edge_schema.id
                        )
                else:
                    edge_id = EdgeSchema.get_edge_id(
                        edge_type, op.source_node_type, op.target_node_type,
                    )
                    edge_table = self._db_schema.get_sql_table_descriptors(edge_id)
                    if not edge_table:
                        edge_table = self._db_schema.get_sql_table_descriptors(
                            edge_type
                        )
                    edge_schema = self._db_schema.get_edge_definition(
                        edge_type, op.source_node_type, op.target_node_type,
                    )

                if edge_table:
                    edge_tables.append(
                        EnrichedRecursiveEdge(
                            edge_type=edge_type,
                            table_descriptor=edge_table,
                            filter_clause=edge_table.filter if edge_table.filter else None,
                        )
                    )

                if (
                    edge_schema
                    and edge_schema.type_property
                    and edge_type_column is None
                ):
                    edge_type_column = edge_schema.type_property.property_name

                if len(edge_tables) == 1 and edge_schema:
                    if edge_schema.source_id_property:
                        source_id_col = edge_schema.source_id_property.property_name
                    if edge_schema.sink_id_property:
                        target_id_col = edge_schema.sink_id_property.property_name
                    if op.collect_edges and not edge_props:
                        for prop in edge_schema.properties:
                            if prop.property_name not in (
                                source_id_col, target_id_col,
                            ):
                                edge_props.append(prop.property_name)
        else:
            wildcard_table = self._db_schema.get_wildcard_edge_table_descriptor()
            if wildcard_table:
                edge_tables.append(
                    EnrichedRecursiveEdge(
                        edge_type=WILDCARD_EDGE_TYPE,
                        table_descriptor=wildcard_table,
                        filter_clause=wildcard_table.filter if wildcard_table.filter else None,
                    )
                )
            wildcard_schema = self._db_schema.get_wildcard_edge_definition()
            if wildcard_schema:
                if wildcard_schema.source_id_property:
                    source_id_col = wildcard_schema.source_id_property.property_name
                if wildcard_schema.sink_id_property:
                    target_id_col = wildcard_schema.sink_id_property.property_name
                if wildcard_schema.type_property:
                    edge_type_column = (
                        wildcard_schema.type_property.property_name
                    )
                if op.collect_edges and not edge_props:
                    for prop in wildcard_schema.properties:
                        if prop.property_name not in (source_id_col, target_id_col):
                            edge_props.append(prop.property_name)

        if not edge_tables:
            return

        # Single-table optimisation
        table_to_filters: dict[str, list[str]] = {}
        for et in edge_tables:
            tname = et.table_descriptor.full_table_name
            if tname not in table_to_filters:
                table_to_filters[tname] = []
            if et.filter_clause:
                table_to_filters[tname].append(et.filter_clause)

        single_table = len(table_to_filters) == 1
        single_table_name: str | None = None
        single_table_filter: str | None = None
        if single_table:
            single_table_name = next(iter(table_to_filters))
            filters_list = table_to_filters[single_table_name]
            if len(filters_list) > 1:
                single_table_filter = " OR ".join(f"({f})" for f in filters_list)
            elif len(filters_list) == 1:
                single_table_filter = filters_list[0]

        # Resolve source and target node info
        source_node = self._resolve_node_info(op.source_node_type)
        target_node = self._resolve_node_info(op.target_node_type)

        # Pre-rewrite filter ASTs for all alias variants
        edge_filter_as_e = None
        start_filter_as_src = None
        start_filter_as_n = None
        sink_filter_as_tgt = None
        sink_filter_as_sink = None

        if op.edge_filter and op.edge_filter_lambda_var:
            edge_filter_ast = op.edge_filter
            # Cypher type(r) -> r.<type column> (resolved from the edge
            # schema). Done BEFORE alias rewriting so the pushed-down CTE
            # filter renders as e.<type column>. When the schema declares
            # no type column, the TYPE call is left as-is and the renderer
            # fails with a clear unsupported-function error (never a
            # silently wrong filter).
            if edge_type_column:
                edge_filter_ast = rewrite_type_calls_to_property(
                    edge_filter_ast,
                    op.edge_filter_lambda_var,
                    edge_type_column,
                )
            edge_filter_as_e = rewrite_predicate_for_edge_alias(
                edge_filter_ast, op.edge_filter_lambda_var,
                edge_alias="e",
            )
        if op.start_node_filter and op.source_alias:
            start_filter_as_src = rewrite_predicate_for_edge_alias(
                op.start_node_filter, op.source_alias,
                edge_alias="src",
            )
            start_filter_as_n = rewrite_predicate_for_edge_alias(
                op.start_node_filter, op.source_alias,
                edge_alias="n",
            )
        if op.sink_node_filter and op.target_alias:
            sink_filter_as_tgt = rewrite_predicate_for_edge_alias(
                op.sink_node_filter, op.target_alias,
                edge_alias="tgt",
            )
            sink_filter_as_sink = rewrite_predicate_for_edge_alias(
                op.sink_node_filter, op.target_alias,
                edge_alias="sink",
            )

        # Barrier filter (is_terminator directive)
        barrier_filter_as_barrier = None
        if op.barrier_filter and op.target_alias:
            barrier_filter_as_barrier = rewrite_predicate_for_edge_alias(
                op.barrier_filter, op.target_alias,
                edge_alias="barrier",
            )

        # Node predicate pushdown eligibility. Conservative: only the
        # single-edge-table case with the plain (non-bidirectional-BFS)
        # rendering path. Everything else keeps correctness via the
        # residual subquery in the final WHERE.
        node_filter_as_nf = None
        node_pushdown_node = None
        if (
            op.node_filter is not None
            and op.node_filter_lambda_var
            and single_table
            and getattr(op, "bidirectional_bfs_mode", "off") == "off"
        ):
            pushdown_node = self._resolve_node_info(WILDCARD_NODE_TYPE)
            if pushdown_node is not None:
                node_filter_as_nf = rewrite_predicate_for_edge_alias(
                    op.node_filter, op.node_filter_lambda_var,
                    edge_alias="nf",
                )
                node_pushdown_node = pushdown_node

        recursive_ops[op.operator_debug_id] = EnrichedRecursiveOp(
            edge_tables=tuple(edge_tables),
            source_id_col=source_id_col,
            target_id_col=target_id_col,
            edge_property_names=tuple(edge_props),
            single_table=single_table,
            single_table_name=single_table_name,
            single_table_filter=single_table_filter,
            source_node=source_node,
            target_node=target_node,
            edge_filter_as_e=edge_filter_as_e,
            start_filter_as_src=start_filter_as_src,
            start_filter_as_n=start_filter_as_n,
            sink_filter_as_tgt=sink_filter_as_tgt,
            sink_filter_as_sink=sink_filter_as_sink,
            barrier_filter_as_barrier=barrier_filter_as_barrier,
            node_filter_as_nf=node_filter_as_nf,
            node_pushdown_node=node_pushdown_node,
        )

    def _resolve_node_info(self, node_type: str) -> EnrichedNodeInfo | None:
        """Resolve node table descriptor, ID column, and property names."""
        table_desc = self._resolve_table_descriptor(node_type)
        if table_desc is None:
            return None

        node_schema = self._db_schema.get_node_definition(node_type)
        if not node_schema and (not node_type or node_type == WILDCARD_NODE_TYPE):
            node_schema = self._db_schema.get_wildcard_node_definition()

        if node_schema and node_schema.node_id_property:
            id_col = node_schema.node_id_property.property_name
        else:
            id_col = "id"

        prop_names: tuple[str, ...] = ()
        if node_schema:
            prop_names = tuple(p.property_name for p in node_schema.properties)

        return EnrichedNodeInfo(
            table_descriptor=table_desc,
            id_column=id_col,
            property_names=prop_names,
        )

    # ------------------------------------------------------------------
    # EXISTS expression enrichment
    # ------------------------------------------------------------------

    def _enrich_exists_in_operator(
        self,
        op: LogicalOperator,
        exists_exprs: dict[int, EnrichedExistsExpr],
    ) -> None:
        """Find all QueryExpressionExists in an operator's expression trees."""
        expressions: list[QueryExpression] = []
        if isinstance(op, SelectionOperator) and op.filter_expression:
            expressions.append(op.filter_expression)
        if isinstance(op, ProjectionOperator):
            if op.filter_expression:
                expressions.append(op.filter_expression)
            if op.having_expression:
                expressions.append(op.having_expression)
            for _alias, proj_expr in op.projections:
                if proj_expr:
                    expressions.append(proj_expr)

        for expr_root in expressions:
            for exists_expr in expr_root.get_children_query_expression_type(
                QueryExpressionExists
            ):
                expr_id = id(exists_expr)
                if expr_id not in exists_exprs:
                    enriched = self._enrich_exists_expr(exists_expr)
                    if enriched is not None:
                        exists_exprs[expr_id] = enriched

    # ------------------------------------------------------------------
    # nodes(path) list-predicate enrichment
    # ------------------------------------------------------------------

    def _enrich_nodes_predicates_in_operator(
        self,
        op: LogicalOperator,
        nodes_predicates: dict[int, EnrichedNodesListPredicate],
        path_var_map: dict[str, RecursiveTraversalOperator],
    ) -> None:
        """Find list predicates over ``nodes(path)`` in expression trees.

        Only predicates whose lambda body accesses PROPERTIES of the lambda
        variable need enrichment — pure-ID lambdas (``n = value``) render
        fine as HOFs over the scalar path array and are left alone.
        """
        if not path_var_map:
            return

        expressions: list[QueryExpression] = []
        if isinstance(op, SelectionOperator) and op.filter_expression:
            expressions.append(op.filter_expression)
        if isinstance(op, ProjectionOperator):
            if op.filter_expression:
                expressions.append(op.filter_expression)
            if op.having_expression:
                expressions.append(op.having_expression)
            for _alias, proj_expr in op.projections:
                if proj_expr:
                    expressions.append(proj_expr)

        for expr_root in expressions:
            for lp in expr_root.get_children_query_expression_type(
                QueryExpressionListPredicate
            ):
                expr_id = id(lp)
                if expr_id in nodes_predicates:
                    continue
                path_op = self._match_nodes_of_path(lp, path_var_map)
                if path_op is None:
                    continue
                if not _lambda_accesses_properties(
                    lp.filter_expression, lp.variable_name
                ):
                    continue
                enriched = self._enrich_nodes_predicate(lp, path_op)
                if enriched is not None:
                    nodes_predicates[expr_id] = enriched

    def _match_nodes_of_path(
        self,
        lp: QueryExpressionListPredicate,
        path_var_map: dict[str, RecursiveTraversalOperator],
    ) -> RecursiveTraversalOperator | None:
        """Return the traversal op if ``lp`` iterates ``nodes(<path var>)``.

        Also matches ``nodes(<path var>)[a..b]`` (a LIST_SLICE wrapping the
        nodes() call): the fallback subquery correlates via
        ARRAY_CONTAINS over whatever the list expression renders to, so a
        sliced path array works unchanged. Note the PUSHDOWN never fires
        for sliced forms — path_analyzer requires a bare nodes(path) —
        because endpoint-filtering the traversal would wrongly constrain
        nodes the slice excludes.
        """
        list_expr = lp.list_expression
        if (
            isinstance(list_expr, QueryExpressionFunction)
            and list_expr.function == Function.LIST_SLICE
            and list_expr.parameters
        ):
            list_expr = list_expr.parameters[0]
        if not isinstance(list_expr, QueryExpressionFunction):
            return None
        if list_expr.function != Function.NODES:
            return None
        for param in list_expr.parameters:
            if (
                isinstance(param, QueryExpressionProperty)
                and not param.property_name
                and param.variable_name in path_var_map
            ):
                return path_var_map[param.variable_name]
        return None

    def _enrich_nodes_predicate(
        self,
        lp: QueryExpressionListPredicate,
        path_op: RecursiveTraversalOperator,
    ) -> EnrichedNodesListPredicate | None:
        """Resolve the node table used to look up path node properties.

        ``nodes(path)`` covers every node the traversal visits —
        intermediate nodes are not constrained to the source/target types —
        so the wildcard node table (the full node set) is the correct
        lookup target, with no type filter.

        Also records (by expression identity) whether this exact ALL was
        extracted into the traversal op's node_filter — the procedural
        renderer enforces those via its frontier filter.
        """
        node_info = self._resolve_node_info(WILDCARD_NODE_TYPE)
        if node_info is None:
            return None

        enforced_op_id: int | None = None
        source_exprs = getattr(path_op, "node_filter_source_exprs", [])
        if any(lp is src for src in source_exprs):
            enforced_op_id = path_op.operator_debug_id

        return EnrichedNodesListPredicate(
            node_table_name=node_info.table_descriptor.full_table_name,
            node_id_column=node_info.id_column,
            node_type_filter=None,
            enforced_by_traversal_op_id=enforced_op_id,
        )

    def _enrich_exists_expr(
        self, expr: QueryExpressionExists
    ) -> EnrichedExistsExpr | None:
        """Resolve all SQL metadata for a single EXISTS expression.

        Mirrors the 7 db_schema lookups in expression_renderer._render_exists().
        """
        if expr.subquery or not expr.pattern_entities:
            return None

        source_node: NodeEntity | None = None
        relationship: RelationshipEntity | None = None
        target_node: NodeEntity | None = None

        for entity in expr.pattern_entities:
            if isinstance(entity, NodeEntity):
                if source_node is None:
                    source_node = entity
                else:
                    target_node = entity
            elif isinstance(entity, RelationshipEntity):
                relationship = entity

        if not relationship or not source_node:
            return None

        rel_name = relationship.entity_name
        source_entity_name = source_node.entity_name or ""
        target_entity_name = target_node.entity_name if target_node else ""

        edge_schema: EdgeSchema | None = None
        rel_table_desc: SQLTableDescriptor | None = None

        if source_entity_name and target_entity_name:
            if not expr.correlation_uses_source:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_id = EdgeSchema.get_edge_id(
                    rel_name, source_entity_name, target_entity_name
                )
            rel_table_desc = self._db_schema.get_sql_table_descriptors(edge_id)
            if not expr.correlation_uses_source:
                edge_schema = self._db_schema.get_edge_definition(
                    rel_name, target_entity_name, source_entity_name
                )
            else:
                edge_schema = self._db_schema.get_edge_definition(
                    rel_name, source_entity_name, target_entity_name
                )
        else:
            result = self._db_schema.find_edge_by_verb(
                rel_name, target_entity_name
            )
            if result:
                edge_schema, rel_table_desc = result
                source_entity_name = edge_schema.source_node_id

        if not rel_table_desc:
            return None

        rel_table = rel_table_desc.full_table_name
        source_id_col = "source_id"
        target_id_col = "target_id"
        if edge_schema:
            if edge_schema.source_id_property:
                source_id_col = edge_schema.source_id_property.property_name
            if edge_schema.sink_id_property:
                target_id_col = edge_schema.sink_id_property.property_name

        # Resolve target node table and ID column
        target_table_name: str | None = None
        target_node_id_col = "id"
        if target_node and target_node.entity_name:
            target_table_desc = self._db_schema.get_sql_table_descriptors(
                target_node.entity_name
            )
            if target_table_desc:
                target_table_name = target_table_desc.full_table_name
                target_node_schema = self._db_schema.get_node_definition(
                    target_node.entity_name
                )
                if target_node_schema and target_node_schema.node_id_property:
                    target_node_id_col = (
                        target_node_schema.node_id_property.property_name
                    )

        # Resolve source node ID column
        source_node_id_col = "id"
        if source_node.entity_name:
            source_node_schema = self._db_schema.get_node_definition(
                source_node.entity_name
            )
            if source_node_schema and source_node_schema.node_id_property:
                source_node_id_col = (
                    source_node_schema.node_id_property.property_name
                )

        return EnrichedExistsExpr(
            rel_table_name=rel_table,
            source_id_col=source_id_col,
            target_id_col=target_id_col,
            source_entity_name=source_entity_name,
            target_table_name=target_table_name,
            target_node_id_col=target_node_id_col,
            source_node_id_col=source_node_id_col,
        )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _lambda_accesses_properties(
    filter_expression: QueryExpression | None, lambda_var: str
) -> bool:
    """True if the lambda body reads a PROPERTY of the lambda variable.

    ``ALL(n IN nodes(p) WHERE n.status = 'A')`` -> True (needs node table).
    ``ANY(n IN nodes(p) WHERE n = 42)``         -> False (IDs suffice; the
    existing HOF/ARRAY_CONTAINS rendering over the scalar path array is
    both valid and faster).
    """
    if filter_expression is None:
        return False
    if (
        isinstance(filter_expression, QueryExpressionProperty)
        and filter_expression.variable_name == lambda_var
        and filter_expression.property_name
    ):
        return True
    for child in filter_expression.children:
        if isinstance(child, QueryExpression) and _lambda_accesses_properties(
            child, lambda_var
        ):
            return True
    return False


def vlp_node_pushdown_active(
    enriched_rec: EnrichedRecursiveOp | None,
    config: dict[str, object],
) -> bool:
    """Single eligibility rule for VLP node predicate pushdown.

    Consulted by BOTH the traversal renderers (to decide whether to filter
    the traversal) and the expression renderer (to decide how to render the
    originating ALL). They MUST agree: in procedural mode the frontier
    filter is the only enforcement of the predicate (the path array is
    never collected), so a renderer applying the pushdown while the
    expression renderer refuses — or vice versa — would either drop the
    filter silently or reject a supported query.
    """
    return (
        enriched_rec is not None
        and enriched_rec.node_filter_as_nf is not None
        and enriched_rec.node_pushdown_node is not None
        and bool(config.get("enable_vlp_node_pushdown", True))
    )
