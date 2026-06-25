"""Join renderer — SQL generation for join operators.

Handles rendering of all join types: standard inner/cross joins, recursive CTE
joins (variable-length path endpoints), aggregation boundary joins (WITH ...
MATCH patterns), and undirected edge UNION ALL optimization.
"""

from __future__ import annotations

from typing import Callable, TYPE_CHECKING

from gsql2rsql.common.exceptions import TranspilerInternalErrorException
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType, Schema, ValueField

if TYPE_CHECKING:
    from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
    from gsql2rsql.renderer.render_context import RenderContext
    from gsql2rsql.renderer.sql_enrichment import EnrichedRecursiveOp


class JoinRenderer:
    """Renders SQL JOIN clauses for all join types.

    Receives a ``RenderContext`` for shared state, an ``ExpressionRenderer``
    for rendering edge filter expressions, and callbacks for operator rendering
    and aggregation boundary references to avoid circular imports.
    """

    def __init__(
        self,
        ctx: "RenderContext",
        expr: "ExpressionRenderer",
        render_operator_fn: Callable[[LogicalOperator, int], str],
        render_agg_boundary_ref_fn: Callable[[AggregationBoundaryOperator, int], str],
    ) -> None:
        self._ctx = ctx
        self._expr = expr
        self._render_operator = render_operator_fn
        self._render_aggregation_boundary_reference = render_agg_boundary_ref_fn

    def _get_enriched_recursive(self, op: RecursiveTraversalOperator) -> EnrichedRecursiveOp | None:
        """Get enriched data for a RecursiveTraversalOperator."""
        if self._ctx.enriched:
            return self._ctx.enriched.recursive_ops.get(
                op.operator_debug_id
            )
        return None

    def _is_node_join_needed(
        self,
        node_alias: str,
        node_info: "EnrichedRecursiveOp | None",
        table_filter: str | None,
        has_sink_filter: bool = False,
    ) -> bool:
        """Check if a source/sink node JOIN is needed.

        A node JOIN can be skipped when ALL of these are true:
        1. Column pruning is enabled and required_columns is populated
        2. No columns with prefix _gsql2rsql_{alias}_ are required
        3. The node table has no type filter (shared-table discriminator)
        4. No sink filter is pushed to this join
        """
        # Conservative: keep JOIN if pruning not active
        if (
            not self._ctx.enable_column_pruning
            or not self._ctx.required_columns
        ):
            return True
        # Type filter requires the JOIN (e.g., node_type = 'Person')
        if table_filter:
            return True
        # Sink filter pushdown requires the JOIN
        if has_sink_filter:
            return True
        # Check if any required column references this node alias
        prefix = f"{self._ctx.COLUMN_PREFIX}{node_alias}_"
        return any(
            c.startswith(prefix) for c in self._ctx.required_columns
        )

    def _render_recursive_join(
        self,
        join_op: JoinOperator,
        recursive_op: RecursiveTraversalOperator,
        target_op: DataSourceOperator,
        depth: int,
    ) -> str:
        """Render a JOIN between recursive CTE and source/target nodes.

        Source and sink node JOINs are eliminated when their columns
        are not referenced downstream and no type/sink filters apply.
        """
        indent = self._ctx.indent(depth)
        cte_name = getattr(recursive_op, "cte_name", "paths")
        min_depth = (
            recursive_op.min_hops
            if recursive_op.min_hops is not None
            else 1
        )

        # Read enriched recursive op data
        enriched_rec = (
            self._ctx.enriched.recursive_ops.get(
                recursive_op.operator_debug_id
            )
            if self._ctx.enriched
            else None
        )
        if not enriched_rec:
            raise TranspilerInternalErrorException(
                "No enriched data for RecursiveTraversalOperator"
            )

        target_entity = target_op.entity
        if target_entity is None:
            raise TranspilerInternalErrorException(
                "Target operator has no entity defined"
            )

        target_node = enriched_rec.target_node
        source_node = enriched_rec.source_node

        if not target_node:
            raise TranspilerInternalErrorException(
                "No enriched node info for target "
                f"{target_entity.entity_name}"
            )

        target_table = target_node.table_descriptor
        target_id_col = target_node.id_column

        target_alias = target_entity.alias or "n"
        source_alias = recursive_op.source_alias or "src"

        if source_node:
            source_table = source_node.table_descriptor
            source_id_col = source_node.id_column
        else:
            source_table = target_table
            source_id_col = target_id_col

        # ---- Determine which JOINs are actually needed ----
        sink_needed = self._is_node_join_needed(
            target_alias,
            enriched_rec,
            target_node.table_descriptor.filter,
            has_sink_filter=bool(
                enriched_rec and enriched_rec.sink_filter_as_sink
            ),
        )
        source_needed = (
            source_alias != target_alias
            and self._is_node_join_needed(
                source_alias,
                enriched_rec,
                (
                    source_node.table_descriptor.filter
                    if source_node
                    else None
                ),
            )
        )

        # ---- SELECT columns ----
        lines: list[str] = []
        lines.append(f"{indent}SELECT")

        field_lines: list[str] = []

        # Project fields from TARGET node (sink)
        if sink_needed:
            if target_node.property_names:
                field_lines.append(
                    f"sink.{target_id_col} AS "
                    f"{self._ctx.COLUMN_PREFIX}"
                    f"{target_alias}_{target_id_col}"
                )
                for prop_name in target_node.property_names:
                    if prop_name != target_id_col:
                        field_lines.append(
                            f"sink.{prop_name} AS "
                            f"{self._ctx.COLUMN_PREFIX}"
                            f"{target_alias}_{prop_name}"
                        )
            else:
                field_lines.append(
                    f"sink.{target_id_col} AS "
                    f"{self._ctx.COLUMN_PREFIX}{target_alias}_id"
                )

        # Project fields from SOURCE node
        if source_needed:
            if source_node and source_node.property_names:
                field_lines.append(
                    f"source.{source_id_col} AS "
                    f"{self._ctx.COLUMN_PREFIX}"
                    f"{source_alias}_{source_id_col}"
                )
                for prop_name in source_node.property_names:
                    if prop_name != source_id_col:
                        field_lines.append(
                            f"source.{prop_name} AS "
                            f"{self._ctx.COLUMN_PREFIX}"
                            f"{source_alias}_{prop_name}"
                        )
            else:
                field_lines.append(
                    f"source.{source_id_col} AS "
                    f"{self._ctx.COLUMN_PREFIX}{source_alias}_id"
                )

        # CTE columns
        field_lines.append("p.start_node")
        field_lines.append("p.end_node")
        field_lines.append("p.depth")

        # Path and path_edges
        if recursive_op.path_variable:
            if recursive_op.collect_nodes:
                path_alias = (
                    f"{self._ctx.COLUMN_PREFIX}"
                    f"{recursive_op.path_variable}_id"
                )
                field_lines.append(f"p.path AS {path_alias}")
            if recursive_op.collect_edges:
                edges_alias = (
                    f"{self._ctx.COLUMN_PREFIX}"
                    f"{recursive_op.path_variable}_edges"
                )
                field_lines.append(
                    f"p.path_edges AS {edges_alias}"
                )
        else:
            if recursive_op.collect_nodes:
                field_lines.append("p.path")
            if recursive_op.collect_edges:
                field_lines.append("p.path_edges")

        if recursive_op.relationship_variable:
            edges_alias = (
                f"{self._ctx.COLUMN_PREFIX}"
                f"{recursive_op.relationship_variable}_edges"
            )
            field_lines.append(
                f"p.path_edges AS {edges_alias}"
            )

        for i, field in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field}")

        # ---- FROM / JOIN ----
        lines.append(f"{indent}FROM {cte_name} p")

        if sink_needed:
            lines.append(
                f"{indent}JOIN "
                f"{target_table.full_table_name} sink"
            )
            lines.append(
                f"{indent}  ON sink.{target_id_col} = p.end_node"
            )

        if source_needed:
            lines.append(
                f"{indent}JOIN "
                f"{source_table.full_table_name} source"
            )
            lines.append(
                f"{indent}  ON source.{source_id_col} "
                f"= p.start_node"
            )

        # ---- WHERE ----
        where_parts = [f"p.depth >= {min_depth}"]
        if recursive_op.max_hops is not None:
            where_parts.append(f"p.depth <= {recursive_op.max_hops}")
        if recursive_op.is_circular:
            where_parts.append("p.start_node = p.end_node")

        if sink_needed and target_node.table_descriptor.filter:
            where_parts.append(
                f"sink.{target_node.table_descriptor.filter}"
            )
        if (
            source_needed
            and source_node
            and source_node.table_descriptor.filter
        ):
            where_parts.append(
                f"source.{source_node.table_descriptor.filter}"
            )

        if sink_needed and enriched_rec.sink_filter_as_sink:
            sink_filter_sql = (
                self._expr.render_edge_filter_expression(
                    enriched_rec.sink_filter_as_sink
                )
            )
            where_parts.append(sink_filter_sql)

        lines.append(
            f"{indent}WHERE {' AND '.join(where_parts)}"
        )

        return "\n".join(lines)

    def _render_boundary_join(
        self,
        join_op: JoinOperator,
        boundary_op: AggregationBoundaryOperator,
        right_op: LogicalOperator,
        depth: int,
    ) -> str:
        """Render a JOIN between aggregation boundary CTE and subsequent MATCH.

        This method generates a query that joins the aggregated CTE result with
        the new MATCH pattern using the projected entity IDs.

        Example: For a query like:
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            WITH c, COUNT(p) AS population
            MATCH (c)<-[:LIVES_IN]-(other:Person)
            RETURN ...

        Generates:
            SELECT
               _left.c,
               _left.population,
               _right._gsql2rsql_c_id,
               _right._gsql2rsql_other_id,
               ...
            FROM (
               SELECT `c`, `population` FROM agg_boundary_1
            ) AS _left
            INNER JOIN (
               ... right side subquery ...
            ) AS _right ON
               _left.c = _right._gsql2rsql_c_id
        """
        indent = self._ctx.indent(depth)
        lines: list[str] = []

        # Use globally unique aliases to avoid collisions with Databricks optimizer
        left_var, right_var = self._ctx.next_join_alias_pair()

        lines.append(f"{indent}SELECT")

        # Collect output fields
        output_fields: list[str] = []

        # Add fields from the boundary (CTE)
        for alias, _ in boundary_op.all_projections:
            output_fields.append(f"{left_var}.`{alias}` AS `{alias}`")

        # Add fields from the right side (new MATCH) - apply column pruning
        right_columns = self._collect_all_column_names(right_op.output_schema)

        # Determine which columns from the right side are actually needed
        # 1. Columns required downstream (in _required_columns)
        # 2. Join key columns (entity ID columns needed for the join condition)
        right_join_keys: set[str] = set()
        for pair in join_op.join_pairs:
            if pair.pair_type == JoinKeyPairType.NODE_ID and pair.node_alias:
                # Get the ID column from the right side's schema
                node_id_col = self._ctx.get_entity_id_column_from_schema(
                    right_op.output_schema, pair.node_alias
                )
                if node_id_col:
                    right_join_keys.add(node_id_col)

        for col in right_columns:
            # Apply column pruning: only include columns that are required or are join keys
            if (
                not self._ctx.enable_column_pruning
                or not self._ctx.required_columns
                or col in self._ctx.required_columns
                or col in right_join_keys
            ):
                output_fields.append(f"{right_var}.{col} AS {col}")

        for i, field in enumerate(output_fields):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field}")

        # FROM boundary CTE reference
        lines.append(f"{indent}FROM (")
        lines.append(self._render_aggregation_boundary_reference(boundary_op, depth + 1))
        lines.append(f"{indent}) AS {left_var}")

        # JOIN with right side
        join_keyword = (
            "INNER JOIN" if join_op.join_type == JoinType.INNER else "LEFT JOIN"
        )
        lines.append(f"{indent}{join_keyword} (")
        lines.append(self._render_operator(right_op, depth + 1))
        lines.append(f"{indent}) AS {right_var} ON")

        # Render join conditions
        # The boundary projects entity variables (e.g., 'c') and we need to join
        # them with the corresponding entity ID from the right side (e.g., '_gsql2rsql_c_id')
        conditions: list[str] = []

        for pair in join_op.join_pairs:
            if pair.pair_type == JoinKeyPairType.NODE_ID:
                node_alias = pair.node_alias
                # The boundary projects the entity variable directly (e.g., 'c')
                # The right side has the entity ID column (e.g., '_gsql2rsql_c_id')
                if node_alias in boundary_op.projected_variables:
                    # Find the ID column name from the right side's schema
                    node_id_col = self._ctx.get_entity_id_column_from_schema(
                        right_op.output_schema, node_alias
                    )
                    if node_id_col:
                        conditions.append(
                            f"{left_var}.`{node_alias}` = {right_var}.{node_id_col}"
                        )

        if conditions:
            for i, cond in enumerate(conditions):
                prefix = "  " if i == 0 else "  AND "
                lines.append(f"{indent}{prefix}{cond}")
        else:
            lines.append(f"{indent}  TRUE")

        return "\n".join(lines)

    def render_join(self, op: JoinOperator, depth: int) -> str:
        """Render a join operator."""
        lines: list[str] = []
        indent = self._ctx.indent(depth)

        left_op = op.in_operator_left
        right_op = op.in_operator_right

        if not left_op or not right_op:
            return ""

        # Use globally unique aliases to avoid collisions with Databricks optimizer
        left_var, right_var = self._ctx.next_join_alias_pair()

        # Check if left side is RecursiveTraversalOperator
        is_recursive_join = isinstance(left_op, RecursiveTraversalOperator)

        if is_recursive_join:
            # Special handling for recursive CTE joins
            assert isinstance(left_op, RecursiveTraversalOperator)
            assert isinstance(right_op, DataSourceOperator)
            return self._render_recursive_join(op, left_op, right_op, depth)

        # Check if left side is AggregationBoundaryOperator
        is_boundary_join = isinstance(left_op, AggregationBoundaryOperator)

        if is_boundary_join:
            # Special handling for aggregation boundary joins
            assert isinstance(left_op, AggregationBoundaryOperator)
            return self._render_boundary_join(op, left_op, right_op, depth)

        lines.append(f"{indent}SELECT")

        # Determine output fields from both sides
        output_fields = self._get_join_output_fields(
            op, left_op, right_op, left_var, right_var
        )
        for i, field_line in enumerate(output_fields):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

        # Check if this join needs undirected optimization
        needs_undirected_opt = self._should_use_undirected_union_optimization(op)

        # FROM left subquery
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(left_op, depth + 1))
        lines.append(f"{indent}) AS {left_var}")

        # JOIN type and right subquery
        if op.join_type == JoinType.CROSS:
            lines.append(f"{indent}CROSS JOIN (")
            if needs_undirected_opt and isinstance(right_op, DataSourceOperator):
                lines.append(
                    self._render_undirected_edge_union(right_op, op, depth + 1)
                )
            else:
                lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var}")
        else:
            join_keyword = (
                "INNER JOIN" if op.join_type == JoinType.INNER else "LEFT JOIN"
            )
            lines.append(f"{indent}{join_keyword} (")
            if needs_undirected_opt and isinstance(right_op, DataSourceOperator):
                lines.append(
                    self._render_undirected_edge_union(right_op, op, depth + 1)
                )
            else:
                lines.append(self._render_operator(right_op, depth + 1))
            lines.append(f"{indent}) AS {right_var} ON")

            # Render join conditions
            conditions = self._render_join_conditions(
                op, left_op, right_op, left_var, right_var
            )
            if conditions:
                for i, cond in enumerate(conditions):
                    prefix = "  " if i == 0 else "  AND "
                    lines.append(f"{indent}{prefix}{cond}")
            else:
                lines.append(f"{indent}  TRUE")

        return "\n".join(lines)

    def _should_use_undirected_union_optimization(
        self, op: JoinOperator
    ) -> bool:
        """
        Determine if a join should use UNION ALL optimization for undirected
        relationships.

        The UNION ALL optimization replaces inefficient OR conditions in JOINs
        with bidirectional edge expansion. This enables hash/merge joins instead
        of nested loops, improving performance from O(n²) to O(n).

        Example transformation:
            Before (slow): JOIN ON (p.id = k.source_id OR p.id = k.sink_id)
            After (fast):  JOIN (SELECT ... UNION ALL SELECT ...) ON p.id = k.node_id

        Args:
            op: The join operator to check for optimization eligibility.

        Returns:
            True if both conditions are met:
            1. Feature flag is enabled (undirected_strategy == 'union_edges')
            2. Join has EITHER-type join pairs (indicates undirected relationship)

        See Also:
            - _render_undirected_edge_union(): Generates the UNION ALL subquery
            - docs/development/UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md
        """
        # Check if any join pair is undirected AND uses UNION strategy
        # The use_union_for_undirected field is set by the planner based on the
        # edge access strategy, moving this semantic decision out of the renderer.
        return any(
            pair.pair_type in (
                JoinKeyPairType.EITHER,
                JoinKeyPairType.EITHER_AS_SOURCE,
                JoinKeyPairType.EITHER_AS_SINK,
            )
            and pair.use_union_for_undirected
            for pair in op.join_pairs
        )

    def _render_undirected_edge_union(
        self, edge_op: DataSourceOperator, join_op: JoinOperator, depth: int
    ) -> str:
        """
        Render an edge table with UNION ALL to expand undirected relationships.

        This method implements the "UNION ALL of edges" optimization strategy
        (Option A) for undirected relationships. Instead of using an OR condition
        in the JOIN clause (which prevents index usage and forces O(n²) nested
        loops), we expand edges bidirectionally before joining.

        Performance Impact:
            - Enables hash/merge join strategies (O(n) vs O(n²))
            - Allows index usage on join columns
            - Query planner can optimize join order
            - Filter pushdown works correctly
            - Trade-off: Reads edge table twice, but much faster overall

        Example Output:
            For Cypher: MATCH (p:Person)-[:KNOWS]-(f:Person)

            Generates:
              SELECT source_id AS _gsql2rsql_k_source_id,
                     sink_id AS _gsql2rsql_k_sink_id,
                     since AS _gsql2rsql_k_since
              FROM graph.Knows
              UNION ALL
              SELECT sink_id AS _gsql2rsql_k_source_id,
                     source_id AS _gsql2rsql_k_sink_id,
                     since AS _gsql2rsql_k_since
              FROM graph.Knows

            This allows simple equality joins:
              JOIN (...) ON p.id = k.source_id

            Instead of inefficient OR joins:
              JOIN (...) ON (p.id = k.source_id OR p.id = k.sink_id)

        Args:
            edge_op: The DataSourceOperator for the edge/relationship table.
            join_op: The JoinOperator containing join pair information.
            depth: Current indentation depth for SQL formatting.

        Returns:
            SQL string with UNION ALL subquery expanding edges bidirectionally.
            Falls back to standard rendering if edge is not a valid relationship.

        Note:
            This method is only called when _should_use_undirected_union_optimization()
            returns True. For directed relationships or when the feature flag is
            disabled, standard rendering is used.

        See Also:
            - _should_use_undirected_union_optimization(): Detection logic
            - _render_join_conditions(): Uses simple equality for optimized joins
            - docs/development/UNDIRECTED_OPTIMIZATION_IMPLEMENTATION.md
        """
        indent = self._ctx.indent(depth)
        lines: list[str] = []

        # Get edge schema - first entity field describes the edge
        if not edge_op.output_schema:
            return self._render_operator(edge_op, depth)

        entity_field = edge_op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return self._render_operator(edge_op, depth)

        # Read SQL table descriptor from enriched data (pre-resolved by SQLEnrichmentPass)
        enriched_ds = (
            self._ctx.enriched.data_sources.get(edge_op.operator_debug_id)
            if self._ctx.enriched
            else None
        )
        if not enriched_ds:
            return self._render_operator(edge_op, depth)

        table_name = enriched_ds.table_descriptor.full_table_name
        alias = entity_field.field_alias

        # Get source/sink columns
        source_join = entity_field.rel_source_join_field
        sink_join = entity_field.rel_sink_join_field

        if not source_join or not sink_join:
            # Not a relationship or missing join fields
            return self._render_operator(edge_op, depth)

        source_col = source_join.field_alias
        sink_col = sink_join.field_alias

        # Collect edge properties from encapsulated fields
        # Skip the join key fields themselves to avoid duplication
        skip_fields = {source_col, sink_col}
        property_fields = [
            f for f in entity_field.encapsulated_fields
            if f.field_alias not in skip_fields
        ]

        # Build WHERE clause from type filter (pre-resolved by enrichment)
        where_clause = ""
        if enriched_ds.type_filter_clause:
            where_clause = f"\n{indent}WHERE {enriched_ds.type_filter_clause}"

        # ========== FORWARD DIRECTION: source -> sink ==========
        # For edge (Alice)-[:KNOWS]->(Bob), this branch represents:
        #   Alice as source, Bob as sink (original direction)
        lines.append(f"{indent}SELECT")
        lines.append(
            f"{indent}   {source_col} AS "
            f"{self._ctx.get_field_name(alias, source_col)}"
        )
        lines.append(
            f"{indent}  ,{sink_col} AS "
            f"{self._ctx.get_field_name(alias, sink_col)}"
        )
        # Include all edge properties (e.g., "since", "weight")
        for prop_field in property_fields:
            prop_col = prop_field.field_alias
            lines.append(
                f"{indent}  ,{prop_col} AS "
                f"{self._ctx.get_field_name(alias, prop_col)}"
            )
        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_name}{where_clause}")

        # ========== UNION ALL: Combine both directions ==========
        lines.append(f"{indent}UNION ALL")

        # ========== REVERSE DIRECTION: sink -> source (SWAPPED) ==========
        # For edge (Alice)-[:KNOWS]->(Bob), this branch represents:
        #   Bob as source, Alice as sink (reversed for undirected semantics)
        # NOTE: Column names stay the same (source_col, sink_col) but values swap
        lines.append(f"{indent}SELECT")
        lines.append(
            f"{indent}   {sink_col} AS "
            f"{self._ctx.get_field_name(alias, source_col)}"  # Sink value → source alias
        )
        lines.append(
            f"{indent}  ,{source_col} AS "
            f"{self._ctx.get_field_name(alias, sink_col)}"  # Source value → sink alias
        )
        # Edge properties remain the same (not directional)
        for prop_field in property_fields:
            prop_col = prop_field.field_alias
            lines.append(
                f"{indent}  ,{prop_col} AS "
                f"{self._ctx.get_field_name(alias, prop_col)}"
            )
        # Reverse branch: exclude self-loops (src = dst) to avoid duplication
        reverse_where_parts: list[str] = []
        if enriched_ds.type_filter_clause:
            reverse_where_parts.append(enriched_ds.type_filter_clause)
        reverse_where_parts.append(f"{source_col} != {sink_col}")
        reverse_where = (
            f"\n{indent}WHERE {' AND '.join(reverse_where_parts)}"
        )
        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_name}{reverse_where}")

        return "\n".join(lines)

    def _get_join_output_fields(
        self,
        op: JoinOperator,
        left_op: LogicalOperator,
        right_op: LogicalOperator,
        left_var: str,
        right_var: str,
    ) -> list[str]:
        """Get output field expressions for a join."""
        fields: list[str] = []
        # Track already-projected aliases to avoid duplicates
        projected_aliases: set[str] = set()

        # Collect field aliases from both sides (top-level entity aliases)
        left_aliases = {f.field_alias for f in left_op.output_schema}
        right_aliases = {f.field_alias for f in right_op.output_schema}

        # Also collect all column names that are actually available on each side
        # This includes encapsulated field names like _gsql2rsql_entity_property
        left_columns = self._collect_all_column_names(left_op.output_schema)
        right_columns = self._collect_all_column_names(right_op.output_schema)

        for field in op.output_schema:
            is_from_left = field.field_alias in left_aliases
            is_from_right = field.field_alias in right_aliases

            if isinstance(field, EntityField):
                # Entity field - output join keys
                if field.entity_type == EntityType.NODE:
                    if field.node_join_field:
                        key_name = self._ctx.resolve_field_key(field.node_join_field, field.field_alias)
                        # Skip if already projected
                        if key_name in projected_aliases:
                            pass  # fall through to encapsulated fields
                        # Column pruning: skip join key if not required
                        # (e.g., source node JOIN was eliminated)
                        elif (
                            self._ctx.enable_column_pruning
                            and self._ctx.required_columns
                            and key_name not in self._ctx.required_columns
                        ):
                            pass  # skip this join key
                        elif key_name not in projected_aliases:
                            # Determine which side of join has this column
                            # Priority: Check actual column presence first (left_columns/right_columns)
                            # then fall back to entity alias membership (defensive)
                            if key_name in left_columns:
                                actual_var = left_var
                            elif key_name in right_columns:
                                actual_var = right_var
                            else:
                                # Fallback: Use entity alias to infer side
                                # (with defensive validation)
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{key_name} AS {key_name}")
                            projected_aliases.add(key_name)
                else:
                    if field.rel_source_join_field:
                        src_key = self._ctx.resolve_field_key(field.rel_source_join_field, field.field_alias)
                        # Skip if already projected
                        if src_key not in projected_aliases:
                            # Determine which side of join has this column
                            if src_key in left_columns:
                                actual_var = left_var
                            elif src_key in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{src_key} AS {src_key}")
                            projected_aliases.add(src_key)
                    if field.rel_sink_join_field:
                        sink_key = self._ctx.resolve_field_key(field.rel_sink_join_field, field.field_alias)
                        # Skip if already projected
                        if sink_key not in projected_aliases:
                            # Determine which side of join has this column
                            if sink_key in left_columns:
                                actual_var = left_var
                            elif sink_key in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{sink_key} AS {sink_key}")
                            projected_aliases.add(sink_key)

                # Output all encapsulated fields (properties)
                # Skip join key fields to avoid duplicates
                skip_fields = set()
                if field.node_join_field:
                    skip_fields.add(field.node_join_field.field_alias)
                if field.rel_source_join_field:
                    skip_fields.add(field.rel_source_join_field.field_alias)
                if field.rel_sink_join_field:
                    skip_fields.add(field.rel_sink_join_field.field_alias)

                for encap_field in field.encapsulated_fields:
                    if encap_field.field_alias not in skip_fields:
                        field_alias = self._ctx.resolve_field_key(encap_field, field.field_alias)
                        # Skip if already projected
                        if field_alias in projected_aliases:
                            continue
                        # Column pruning: only include if required or pruning disabled
                        if (
                            not self._ctx.enable_column_pruning
                            or not self._ctx.required_columns
                            or field_alias in self._ctx.required_columns
                        ):
                            # Determine which side of join has this column
                            if field_alias in left_columns:
                                actual_var = left_var
                            elif field_alias in right_columns:
                                actual_var = right_var
                            else:
                                actual_var = self._determine_column_side(
                                    field.field_alias,
                                    is_from_left,
                                    is_from_right,
                                    left_var,
                                    right_var,
                                )
                            fields.append(f"{actual_var}.{field_alias} AS {field_alias}")
                            projected_aliases.add(field_alias)

            elif isinstance(field, ValueField):
                # Use pre-rendered field name if available (VLP relationship variables)
                # For VLP relationship variables (e.g., 'e' in [e*1..3]), the field_name
                # is set to _gsql2rsql_{var}_edges, which is the actual SQL column name
                if field.field_name and field.field_name.startswith(self._ctx.COLUMN_PREFIX):
                    sql_name = field.field_name
                else:
                    sql_name = field.field_alias

                # Skip if already projected
                if sql_name in projected_aliases:
                    continue
                # Column pruning for value fields
                # Check both _required_columns (for property refs like `p.name`)
                # and _required_value_fields (for bare variable refs like `shared_cards`)
                if (
                    not self._ctx.enable_column_pruning
                    or not self._ctx.required_columns
                    or field.field_alias in self._ctx.required_columns
                    or field.field_alias in self._ctx.required_value_fields
                    or sql_name in self._ctx.required_columns
                ):
                    # Determine which side of join has this column
                    if sql_name in left_columns:
                        actual_var = left_var
                    elif sql_name in right_columns:
                        actual_var = right_var
                    else:
                        actual_var = self._determine_column_side(
                            field.field_alias,
                            is_from_left,
                            is_from_right,
                            left_var,
                            right_var,
                        )
                    fields.append(f"{actual_var}.{sql_name} AS {sql_name}")
                    projected_aliases.add(sql_name)

        # IMPORTANT: Also propagate required columns from the left side that weren't
        # already projected. This handles cases where an entity (like 'c') is in the
        # left side of the join but not in op.output_schema, yet its properties
        # (like _gsql2rsql_c_id, _gsql2rsql_c_name) are needed downstream.
        #
        # This is especially important for joins after recursive traversals, where
        # the source node columns (e.g., _gsql2rsql_c_id from source.id) are rendered but
        # not tracked in the logical plan's output schema.
        if self._ctx.enable_column_pruning and self._ctx.required_columns:
            # Collect entity aliases that we know are available on the left side
            # This includes entities from output_schema and entities from recursive
            # traversal sources (which aren't in output_schema but are rendered)
            left_entity_aliases: set[str] = set()
            for field in left_op.output_schema:
                left_entity_aliases.add(field.field_alias)

            # Also check if left_op contains a recursive traversal - if so, its
            # source alias should be considered available
            if isinstance(left_op, RecursiveTraversalOperator):
                if left_op.source_alias:
                    left_entity_aliases.add(left_op.source_alias)
            elif op.recursive_source_alias:
                left_entity_aliases.add(op.recursive_source_alias)

            for required_col in self._ctx.required_columns:
                if required_col in projected_aliases:
                    continue  # Already projected

                # Check if this column belongs to a known left-side entity
                # Column pattern: _gsql2rsql_{entity}_{property}
                col_entity = None
                if required_col.startswith(self._ctx.COLUMN_PREFIX):
                    parts = required_col[len(self._ctx.COLUMN_PREFIX):].split("_", 1)
                    if len(parts) >= 1:
                        col_entity = parts[0]

                if required_col in left_columns:
                    fields.append(f"{left_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)
                elif required_col in right_columns:
                    fields.append(f"{right_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)
                elif col_entity and col_entity in left_entity_aliases:
                    # Column belongs to an entity we know is on the left side
                    # (including recursive traversal source nodes)
                    fields.append(f"{left_var}.{required_col} AS {required_col}")
                    projected_aliases.add(required_col)

        return fields

    def _collect_all_column_names(self, schema: Schema) -> set[str]:
        """Collect all column names from a schema, including encapsulated fields."""
        columns: set[str] = set()
        for field in schema:
            if isinstance(field, EntityField):
                # Add the node/relationship join key columns
                if field.node_join_field:
                    columns.add(self._ctx.resolve_field_key(
                        field.node_join_field, field.field_alias,
                    ))
                if field.rel_source_join_field:
                    columns.add(self._ctx.resolve_field_key(
                        field.rel_source_join_field, field.field_alias,
                    ))
                if field.rel_sink_join_field:
                    columns.add(self._ctx.resolve_field_key(
                        field.rel_sink_join_field, field.field_alias,
                    ))
                # Add all encapsulated fields
                for encap_field in field.encapsulated_fields:
                    columns.add(self._ctx.resolve_field_key(
                        encap_field, field.field_alias,
                    ))
            elif isinstance(field, ValueField):
                # Use pre-rendered field name if available (VLP relationship variables)
                if field.field_name and field.field_name.startswith(self._ctx.COLUMN_PREFIX):
                    columns.add(field.field_name)
                else:
                    columns.add(field.field_alias)
        return columns

    def _determine_column_side(
        self,
        field_alias: str,
        is_from_left: bool,
        is_from_right: bool,
        left_var: str,
        right_var: str,
    ) -> str:
        """
        Determine which side of a join a field belongs to (defensive programming).

        This method implements defensive validation to catch bugs in the planner
        or resolver that might create orphaned fields or ambiguous references.

        Args:
            field_alias: The field alias to locate (e.g., "p", "k", "f")
            is_from_left: Whether field is in left output schema
            is_from_right: Whether field is in right output schema
            left_var: SQL variable name for left side (e.g., "_left")
            right_var: SQL variable name for right side (e.g., "_right")

        Returns:
            The SQL variable name to use (_left or _right)

        Raises:
            RuntimeError: If field is not found in either side (orphaned field)

        Examples:
            # Normal case: field only in left
            >>> _determine_column_side("p", True, False, "_left", "_right")
            "_left"

            # Normal case: field only in right
            >>> _determine_column_side("f", False, True, "_left", "_right")
            "_right"

            # Ambiguous case: field in both (prioritizes left, could log warning)
            >>> _determine_column_side("shared", True, True, "_left", "_right")
            "_left"

            # Bug case: field not in either (throws error)
            >>> _determine_column_side("orphan", False, False, "_left", "_right")
            RuntimeError: Field 'orphan' not found in left or right join schema
        """
        # Case 1: Field exists in both sides (ambiguous - rare but possible)
        # This can happen if there's a naming collision after join operations.
        # Prioritize left side for consistency with original logic.
        if is_from_left and is_from_right:
            # NOTE: In the future, we could log a warning here if needed
            # for debugging ambiguous cases, but for now we silently prefer left.
            return left_var

        # Case 2: Field exists only in left side (normal case)
        elif is_from_left:
            return left_var

        # Case 3: Field exists only in right side (normal case)
        elif is_from_right:
            return right_var

        # Case 4: Field does NOT exist in either side (BUG in planner/resolver!)
        # This indicates a serious bug where the planner created a field reference
        # that wasn't properly included in the join output schemas.
        # Fail-fast with a clear error message rather than generating invalid SQL.
        else:
            raise RuntimeError(
                f"Field '{field_alias}' not found in left or right join output schemas. "
                f"This indicates a bug in the query planner or resolver. "
                f"The field should have been included in one of the join operator's "
                f"output schemas during query planning."
            )

    def _render_join_conditions(
        self,
        op: JoinOperator,
        left_op: LogicalOperator,
        right_op: LogicalOperator,
        left_var: str,
        right_var: str,
    ) -> list[str]:
        """Render join conditions."""
        conditions: list[str] = []

        left_aliases = {f.field_alias for f in left_op.output_schema}

        for pair in op.join_pairs:
            # Find the node and relationship fields
            node_alias = pair.node_alias
            rel_alias = pair.relationship_or_node_alias

            # Determine which side each is on
            node_on_left = node_alias in left_aliases
            node_var = left_var if node_on_left else right_var
            rel_var = right_var if node_on_left else left_var

            # Get the node's join key
            node_field = next(
                (f for f in op.input_schema if f.field_alias == node_alias),
                None,
            )
            rel_field = next(
                (f for f in op.input_schema if f.field_alias == rel_alias),
                None,
            )

            if not node_field or not rel_field:
                continue

            if isinstance(node_field, EntityField) and isinstance(
                rel_field, EntityField
            ):
                # ========== FIX: Variable-length path field naming ==========
                # For normal entities (from DataSourceOperator), we construct the
                # field name from alias + field_alias (e.g., "peer" + "id" -> "_gsql2rsql_peer_id").
                #
                # However, for entities from variable-length paths (RecursiveTraversalOperator),
                # the fields are already projected with full SQL names in _render_recursive_join()
                # (e.g., "sink.id AS _gsql2rsql_peer_id"). In this case, the field_name
                # attribute is set to the complete SQL column name.
                #
                # Using field_name directly when it's already a full SQL name prevents
                # double-prefixing (e.g., "_gsql2rsql_peer_peer_id" ❌).
                if node_field.node_join_field:
                    node_key = self._ctx.resolve_field_key(
                        node_field.node_join_field, node_alias,
                    )
                else:
                    node_key = self._ctx.get_field_name(node_alias, "id")

                if pair.pair_type == JoinKeyPairType.SOURCE:
                    if rel_field.rel_source_join_field:
                        rel_key = self._ctx.resolve_field_key(
                            rel_field.rel_source_join_field, rel_alias,
                        )
                    else:
                        rel_key = self._ctx.get_field_name(rel_alias, "source_id")
                elif pair.pair_type == JoinKeyPairType.SINK:
                    if rel_field.rel_sink_join_field:
                        rel_key = self._ctx.resolve_field_key(
                            rel_field.rel_sink_join_field, rel_alias,
                        )
                    else:
                        rel_key = self._ctx.get_field_name(rel_alias, "sink_id")
                elif pair.pair_type == JoinKeyPairType.NODE_ID:
                    if rel_field.node_join_field:
                        rel_key = self._ctx.resolve_field_key(
                            rel_field.node_join_field, rel_alias,
                        )
                    else:
                        rel_key = self._ctx.get_field_name(rel_alias, "id")
                elif pair.pair_type in (JoinKeyPairType.EITHER_AS_SOURCE, JoinKeyPairType.EITHER_AS_SINK):
                    # Undirected relationship with explicit source/sink position
                    # Get both keys for potential OR fallback
                    source_key = None
                    sink_key = None

                    if rel_field.rel_source_join_field:
                        source_key = self._ctx.resolve_field_key(
                            rel_field.rel_source_join_field, rel_alias,
                        )
                    if rel_field.rel_sink_join_field:
                        sink_key = self._ctx.resolve_field_key(
                            rel_field.rel_sink_join_field, rel_alias,
                        )

                    if pair.use_union_for_undirected:
                        # OPTIMIZED: UNION ALL expansion - use appropriate key
                        # Decision made by planner based on edge access strategy
                        if pair.pair_type == JoinKeyPairType.EITHER_AS_SOURCE:
                            rel_key = source_key or self._ctx.get_field_name(rel_alias, "source_id")
                        else:  # EITHER_AS_SINK
                            rel_key = sink_key or self._ctx.get_field_name(rel_alias, "sink_id")
                    else:
                        # LEGACY: OR condition for compatibility
                        if source_key and sink_key:
                            conditions.append(
                                f"({node_var}.{node_key} = {rel_var}.{source_key} "
                                f"OR {node_var}.{node_key} = {rel_var}.{sink_key})"
                            )
                            continue
                        else:
                            # Fallback to available key
                            rel_key = source_key or sink_key or self._ctx.get_field_name(rel_alias, "id")
                else:
                    # EITHER/BOTH - legacy undirected handling (for VLP and backwards compatibility)
                    # For Cypher: (a)-[:REL]-(b) matches both (a)-[:REL]->(b) and (a)<-[:REL]-(b)
                    source_key = None
                    sink_key = None

                    if rel_field.rel_source_join_field:
                        source_key = self._ctx.resolve_field_key(rel_field.rel_source_join_field, rel_alias)
                    if rel_field.rel_sink_join_field:
                        sink_key = self._ctx.resolve_field_key(rel_field.rel_sink_join_field, rel_alias)

                    if source_key and sink_key:
                        # ========== STRATEGY SELECTION: OR vs UNION ALL ==========
                        # Decision now comes from planner via use_union_for_undirected
                        if pair.use_union_for_undirected:
                            # **OPTIMIZED (UNION ALL of edges - default)**
                            # Edges are already expanded bidirectionally via UNION ALL
                            # in _render_undirected_edge_union(), so we can use simple
                            # equality join here. This enables hash/merge joins instead
                            # of nested loops.
                            #
                            # Example:
                            #   JOIN (... UNION ALL ...) k ON p.id = k.source_id
                            #
                            # Performance: O(n) with hash join
                            conditions.append(
                                f"{node_var}.{node_key} = {rel_var}.{source_key}"
                            )
                        else:
                            # **LEGACY (OR condition - disabled by default)**
                            # Use OR to match both directions in a single join.
                            # This prevents index usage and forces nested loop joins.
                            #
                            # Example:
                            #   JOIN k ON (p.id = k.source_id OR p.id = k.sink_id)
                            #
                            # Performance: O(n²) with nested loop
                            # NOTE: Only use for small datasets or debugging
                            conditions.append(
                                f"({node_var}.{node_key} = {rel_var}.{source_key} "
                                f"OR {node_var}.{node_key} = {rel_var}.{sink_key})"
                            )
                        continue
                    elif source_key:
                        rel_key = source_key
                    elif sink_key:
                        rel_key = sink_key
                    else:
                        continue

                conditions.append(f"{node_var}.{node_key} = {rel_var}.{rel_key}")

        return conditions

