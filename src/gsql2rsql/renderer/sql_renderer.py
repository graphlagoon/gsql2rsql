"""SQL Renderer - Converts logical plan to Databricks SQL with WITH RECURSIVE support."""

from __future__ import annotations

from typing import Any

from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
)
from gsql2rsql.common.logging import ILoggable
from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionProperty,
)
from gsql2rsql.parser.operators import (
    Function,
)
from gsql2rsql.renderer.dialect import (
    OPERATOR_PATTERNS,
)
from gsql2rsql.planner.column_ref import ResolvedColumnRef, ResolvedProjection
from gsql2rsql.planner.column_resolver import ResolutionResult
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    DataSourceOperator,
    JoinKeyPairType,
    JoinOperator,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
    SetOperationType,
    SetOperator,
    UnwindOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType, Schema
from gsql2rsql.renderer.expression_renderer import ExpressionRenderer
from gsql2rsql.renderer.join_renderer import JoinRenderer
from gsql2rsql.renderer.procedural_bfs_renderer import ProceduralBFSRenderer
from gsql2rsql.renderer.recursive_cte_renderer import RecursiveCTERenderer
from gsql2rsql.renderer.render_context import RenderContext
from gsql2rsql.renderer.schema_provider import ISQLDBSchemaProvider
from gsql2rsql.renderer.sql_enrichment import EnrichedPlanData, SQLEnrichmentPass


class SQLRenderer:
    """
    Renders a logical plan to Databricks SQL.

    This class converts the relational algebra operators in the logical plan
    into equivalent Databricks SQL query statements, with support for
    WITH RECURSIVE CTEs for variable-length path traversals.
    """

    # Prefix for generated column names to avoid collisions with user identifiers.
    # Uses _gsql2rsql_ to match the naming convention from column_ref.py.
    COLUMN_PREFIX = "_gsql2rsql_"

    def __init__(
        self,
        db_schema_provider: ISQLDBSchemaProvider,
        logger: ILoggable | None = None,
        *,
        enable_column_pruning: bool = True,
        config: dict[str, Any] | None = None,
        vlp_rendering_mode: str = "cte",
        materialization_strategy: str = "temp_tables",
    ) -> None:
        """
        Initialize the SQL renderer.

        Args:
            db_schema_provider: The SQL database schema provider.
            logger: Optional logger for debugging.
            enable_column_pruning: Enable column pruning optimization.
            config: Optional configuration dictionary.
                Supported keys:
                - 'undirected_strategy': Strategy for undirected relationships.
                  Values: 'union_edges' (default) or 'or_join'
        """
        self._graph_def = db_schema_provider
        self._logger = logger
        self._cte_counter = 0
        # Global counter for unique JOIN aliases to prevent Databricks optimizer issues.
        # Using a global counter (not depth-based) ensures uniqueness across multiple
        # MATCH patterns in the same query.
        self._join_alias_counter = 0
        # Column pruning: set of required column aliases (e.g., "_gsql2rsql_p_name")
        self._required_columns: set[str] = set()
        # Bare variable references (e.g., "shared_cards") - used for ValueFields
        self._required_value_fields: set[str] = set()
        # Enable column pruning by default
        self._enable_column_pruning = enable_column_pruning
        # Resolution result from ColumnResolver (Phase 1 of renderer refactoring).
        # When available, the renderer uses resolved column references instead of
        # inferring them from schema lookups. This makes the renderer "stupid and safe".
        # Set during render_plan() if the plan has been resolved.
        self._resolution_result: ResolutionResult | None = None

        # Configuration
        self._config = config or {}
        self._vlp_rendering_mode = vlp_rendering_mode
        self._materialization_strategy = materialization_strategy

    @property
    def _db_schema(self) -> ISQLDBSchemaProvider:
        """Get the database schema provider (guaranteed non-None after __init__)."""
        assert self._graph_def is not None
        return self._graph_def

    @property
    def _resolved(self) -> ResolutionResult:
        """Get the resolution result (guaranteed non-None during rendering)."""
        assert self._resolution_result is not None
        return self._resolution_result

    def _next_join_alias_pair(self) -> tuple[str, str]:
        """Generate unique alias pair for JOIN subqueries.

        Returns a tuple (left_var, right_var) with globally unique names.
        This prevents alias collisions that can confuse query optimizers
        like Databricks when they flatten nested subqueries.

        Returns:
            Tuple of (left_alias, right_alias), e.g., ("_left_0", "_right_0")
        """
        alias_id = self._join_alias_counter
        self._join_alias_counter += 1
        return (f"_left_{alias_id}", f"_right_{alias_id}")

    def _create_context(
        self,
        enriched: EnrichedPlanData | None = None,
    ) -> RenderContext:
        """Create a RenderContext from the current renderer state.

        The context shares mutable references to required_columns and
        required_value_fields so sub-renderers can both read and write them.
        Counter state is copied; sub-renderers should use the context's
        counters and the orchestrator syncs back after each call if needed.
        """
        ctx = RenderContext(
            db_schema=self._db_schema,
            resolution_result=self._resolution_result,
            required_columns=self._required_columns,
            required_value_fields=self._required_value_fields,
            enable_column_pruning=self._enable_column_pruning,
            config=self._config,
            enriched=enriched,
            vlp_rendering_mode=self._vlp_rendering_mode,
            materialization_strategy=self._materialization_strategy,
        )
        ctx.cte_counter = self._cte_counter
        ctx.join_alias_counter = self._join_alias_counter
        return ctx

    def render_plan(self, plan: LogicalPlan) -> str:
        """
        Render a logical plan to Databricks SQL.

        The renderer is now "stupid and safe" - it requires pre-resolved plans
        and only performs mechanical SQL generation. All semantic decisions
        (column resolution, scope checking) are handled by ColumnResolver.

        Args:
            plan: The logical plan to render. MUST be resolved via plan.resolve()
                  before calling this method.

        Returns:
            The rendered Databricks SQL query string.

        Raises:
            ValueError: If the plan has not been resolved.

        Trade-offs:
            - Enforces separation of concerns (resolver = semantic, renderer = mechanical)
            - Prevents subtle bugs from schema-based guessing
            - Requires calling plan.resolve() before rendering
        """
        if not plan.terminal_operators:
            return ""

        # REQUIRE RESOLUTION: Renderer is now stupid and safe
        if not plan.is_resolved or plan.resolution_result is None:
            raise ValueError(
                "SQLRenderer requires a resolved plan. Call plan.resolve(original_query) "
                "before rendering. The renderer no longer performs column resolution - "
                "that is the job of ColumnResolver."
            )

        # Reset counters for fresh rendering
        self._cte_counter = 0
        self._join_alias_counter = 0

        # Store resolution result - this is now guaranteed to be non-None
        self._resolution_result = plan.resolution_result

        # SQL Enrichment: resolve SQL-specific metadata in a single pass
        # before any rendering begins.
        enrichment = SQLEnrichmentPass(self._db_schema)
        enriched = enrichment.enrich(plan)

        # Column pruning: collect required columns before rendering
        self._required_columns = set()
        self._required_value_fields = set()
        if self._enable_column_pruning:
            for terminal_op in plan.terminal_operators:
                self._collect_required_columns(terminal_op)

        # Create render context — shared state object for sub-renderers.
        # The context shares mutable references to required_columns/required_value_fields.
        self._ctx = self._create_context(enriched=enriched)
        self._expr = ExpressionRenderer(self._ctx)
        self._cte = RecursiveCTERenderer(
            self._ctx, self._expr, self._render_operator,
        )
        self._proc_bfs = ProceduralBFSRenderer(
            self._ctx, self._expr, self._render_operator,
        )
        self._join = JoinRenderer(
            self._ctx, self._expr,
            self._render_operator,
            self._cte.render_aggregation_boundary_reference,
        )

        # Fork: procedural BFS mode
        if self._vlp_rendering_mode == "procedural":
            has_recursive = any(
                isinstance(op, RecursiveTraversalOperator)
                for start_op in plan.starting_operators
                for op in self._walk_operators(start_op)
            )
            if has_recursive:
                return self._render_plan_procedural(plan)

        # Collect any operators that need CTEs
        # Use a shared visited set to avoid duplicates across starting operators
        ctes: list[str] = []
        visited: set[int] = set()
        has_recursive_cte = False
        for start_op in plan.starting_operators:
            has_recursive = self._collect_ctes(start_op, ctes, visited)
            has_recursive_cte = has_recursive_cte or has_recursive

        # Render from the terminal operator
        terminal_op = plan.terminal_operators[0]
        main_query = self._render_operator(terminal_op, depth=0)

        # Combine CTEs with main query
        if ctes:
            # Use WITH RECURSIVE only if there are recursive traversal CTEs
            cte_keyword = "WITH RECURSIVE" if has_recursive_cte else "WITH"
            cte_block = f"{cte_keyword}\n" + ",\n".join(ctes)
            return f"{cte_block}\n{main_query}"

        return main_query

    def _collect_ctes(
        self,
        op: LogicalOperator,
        ctes: list[str],
        visited: set[int],
    ) -> bool:
        """Collect CTE definitions from the plan.

        Args:
            op: The operator to start traversal from
            ctes: List to collect CTE definitions into
            visited: Set of already-visited operator IDs to avoid duplicates

        Returns:
            True if any RecursiveTraversalOperator CTEs were found
        """
        # Avoid visiting the same operator twice
        op_id = id(op)
        if op_id in visited:
            return False
        visited.add(op_id)

        has_recursive = False

        if isinstance(op, RecursiveTraversalOperator):
            cte = self._cte.render_recursive_cte(op)
            ctes.append(cte)
            has_recursive = True
        elif isinstance(op, AggregationBoundaryOperator):
            cte = self._cte.render_aggregation_boundary_cte(op)
            ctes.append(cte)

        for out_op in op.out_operators:
            child_has_recursive = self._collect_ctes(out_op, ctes, visited)
            has_recursive = has_recursive or child_has_recursive

        return has_recursive

    @staticmethod
    def _walk_operators(
        op: LogicalOperator,
    ) -> list[LogicalOperator]:
        """Yield all operators reachable from *op* (BFS)."""
        result: list[LogicalOperator] = []
        stack = [op]
        visited: set[int] = set()
        while stack:
            current = stack.pop()
            cid = id(current)
            if cid in visited:
                continue
            visited.add(cid)
            result.append(current)
            stack.extend(current.out_operators)
        return result

    def _render_plan_procedural(self, plan: LogicalPlan) -> str:
        """Render plan using procedural BFS instead of WITH RECURSIVE CTEs."""
        declarations: list[str] = []
        procedural_blocks: list[str] = []

        # Collect procedural blocks for each RecursiveTraversalOperator
        # and non-recursive CTEs (AggregationBoundary etc.)
        non_recursive_ctes: list[str] = []
        visited: set[int] = set()
        for start_op in plan.starting_operators:
            self._collect_procedural_blocks(
                start_op, declarations, procedural_blocks,
                non_recursive_ctes, visited,
            )

        # Render the main query (same as CTE path)
        terminal_op = plan.terminal_operators[0]
        main_query = self._render_operator(terminal_op, depth=0)

        # Handle non-recursive CTEs (AggregationBoundary etc.)
        if non_recursive_ctes:
            cte_block = "WITH\n" + ",\n".join(non_recursive_ctes)
            main_query = f"{cte_block}\n{main_query}"

        # Wrap in BEGIN...END
        parts: list[str] = ["BEGIN"]
        for d in declarations:
            for line in d.split("\n"):
                parts.append(f"  {line}")
        parts.append("")
        for block in procedural_blocks:
            for line in block.split("\n"):
                parts.append(f"  {line}")
            parts.append("")
        parts.append(f"  {main_query};")
        parts.append("END")
        return "\n".join(parts)

    def _collect_procedural_blocks(
        self,
        op: LogicalOperator,
        declarations: list[str],
        procedural_blocks: list[str],
        non_recursive_ctes: list[str],
        visited: set[int],
    ) -> None:
        """Walk plan tree, collecting procedural BFS blocks and non-recursive CTEs."""
        op_id = id(op)
        if op_id in visited:
            return
        visited.add(op_id)

        if isinstance(op, RecursiveTraversalOperator):
            decl, body = self._proc_bfs.render_procedural_block(op)
            declarations.append(decl)
            procedural_blocks.append(body)
        elif isinstance(op, AggregationBoundaryOperator):
            cte = self._cte.render_aggregation_boundary_cte(op)
            non_recursive_ctes.append(cte)

        for out_op in op.out_operators:
            self._collect_procedural_blocks(
                out_op, declarations, procedural_blocks,
                non_recursive_ctes, visited,
            )

    def _get_resolved_ref(
        self,
        variable: str,
        property_name: str | None,
        context_op: LogicalOperator,
    ) -> ResolvedColumnRef | None:
        """Look up a resolved column reference from the ResolutionResult.

        This is the primary interface for the renderer to access pre-resolved
        column information. When available, this avoids the need for schema
        lookups and guessing in _find_entity_field().

        Args:
            variable: The variable name (e.g., "p")
            property_name: The property name (e.g., "name") or None for bare refs
            context_op: The operator context for the lookup

        Returns:
            ResolvedColumnRef if the reference was resolved, None otherwise.
            Returns None if:
            - No resolution result is available (unresolved plan)
            - The operator has no resolved expressions
            - The specific reference was not found

        Trade-offs:
            - Prefers resolution lookup over schema search for correctness
            - Falls back to None (allowing legacy path) for backwards compat
        """
        if self._resolution_result is None:
            return None

        op_id = context_op.operator_debug_id
        resolved_exprs = self._resolution_result.resolved_expressions.get(op_id, [])

        # Search through all resolved expressions for this operator
        for resolved_expr in resolved_exprs:
            ref = resolved_expr.get_ref(variable, property_name)
            if ref is not None:
                return ref

        # Also check resolved projections if this is a ProjectionOperator
        if op_id in self._resolution_result.resolved_projections:
            for resolved_proj in self._resolution_result.resolved_projections[op_id]:
                ref = resolved_proj.expression.get_ref(variable, property_name)
                if ref is not None:
                    return ref

        return None

    def _collect_required_columns_from_resolution(
        self, op: LogicalOperator
    ) -> None:
        """Collect required columns using resolution result (Phase 4 optimization).

        Uses pre-resolved column references instead of walking AST.
        This is more accurate since resolution has already validated all references.

        Trade-offs:
            - Faster: O(n) direct lookup vs O(n) AST walk
            - More accurate: Uses validated references
            - Limitation: Still needs to handle join keys separately (from schema)
        """
        assert self._resolution_result is not None

        op_id = op.operator_debug_id

        # Collect from resolved expressions for this operator
        if op_id in self._resolution_result.resolved_expressions:
            for resolved_expr in self._resolution_result.resolved_expressions[op_id]:
                # Detect path variables consumed by relationships():
                # relationships(p) only needs _edges, not the path
                # nodes array (_id). Collect which bare path vars
                # are superseded by a relationships() ref.
                relationships_vars: set[str] = set()
                for key in resolved_expr.column_refs:
                    if key.startswith("relationships("):
                        # Extract var name from "relationships(p)"
                        var = key[len("relationships("):-1]
                        relationships_vars.add(var)

                for ref in resolved_expr.all_refs():
                    # Skip the bare path ref when relationships()
                    # supersedes it — only _edges is needed
                    if (
                        ref.original_property is None
                        and ref.original_variable in relationships_vars
                    ):
                        continue
                    self._required_columns.add(ref.sql_column_name)
                    # Track bare variable references
                    if ref.original_property is None:
                        self._required_value_fields.add(
                            ref.original_variable
                        )

        # Collect from resolved projections for ProjectionOperators
        if op_id in self._resolution_result.resolved_projections:
            for resolved_proj in self._resolution_result.resolved_projections[op_id]:
                for ref in resolved_proj.expression.all_refs():
                    self._required_columns.add(ref.sql_column_name)
                    if ref.original_property is None:
                        self._required_value_fields.add(ref.original_variable)

                # IMPORTANT: When returning an entity (RETURN a, RETURN r),
                # we need ALL its properties for NAMED_STRUCT, not just the ID.
                # Add all entity columns to _required_columns.
                if resolved_proj.is_entity_ref:
                    self._add_entity_columns_to_required(op, resolved_proj)

                # ALSO: Handle collect(entity) - need ALL entity properties for NAMED_STRUCT
                # inside COLLECT_LIST(NAMED_STRUCT(...))
                self._add_entity_columns_for_collect(op, resolved_proj)

        # Handle JoinOperator join keys (still need schema for join keys)
        if isinstance(op, JoinOperator):
            self._collect_join_key_columns(op)

        # Handle SetOperator (recurse into both sides)
        if isinstance(op, SetOperator):
            if op.in_operator_left:
                self._collect_required_columns_from_resolution(op.in_operator_left)
            if op.in_operator_right:
                self._collect_required_columns_from_resolution(op.in_operator_right)
            return  # Don't recurse further for set operators

        # Recurse into input operators using polymorphic method
        for in_op in op.in_operators:
            self._collect_required_columns_from_resolution(in_op)

    def _collect_join_key_columns(self, op: JoinOperator) -> None:
        """Collect join key columns from JoinOperator (used by both resolution and legacy paths).

        Join keys come from schema, not from expressions, so this is shared logic.

        For VLP recursive JoinOperators (identified by recursive_source_alias),
        node ID join keys are NOT added as required columns because:
        1. The CTE already provides start_node/end_node — the renderer uses those
           directly instead of fetching node_id from the nodes table.
        2. Adding them creates a circular dependency: the node JOIN is "needed"
           because its join key is required, but the key is only required because
           we assumed the JOIN exists.
        The renderer's _is_node_join_needed() separately decides whether the
        actual source/sink node JOINs are needed based on downstream column usage.
        """
        # A JoinOperator is "recursive-related" if it IS the recursive
        # join (recursive_source_alias set) OR if it's the parent join
        # that connects a root DataSource to a recursive sub-tree
        # (e.g., NODE_ID pair where one child is DataSource and the
        # other is a recursive join). We must NOT skip join keys when
        # joining two recursive sub-trees (double VLP pattern).
        is_recursive_join = op.recursive_source_alias is not None
        if not is_recursive_join:
            has_datasource_child = any(
                isinstance(child, DataSourceOperator)
                for child in op.in_operators
            )
            has_recursive_child = any(
                isinstance(child, JoinOperator)
                and child.recursive_source_alias is not None
                for child in op.in_operators
            )
            is_recursive_join = (
                has_datasource_child and has_recursive_child
            )

        for pair in op.join_pairs:
            node_alias = pair.node_alias
            rel_alias = pair.relationship_or_node_alias

            # Find the fields in the input schema
            node_field = next(
                (f for f in op.input_schema if f.field_alias == node_alias),
                None,
            )
            rel_field = next(
                (f for f in op.input_schema if f.field_alias == rel_alias),
                None,
            )

            # Add node's join key column.
            # For VLP recursive JoinOperators, skip: the CTE provides
            # start_node/end_node directly, so the node ID from a node
            # table JOIN is not inherently required. The renderer decides
            # separately whether the node JOIN is needed.
            if (
                node_field
                and isinstance(node_field, EntityField)
                and not is_recursive_join
            ):
                if node_field.node_join_field:
                    # Use pre-rendered field name if available (varlen paths)
                    if node_field.node_join_field.field_name and node_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                        node_key = node_field.node_join_field.field_name
                    else:
                        node_key = self._get_field_name(
                            node_alias, node_field.node_join_field.field_alias
                        )
                    self._required_columns.add(node_key)

            # Add relationship/node's join key column based on pair type.
            # Same recursive-join guard applies to the rel side.
            if (
                rel_field
                and isinstance(rel_field, EntityField)
                and not is_recursive_join
            ):
                if pair.pair_type == JoinKeyPairType.SOURCE:
                    if rel_field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.rel_source_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type == JoinKeyPairType.SINK:
                    if rel_field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.rel_sink_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type == JoinKeyPairType.NODE_ID:
                    # Node to node join
                    if rel_field.node_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.node_join_field.field_name and rel_field.node_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            rel_key = rel_field.node_join_field.field_name
                        else:
                            rel_key = self._get_field_name(
                                rel_alias, rel_field.node_join_field.field_alias
                            )
                        self._required_columns.add(rel_key)
                elif pair.pair_type in (
                    JoinKeyPairType.EITHER,
                    JoinKeyPairType.BOTH,
                    JoinKeyPairType.EITHER_AS_SOURCE,
                    JoinKeyPairType.EITHER_AS_SINK,
                ):
                    # Undirected or BOTH - need both source and sink keys
                    if rel_field.rel_source_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_source_join_field.field_name and rel_field.rel_source_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            source_key = rel_field.rel_source_join_field.field_name
                        else:
                            source_key = self._get_field_name(
                                rel_alias, rel_field.rel_source_join_field.field_alias
                            )
                        self._required_columns.add(source_key)
                    if rel_field.rel_sink_join_field:
                        # Use pre-rendered field name if available (varlen paths)
                        if rel_field.rel_sink_join_field.field_name and rel_field.rel_sink_join_field.field_name.startswith(self.COLUMN_PREFIX):
                            sink_key = rel_field.rel_sink_join_field.field_name
                        else:
                            sink_key = self._get_field_name(
                                rel_alias, rel_field.rel_sink_join_field.field_alias
                            )
                        self._required_columns.add(sink_key)

    def _add_entity_columns_to_required(
        self,
        op: LogicalOperator,
        resolved_proj: "ResolvedProjection",
    ) -> None:
        """Add all entity columns to _required_columns for RETURN entity.

        When RETURN a (node) or RETURN r (edge) is used, we need ALL the entity's
        properties available for NAMED_STRUCT, not just the ID column.

        This method looks up the entity in the operator's input schema and adds:
        - For nodes: node_id + all encapsulated properties
        - For edges: src, dst + all encapsulated properties
        """
        # Get the entity variable name from the expression
        refs = list(resolved_proj.expression.all_refs())
        if not refs:
            return

        entity_var = refs[0].original_variable
        prefix = f"{self.COLUMN_PREFIX}{entity_var}_"

        # Look up the EntityField in the input schema
        entity_field: EntityField | None = None
        if hasattr(op, 'input_schema') and op.input_schema:
            for field in op.input_schema:
                if isinstance(field, EntityField) and field.field_alias == entity_var:
                    entity_field = field
                    break

        if not entity_field:
            return

        # Add all columns for this entity
        # DEFENSIVE: Some field_names may already be fully prefixed (e.g., _gsql2rsql_sink_id)
        # In that case, use them directly instead of double-prefixing.
        def add_column(prop_name: str) -> None:
            if prop_name.startswith(self.COLUMN_PREFIX):
                # Already prefixed - use directly
                self._required_columns.add(prop_name)
            else:
                # Add prefix
                self._required_columns.add(f"{prefix}{prop_name}")

        if entity_field.entity_type == EntityType.NODE:
            # Node: add node_id and all encapsulated properties
            if entity_field.node_join_field:
                add_column(entity_field.node_join_field.field_name)
            for prop_field in entity_field.encapsulated_fields:
                add_column(prop_field.field_name)
        else:
            # Edge/Relationship: add src, dst, and all encapsulated properties
            if entity_field.rel_source_join_field:
                self._required_columns.add(f"{prefix}src")
            if entity_field.rel_sink_join_field:
                self._required_columns.add(f"{prefix}dst")
            for prop_field in entity_field.encapsulated_fields:
                add_column(prop_field.field_name)

    def _add_entity_columns_for_collect(
        self,
        op: LogicalOperator,
        resolved_proj: "ResolvedProjection",
    ) -> None:
        """Add all entity columns to _required_columns for collect(entity) aggregations.

        When we have collect(b) or collect(r), the entity inside the collect
        needs ALL its properties for the NAMED_STRUCT wrapping, not just the ID.

        This method checks if the projection is a collect() aggregation with a
        bare entity reference inside, and if so, adds all entity columns.

        Args:
            op: The operator context
            resolved_proj: The resolved projection to check
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionAggregationFunction,
            QueryExpressionProperty,
        )
        from gsql2rsql.parser.operators import AggregationFunction

        # Get the original expression from the projection
        # The resolved_proj.expression contains the ResolvedExpression which wraps the original
        original_expr = resolved_proj.expression.original_expression

        # Check if this is a COLLECT aggregation
        if not isinstance(original_expr, QueryExpressionAggregationFunction):
            return
        if original_expr.aggregation_function != AggregationFunction.COLLECT:
            return

        # Check if the inner expression is a bare entity reference
        inner_expr = original_expr.inner_expression
        if not isinstance(inner_expr, QueryExpressionProperty):
            return
        if inner_expr.property_name is not None:
            # Not a bare entity, it's a property access (e.g., collect(b.name))
            return

        # This is collect(entity) - add all entity columns
        entity_var = inner_expr.variable_name
        prefix = f"{self.COLUMN_PREFIX}{entity_var}_"

        # Look up the EntityField in the input schema
        entity_field: EntityField | None = None
        if hasattr(op, 'input_schema') and op.input_schema:
            for field in op.input_schema:
                if isinstance(field, EntityField) and field.field_alias == entity_var:
                    entity_field = field
                    break

        if not entity_field:
            return

        # Add all columns for this entity
        # DEFENSIVE: Some field_names may already be fully prefixed (e.g., _gsql2rsql_sink_id)
        # In that case, use them directly instead of double-prefixing.
        def add_column(prop_name: str) -> None:
            if prop_name.startswith(self.COLUMN_PREFIX):
                # Already prefixed - use directly
                self._required_columns.add(prop_name)
            else:
                # Add prefix
                self._required_columns.add(f"{prefix}{prop_name}")

        if entity_field.entity_type == EntityType.NODE:
            # Node: add node_id and all encapsulated properties
            if entity_field.node_join_field:
                add_column(entity_field.node_join_field.field_name)
            for prop_field in entity_field.encapsulated_fields:
                add_column(prop_field.field_name)
        else:
            # Edge/Relationship: add src, dst, and all encapsulated properties
            if entity_field.rel_source_join_field:
                self._required_columns.add(f"{prefix}src")
            if entity_field.rel_sink_join_field:
                self._required_columns.add(f"{prefix}dst")
            for prop_field in entity_field.encapsulated_fields:
                add_column(prop_field.field_name)

    def _collect_required_columns(self, op: LogicalOperator) -> None:
        """
        Collect required column aliases from the operator tree (column pruning).

        Uses pre-resolved column references from ResolutionResult for accuracy.
        This is an optimization to only output required columns in intermediate
        subqueries, improving query performance.

        The renderer now requires resolution, so this method always uses the
        resolution-based path.
        """
        # Always use resolution (guaranteed to be non-None after render_plan check)
        assert self._resolution_result is not None, "Resolution required"
        self._collect_required_columns_from_resolution(op)

    def _get_entity_id_column_from_schema(
        self, schema: Schema, entity_alias: str
    ) -> str | None:
        """Get the ID column name for an entity from a schema.

        Looks for an EntityField with the given alias and returns its ID column
        name in the rendered format (e.g., '_gsql2rsql_c_id').

        Args:
            schema: The schema to search in
            entity_alias: The entity alias to find (e.g., 'c')

        Returns:
            The ID column name (e.g., '_gsql2rsql_c_id') or None if not found
        """
        for field in schema:
            if isinstance(field, EntityField) and field.field_alias == entity_alias:
                if field.entity_type == EntityType.NODE and field.node_join_field:
                    return self._get_field_name(
                        entity_alias, field.node_join_field.field_alias
                    )
                elif field.rel_source_join_field:
                    return self._get_field_name(
                        entity_alias, field.rel_source_join_field.field_alias
                    )
        return None

    def _get_field_name(self, prefix: str, field_name: str) -> str:
        """Generate a field name with entity prefix.

        Delegates to RenderContext when available, falls back to direct computation.
        """
        if hasattr(self, '_ctx'):
            return self._ctx.get_field_name(prefix, field_name)
        clean_prefix = "".join(
            c if c.isalnum() or c == "_" else "" for c in prefix
        )
        return f"{self.COLUMN_PREFIX}{clean_prefix}_{field_name}"

    def _indent(self, depth: int) -> str:
        """Get indentation string for a given depth."""
        return "  " * depth

    def _render_operator(self, op: LogicalOperator, depth: int) -> str:
        """Render a logical operator to SQL."""
        if isinstance(op, DataSourceOperator):
            return self._render_data_source(op, depth)
        elif isinstance(op, JoinOperator):
            return self._join.render_join(op, depth)
        elif isinstance(op, SelectionOperator):
            return self._render_selection(op, depth)
        elif isinstance(op, ProjectionOperator):
            return self._render_projection(op, depth)
        elif isinstance(op, SetOperator):
            return self._render_set_operator(op, depth)
        elif isinstance(op, RecursiveTraversalOperator):
            if self._ctx.vlp_rendering_mode == "procedural":
                return self._proc_bfs.render_procedural_reference(
                    op, depth,
                )
            return self._cte.render_recursive_reference(op, depth)
        elif isinstance(op, UnwindOperator):
            return self._render_unwind(op, depth)
        elif isinstance(op, AggregationBoundaryOperator):
            return self._cte.render_aggregation_boundary_reference(op, depth)
        else:
            raise TranspilerNotSupportedException(
                f"Operator type {type(op).__name__}"
            )

    def _render_data_source(self, op: DataSourceOperator, depth: int) -> str:
        """Render a data source operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.output_schema:
            return ""

        entity_field = op.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return ""

        # Read SQL table descriptor from enriched data (pre-resolved by SQLEnrichmentPass)
        enriched_ds = (
            self._ctx.enriched.data_sources.get(op.operator_debug_id)
            if self._ctx.enriched
            else None
        )
        if not enriched_ds:
            raise TranspilerInternalErrorException(
                f"No enriched data for DataSource {entity_field.bound_entity_name}"
            )
        table_desc = enriched_ds.table_descriptor

        lines.append(f"{indent}SELECT")

        # Render fields
        field_lines: list[str] = []

        # Always include join key fields first
        if entity_field.entity_type == EntityType.NODE:
            if entity_field.node_join_field:
                key_name = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.node_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.node_join_field.field_alias} AS {key_name}"
                )
        else:  # Relationship
            if entity_field.rel_source_join_field:
                src_key = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.rel_source_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.rel_source_join_field.field_alias} AS {src_key}"
                )
            if entity_field.rel_sink_join_field:
                sink_key = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.rel_sink_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.rel_sink_join_field.field_alias} AS {sink_key}"
                )

        # Add other referenced fields
        # With column pruning enabled, only include fields that are actually used
        skip_fields = set()
        if entity_field.node_join_field:
            skip_fields.add(entity_field.node_join_field.field_alias)
        if entity_field.rel_source_join_field:
            skip_fields.add(entity_field.rel_source_join_field.field_alias)
        if entity_field.rel_sink_join_field:
            skip_fields.add(entity_field.rel_sink_join_field.field_alias)

        for encap_field in entity_field.encapsulated_fields:
            if encap_field.field_alias not in skip_fields:
                field_alias = self._get_field_name(
                    entity_field.field_alias, encap_field.field_alias
                )
                # Column pruning: only include if required or pruning disabled
                if (
                    not self._enable_column_pruning
                    or not self._required_columns
                    or field_alias in self._required_columns
                ):
                    field_lines.append(f"{encap_field.field_alias} AS {field_alias}")

        # If no fields selected, select key field
        if not field_lines:
            if entity_field.node_join_field:
                key_name = self._get_field_name(
                    entity_field.field_alias,
                    entity_field.node_join_field.field_alias,
                )
                field_lines.append(
                    f"{entity_field.node_join_field.field_alias} AS {key_name}"
                )
            else:
                field_lines.append("1 AS _dummy")

        # Format fields
        for i, field_line in enumerate(field_lines):
            prefix = " " if i == 0 else ","
            lines.append(f"{indent}  {prefix}{field_line}")

        lines.append(f"{indent}FROM")
        lines.append(f"{indent}  {table_desc.full_table_name}")

        # Collect all filters to apply
        filters: list[str] = []

        # Type filter from enrichment (edge type filters, single or multi-edge OR)
        if enriched_ds.type_filter_clause:
            filters.append(enriched_ds.type_filter_clause)

        # Add pushed-down filter from optimizer (e.g., p.name = 'Alice')
        if op.filter_expression:
            # Render the filter expression using raw column names
            # (not aliased names like _gsql2rsql_p_name)
            rendered_filter = self._render_datasource_filter(
                op.filter_expression, entity_field.field_alias
            )
            filters.append(rendered_filter)

        # Render WHERE clause with all filters
        if filters:
            combined_filter = " AND ".join(f"({f})" for f in filters)
            lines.append(f"{indent}WHERE {combined_filter}")

        return "\n".join(lines)

    def _render_datasource_filter(
        self,
        expr: QueryExpression,
        entity_alias: str,
    ) -> str:
        """Render a filter expression for a DataSource using raw column names.

        Unlike render_expression which uses aliased names like _gsql2rsql_p_name,
        this method renders expressions using raw column names from the table.

        Args:
            expr: The filter expression to render.
            entity_alias: The entity alias (e.g., 'p') to match against.

        Returns:
            SQL string with raw column names.
        """
        from gsql2rsql.parser.ast import (
            QueryExpressionBinary,
            QueryExpressionFunction,
            QueryExpressionList,
            QueryExpressionParameter,
            QueryExpressionProperty,
            QueryExpressionValue,
        )

        if isinstance(expr, QueryExpressionProperty):
            # Use raw column name, not aliased
            if expr.variable_name == entity_alias and expr.property_name:
                return expr.property_name
            # Fallback to aliased name for other variables
            return self._get_field_name(expr.variable_name, expr.property_name or "")

        elif isinstance(expr, QueryExpressionValue):
            return self._expr.render_value(expr)

        elif isinstance(expr, QueryExpressionParameter):
            return self._expr.render_parameter(expr)

        elif isinstance(expr, QueryExpressionBinary):
            if not expr.operator or not expr.left_expression or not expr.right_expression:
                return "NULL"
            left = self._render_datasource_filter(expr.left_expression, entity_alias)
            right = self._render_datasource_filter(expr.right_expression, entity_alias)
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)

        elif isinstance(expr, QueryExpressionFunction):
            # Render function arguments with raw column names
            params = [
                self._render_datasource_filter(p, entity_alias)
                for p in expr.parameters
            ]
            func = expr.function
            if func == Function.NOT:
                return f"NOT ({params[0]})" if params else "NOT (NULL)"
            elif func == Function.NEGATIVE:
                return f"-({params[0]})" if params else "-(NULL)"
            elif func == Function.IS_NULL:
                return f"({params[0]}) IS NULL" if params else "NULL IS NULL"
            elif func == Function.IS_NOT_NULL:
                return f"({params[0]}) IS NOT NULL" if params else "NULL IS NOT NULL"
            elif func == Function.COALESCE:
                if params:
                    return f"COALESCE({', '.join(params)})"
                return "NULL"
            elif func == Function.STRING_STARTS_WITH:
                if len(params) >= 2:
                    return f"STARTSWITH({params[0]}, {params[1]})"
                return "NULL"
            elif func == Function.STRING_ENDS_WITH:
                if len(params) >= 2:
                    return f"ENDSWITH({params[0]}, {params[1]})"
                return "NULL"
            elif func == Function.STRING_CONTAINS:
                if len(params) >= 2:
                    return f"CONTAINS({params[0]}, {params[1]})"
                return "NULL"
            raise NotImplementedError(
                f"_render_datasource_filter: unsupported function {func!r}. "
                f"Add an explicit handler for this function."
            )

        elif isinstance(expr, QueryExpressionList):
            # Render list literals for IN operator: [1, 2, 3] -> (1, 2, 3)
            items = [self._render_datasource_filter(item, entity_alias) for item in expr.items]
            return f"({', '.join(items)})"

        # For other expression types, fall back to standard rendering
        # This shouldn't happen for simple property filters
        return str(expr)

    def _render_selection(self, op: SelectionOperator, depth: int) -> str:
        """Render a selection (WHERE) operator."""
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        lines.append(f"{indent}SELECT *")
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _filter")

        if op.filter_expression:
            filter_sql = self._expr.render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {filter_sql}")

        return "\n".join(lines)

    def _render_unwind(self, op: UnwindOperator, depth: int) -> str:
        """Render an UNWIND operator using Databricks SQL TVF syntax.

        UNWIND expression AS variable becomes:
        SELECT _unwind_source.*, variable
        FROM (inner) AS _unwind_source,
        EXPLODE(expression) AS _exploded(variable)

        For NULL/empty array preservation, use EXPLODE_OUTER instead.
        The preserve_nulls flag on the operator controls this behavior.
        """
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        var_name = op.variable_name

        # Render the list expression
        if op.list_expression:
            list_sql = self._expr.render_expression(op.list_expression, op)
        else:
            list_sql = "ARRAY()"

        # Choose explode function based on NULL preservation needs
        # Default to EXPLODE (drops rows with NULL/empty arrays)
        # Use EXPLODE_OUTER if we need to preserve rows with NULL/empty arrays
        explode_func = "EXPLODE_OUTER" if getattr(op, 'preserve_nulls', False) else "EXPLODE"

        # Build the SELECT with all columns from source plus the unwound variable
        lines.append(f"{indent}SELECT")
        lines.append(f"{indent}   _unwind_source.*")
        lines.append(f"{indent}  ,{var_name}")
        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _unwind_source")

        # Use LATERAL correlation with TVF for column resolution from _unwind_source.
        # The plain comma-join (FROM x, EXPLODE(col)) doesn't work when 'col' is
        # a column inside subquery 'x'. LATERAL enables the correlation.
        # See: https://docs.databricks.com/aws/en/sql/language-manual/functions/explode
        lines[-1] += ","  # Add comma after _unwind_source
        lines.append(
            f"{indent}LATERAL {explode_func}({list_sql}) AS _exploded({var_name})"
        )

        return "\n".join(lines)

    def _render_projection(self, op: ProjectionOperator, depth: int) -> str:
        """Render a projection (SELECT) operator.

        Handles both regular projections and flattened projections (where a
        SelectionOperator was merged in via SubqueryFlatteningOptimizer).

        SQL clause order: SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT
        """
        lines: list[str] = []
        indent = self._indent(depth)

        if not op.in_operator:
            return ""

        # Render projection fields
        # Note: Check for aggregation first since we need it for alias logic
        has_aggregation = any(self._expr.has_aggregation(expr) for _, expr in op.projections)

        # Detect if we need DISTINCT → GROUP BY TO_JSON workaround.
        # Spark cannot do SELECT DISTINCT on columns with MAP type (including
        # STRUCTs containing MAP fields, or bare MAP columns from property access).
        # Always use GROUP BY TO_JSON(NAMED_STRUCT('_', col)) instead of DISTINCT.
        # The NAMED_STRUCT wrapper ensures TO_JSON works on all types (scalar, struct, map).
        use_distinct_json = op.is_distinct and not has_aggregation
        group_by_json_exprs: list[str] = []

        # Render SELECT clause
        distinct = "DISTINCT " if op.is_distinct and not use_distinct_json else ""
        lines.append(f"{indent}SELECT {distinct}")

        # Get resolved projections for entity struct rendering
        resolved_projections_map: dict[str, "ResolvedProjection"] = {}
        if op.operator_debug_id in self._resolved.resolved_projections:
            for rp in self._resolved.resolved_projections[op.operator_debug_id]:
                resolved_projections_map[rp.alias] = rp

        # Track which entity variables are rendered as STRUCT (to skip extra columns)
        entities_rendered_as_struct: set[str] = set()

        for i, (alias, expr) in enumerate(op.projections):
            rendered = self._expr.render_expression(expr, op)
            prefix = " " if i == 0 else ","

            # Check if this is a bare entity reference (not an aggregate, not a property access)
            is_bare_entity = (
                isinstance(expr, QueryExpressionProperty)
                and expr.property_name is None
                and not self._expr.has_aggregation(expr)
            )

            # NEW: For root projection (depth == 0), render entities as NAMED_STRUCT
            # This implements OpenCypher semantics where RETURN a returns the whole entity
            if depth == 0 and is_bare_entity and not has_aggregation:
                # Get the resolved projection for this alias
                resolved_proj = resolved_projections_map.get(alias)
                if resolved_proj and resolved_proj.is_entity_ref:
                    # Type assertion: is_bare_entity implies expr is QueryExpressionProperty
                    assert isinstance(expr, QueryExpressionProperty)
                    entity_var = expr.variable_name
                    # Render as NAMED_STRUCT with all properties
                    # Returns None if the variable is not an entity (e.g., UNWIND variables)
                    struct_rendered = self._expr.render_entity_as_struct(resolved_proj, entity_var, op)
                    if struct_rendered is not None:
                        rendered = struct_rendered
                        entities_rendered_as_struct.add(entity_var)
                    output_alias = alias
                else:
                    output_alias = alias
            elif (has_aggregation or depth > 0) and is_bare_entity:
                # Bug fix: In aggregation contexts or intermediate projections, entity IDs should
                # keep their full column names instead of being aliased to short names. This
                # prevents UNRESOLVED_COLUMN errors in PySpark when outer queries try to
                # reference the original column name.
                #
                # Example bug: WITH p, COUNT(t) AS total
                #   - Buggy:  _gsql2rsql_p_id AS p  (aliases away the column)
                #   - Fixed:  _gsql2rsql_p_id AS _gsql2rsql_p_id  (preserves column name)
                output_alias = rendered
            else:
                # Use user-provided alias (normal behavior)
                output_alias = alias

            # If using DISTINCT→GROUP BY TO_JSON workaround, wrap each column.
            # Use NAMED_STRUCT wrapper so TO_JSON works on ALL types (including scalars).
            if use_distinct_json:
                group_by_json_exprs.append(f"TO_JSON(NAMED_STRUCT('_', {rendered}))")
                rendered = f"FIRST({rendered})"

            lines.append(f"{indent}  {prefix}{rendered} AS {output_alias}")

        # Bug #1 Fix: When projecting entity variables through a WITH clause,
        # we need to also project any entity properties that are required downstream.
        # For example: WITH c, COUNT(p) AS pop -> if downstream needs c.name,
        # we must project _gsql2rsql_c_name in addition to _gsql2rsql_c_id.
        #
        # This applies to:
        # 1. ALL aggregating projections (GROUP BY loses columns not in SELECT/GROUP BY)
        # 2. INTERMEDIATE non-aggregating projections (depth > 0) where entity variables
        #    are passed through and downstream needs their properties
        #
        # NOTE: For root projections (depth == 0) with entity returns rendered as STRUCT,
        # we DON'T add extra columns because the STRUCT already contains all properties.
        has_entity_return = False
        if op.operator_debug_id in self._resolved.resolved_projections:
            has_entity_return = any(
                proj.is_entity_ref
                for proj in self._resolved.resolved_projections[op.operator_debug_id]
            )

        extra_columns: list[str] = []
        # Only add extra columns for intermediate projections or aggregations
        # Skip for root projections where entities are rendered as STRUCT
        should_add_extra_columns = (
            self._required_columns
            and (has_aggregation or depth > 0)
            and not (depth == 0 and has_entity_return and not has_aggregation)
        )
        if should_add_extra_columns:
            extra_columns = self._expr.get_entity_properties_for_aggregation(op)
            # Filter out columns for entities that were rendered as STRUCT
            if entities_rendered_as_struct:
                extra_columns = [
                    col for col in extra_columns
                    if not any(
                        col.startswith(f"{self.COLUMN_PREFIX}{ent}_")
                        for ent in entities_rendered_as_struct
                    )
                ]
            for col_alias in extra_columns:
                if use_distinct_json:
                    # When using DISTINCT→GROUP BY TO_JSON workaround, extra entity
                    # property columns must be aggregated with FIRST() since they are
                    # not in the GROUP BY clause (only the entity key is).
                    lines.append(f"{indent}  ,FIRST({col_alias}) AS {col_alias}")
                else:
                    lines.append(f"{indent}  ,{col_alias} AS {col_alias}")

        lines.append(f"{indent}FROM (")
        lines.append(self._render_operator(op.in_operator, depth + 1))
        lines.append(f"{indent}) AS _proj")

        # WHERE clause (from flattened SelectionOperator)
        # Applied BEFORE GROUP BY - filters individual rows
        if op.filter_expression:
            filter_sql = self._expr.render_expression(op.filter_expression, op)
            lines.append(f"{indent}WHERE {filter_sql}")

        # GROUP BY for DISTINCT workaround (replaces SELECT DISTINCT with GROUP BY TO_JSON)
        # This handles Spark's inability to compare MAP types in DISTINCT operations
        if use_distinct_json and group_by_json_exprs:
            group_by = ", ".join(group_by_json_exprs)
            lines.append(f"{indent}GROUP BY {group_by}")

        # Group by for aggregations
        if has_aggregation:
            # First, identify which aliases are aggregates
            aggregate_aliases: set[str] = {
                alias for alias, expr in op.projections
                if self._expr.has_aggregation(expr)
            }
            # Non-aggregate expressions go in GROUP BY, but only if they don't
            # reference any aggregate aliases (e.g., similarity_score = ... + shared_merchants
            # shouldn't be in GROUP BY if shared_merchants is an aggregate)
            non_agg_exprs = [
                self._expr.render_expression(expr, op)
                for alias, expr in op.projections
                if not self._expr.has_aggregation(expr)
                and not self._expr.references_aliases(expr, aggregate_aliases)
            ]
            # Also include extra entity property columns in GROUP BY
            all_group_by_cols = non_agg_exprs + extra_columns
            if all_group_by_cols:
                group_by = ", ".join(all_group_by_cols)
                lines.append(f"{indent}GROUP BY {group_by}")

        # HAVING clause (filter on aggregated columns)
        # Applied AFTER GROUP BY - filters groups
        # Note: If there's no aggregation but having_expression is set,
        # treat it as a regular WHERE clause (e.g., WITH ... WHERE on computed columns)
        needs_subquery_wrap = False
        if op.having_expression:
            having_sql = self._expr.render_expression(op.having_expression, op)
            if has_aggregation:
                lines.append(f"{indent}HAVING {having_sql}")
            else:
                # No aggregation - check if the expression references aliases
                # defined in the current projection (e.g., return_rate > 0.5 where
                # return_rate is computed in this SELECT). SQL doesn't allow this,
                # so we need to wrap in a subquery.
                projection_aliases = {alias for alias, _ in op.projections}
                if self._expr.references_aliases(op.having_expression, projection_aliases):
                    # Mark that we need to wrap this in a subquery
                    needs_subquery_wrap = True
                else:
                    # Filter doesn't reference computed aliases, can use WHERE
                    lines.append(f"{indent}WHERE {having_sql}")

        # Order by
        # When entities are rendered as NAMED_STRUCT, ORDER BY expressions that reference
        # entity properties need to use struct field access (e.g., a.id instead of _gsql2rsql_a_id)
        if op.order_by:
            order_parts: list[str] = []
            for expr, is_desc in op.order_by:
                # Check if this is a property access on an entity rendered as struct
                rendered = self._expr.render_order_by_expression(
                    expr, op, entities_rendered_as_struct, resolved_projections_map,
                    op.projections
                )
                direction = "DESC" if is_desc else "ASC"
                order_parts.append(f"{rendered} {direction}")
            lines.append(f"{indent}ORDER BY {', '.join(order_parts)}")

        # Limit and skip (Databricks uses LIMIT/OFFSET)
        if op.limit is not None or op.skip is not None:
            if op.limit is not None:
                lines.append(f"{indent}LIMIT {op.limit}")
            if op.skip is not None:
                lines.append(f"{indent}OFFSET {op.skip}")

        # If we need to wrap in a subquery (because having_expression references
        # aliases defined in this SELECT), wrap the entire query
        if needs_subquery_wrap and op.having_expression:
            inner_sql = "\n".join(lines)
            having_sql = self._expr.render_expression(op.having_expression, op)
            # Build outer wrapper that projects all columns and applies the filter
            outer_lines = [
                f"{indent}SELECT *",
                f"{indent}FROM (",
                inner_sql,
                f"{indent}) AS _filter",
                f"{indent}WHERE {having_sql}",
            ]
            return "\n".join(outer_lines)

        return "\n".join(lines)

    def _render_set_operator(self, op: SetOperator, depth: int) -> str:
        """Render a set operator (UNION, etc.)."""
        lines: list[str] = []
        indent = self._indent(depth)

        left_op = op.in_operator_left
        right_op = op.in_operator_right

        if not left_op or not right_op:
            return ""

        lines.append(self._render_operator(left_op, depth))

        if op.set_operation == SetOperationType.UNION_ALL:
            lines.append(f"{indent}UNION ALL")
        elif op.set_operation == SetOperationType.UNION:
            lines.append(f"{indent}UNION")
        elif op.set_operation == SetOperationType.INTERSECT:
            lines.append(f"{indent}INTERSECT")
        elif op.set_operation == SetOperationType.EXCEPT:
            lines.append(f"{indent}EXCEPT")

        lines.append(self._render_operator(right_op, depth))

        return "\n".join(lines)

