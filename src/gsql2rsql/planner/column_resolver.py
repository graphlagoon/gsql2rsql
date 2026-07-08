"""Column resolution pass for the transpiler.

This module provides the ColumnResolver class which performs a full resolution
pass over the logical plan, validating all column references and creating
ResolvedColumnRef objects for use during rendering.

The resolution pass:
1. Builds a symbol table by visiting operators in topological order
2. Resolves all column references in expressions
3. Validates property accesses against entity schemas
4. Provides detailed error messages for invalid references
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from gsql2rsql.common.exceptions import (
    ColumnResolutionError,
    ColumnResolutionErrorContext,
    levenshtein_distance,
)
from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionCaseExpression,
    QueryExpressionExists,
    QueryExpressionFunction,
    QueryExpressionList,
    QueryExpressionListComprehension,
    QueryExpressionListPredicate,
    QueryExpressionMapLiteral,
    QueryExpressionProperty,
    QueryExpressionReduce,
    QueryExpressionValue,
    QueryExpressionWithAlias,
)
from gsql2rsql.parser.operators import Function
from gsql2rsql.planner.column_ref import (
    ColumnRefType,
    ResolvedColumnRef,
    ResolvedExpression,
    ResolvedProjection,
    compute_sql_column_name,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    DataSourceOperator,
    LogicalOperator,
    ProjectionOperator,
    RecursiveTraversalOperator,
    SelectionOperator,
    UnwindOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType, ValueField
from gsql2rsql.planner.symbol_table import (
    SymbolEntry,
    SymbolInfo,
    SymbolTable,
    SymbolType,
)

if TYPE_CHECKING:
    from gsql2rsql.planner.logical_plan import LogicalPlan


@dataclass
class ResolutionResult:
    """Result of the column resolution pass.

    Contains the resolved plan and any warnings/info collected during resolution.
    """

    # Resolved expressions per operator (operator_id -> resolved expressions)
    resolved_expressions: dict[int, list[ResolvedExpression]] = field(default_factory=dict)

    # Resolved projections per ProjectionOperator
    resolved_projections: dict[int, list[ResolvedProjection]] = field(default_factory=dict)

    # Final symbol table state
    symbol_table: SymbolTable | None = None

    # Statistics
    total_references_resolved: int = 0
    total_expressions_resolved: int = 0

    # Warnings (non-fatal issues)
    warnings: list[str] = field(default_factory=list)


class ColumnResolver:
    """Resolves and validates all column references in a logical plan.

    The resolver performs a single pass over the logical plan, building a symbol
    table and resolving all column references. It provides detailed error messages
    when resolution fails.

    Usage:
        resolver = ColumnResolver()
        result = resolver.resolve(plan, original_query_text)

        # Access resolved expressions
        for op_id, exprs in result.resolved_expressions.items():
            for expr in exprs:
                for ref in expr.all_refs():
                    print(f"{ref.original_text} -> {ref.sql_column_name}")

    The resolver validates:
    - All variable references exist in scope
    - All property accesses are valid for the entity type
    - Scope boundaries (WITH aggregation) are respected
    - Type compatibility (where determinable)
    """

    def __init__(self) -> None:
        """Initialize the resolver."""
        self._symbol_table: SymbolTable = SymbolTable()
        self._original_query: str = ""
        self._current_operator: LogicalOperator | None = None
        self._current_phase: str = ""
        self._current_part_index: int = 0
        self._result: ResolutionResult = ResolutionResult()
        self._allow_out_of_scope_lookups: bool = False  # For AggregationBoundaryOperator
        self._graph_schema: Any = None  # Graph schema for looking up entity properties

    def resolve(
        self,
        plan: LogicalPlan,
        original_query: str = "",
    ) -> ResolutionResult:
        """Resolve all column references in a logical plan.

        Args:
            plan: The logical plan to resolve
            original_query: The original Cypher query text (for error messages)

        Returns:
            ResolutionResult containing all resolved references

        Raises:
            ColumnResolutionError: If any reference cannot be resolved
        """
        self._symbol_table = SymbolTable()
        self._original_query = original_query
        self._result = ResolutionResult()
        self._graph_schema = plan.graph_schema  # Store for EXISTS pattern resolution

        # Phase 1: Build symbol table by visiting operators in topological order
        self._current_phase = "symbol_table_building"
        all_operators = self._get_operators_in_topological_order(plan)

        for op in all_operators:
            self._current_operator = op
            self._build_symbols_for_operator(op)

        # Phase 2: Resolve all expressions
        # IMPORTANT: Process operators in topological order, allowing scope changes
        # to flow from upstream to downstream operators
        self._current_phase = "expression_resolution"

        # Reset symbol table to start of Phase 2
        # We need to rebuild the symbol table state as we process operators
        # This ensures each operator sees the correct scope
        self._symbol_table = SymbolTable()

        for op in all_operators:
            self._current_operator = op
            # Rebuild symbols for this operator (adds entities/variables to scope)
            self._build_symbols_for_operator(op)
            # Now resolve expressions with current symbol table state
            self._resolve_operator_expressions(op)

        # Use final symbol table state for result
        self._result.symbol_table = self._symbol_table
        return self._result

    def _get_operators_in_topological_order(
        self, plan: LogicalPlan
    ) -> list[LogicalOperator]:
        """Get all operators in topological order (sources first).

        Args:
            plan: The logical plan

        Returns:
            List of operators ordered from sources to terminals
        """
        result: list[LogicalOperator] = []
        visited: set[int] = set()

        def visit(op: LogicalOperator) -> None:
            op_id = id(op)
            if op_id in visited:
                return
            visited.add(op_id)

            # Visit inputs first
            for in_op in op.graph_in_operators:
                visit(in_op)

            result.append(op)

        # Start from terminal operators and work backwards
        for terminal in plan.terminal_operators:
            visit(terminal)

        return result

    def _build_symbols_for_operator(self, op: LogicalOperator) -> None:
        """Build symbol table entries for an operator.

        Different operators introduce symbols differently:
        - DataSourceOperator: Introduces entity symbol
        - ProjectionOperator: May introduce value symbols (aliases)
        - AggregationBoundaryOperator: Clears scope, introduces projected symbols
        - UnwindOperator: Introduces value symbol for unwound variable
        - JoinOperator: Merges schemas (no new symbols)
        - SelectionOperator: No new symbols

        Args:
            op: The operator to process
        """
        if isinstance(op, DataSourceOperator):
            self._build_symbols_for_datasource(op)
        elif isinstance(op, AggregationBoundaryOperator):
            self._build_symbols_for_aggregation_boundary(op)
        elif isinstance(op, UnwindOperator):
            self._build_symbols_for_unwind(op)
        elif isinstance(op, ProjectionOperator):
            self._build_symbols_for_projection(op)
        elif isinstance(op, RecursiveTraversalOperator):
            self._build_symbols_for_recursive_traversal(op)
        # JoinOperator, SelectionOperator, etc. don't introduce new symbols

    def _build_symbols_for_datasource(self, op: DataSourceOperator) -> None:
        """Build symbol for a DataSource operator (entity definition).

        Args:
            op: The DataSourceOperator
        """
        if not op.entity or not op.entity.alias:
            return

        alias = op.entity.alias
        entity_name = op.entity.entity_name or "unknown"

        # Get EntityField from output_schema if available
        entity_field: EntityField | None = None
        properties: list[str] = []

        for fld in op.output_schema:
            if isinstance(fld, EntityField) and fld.field_alias == alias:
                entity_field = fld
                properties = [vf.field_name for vf in fld.encapsulated_fields]
                break

        entry = SymbolEntry(
            name=alias,
            symbol_type=SymbolType.ENTITY,
            definition_operator_id=op.operator_debug_id,
            definition_location=f"MATCH ({alias}:{entity_name})",
            scope_level=self._symbol_table.current_level,
            entity_info=entity_field,
            data_type_name=entity_name,
            properties=properties,
        )

        self._symbol_table.define_or_update(alias, entry)

    def _build_symbols_for_aggregation_boundary(
        self, op: AggregationBoundaryOperator
    ) -> None:
        """Build symbols for an aggregation boundary.

        An aggregation boundary clears the current scope and introduces
        only the projected variables.

        CRITICAL: We must look up entity information BEFORE clearing scope,
        otherwise the lookups will fail.

        Args:
            op: The AggregationBoundaryOperator
        """
        # FIRST: Look up entity information for projections BEFORE clearing scope
        projection_info: list[tuple[str, bool, Any, str | None, list[str], bool]] = []

        for alias, expr in op.all_projections:
            # Determine if this is an entity reference or computed value
            is_entity = False
            entity_info = None
            data_type_name = None
            properties: list[str] = []

            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    # Bare entity reference like "WITH a, ..."
                    existing = self._symbol_table.lookup(expr.variable_name)
                    if existing and existing.symbol_type == SymbolType.ENTITY:
                        is_entity = True
                        entity_info = existing.entity_info
                        data_type_name = existing.data_type_name
                        properties = existing.properties

            # Check if this is an aggregation result
            is_aggregated = self._expression_contains_aggregation(expr)
            if is_aggregated:
                data_type_name = self._infer_aggregation_type(expr)

            projection_info.append((alias, is_entity, entity_info, data_type_name, properties, is_aggregated))

        # SECOND: Clear scope - after aggregation, only projected variables are visible
        self._symbol_table.clear_scope_for_aggregation(
            f"WITH aggregation at operator {op.operator_debug_id}"
        )

        # THIRD: Define symbols using the information we collected before clearing
        for alias, is_entity, entity_info, data_type_name, properties, is_aggregated in projection_info:
            entry = SymbolEntry(
                name=alias,
                symbol_type=SymbolType.ENTITY if is_entity else SymbolType.VALUE,
                definition_operator_id=op.operator_debug_id,
                definition_location="WITH (aggregation)",
                scope_level=self._symbol_table.current_level,
                entity_info=entity_info,
                data_type_name=data_type_name,
                properties=properties,
                is_aggregated=is_aggregated,
            )

            self._symbol_table.define_or_update(alias, entry)

    def _build_symbols_for_unwind(self, op: UnwindOperator) -> None:
        """Build symbol for an UNWIND operator.

        For VLP relationship variables, the unwound element has struct type
        with properties like src, dst, weight, etc. This method extracts
        those properties from the ValueField's structured_type so that
        property access (e.g., r.src) can be validated.

        Args:
            op: The UnwindOperator
        """
        from gsql2rsql.planner.data_types import StructType

        # Find the ValueField for the unwound variable in output_schema
        value_field: ValueField | None = None
        properties: list[str] = []
        data_type_name = "ANY"

        if op.output_schema:
            for fld in op.output_schema.fields:
                if isinstance(fld, ValueField) and fld.field_alias == op.variable_name:
                    value_field = fld
                    # Extract properties from structured_type if it's a StructType
                    if fld.structured_type and isinstance(fld.structured_type, StructType):
                        properties = [sf.name for sf in fld.structured_type.fields]
                        data_type_name = fld.structured_type.name or "STRUCT"
                    break

        entry = SymbolEntry(
            name=op.variable_name,
            symbol_type=SymbolType.VALUE,
            definition_operator_id=op.operator_debug_id,
            definition_location=f"UNWIND ... AS {op.variable_name}",
            scope_level=self._symbol_table.current_level,
            data_type_name=data_type_name,
            value_info=value_field,
            properties=properties,
        )

        self._symbol_table.define_or_update(op.variable_name, entry)

    def _build_symbols_for_recursive_traversal(
        self, op: RecursiveTraversalOperator
    ) -> None:
        """Build symbols for a RecursiveTraversal operator.

        RecursiveTraversal introduces:
        - path_variable (if specified) - the named path variable
        - relationship_variable (if specified) - the list of relationships traversed
        - Target nodes are handled by their own DataSourceOperators

        Args:
            op: The RecursiveTraversalOperator
        """
        # Add path variable if specified
        if op.path_variable:
            entry = SymbolEntry(
                name=op.path_variable,
                symbol_type=SymbolType.PATH,
                definition_operator_id=op.operator_debug_id,
                definition_location=f"MATCH {op.path_variable} = ...",
                scope_level=self._symbol_table.current_level,
                data_type_name="PATH",  # Path type
            )
            self._symbol_table.define_or_update(op.path_variable, entry)

        # Add relationship variable if specified (e.g., 'r' in [r*1..3])
        # In Cypher, this represents the list of relationships traversed
        if op.relationship_variable:
            edge_type = "|".join(op.edge_types) if op.edge_types else "Relationship"
            entry = SymbolEntry(
                name=op.relationship_variable,
                symbol_type=SymbolType.ENTITY,
                definition_operator_id=op.operator_debug_id,
                definition_location=f"MATCH -[{op.relationship_variable}*]-",
                scope_level=self._symbol_table.current_level,
                data_type_name=f"LIST<{edge_type}>",  # List of relationships
                is_vlp_relationship=True,  # Mark as VLP relationship for correct SQL mapping
            )
            self._symbol_table.define_or_update(op.relationship_variable, entry)

    def _build_symbols_for_projection(self, op: ProjectionOperator) -> None:
        """Build symbols for a projection operator.

        Projections that rename or compute values create new symbols.

        NOTE: Scope clearing for aggregating projections is handled in
        _resolve_operator_expressions AFTER resolving the projection's
        own expressions (so COUNT(f) can reference f).

        Args:
            op: The ProjectionOperator
        """
        # Normal projection processing
        for alias, expr in op.projections:
            # Check if this creates a new name (alias differs from source)
            creates_new_name = True
            source_entry: SymbolEntry | None = None

            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    # Bare entity reference - doesn't create new name if alias matches
                    if expr.variable_name == alias:
                        creates_new_name = False
                    source_entry = self._symbol_table.lookup(expr.variable_name)

            if creates_new_name:
                # Determine type from expression
                is_aggregated = self._expression_contains_aggregation(expr)
                data_type_name = None

                if is_aggregated:
                    data_type_name = self._infer_aggregation_type(expr)
                elif source_entry:
                    data_type_name = source_entry.data_type_name

                entry = SymbolEntry(
                    name=alias,
                    symbol_type=SymbolType.VALUE,
                    definition_operator_id=op.operator_debug_id,
                    definition_location=f"RETURN/WITH AS {alias}",
                    scope_level=self._symbol_table.current_level,
                    data_type_name=data_type_name,
                    is_aggregated=is_aggregated,
                )

                self._symbol_table.define_or_update(alias, entry)

    def _resolve_operator_expressions(self, op: LogicalOperator) -> None:
        """Resolve all expressions in an operator.

        Args:
            op: The operator to process
        """
        resolved_exprs: list[ResolvedExpression] = []

        if isinstance(op, SelectionOperator):
            if op.filter_expression:
                resolved = self._resolve_expression(op.filter_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, ProjectionOperator):
            resolved_projections: list[ResolvedProjection] = []
            for alias, expr in op.projections:
                resolved_expr = self._resolve_expression(expr)
                resolved_exprs.append(resolved_expr)

                # Check if this is an entity return and expand to all properties
                # For "RETURN p", we need to include ALL properties of p, not just the ID
                if isinstance(expr, QueryExpressionProperty) and expr.property_name is None:
                    # This is a bare entity reference - check if it's marked as entity return
                    entity_var = expr.variable_name
                    entity_entry = self._symbol_table.lookup(entity_var)

                    if entity_entry and entity_entry.symbol_type == SymbolType.ENTITY:
                        # Query schema for all properties of this entity
                        if self._graph_schema and entity_entry.data_type_name:
                            node_def = self._graph_schema.get_node_definition(entity_entry.data_type_name)
                            if node_def and node_def.properties:
                                # Add ResolvedColumnRef for each property to mark them as "used"
                                for prop in node_def.properties:
                                    prop_name = prop.property_name
                                    sql_col_name = compute_sql_column_name(entity_var, prop_name)

                                    # Create a ResolvedColumnRef for this property
                                    prop_ref = ResolvedColumnRef(
                                        original_variable=entity_var,
                                        original_property=prop_name,
                                        ref_type=ColumnRefType.ENTITY_PROPERTY,
                                        source_operator_id=entity_entry.definition_operator_id,
                                        sql_column_name=sql_col_name,
                                        data_type_name=prop.data_type.__name__ if prop.data_type else None,
                                        derivation=f"from entity return {entity_var} (expanded from schema)",
                                        is_entity_return=False,  # This is a property, not the entity itself
                                    )

                                    # Add to resolved expression's column refs
                                    key = f"{entity_var}.{prop_name}"
                                    resolved_expr.column_refs[key] = prop_ref
                                    self._result.total_references_resolved += 1

                # Create ResolvedProjection
                is_entity_ref = (
                    isinstance(expr, QueryExpressionProperty)
                    and expr.property_name is None
                )
                entity_id_column = None
                if is_entity_ref and isinstance(expr, QueryExpressionProperty):
                    # Only set entity_id_column for NODES, not relationships.
                    # Relationships don't have an 'id' column - they have src/dst.
                    # Check if this entity is a node by looking up schema.
                    entity_var = expr.variable_name
                    entity_entry = self._symbol_table.lookup(entity_var)
                    node_def = None
                    if entity_entry and self._graph_schema and entity_entry.data_type_name:
                        node_def = self._graph_schema.get_node_definition(
                            entity_entry.data_type_name
                        )
                    if node_def is not None:
                        # Use the actual node ID property name from schema
                        # (e.g., "id" for YAML schemas, "node_id" for single-table schemas)
                        id_prop_name = None
                        if node_def.node_id_property:
                            id_prop_name = node_def.node_id_property.property_name
                        entity_id_column = compute_sql_column_name(
                            expr.variable_name, id_prop_name
                        )

                resolved_projections.append(ResolvedProjection(
                    alias=alias,
                    expression=resolved_expr,
                    sql_output_name=alias,  # Use user's alias for output
                    is_entity_ref=is_entity_ref,
                    entity_id_column=entity_id_column,
                ))

            self._result.resolved_projections[op.operator_debug_id] = resolved_projections

            # Also resolve filter and having expressions
            if op.filter_expression:
                resolved_exprs.append(self._resolve_expression(op.filter_expression))
            if op.having_expression:
                resolved_exprs.append(self._resolve_expression(op.having_expression))

            # Resolve ORDER BY expressions
            for order_expr, _ in op.order_by:
                resolved_exprs.append(self._resolve_expression(order_expr))

            # SCOPE BOUNDARY: If this projection contains aggregation AND has
            # downstream operators, it creates a scope boundary.
            # Clear scope AFTER resolving expressions (so COUNT(f) can see f),
            # but BEFORE downstream operators are processed.
            if self._projection_has_aggregation(op) and op.out_operators:
                self._apply_aggregation_scope_boundary(op)

        elif isinstance(op, DataSourceOperator):
            if op.filter_expression:
                resolved = self._resolve_expression(op.filter_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, AggregationBoundaryOperator):
            # SPECIAL CASE: Allow lookups from out-of-scope when resolving
            # aggregation expressions like COUNT(p) where p is pre-aggregation
            self._allow_out_of_scope_lookups = True
            try:
                for alias, expr in op.all_projections:
                    resolved_exprs.append(self._resolve_expression(expr))
                if op.having_filter:
                    resolved_exprs.append(self._resolve_expression(op.having_filter))
            finally:
                self._allow_out_of_scope_lookups = False

        elif isinstance(op, UnwindOperator):
            if op.list_expression:
                resolved = self._resolve_expression(op.list_expression)
                resolved_exprs.append(resolved)

        elif isinstance(op, RecursiveTraversalOperator):
            if op.edge_filter:
                # The edge_filter comes from PathExpressionAnalyzer extracting predicates
                # from ALL(r IN relationships(path) WHERE r.amount > 100).
                # The lambda variable (e.g., 'r') needs to be temporarily bound.
                lambda_var = op.edge_filter_lambda_var
                existing_entry = None

                if lambda_var:
                    # Save existing entry if any
                    existing_entry = self._symbol_table.lookup(lambda_var)

                    # Create temporary symbol for the lambda variable
                    # It represents an edge element in the path
                    temp_entry = SymbolEntry(
                        name=lambda_var,
                        symbol_type=SymbolType.VALUE,
                        definition_operator_id=op.operator_debug_id,
                        definition_location=f"ALL({lambda_var} IN relationships(path) WHERE ...)",
                        scope_level=self._symbol_table.current_level,
                        data_type_name="EDGE",  # Edge element type
                        # Include edge properties if available
                        properties=op.edge_properties or [],
                    )
                    self._symbol_table.define_or_update(lambda_var, temp_entry)

                resolved = self._resolve_expression(op.edge_filter)
                resolved_exprs.append(resolved)

                # Restore original entry
                if lambda_var:
                    if existing_entry:
                        self._symbol_table.define_or_update(lambda_var, existing_entry)
                    # Note: we leave the temp entry if there was no existing
                    # since the scope will be cleaned up naturally

        if resolved_exprs:
            self._result.resolved_expressions[op.operator_debug_id] = resolved_exprs
            self._result.total_expressions_resolved += len(resolved_exprs)

    def _resolve_expression(self, expr: QueryExpression) -> ResolvedExpression:
        """Resolve all column references in an expression.

        Args:
            expr: The expression to resolve

        Returns:
            ResolvedExpression with all references resolved

        Raises:
            ColumnResolutionError: If any reference cannot be resolved
        """
        resolved = ResolvedExpression(original_expression=expr)
        self._resolve_expression_recursive(expr, resolved)
        return resolved

    def _resolve_expression_recursive(
        self,
        expr: QueryExpression,
        resolved: ResolvedExpression,
    ) -> None:
        """Recursively resolve column references in an expression.

        Args:
            expr: The expression to resolve
            resolved: The ResolvedExpression to populate
        """
        if isinstance(expr, QueryExpressionProperty):
            ref = self._resolve_property_reference(expr)
            key = ref.original_text
            resolved.column_refs[key] = ref
            self._result.total_references_resolved += 1

        elif isinstance(expr, QueryExpressionBinary):
            if expr.left_expression:
                self._resolve_expression_recursive(expr.left_expression, resolved)
            if expr.right_expression:
                self._resolve_expression_recursive(expr.right_expression, resolved)

        elif isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters or []:
                self._resolve_expression_recursive(param, resolved)

            # Special handling for relationships(path) function:
            # The function returns path_edges column, not the path_id column.
            # We need to add an explicit reference to the _edges column for column pruning.
            if expr.function == Function.RELATIONSHIPS and expr.parameters:
                param = expr.parameters[0]
                if isinstance(param, QueryExpressionProperty) and param.property_name is None:
                    # This is a bare path variable reference like relationships(p)
                    path_var = param.variable_name
                    # Create a column reference for the _edges column
                    # The sql_column_name follows the pattern: _gsql2rsql_{path_var}_edges
                    edges_col = f"_gsql2rsql_{path_var}_edges"
                    edges_ref = ResolvedColumnRef(
                        ref_type=ColumnRefType.VALUE_ALIAS,  # Path edges are a value column
                        sql_column_name=edges_col,
                        original_variable=path_var,
                        original_property="_edges",  # Mark as edges property
                        derivation=f"relationships({path_var}) -> {edges_col}",
                    )
                    resolved.column_refs[f"relationships({path_var})"] = edges_ref
                    self._result.total_references_resolved += 1

        elif isinstance(expr, QueryExpressionAggregationFunction):
            if expr.inner_expression:
                self._resolve_expression_recursive(expr.inner_expression, resolved)
            # Resolve ORDER BY expressions for ordered aggregations (e.g., COLLECT(x ORDER BY y))
            for order_expr, _ in expr.order_by or []:
                self._resolve_expression_recursive(order_expr, resolved)

        elif isinstance(expr, QueryExpressionCaseExpression):
            if expr.test_expression:
                self._resolve_expression_recursive(expr.test_expression, resolved)
            for when_expr, then_expr in expr.alternatives or []:
                self._resolve_expression_recursive(when_expr, resolved)
                self._resolve_expression_recursive(then_expr, resolved)
            if expr.else_expression:
                self._resolve_expression_recursive(expr.else_expression, resolved)

        elif isinstance(expr, QueryExpressionList):
            for item in expr.items or []:
                self._resolve_expression_recursive(item, resolved)

        elif isinstance(expr, QueryExpressionListPredicate):
            # Resolve the list expression first (e.g., amounts)
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)

            # Add the loop variable as a temporary local symbol
            # e.g., in ALL(x IN amounts WHERE x > 1000), 'x' is a local binding
            loop_var = expr.variable_name
            temp_entry = SymbolEntry(
                name=loop_var,
                symbol_type=SymbolType.VALUE,
                definition_operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
                definition_location=f"{expr.predicate_type.name}({loop_var} IN ...)",
                scope_level=self._symbol_table.current_level,
                data_type_name="ANY",  # Element type of list
            )
            # Save existing entry if any (for nested predicates)
            existing_entry = self._symbol_table.lookup(loop_var)
            self._symbol_table.define_or_update(loop_var, temp_entry)

            # Now resolve filter expression with the loop variable in scope
            if expr.filter_expression:
                self._resolve_expression_recursive(expr.filter_expression, resolved)

            # Restore the original entry if it existed
            if existing_entry:
                self._symbol_table.define_or_update(loop_var, existing_entry)

        elif isinstance(expr, QueryExpressionListComprehension):
            # Resolve the list expression first (e.g., nodes(path))
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)

            # Add the loop variable as a temporary local symbol
            # e.g., in [n IN nodes(path) | n.id], 'n' is a local binding
            loop_var = expr.variable_name
            temp_entry = SymbolEntry(
                name=loop_var,
                symbol_type=SymbolType.VALUE,
                definition_operator_id=0,
                definition_location=f"[{loop_var} IN ...]",
                scope_level=self._symbol_table.current_level,
                data_type_name="ANY",  # Element type of list
            )
            # Save existing entry if any (for nested comprehensions)
            existing_entry = self._symbol_table.lookup(loop_var)
            self._symbol_table.define_or_update(loop_var, temp_entry)

            # Now resolve filter and map expressions with the loop variable in scope
            if expr.filter_expression:
                self._resolve_expression_recursive(expr.filter_expression, resolved)
            if expr.map_expression:
                self._resolve_expression_recursive(expr.map_expression, resolved)

            # Restore the original entry or remove the temporary one
            if existing_entry:
                self._symbol_table.define_or_update(loop_var, existing_entry)
            else:
                # Remove the temporary entry - not strictly necessary but cleaner
                # The symbol table doesn't have a remove method, so we leave it
                pass

        elif isinstance(expr, QueryExpressionReduce):
            # Resolve initial value and list expression first (use outer scope)
            if expr.initial_value:
                self._resolve_expression_recursive(expr.initial_value, resolved)
            if expr.list_expression:
                self._resolve_expression_recursive(expr.list_expression, resolved)

            # Add the accumulator and loop variable as temporary local symbols
            # e.g., in REDUCE(total = 0, x IN amounts | total + x), both 'total' and 'x' are local bindings
            accumulator_var = expr.accumulator_name
            loop_var = expr.variable_name

            # Define accumulator variable
            acc_entry = SymbolEntry(
                name=accumulator_var,
                symbol_type=SymbolType.VALUE,
                definition_operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
                definition_location=f"REDUCE({accumulator_var} = ...)",
                scope_level=self._symbol_table.current_level,
                data_type_name="ANY",  # Type from initial_value
            )
            # Define loop variable
            loop_entry = SymbolEntry(
                name=loop_var,
                symbol_type=SymbolType.VALUE,
                definition_operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
                definition_location=f"REDUCE(... {loop_var} IN ...)",
                scope_level=self._symbol_table.current_level,
                data_type_name="ANY",  # Element type of list
            )

            # Save existing entries if any
            existing_acc_entry = self._symbol_table.lookup(accumulator_var)
            existing_loop_entry = self._symbol_table.lookup(loop_var)
            self._symbol_table.define_or_update(accumulator_var, acc_entry)
            self._symbol_table.define_or_update(loop_var, loop_entry)

            # Now resolve reducer expression with both variables in scope
            if expr.reducer_expression:
                self._resolve_expression_recursive(expr.reducer_expression, resolved)

            # Restore the original entries if they existed
            if existing_acc_entry:
                self._symbol_table.define_or_update(accumulator_var, existing_acc_entry)
            if existing_loop_entry:
                self._symbol_table.define_or_update(loop_var, existing_loop_entry)

        elif isinstance(expr, QueryExpressionWithAlias):
            if expr.inner_expression:
                self._resolve_expression_recursive(expr.inner_expression, resolved)

        elif isinstance(expr, QueryExpressionMapLiteral):
            for key, value in expr.entries or []:
                self._resolve_expression_recursive(value, resolved)

        elif isinstance(expr, QueryExpressionExists):
            # EXISTS subqueries have their own scope
            self._resolve_exists_expression(expr, resolved)

        elif isinstance(expr, QueryExpressionValue):
            # Literal values don't need resolution
            pass

    def _resolve_property_reference(
        self, expr: QueryExpressionProperty
    ) -> ResolvedColumnRef:
        """Resolve a property reference (variable or variable.property).

        Args:
            expr: The property expression

        Returns:
            ResolvedColumnRef for this reference

        Raises:
            ColumnResolutionError: If the reference cannot be resolved
        """
        variable = expr.variable_name
        property_name = expr.property_name

        # Look up variable in symbol table
        entry = self._symbol_table.lookup(variable)

        # If not found and we're in an aggregation boundary context,
        # try looking in out-of-scope symbols (for expressions like COUNT(p))
        if entry is None and self._allow_out_of_scope_lookups:
            entry = self._symbol_table.lookup_out_of_scope(variable)

        if entry is None:
            self._raise_undefined_variable_error(expr)

        # Determine reference type and validate
        is_entity_return = False
        if property_name is None:
            # Bare entity reference (e.g., "p" in RETURN p)
            ref_type = ColumnRefType.ENTITY_ID

            # If this is a value symbol (not entity), use VALUE_ALIAS type
            if entry.symbol_type == SymbolType.VALUE:
                ref_type = ColumnRefType.VALUE_ALIAS
                sql_name = variable  # Use the alias directly
            elif entry.symbol_type == SymbolType.PATH:
                # PATH variables use their own column naming (e.g., _gsql2rsql_path_id)
                sql_name = compute_sql_column_name(variable, None)
            elif entry.is_vlp_relationship:
                # VLP relationship variable (e.g., 'e' in [e*1..3])
                # Maps to _gsql2rsql_{variable}_edges column from the recursive CTE
                # This matches what RecursiveTraversalOperator declares in its output schema
                sql_name = f"_gsql2rsql_{variable}_edges"
            else:
                # This is a bare entity reference - mark as potential entity return
                # (will be used in projection context to expand to all properties)
                is_entity_return = True

                # For nodes: use the actual node ID property from schema
                # For relationships: use _gsql2rsql_{var}_src since edges don't have an 'id' column
                # Check if this is a relationship by trying to get node definition
                node_def = None
                if self._graph_schema and entry.data_type_name:
                    node_def = self._graph_schema.get_node_definition(entry.data_type_name)

                if node_def is not None:
                    # Use the actual node ID property name from schema
                    # (e.g., "id" for YAML schemas, "node_id" for single-table schemas)
                    id_prop_name = None
                    if node_def.node_id_property:
                        id_prop_name = node_def.node_id_property.property_name
                    sql_name = compute_sql_column_name(variable, id_prop_name)
                elif (
                    entry.entity_info is not None
                    and entry.entity_info.entity_type == EntityType.NODE
                ):
                    # Schema lookup failed but entity_info confirms this is a NODE
                    # This can happen when nodes are projected through WITH clause after VLP
                    # Use default node_id column name (commonly "node_id" for GraphContext)
                    # See docs_help_dev/WITH_VLP_BUG_ANALYSIS.md for details
                    sql_name = compute_sql_column_name(variable, "node_id")
                else:
                    # Relationship: use the schema's real source column as the
                    # primary identifier (edges don't have an 'id' column).
                    # Hardcoding 'src' breaks custom schemas where the source
                    # column has a different name (e.g. source_node_id).
                    src_prop = "src"
                    if (
                        entry.entity_info is not None
                        and entry.entity_info.rel_source_join_field is not None
                    ):
                        src_prop = entry.entity_info.rel_source_join_field.field_name
                    sql_name = compute_sql_column_name(variable, src_prop)

        else:
            # Property access (e.g., "p.name" or "r.src")

            # Check if this is a struct field access (e.g., r.src after UNWIND e AS r)
            if entry.symbol_type == SymbolType.VALUE and entry.properties:
                # This is a VALUE symbol with known properties (unwound struct)
                if property_name in entry.properties:
                    # Valid struct field access - use variable.field syntax
                    ref_type = ColumnRefType.STRUCT_FIELD
                    sql_name = f"{variable}.{property_name}"
                else:
                    # Invalid property on struct
                    self._raise_invalid_property_error(expr, entry)
            else:
                # Entity property access (e.g., p.name)
                ref_type = ColumnRefType.ENTITY_PROPERTY

                # Validate property exists on entity
                if entry.symbol_type == SymbolType.ENTITY:
                    if entry.properties and property_name not in entry.properties:
                        # Property doesn't exist - but only warn if we have property info
                        # Some entities might not have full schema info
                        if entry.properties:
                            self._raise_invalid_property_error(expr, entry)

                sql_name = compute_sql_column_name(variable, property_name)

        return ResolvedColumnRef(
            original_variable=variable,
            original_property=property_name,
            ref_type=ref_type,
            source_operator_id=entry.definition_operator_id,
            sql_column_name=sql_name,
            data_type_name=entry.data_type_name,
            derivation=f"from {entry.definition_location}",
            is_entity_return=is_entity_return,
        )

    def _resolve_exists_expression(
        self, expr: QueryExpressionExists, resolved: ResolvedExpression
    ) -> None:
        """Resolve column references inside an EXISTS expression.

        EXISTS patterns have their own nested scope with entities defined
        by the pattern. We need to:
        1. Create a nested scope
        2. Define symbols for pattern entities
        3. Resolve the WHERE expression
        4. Exit the nested scope

        Args:
            expr: The EXISTS expression
            resolved: The resolved expression to add refs to
        """
        from gsql2rsql.parser.ast import NodeEntity, RelationshipEntity

        # Enter nested scope for EXISTS pattern
        self._symbol_table.enter_scope()

        try:
            # Define symbols for each entity in the pattern
            for entity in expr.pattern_entities:
                if isinstance(entity, NodeEntity):
                    # Define node symbol
                    alias = entity.alias or "_anon"
                    label = entity.entity_name or "Node"

                    # Get properties from schema if available
                    properties: list[str] = []
                    if hasattr(self, '_graph_schema') and self._graph_schema:
                        node_def = self._graph_schema.get_node_definition(label)
                        if node_def and node_def.properties:
                            properties = [p.property_name for p in node_def.properties]

                    entry = SymbolEntry(
                        name=alias,
                        symbol_type=SymbolType.ENTITY,
                        definition_operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
                        definition_location="EXISTS pattern",
                        scope_level=self._symbol_table.current_level,
                        data_type_name=label,
                        properties=properties,
                    )
                    self._symbol_table.define_or_update(alias, entry)

                elif isinstance(entity, RelationshipEntity):
                    # Define relationship symbol if it has an alias
                    if entity.alias:
                        entry = SymbolEntry(
                            name=entity.alias,
                            symbol_type=SymbolType.ENTITY,
                            definition_operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
                            definition_location="EXISTS pattern",
                            scope_level=self._symbol_table.current_level,
                            data_type_name=entity.entity_name or "Relationship",
                            properties=[],
                        )
                        self._symbol_table.define_or_update(entity.alias, entry)

            # Resolve the WHERE expression inside EXISTS
            if expr.where_expression:
                self._resolve_expression_recursive(expr.where_expression, resolved)

        finally:
            # Exit nested scope
            self._symbol_table.exit_scope()

    def _raise_undefined_variable_error(
        self, expr: QueryExpressionProperty
    ) -> NoReturn:
        """Raise a detailed error for an undefined variable.

        Args:
            expr: The expression with the undefined variable

        Raises:
            ColumnResolutionError: Always raises with full context
        """
        variable = expr.variable_name

        # Compute suggestions
        suggestions = self._compute_suggestions(variable)

        # Get out-of-scope symbols that might be relevant
        out_of_scope = self._symbol_table.get_out_of_scope_symbols()

        # Build hints
        hints = self._build_hints_for_undefined_variable(variable, out_of_scope)

        # Find approximate position of error in query
        error_offset, error_length = self._find_token_position(variable, expr.property_name)

        context = ColumnResolutionErrorContext(
            error_type="UndefinedVariable",
            message=f"Variable '{variable}' is not defined",
            query_text=self._original_query,
            error_offset=error_offset,
            error_length=error_length,
            available_symbols=self._symbol_table.get_available_symbols(),
            out_of_scope_symbols=out_of_scope,
            suggestions=suggestions,
            hints=hints,
            operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
            operator_type=type(self._current_operator).__name__ if self._current_operator else "",
            resolution_phase=self._current_phase,
            symbol_table_dump=self._symbol_table.dump(),
        )

        raise ColumnResolutionError(
            f"Variable '{variable}' is not defined",
            context=context,
        )

    def _raise_invalid_property_error(
        self, expr: QueryExpressionProperty, entry: SymbolEntry
    ) -> NoReturn:
        """Raise a detailed error for an invalid property access.

        Args:
            expr: The expression with invalid property
            entry: The symbol entry for the entity

        Raises:
            ColumnResolutionError: Always raises with full context
        """
        variable = expr.variable_name
        property_name = expr.property_name

        # Compute property suggestions
        suggestions: list[str] = []
        if entry.properties and property_name is not None:
            for prop in entry.properties:
                dist = levenshtein_distance(property_name, prop)
                if dist <= 3:
                    suggestions.append(
                        f"Did you mean '{variable}.{prop}'? "
                        f"({dist} character{'s' if dist > 1 else ''} difference)"
                    )

        # Build hints
        hints = [
            f"Entity '{variable}' is of type '{entry.data_type_name}'.\n"
            f"Available properties: {', '.join(entry.properties)}"
        ]

        # Find approximate position of error in query
        error_offset, error_length = self._find_token_position(variable, property_name)

        context = ColumnResolutionErrorContext(
            error_type="InvalidPropertyAccess",
            message=f"Entity '{variable}' has no property '{property_name}'",
            query_text=self._original_query,
            error_offset=error_offset,
            error_length=error_length,
            available_symbols=self._symbol_table.get_available_symbols(),
            suggestions=suggestions,
            hints=hints,
            operator_id=self._current_operator.operator_debug_id if self._current_operator else 0,
            operator_type=type(self._current_operator).__name__ if self._current_operator else "",
            resolution_phase=self._current_phase,
            symbol_table_dump=self._symbol_table.dump(),
        )

        raise ColumnResolutionError(
            f"Entity '{variable}' has no property '{property_name}'",
            context=context,
        )

    def _find_token_position(
        self, variable: str, property_name: str | None = None
    ) -> tuple[int, int]:
        """Find the approximate position of a variable or property in the query.

        This is a best-effort heuristic since AST nodes don't track positions.
        It searches for the variable (or variable.property) pattern in the query text.

        LIMITATIONS:
        - If the same variable appears multiple times, this will find the first occurrence
        - Doesn't account for variable occurrences in comments or strings
        - Position may not be exact, but is better than showing nothing

        TODO: Add position tracking to AST nodes during parsing for exact positions.

        Args:
            variable: The variable name to find
            property_name: Optional property name (for property access errors)

        Returns:
            Tuple of (error_offset, error_length) where:
            - error_offset: Character offset in query text (0 if not found)
            - error_length: Length of the token (variable or variable.property)
        """
        if not self._original_query:
            return (0, len(variable))

        # Build the search pattern
        if property_name:
            # Look for "variable.property"
            pattern = rf'\b{re.escape(variable)}\.{re.escape(property_name)}\b'
            token_length = len(variable) + 1 + len(property_name)  # variable + . + property
        else:
            # Look for bare variable (ensure it's a word boundary)
            pattern = rf'\b{re.escape(variable)}\b'
            token_length = len(variable)

        # Search for the pattern
        match = re.search(pattern, self._original_query)
        if match:
            return (match.start(), token_length)

        # Fallback: if property search failed, try just the variable
        if property_name:
            pattern = rf'\b{re.escape(variable)}\b'
            match = re.search(pattern, self._original_query)
            if match:
                return (match.start(), len(variable))

        # Not found - return start of query
        return (0, token_length)

    def _compute_suggestions(self, target: str) -> list[str]:
        """Compute 'did you mean' suggestions for a variable name.

        Args:
            target: The variable name that wasn't found

        Returns:
            List of suggestion strings
        """
        suggestions = []
        all_names = self._symbol_table.all_names()

        # Score by edit distance
        scored = [
            (name, levenshtein_distance(target, name))
            for name in all_names
        ]
        scored.sort(key=lambda x: x[1])

        for name, dist in scored[:3]:
            if dist <= 3:
                suggestions.append(
                    f"Did you mean '{name}'? "
                    f"({dist} character{'s' if dist > 1 else ''} difference)"
                )

        # Also suggest out-of-scope variables
        for sym_info, reason in self._symbol_table.get_out_of_scope_symbols():
            if sym_info.name == target:
                suggestions.append(
                    f"Variable '{target}' exists but is out of scope: {reason}"
                )

        return suggestions

    def _build_hints_for_undefined_variable(
        self,
        variable: str,
        out_of_scope: list[tuple[SymbolInfo, str]],
    ) -> list[str]:
        """Build contextual hints for an undefined variable error.

        Args:
            variable: The undefined variable name
            out_of_scope: List of out-of-scope symbols with reasons

        Returns:
            List of hint strings
        """
        hints = []

        # Check if variable is out of scope due to aggregation
        for sym_info, reason in out_of_scope:
            if sym_info.name == variable and "aggregation" in reason.lower():
                hints.append(
                    "After a WITH clause containing aggregation (like COUNT, SUM, etc.),\n"
                    "only variables explicitly listed in the WITH clause are accessible.\n\n"
                    f"To keep '{variable}' available, project it:\n"
                    f"  WITH {variable}, COUNT(...) AS count_result\n\n"
                    f"Or, if you only need '{variable}' for aggregation, it's working correctly -\n"
                    f"'{variable}' is aggregated and no longer available as an entity."
                )
                break

        if not hints:
            hints.append(
                f"Make sure '{variable}' is defined in a MATCH clause before use.\n"
                "Variables must be defined before they can be referenced in WHERE, "
                "WITH, or RETURN clauses."
            )

        return hints

    def _projection_has_aggregation(self, op: ProjectionOperator) -> bool:
        """Check if a projection operator contains any aggregation.

        Args:
            op: The ProjectionOperator to check

        Returns:
            True if any projection expression contains aggregation
        """
        for _, expr in op.projections:
            if self._expression_contains_aggregation(expr):
                return True
        return False

    def _apply_aggregation_scope_boundary(self, op: ProjectionOperator) -> None:
        """Apply scope boundary after an aggregating projection.

        This clears the current scope and defines only the projected symbols.
        Called AFTER expressions in the projection are resolved, so that
        COUNT(f) can see f, but downstream operators cannot.

        IMPORTANT: Only clears symbols from UPSTREAM operators (with lower IDs)
        to avoid clearing symbols from downstream operators that were defined
        in Phase 1.

        Args:
            op: The aggregating ProjectionOperator
        """
        # Clear scope - but only for symbols from upstream operators
        # This prevents clearing symbols from downstream operators (like 'city' from op 7)
        # when resolving an upstream aggregation (like op 6)
        self._symbol_table.clear_scope_for_aggregation(
            f"WITH aggregation at ProjectionOperator {op.operator_debug_id}",
            max_operator_id=op.operator_debug_id,
        )

        # Define only the projected symbols
        for alias, expr in op.projections:
            is_entity = False
            data_type_name = None
            properties: list[str] = []

            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    # Bare entity reference - look up in out-of-scope symbols
                    for sym_info, _ in self._symbol_table.get_out_of_scope_symbols():
                        if sym_info.name == expr.variable_name:
                            if sym_info.symbol_type == "entity":
                                is_entity = True
                                data_type_name = sym_info.data_type
                                properties = sym_info.properties or []
                            break

            is_aggregated = self._expression_contains_aggregation(expr)
            if is_aggregated:
                data_type_name = self._infer_aggregation_type(expr)

            entry = SymbolEntry(
                name=alias,
                symbol_type=SymbolType.ENTITY if is_entity else SymbolType.VALUE,
                definition_operator_id=op.operator_debug_id,
                definition_location=f"WITH AS {alias}",
                scope_level=self._symbol_table.current_level,
                data_type_name=data_type_name,
                properties=properties,
                is_aggregated=is_aggregated,
            )
            self._symbol_table.define_or_update(alias, entry)

    def _expression_contains_aggregation(self, expr: QueryExpression) -> bool:
        """Check if an expression contains aggregation functions.

        Args:
            expr: The expression to check

        Returns:
            True if the expression contains aggregation
        """
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True

        if isinstance(expr, QueryExpressionBinary):
            left_has = self._expression_contains_aggregation(expr.left_expression) if expr.left_expression else False
            right_has = self._expression_contains_aggregation(expr.right_expression) if expr.right_expression else False
            return left_has or right_has

        if isinstance(expr, QueryExpressionFunction):
            for param in expr.parameters or []:
                if self._expression_contains_aggregation(param):
                    return True

        return False

    def _infer_aggregation_type(self, expr: QueryExpression) -> str:
        """Infer the result type of an aggregation expression.

        Args:
            expr: The aggregation expression

        Returns:
            Type name string (e.g., "INTEGER", "DOUBLE", "ARRAY")
        """
        if isinstance(expr, QueryExpressionAggregationFunction):
            agg_name = expr.aggregation_function.name if expr.aggregation_function else ""
            if agg_name in ("COUNT",):
                return "INTEGER"
            if agg_name in ("SUM", "AVG"):
                return "DOUBLE"
            if agg_name in ("COLLECT",):
                return "ARRAY"
            if agg_name in ("MIN", "MAX"):
                return "ANY"  # Depends on input type
            return "ANY"

        return "ANY"


def resolve_plan(
    plan: LogicalPlan,
    original_query: str = "",
) -> ResolutionResult:
    """Convenience function to resolve a logical plan.

    Args:
        plan: The logical plan to resolve
        original_query: The original Cypher query text

    Returns:
        ResolutionResult containing all resolved references
    """
    resolver = ColumnResolver()
    return resolver.resolve(plan, original_query)
