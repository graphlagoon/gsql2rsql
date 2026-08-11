"""Aggregation boundary handling for WITH clauses with aggregation.

This module handles the creation of AggregationBoundaryOperator for
queries where MATCH clauses follow a WITH clause containing aggregation:

    MATCH (a)-[:R1]->(b)
    WITH a, COUNT(b) AS cnt    -- Creates aggregation boundary
    MATCH (a)-[:R2]->(c)       -- Joins with aggregated result
    RETURN a, cnt, COUNT(c)

The aggregation boundary creates a materialization point (CTE) that:
1. Aggregates the input according to group keys
2. Makes only projected columns visible downstream
3. Allows subsequent MATCHes to join with the aggregated result
"""

from __future__ import annotations

from collections.abc import Callable

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.parser.ast import (
    MatchClause,
    NodeEntity,
    PartialQueryNode,
    QueryExpression,
    QueryExpressionAggregationFunction,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionProperty,
    QueryExpressionValue,
    SingleQueryNode,
)
from gsql2rsql.planner.operators import (
    AggregationBoundaryOperator,
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    ProjectionOperator,
    SelectionOperator,
    UnwindOperator,
)


def _extract_int_value(expr: QueryExpression | None) -> int | None:
    """Extract the integer from a literal SKIP/LIMIT expression.

    Anything other than a literal (a ``$param``, an arithmetic expression)
    must be rejected loudly: returning ``None`` here silently drops the
    clause, so ``LIMIT $n`` used to return the entire result set.
    """
    if expr is None:
        return None
    if isinstance(expr, QueryExpressionValue) and expr.value is not None:
        return int(expr.value)
    raise TranspilerNotSupportedException(
        f"SKIP/LIMIT requires an integer literal; got '{expr}'. "
        f"Parameters (e.g. LIMIT $n) are not supported — inline the value "
        f"before transpiling."
    )


def part_creates_aggregation_boundary(
    query_node: SingleQueryNode,
    part_idx: int,
) -> bool:
    """Check if a partial query creates an aggregation boundary.

    A boundary is created when:
    1. The part has aggregation in its return_body (WITH clause)
    2. There is a subsequent part with MATCH clauses

    Args:
        query_node: The full query
        part_idx: Index of the part to check

    Returns:
        True if this part should create an aggregation boundary
    """
    part = query_node.parts[part_idx]

    # Must have aggregation
    if not has_aggregation_in_projections(part):
        return False

    # Must have a subsequent part with MATCH
    for subsequent_part in query_node.parts[part_idx + 1:]:
        if subsequent_part.match_clauses:
            return True

    return False


def create_aggregation_boundary(
    part: PartialQueryNode,
    input_op: LogicalOperator,
    all_ops: list[LogicalOperator],
) -> AggregationBoundaryOperator:
    """Create an AggregationBoundaryOperator from a WITH clause.

    This extracts group keys and aggregates from the WITH clause's
    return_body and creates a boundary operator that can be rendered
    as a CTE.

    Args:
        part: The partial query containing the aggregating WITH
        input_op: The upstream operator (match tree)
        all_ops: List to collect created operators

    Returns:
        The created AggregationBoundaryOperator
    """
    group_keys: list[tuple[str, QueryExpression]] = []
    aggregates: list[tuple[str, QueryExpression]] = []
    projected_variables: set[str] = set()

    # Separate group keys from aggregates
    for ret in part.return_body:
        alias = ret.alias
        expr = ret.inner_expression

        if has_aggregation_in_expression(expr):
            aggregates.append((alias, expr))
        else:
            group_keys.append((alias, expr))

            # Track projected entity variables for later joins
            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name is None:
                    projected_variables.add(expr.variable_name)

    # Generate a unique CTE name
    cte_name = f"agg_boundary_{len([op for op in all_ops if isinstance(op, AggregationBoundaryOperator)]) + 1}"

    boundary_op = AggregationBoundaryOperator(
        group_keys=group_keys,
        aggregates=aggregates,
        having_filter=part.having_expression,
        order_by=[
            (item.expression, item.order.name == "DESC")
            for item in part.order_by
        ],
        limit=(
            _extract_int_value(part.limit_clause.limit_expression)
            if part.limit_clause else None
        ),
        skip=(
            _extract_int_value(part.limit_clause.skip_expression)
            if part.limit_clause and part.limit_clause.skip_expression else None
        ),
        cte_name=cte_name,
        projected_variables=projected_variables,
    )
    boundary_op.set_in_operator(input_op)
    all_ops.append(boundary_op)

    return boundary_op


def create_match_after_boundary_tree(
    part: PartialQueryNode,
    boundary: AggregationBoundaryOperator,
    all_ops: list[LogicalOperator],
    create_match_tree_fn: Callable[
        [MatchClause, list[LogicalOperator], list[QueryExpression] | None],
        LogicalOperator
    ],
) -> LogicalOperator:
    """Create logical tree for MATCH clauses after an aggregation boundary.

    This function:
    1. Builds the match tree for the new MATCH clauses
    2. Joins the match tree with the aggregation boundary
    3. Processes any WHERE/RETURN clauses

    Args:
        part: The partial query containing the MATCH after aggregation
        boundary: The aggregation boundary operator to join with
        all_ops: List to collect created operators
        create_match_tree_fn: Function to create match trees (from LogicalPlan)

    Returns:
        The final operator for this partial query
    """
    current_op: LogicalOperator = boundary
    return_exprs = [
        ret.inner_expression for ret in part.return_body
    ] if part.return_body else []

    # Track node aliases for correlation detection
    seen_node_aliases: set[str] = set()
    seen_node_aliases.update(boundary.projected_variables)

    for match_clause in part.match_clauses:
        match_op = create_match_tree_fn(match_clause, all_ops, return_exprs)

        # Join the match tree with the current operator
        join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
        join_op = JoinOperator(join_type=join_type)
        join_op.set_in_operators(current_op, match_op)

        # Add join pairs for shared variables with the boundary
        for entity in match_clause.pattern_parts:
            if isinstance(entity, NodeEntity) and entity.alias in boundary.projected_variables:
                join_op.add_join_pair(JoinKeyPair(
                    node_alias=entity.alias,
                    relationship_or_node_alias=boundary.cte_name,
                    pair_type=JoinKeyPairType.NODE_ID,
                ))
            elif isinstance(entity, NodeEntity) and entity.alias in seen_node_aliases:
                join_op.add_join_pair(JoinKeyPair(
                    node_alias=entity.alias,
                    relationship_or_node_alias=entity.alias,
                    pair_type=JoinKeyPairType.NODE_ID,
                ))

        all_ops.append(join_op)
        current_op = join_op

        # Track aliases from this match
        for entity in match_clause.pattern_parts:
            if isinstance(entity, NodeEntity) and entity.alias:
                seen_node_aliases.add(entity.alias)

        # Process WHERE from match clause
        if match_clause.where_expression:
            select_op = SelectionOperator(
                filter_expression=match_clause.where_expression
            )
            select_op.set_in_operator(current_op)
            all_ops.append(select_op)
            current_op = select_op

    # Process UNWIND clauses
    for unwind_clause in part.unwind_clauses:
        unwind_op = UnwindOperator(
            list_expression=unwind_clause.list_expression,
            variable_name=unwind_clause.variable_name,
        )
        unwind_op.set_in_operator(current_op)
        all_ops.append(unwind_op)
        current_op = unwind_op

    # Process WHERE from PartialQueryNode
    if part.where_expression:
        select_op = SelectionOperator(filter_expression=part.where_expression)
        select_op.set_in_operator(current_op)
        all_ops.append(select_op)
        current_op = select_op

    # Process RETURN clause
    if part.return_body:
        proj_op = ProjectionOperator(
            projections=[
                (ret.alias, ret.inner_expression) for ret in part.return_body
            ],
            is_distinct=part.is_distinct,
            order_by=[
                (item.expression, item.order.name == "DESC")
                for item in part.order_by
            ],
            limit=(
                _extract_int_value(part.limit_clause.limit_expression)
                if part.limit_clause else None
            ),
            skip=(
                _extract_int_value(part.limit_clause.skip_expression)
                if part.limit_clause and part.limit_clause.skip_expression else None
            ),
            having_expression=part.having_expression,
        )
        proj_op.set_in_operator(current_op)
        all_ops.append(proj_op)
        current_op = proj_op

    return current_op


def create_match_tree_for_boundary(
    part: PartialQueryNode,
    all_ops: list[LogicalOperator],
    previous_op: LogicalOperator | None,
    create_match_tree_fn: Callable[
        [MatchClause, list[LogicalOperator], list[QueryExpression] | None],
        LogicalOperator
    ],
) -> LogicalOperator:
    """Create the match tree for a part that creates an aggregation boundary.

    This is similar to create_partial_query_tree but stops before creating
    the projection - the projection will be handled by the boundary operator.

    Args:
        part: The partial query containing MATCH clauses
        all_ops: List to collect created operators
        previous_op: The upstream operator (if any)
        create_match_tree_fn: Function to create match trees

    Returns:
        The operator representing the match tree (before aggregation)
    """
    from gsql2rsql.common.exceptions import TranspilerInternalErrorException

    current_op = previous_op

    # Collect return expressions for path analysis optimization
    return_exprs = [
        ret.inner_expression for ret in part.return_body
    ] if part.return_body else []

    # Track node aliases seen across all match clauses for correlation
    seen_node_aliases: set[str] = set()

    # Process MATCH clauses
    for match_clause in part.match_clauses:
        match_op = create_match_tree_fn(match_clause, all_ops, return_exprs)

        if current_op is not None:
            # Join with previous result
            join_type = (
                JoinType.LEFT if match_clause.is_optional else JoinType.INNER
            )
            join_op = JoinOperator(join_type=join_type)
            join_op.set_in_operators(current_op, match_op)

            # Add join pairs for shared node variables (correlation)
            for entity in match_clause.pattern_parts:
                if (isinstance(entity, NodeEntity) and
                        entity.alias in seen_node_aliases):
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=entity.alias,
                        relationship_or_node_alias=entity.alias,
                        pair_type=JoinKeyPairType.NODE_ID,
                    ))

            all_ops.append(join_op)
            current_op = join_op
        else:
            current_op = match_op

        # Track all node aliases from this match clause
        for entity in match_clause.pattern_parts:
            if isinstance(entity, NodeEntity) and entity.alias:
                seen_node_aliases.add(entity.alias)

        # Process WHERE clause from MatchClause
        if match_clause.where_expression and current_op:
            select_op = SelectionOperator(
                filter_expression=match_clause.where_expression
            )
            select_op.set_in_operator(current_op)
            all_ops.append(select_op)
            current_op = select_op

    # Process UNWIND clauses
    for unwind_clause in part.unwind_clauses:
        if current_op:
            unwind_op = UnwindOperator(
                list_expression=unwind_clause.list_expression,
                variable_name=unwind_clause.variable_name,
            )
            unwind_op.set_in_operator(current_op)
            all_ops.append(unwind_op)
            current_op = unwind_op

    # Process WHERE clause from PartialQueryNode (for compatibility)
    if part.where_expression and current_op:
        select_op = SelectionOperator(filter_expression=part.where_expression)
        select_op.set_in_operator(current_op)
        all_ops.append(select_op)
        current_op = select_op

    # NOTE: We do NOT create a ProjectionOperator here
    # The aggregation boundary will handle the projection

    if current_op is None:
        raise TranspilerInternalErrorException("Empty match tree for boundary")

    return current_op


def has_aggregation_in_expression(expr: QueryExpression) -> bool:
    """Check if an expression contains an aggregation function.

    Args:
        expr: The expression to check for aggregation functions.

    Returns:
        True if the expression contains COUNT, SUM, AVG, etc.
    """
    if isinstance(expr, QueryExpressionAggregationFunction):
        return True
    if isinstance(expr, QueryExpressionBinary):
        left_has = (
            has_aggregation_in_expression(expr.left_expression)
            if expr.left_expression
            else False
        )
        right_has = (
            has_aggregation_in_expression(expr.right_expression)
            if expr.right_expression
            else False
        )
        return left_has or right_has
    if isinstance(expr, QueryExpressionFunction):
        return any(
            has_aggregation_in_expression(p) for p in expr.parameters
        )
    return False


def has_aggregation_in_projections(part: PartialQueryNode) -> bool:
    """Check if a partial query's return body contains aggregations.

    Args:
        part: The partial query node to check.

    Returns:
        True if any projection in return_body contains an aggregation.
    """
    if not part.return_body:
        return False
    return any(
        has_aggregation_in_expression(ret.inner_expression)
        for ret in part.return_body
    )


def detect_match_after_aggregating_with(
    query_node: SingleQueryNode,
) -> tuple[bool, int]:
    """Detect if query has MATCH clauses after a WITH that aggregates.

    This pattern requires special handling because:
    1. The aggregating WITH creates a materialization boundary
    2. Variables not projected are no longer visible
    3. Subsequent MATCHes must join with the aggregated result

    Args:
        query_node: The single query node to analyze.

    Returns:
        A tuple of (detected, boundary_index) where:
        - detected: True if the pattern exists
        - boundary_index: Index of the first aggregating WITH (-1 if not detected)
    """
    has_aggregating_with = False
    boundary_index = -1

    for i, part in enumerate(query_node.parts):
        if has_aggregating_with and part.match_clauses:
            return True, boundary_index

        if has_aggregation_in_projections(part):
            has_aggregating_with = True
            boundary_index = i

    return False, -1
