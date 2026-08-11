"""Standard match tree building for fixed-length MATCH patterns.

This module handles the creation of logical operator trees for
standard (non-VLP) MATCH patterns like:
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    MATCH (a)-[:R1]->(b), (a)-[:R2]->(c)

It includes:
- Creating DataSourceOperators for nodes and relationships
- Building JoinOperators to connect entities
- Converting inline property filters to WHERE predicates
- Processing partial queries (MATCH + WHERE + RETURN)
"""

from __future__ import annotations

from collections.abc import Callable

from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
)
from gsql2rsql.parser.ast import (
    Entity,
    MatchClause,
    NodeEntity,
    PartialQueryNode,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionMapLiteral,
    QueryExpressionProperty,
    QueryExpressionValue,
    RelationshipDirection,
    RelationshipEntity,
    TreeNode,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
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


def _contains_is_terminator(expr: TreeNode) -> bool:
    """Check if an expression tree contains an is_terminator() call."""
    if (
        isinstance(expr, QueryExpressionFunction)
        and expr.function == Function.IS_TERMINATOR
    ):
        return True
    if hasattr(expr, "children"):
        return any(_contains_is_terminator(c) for c in expr.children)
    return False


def _extract_filters_by_alias(
    expr: QueryExpression | None,
) -> dict[str, list[QueryExpression]]:
    """Extract filters from an expression, grouped by the node alias they reference.

    For filter pushdown optimization, we need to track which filters apply to which
    node aliases so we can push them into VLP base cases in subsequent MATCH clauses.

    Args:
        expr: A WHERE expression (possibly compound with AND)

    Returns:
        Dict mapping node alias to list of filter expressions that reference ONLY
        that alias. Filters referencing multiple aliases are not included.

    Example:
        WHERE root.id = "123" AND root.age > 30 AND target.name = "Bob"
        Returns:
            {
                "root": [root.id = "123", root.age > 30],
                "target": [target.name = "Bob"]
            }
    """
    if expr is None:
        return {}

    result: dict[str, list[QueryExpression]] = {}

    def collect_single_alias_filters(e: QueryExpression) -> None:
        """Recursively collect filters that reference a single alias."""
        if isinstance(e, QueryExpressionBinary):
            # If this is an AND, recurse into both sides
            if e.operator and e.operator.name.name == "AND":
                if e.left_expression:
                    collect_single_alias_filters(e.left_expression)
                if e.right_expression:
                    collect_single_alias_filters(e.right_expression)
            else:
                # This is a comparison or other binary expression
                # Check if it references a single alias
                alias = _get_single_alias_from_expression(e)
                if alias:
                    if alias not in result:
                        result[alias] = []
                    result[alias].append(e)
        elif isinstance(e, QueryExpressionProperty):
            # A standalone property reference (e.g., in EXISTS)
            # Not a filter by itself, skip
            pass

    collect_single_alias_filters(expr)
    return result


def _get_single_alias_from_expression(expr: QueryExpression) -> str | None:
    """Get the single alias referenced by an expression, or None if multiple.

    Returns the alias if ALL property references in the expression use the same
    variable name. Returns None if:
    - No property references exist
    - Multiple different aliases are referenced

    Examples:
        root.id = "123"          -> "root"
        root.age > 30            -> "root"
        root.id = target.id      -> None (multiple aliases)
        1 = 1                    -> None (no property refs)
    """
    properties = _collect_property_refs(expr)

    if not properties:
        return None

    aliases = {prop.variable_name for prop in properties}

    if len(aliases) == 1:
        return next(iter(aliases))

    return None


def _collect_property_refs(expr: QueryExpression) -> list[QueryExpressionProperty]:
    """Collect all property references from an expression tree."""
    properties: list[QueryExpressionProperty] = []

    if isinstance(expr, QueryExpressionProperty):
        properties.append(expr)
    elif isinstance(expr, QueryExpressionBinary):
        if expr.left_expression is not None:
            properties.extend(_collect_property_refs(expr.left_expression))
        if expr.right_expression is not None:
            properties.extend(_collect_property_refs(expr.right_expression))
    elif hasattr(expr, "children"):
        for child in expr.children:
            if isinstance(child, QueryExpression):
                properties.extend(_collect_property_refs(child))

    return properties


def _has_variable_length_path(match_clause: MatchClause) -> bool:
    """Check if a MATCH clause contains a variable-length path."""
    for entity in match_clause.pattern_parts:
        if isinstance(entity, RelationshipEntity) and entity.is_variable_length:
            return True
    return False


def _get_vlp_source_alias(match_clause: MatchClause) -> str | None:
    """Get the source node alias for a VLP in the match clause.

    Returns None if no VLP exists or if the source node has no alias.
    """
    for i, entity in enumerate(match_clause.pattern_parts):
        if isinstance(entity, RelationshipEntity) and entity.is_variable_length:
            if i > 0:
                prev = match_clause.pattern_parts[i - 1]
                if isinstance(prev, NodeEntity) and prev.alias:
                    return prev.alias
    return None


def _inject_filters_into_where(
    match_clause: MatchClause,
    filters: list[QueryExpression],
) -> None:
    """Inject filters into a match clause's where_expression.

    Combines filters with AND. If where_expression is None, sets it directly.
    If where_expression exists, prepends filters with AND.
    """
    if not filters:
        return

    and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)

    # Combine all filters with AND
    combined: QueryExpression = filters[0]
    for f in filters[1:]:
        combined = QueryExpressionBinary(
            left_expression=combined,
            right_expression=f,
            operator=and_op,
        )

    # Merge with existing where_expression
    if match_clause.where_expression is None:
        match_clause.where_expression = combined
    else:
        match_clause.where_expression = QueryExpressionBinary(
            left_expression=combined,
            right_expression=match_clause.where_expression,
            operator=and_op,
        )


def create_standard_match_tree(
    match_clause: MatchClause,
    all_ops: list[LogicalOperator],
) -> LogicalOperator:
    """Create standard logical tree for fixed-length MATCH clause.

    Note: Auto-alias assignment and inline property conversion should
    be done before calling this function.

    Args:
        match_clause: The MATCH clause to process
        all_ops: List to collect created operators

    Returns:
        The root operator of the match tree
    """
    # Create data source operators for each entity
    entity_ops: dict[str, DataSourceOperator] = {}
    prev_node: NodeEntity | None = None
    seen_node_aliases: set[str] = set()

    for entity in match_clause.pattern_parts:
        # Check if this entity alias was already seen (shared variable)
        if entity.alias in entity_ops:
            if isinstance(entity, NodeEntity):
                prev_node = entity
            continue

        ds_op = DataSourceOperator(entity=entity)
        entity_ops[entity.alias] = ds_op
        all_ops.append(ds_op)

        # Track nodes for relationship connections
        if isinstance(entity, NodeEntity):
            prev_node = entity
            seen_node_aliases.add(entity.alias)
        elif isinstance(entity, RelationshipEntity) and prev_node:
            entity.left_entity_name = prev_node.entity_name

    # Update right entity names for relationships
    prev_entity: Entity | None = None
    for entity in match_clause.pattern_parts:
        if isinstance(entity, NodeEntity) and prev_entity:
            if isinstance(prev_entity, RelationshipEntity):
                prev_entity.right_entity_name = entity.entity_name
        prev_entity = entity

    # Join all entities together
    current_op: LogicalOperator | None = None
    prev_node_alias: str | None = None
    joined_node_aliases: set[str] = set()

    for entity_idx, entity in enumerate(match_clause.pattern_parts):
        # Handle shared node variables
        if isinstance(entity, NodeEntity) and entity.alias in joined_node_aliases:
            # No new data source: the node is already materialised. But the
            # repetition still constrains the pattern — the preceding
            # relationship's far endpoint must equal this node (self-loops
            # `(a)-[:R]->(a)`, closed cycles `(a)->(b)->(a)`). Attach the
            # SINK pair to the join that brought that relationship in;
            # skipping it returns every path instead of every cycle.
            if entity_idx > 0 and isinstance(current_op, JoinOperator):
                prev_ent = match_clause.pattern_parts[entity_idx - 1]
                if isinstance(prev_ent, RelationshipEntity):
                    current_op.add_join_pair(
                        JoinKeyPair(
                            node_alias=entity.alias,
                            relationship_or_node_alias=prev_ent.alias,
                            pair_type=determine_sink_join_type(prev_ent),
                        )
                    )
            prev_node_alias = entity.alias
            continue

        ds_op = entity_ops[entity.alias]

        if current_op is None:
            current_op = ds_op
            if isinstance(entity, NodeEntity):
                prev_node_alias = entity.alias
                joined_node_aliases.add(entity.alias)
        else:
            join_op = JoinOperator(join_type=JoinType.INNER)

            if isinstance(entity, RelationshipEntity):
                if prev_node_alias:
                    pair_type = determine_join_pair_type(entity)
                    join_op.add_join_pair(JoinKeyPair(
                        node_alias=prev_node_alias,
                        relationship_or_node_alias=entity.alias,
                        pair_type=pair_type,
                    ))
            elif isinstance(entity, NodeEntity):
                # Join node to its immediately preceding relationship
                if entity_idx > 0:
                    prev_ent = match_clause.pattern_parts[entity_idx - 1]
                    if isinstance(prev_ent, RelationshipEntity):
                        pair_type = determine_sink_join_type(prev_ent)
                        join_op.add_join_pair(
                            JoinKeyPair(
                                node_alias=entity.alias,
                                relationship_or_node_alias=prev_ent.alias,
                                pair_type=pair_type,
                            )
                        )
                prev_node_alias = entity.alias
                joined_node_aliases.add(entity.alias)

            join_op.set_in_operators(current_op, ds_op)
            all_ops.append(join_op)
            current_op = join_op

    if current_op is None:
        raise TranspilerInternalErrorException("Empty match clause")

    return current_op


def create_partial_query_tree(
    part: PartialQueryNode,
    all_ops: list[LogicalOperator],
    previous_op: LogicalOperator | None,
    create_match_tree_fn: Callable[
        [MatchClause, list[LogicalOperator], list[QueryExpression] | None],
        LogicalOperator
    ],
) -> LogicalOperator:
    """Create logical tree for a partial query (MATCH...RETURN).

    This function includes cross-clause filter propagation optimization:
    When a MATCH clause with VLP (variable-length path) references a node
    that was filtered in a previous MATCH clause, the filter is injected
    into the VLP match clause's where_expression. This enables the filter
    to be pushed into the CTE base case for significant performance gains.

    Example:
        MATCH (root { id: "123" })
        MATCH p = (root)-[:KNOWS*1..3]->(target)

    Without optimization: CTE explores ALL paths, filters afterwards.
    With optimization: CTE filters at base case (only paths from "123").

    Args:
        part: The partial query node
        all_ops: List to collect created operators
        previous_op: Upstream operator (if any)
        create_match_tree_fn: Function to create match trees

    Returns:
        The root operator of the partial query tree
    """
    current_op = previous_op

    # Collect return expressions for path analysis optimization
    return_exprs = [
        ret.inner_expression for ret in part.return_body
    ] if part.return_body else []

    # Track node aliases seen across all match clauses for correlation
    seen_node_aliases: set[str] = set()

    # ═══════════════════════════════════════════════════════════════════
    # CROSS-CLAUSE FILTER PROPAGATION
    # ═══════════════════════════════════════════════════════════════════
    # Track filters from previous MATCH clauses by the node alias they
    # reference. When we encounter a VLP in a subsequent MATCH, we inject
    # applicable filters into its where_expression so they can be pushed
    # into the CTE base case.
    #
    # This is a critical optimization: without it, queries like
    #   MATCH (root { id: "x" }) MATCH p = (root)-[*1..3]->()
    # would explore ALL paths in the graph before filtering by root.id.
    # ═══════════════════════════════════════════════════════════════════
    accumulated_filters: dict[str, list[QueryExpression]] = {}

    # Process MATCH clauses
    for match_clause in part.match_clauses:
        # ───────────────────────────────────────────────────────────────
        # OPTIMIZATION: Inject filters from previous matches into VLP
        # ───────────────────────────────────────────────────────────────
        # Before creating the match tree, check if this match has a VLP
        # and if any previous matches have filters on the VLP's source node.
        # If so, inject those filters into this match's where_expression.
        # ───────────────────────────────────────────────────────────────
        if _has_variable_length_path(match_clause):
            source_alias = _get_vlp_source_alias(match_clause)
            if source_alias and source_alias in accumulated_filters:
                # Inject filters from previous matches into this match
                _inject_filters_into_where(
                    match_clause,
                    accumulated_filters[source_alias],
                )

        match_op = create_match_tree_fn(match_clause, all_ops, return_exprs)

        if current_op is not None:
            join_type = JoinType.LEFT if match_clause.is_optional else JoinType.INNER
            join_op = JoinOperator(join_type=join_type)
            join_op.set_in_operators(current_op, match_op)

            # Add join pairs for shared node variables (correlation)
            for entity in match_clause.pattern_parts:
                if isinstance(entity, NodeEntity) and entity.alias in seen_node_aliases:
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

        # ───────────────────────────────────────────────────────────────
        # ACCUMULATE FILTERS: Extract filters for future VLP optimization
        # ───────────────────────────────────────────────────────────────
        # Extract filters from this match clause's where_expression and
        # add them to accumulated_filters for use by subsequent VLPs.
        # Note: We do this BEFORE the where is consumed below.
        # ───────────────────────────────────────────────────────────────
        if match_clause.where_expression:
            filters_by_alias = _extract_filters_by_alias(match_clause.where_expression)
            for alias, filters in filters_by_alias.items():
                if alias not in accumulated_filters:
                    accumulated_filters[alias] = []
                accumulated_filters[alias].extend(filters)

        # Process WHERE clause from MatchClause
        if match_clause.where_expression and current_op:
            if _contains_is_terminator(match_clause.where_expression):
                raise TranspilerNotSupportedException(
                    "is_terminator() can only be used in variable-length path "
                    "queries (e.g., MATCH p = (a)-[:REL*1..5]->(b) "
                    "WHERE is_terminator(b.prop = value))"
                )
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
        if _contains_is_terminator(part.where_expression):
            raise TranspilerNotSupportedException(
                "is_terminator() can only be used in variable-length path "
                "queries (e.g., MATCH p = (a)-[:REL*1..5]->(b) "
                "WHERE is_terminator(b.prop = value))"
            )
        select_op = SelectionOperator(filter_expression=part.where_expression)
        select_op.set_in_operator(current_op)
        all_ops.append(select_op)
        current_op = select_op

    # Process RETURN clause
    if part.return_body and current_op:
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

    if current_op is None:
        raise TranspilerInternalErrorException("Empty partial query")

    return current_op


def convert_inline_properties_to_where(
    match_clause: MatchClause,
) -> QueryExpression | None:
    """Convert inline property filters to WHERE predicates.

    Extracts inline properties from all entities in the MATCH pattern
    and converts them to equality predicates combined with AND.

    IMPORTANT: Currently only supports LITERAL values (strings, numbers,
    booleans, null). Variable references (e.g., {id: variable}) are
    skipped to avoid issues with UNWIND variable scoping.

    Example:
        MATCH (a:Person {name: 'Alice', age: 30})-[r:KNOWS {since: 2020}]->(b)
        Produces:
        a.name = 'Alice' AND a.age = 30 AND r.since = 2020

    Args:
        match_clause: The MATCH clause containing pattern entities

    Returns:
        QueryExpression combining all inline property filters, or None
        if no inline properties are present
    """
    predicates: list[QueryExpression] = []

    for entity in match_clause.pattern_parts:
        # Only NodeEntity and RelationshipEntity have inline_properties
        if not isinstance(entity, (NodeEntity, RelationshipEntity)):
            continue

        inline_props = entity.inline_properties
        if not inline_props or not isinstance(inline_props, QueryExpressionMapLiteral):
            continue

        for prop_name, prop_value in inline_props.entries:
            # Skip variable references for now
            if isinstance(prop_value, QueryExpressionProperty):
                continue

            predicate = QueryExpressionBinary(
                left_expression=QueryExpressionProperty(
                    variable_name=entity.alias, property_name=prop_name
                ),
                right_expression=prop_value,
                operator=BinaryOperatorInfo(
                    BinaryOperator.EQ, BinaryOperatorType.COMPARISON
                ),
            )
            predicates.append(predicate)

    if not predicates:
        return None

    if len(predicates) == 1:
        return predicates[0]

    # Combine all predicates with AND
    and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
    result = predicates[0]
    for pred in predicates[1:]:
        result = QueryExpressionBinary(
            left_expression=result,
            right_expression=pred,
            operator=and_op,
        )

    return result


def determine_join_pair_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's source node.

    For undirected relationships, returns EITHER_AS_SOURCE to signal that:
    1. This is an undirected relationship (needs UNION ALL expansion)
    2. This node is on the source side
    """
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SOURCE
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SINK
    else:
        return JoinKeyPairType.EITHER_AS_SOURCE


def determine_sink_join_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's sink node.

    For undirected relationships, returns EITHER_AS_SINK to signal that:
    1. This is an undirected relationship (needs UNION ALL expansion)
    2. This node is on the sink side
    """
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SINK
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SOURCE
    else:
        return JoinKeyPairType.EITHER_AS_SINK
