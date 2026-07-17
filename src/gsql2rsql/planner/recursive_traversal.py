"""Recursive traversal (VLP) planning for variable-length paths.

This module handles the creation of RecursiveTraversalOperator for
variable-length path patterns like:
    MATCH path = (a)-[:TRANSFER*1..5]->(b)

It includes:
- Building the recursive CTE structure
- Extracting source/sink node filters for optimization
- Integration with PathExpressionAnalyzer for edge collection decisions
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gsql2rsql.common.exceptions import TranspilerNotSupportedException
from gsql2rsql.common.logging import LogLevel
from gsql2rsql.common.schema import EdgeAccessStrategy, IGraphSchemaProvider
from gsql2rsql.parser.ast import (
    Entity,
    MatchClause,
    NodeEntity,
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionProperty,
    RelationshipDirection,
    RelationshipEntity,
    TreeNode,
)
from gsql2rsql.parser.operators import Function
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinKeyPair,
    JoinKeyPairType,
    JoinOperator,
    JoinType,
    LogicalOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.path_analyzer import (
    PathExpressionAnalyzer,
    remove_pushed_predicates,
)

if TYPE_CHECKING:
    from gsql2rsql.common.logging import ILoggable


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


def create_recursive_match_tree(
    match_clause: MatchClause,
    rel: RelationshipEntity,
    source_node: NodeEntity,
    target_node: NodeEntity,
    all_ops: list[LogicalOperator],
    return_exprs: list[QueryExpression] | None,
    graph_schema: IGraphSchemaProvider | None,
    logger: ILoggable | None = None,
) -> LogicalOperator:
    """Create logical tree for variable-length path traversal.

    This function uses PathExpressionAnalyzer to optimize the recursive CTE:

    1. EDGE COLLECTION OPTIMIZATION:
       Only collects edge properties (path_edges array) when relationships(path)
       is actually used. This avoids the overhead of building NAMED_STRUCT arrays
       when they're not needed (e.g., when only SIZE(path) is used).

    2. PREDICATE PUSHDOWN:
       Extracts edge predicates from ALL() expressions for pushdown
       into the CTE's WHERE clause, enabling early path elimination.

    Args:
        match_clause: The MATCH clause containing the variable-length pattern
        rel: The variable-length relationship entity (e.g., [:TRANSFER*2..4])
        source_node: The source node of the path
        target_node: The target node of the path
        all_ops: List to collect all created operators
        return_exprs: RETURN clause expressions for path usage analysis
        graph_schema: Graph schema provider for node/edge definitions
        logger: Optional logger for debugging
    """
    # Create data source for source node
    source_ds = DataSourceOperator(entity=source_node)
    all_ops.append(source_ds)

    # Parse edge types from entity_name
    # e.g., "KNOWS|FOLLOWS" -> ["KNOWS", "FOLLOWS"]
    edge_types: list[str] = []
    if rel.entity_name:
        edge_types = [
            t.strip() for t in rel.entity_name.split("|") if t.strip()
        ]

    # Extract source node filter from WHERE clause for optimization
    start_node_filter, remaining_where = extract_source_node_filter(
        match_clause.where_expression,
        source_node.alias,
    )

    # Extract barrier filter (is_terminator directive) BEFORE sink filter.
    # Must be first: extract_source_node_filter would incorrectly claim
    # is_terminator(b.x) as a sink filter since it references only 'b'.
    barrier_filter, remaining_where = extract_barrier_filter(
        remaining_where,
        target_node.alias,
    )

    # Validate: if is_terminator() was NOT extracted but is still
    # present, the predicate references the wrong variable (or the
    # target node is anonymous).
    if (
        barrier_filter is None
        and remaining_where is not None
        and _contains_is_terminator(remaining_where)
    ):
        target_desc = (
            f"'{target_node.alias}'"
            if target_node.alias
            else "anonymous (no alias)"
        )
        raise TranspilerNotSupportedException(
            "is_terminator() predicate does not reference "
            "the target node of the variable-length path "
            f"(target is {target_desc}). The predicate "
            "inside is_terminator() must reference only "
            "the target node variable."
        )

    # Extract sink (target) node filter for optimization
    # This filter will be applied in the recursive join WHERE clause
    sink_node_filter, remaining_where = extract_source_node_filter(
        remaining_where,
        target_node.alias,
    )

    # Update match_clause with remaining WHERE (without source/sink/barrier filters)
    match_clause.where_expression = remaining_where

    # =====================================================================
    # PATH USAGE ANALYSIS
    # =====================================================================
    # Use PathExpressionAnalyzer to determine if edge collection is needed.
    #
    # WHY: Collecting edge properties in the CTE is expensive. We only want
    # to do it when relationships(path) is actually used, for example in:
    #   - ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    #   - REDUCE(sum = 0, r IN relationships(path) | sum + r.amount)
    #   - [r IN relationships(path) | r.timestamp]
    #
    # We DON'T need edge collection for:
    #   - SIZE(path)           -- Only uses path length
    #   - nodes(path)          -- Only uses node IDs (already in path array)
    #   - [n IN nodes(path)]   -- Uses nodes, not relationships
    #
    # This analysis examines both WHERE clause and RETURN expressions.
    # =====================================================================
    # The user-declared relationship variable ([arestas*1..3]) IS the list
    # of traversed relationships — ALL(r IN arestas ...) is equivalent to
    # ALL(r IN relationships(path) ...). Pass it so the analyzer treats
    # direct references to it as relationships(path).
    rel_variable = (
        rel.alias if rel.alias and not rel.alias.startswith("_anon") else ""
    )
    path_analyzer = PathExpressionAnalyzer()
    path_info = path_analyzer.analyze(
        path_variable=match_clause.path_variable,
        where_expr=match_clause.where_expression,
        return_exprs=return_exprs,
        relationship_variable=rel_variable,
    )

    # Log analysis result for debugging
    if logger:
        logger.log(
            LogLevel.DEBUG,
            f"Path analysis for '{match_clause.path_variable}': "
            f"needs_edge_collection={path_info.needs_edge_collection}, "
            f"has_pushable_predicates={path_info.has_pushable_predicates}"
        )

    # =====================================================================
    # REMOVE REDUNDANT PREDICATES
    # =====================================================================
    # If we pushed predicates into the CTE, remove them from the WHERE
    # clause to avoid redundant FORALL evaluations.
    #
    # Example transformation:
    #   BEFORE: WHERE a.x > 1 AND ALL(r IN relationships(path) WHERE r.amt > 1000)
    #   AFTER:  WHERE a.x > 1
    #
    # The ALL predicate is now handled by the CTE's WHERE clause, so the
    # FORALL in the final query is redundant.
    # =====================================================================
    if path_info.pushed_all_expressions:
        match_clause.where_expression = remove_pushed_predicates(
            match_clause.where_expression,
            path_info.pushed_all_expressions,
        )
        if logger:
            logger.log(
                LogLevel.DEBUG,
                f"Removed {len(path_info.pushed_all_expressions)} pushed predicates "
                f"from WHERE clause"
            )

    # Get node ID columns from schema (supports wildcard for unlabeled nodes)
    source_id_col = "id"  # Default
    target_id_col = "id"  # Default

    if graph_schema:
        source_node_schema = graph_schema.get_node_definition(source_node.entity_name)
        if not source_node_schema and not source_node.entity_name:
            source_node_schema = graph_schema.get_wildcard_node_definition()

        target_node_schema = graph_schema.get_node_definition(target_node.entity_name)
        if not target_node_schema and not target_node.entity_name:
            target_node_schema = graph_schema.get_wildcard_node_definition()

        if source_node_schema and source_node_schema.node_id_property:
            source_id_col = source_node_schema.node_id_property.property_name

        if target_node_schema and target_node_schema.node_id_property:
            target_id_col = target_node_schema.node_id_property.property_name

    # Extract edge properties from schema for struct type definition.
    # This enables property access like r.weight after UNWIND e AS r.
    edge_props: list[str] = []
    if graph_schema and edge_types:
        # Get edge schema for the first edge type (all edges should have same props)
        edge_schema = None
        src_name = source_node.entity_name or ""
        tgt_name = target_node.entity_name or ""
        if src_name and tgt_name:
            edge_schema = graph_schema.get_edge_definition(
                edge_types[0], src_name, tgt_name
            )
        if not edge_schema:
            # Try wildcard lookup
            edge_schema = graph_schema.get_wildcard_edge_definition()
        if edge_schema:
            # Get edge src/dst column names from schema
            edge_src_col = "src"
            edge_dst_col = "dst"
            if edge_schema.source_id_property:
                edge_src_col = edge_schema.source_id_property.property_name
            if edge_schema.sink_id_property:
                edge_dst_col = edge_schema.sink_id_property.property_name
            for prop in edge_schema.properties:
                edge_props.append(prop.property_name)
    elif graph_schema:
        # No specific edge types - use wildcard edge
        edge_schema = graph_schema.get_wildcard_edge_definition()
        if edge_schema:
            for prop in edge_schema.properties:
                edge_props.append(prop.property_name)

    # Determine if we need internal UNION ALL for bidirectional traversal.
    # This is a planner decision based on:
    # - Direction is BOTH (undirected pattern like (a)-[:TYPE*]-(b))
    # - EdgeAccessStrategy is EDGE_LIST (edges stored as directed pairs)
    use_internal_union = (
        rel.direction == RelationshipDirection.BOTH
        and graph_schema is not None
        and graph_schema.get_edge_access_strategy() == EdgeAccessStrategy.EDGE_LIST
    )

    # Planner decision: swap source/sink columns for BACKWARD direction.
    swap_source_sink = rel.direction == RelationshipDirection.BACKWARD

    # Create recursive traversal operator with optimized settings
    recursive_op = RecursiveTraversalOperator(
        edge_types=edge_types,
        source_node_type=source_node.entity_name,
        target_node_type=target_node.entity_name,
        min_hops=rel.min_hops if rel.min_hops is not None else 1,
        max_hops=rel.max_hops,
        source_id_column=source_id_col,
        target_id_column=target_id_col,
        start_node_filter=start_node_filter,
        sink_node_filter=sink_node_filter,
        barrier_filter=barrier_filter,
        cte_name=f"paths_{rel.alias or 'r'}",
        source_alias=source_node.alias,
        target_alias=target_node.alias,
        path_variable=match_clause.path_variable,
        # Only pass relationship_variable if the user explicitly defined an alias
        # (not auto-generated _anon* aliases from logical_plan.py)
        relationship_variable=(
            rel.alias if rel.alias and not rel.alias.startswith("_anon") else ""
        ),
        # Optimization: Only collect edges when relationships(path) is used
        collect_edges=path_info.needs_edge_collection,
        collect_nodes=path_info.needs_node_collection,
        # Edge properties from schema for struct type definition
        edge_properties=edge_props,
        # Predicate pushdown: Filter edges DURING CTE recursion
        edge_filter=path_info.combined_edge_predicate,
        edge_filter_lambda_var=path_info.edge_lambda_variable,
        # Node predicate pushdown (optimization only; residual kept in WHERE)
        node_filter=path_info.combined_node_predicate,
        node_filter_lambda_var=path_info.node_lambda_variable,
        node_filter_source_exprs=path_info.node_pushed_source_exprs,
        # Direction for undirected traversal support
        direction=rel.direction,
        # Planner decision: use internal UNION for bidirectional traversal
        use_internal_union_for_bidirectional=use_internal_union,
        # Planner decision: swap source/sink for backward traversal
        swap_source_sink=swap_source_sink,
    )
    recursive_op.add_in_operator(source_ds)
    source_ds.add_out_operator(recursive_op)
    all_ops.append(recursive_op)

    # Create data source for target node
    target_ds = DataSourceOperator(entity=target_node)
    all_ops.append(target_ds)

    # Join recursive result with target node
    join_op = JoinOperator(join_type=JoinType.INNER)
    join_op.recursive_source_alias = source_node.alias
    join_op.add_join_pair(
        JoinKeyPair(
            node_alias=target_node.alias,
            relationship_or_node_alias=recursive_op.cte_name,
            pair_type=JoinKeyPairType.SINK,
        )
    )
    join_op.set_in_operators(recursive_op, target_ds)
    all_ops.append(join_op)

    # Process remaining pattern parts after the VLP
    current_op = _process_remaining_pattern_parts(
        match_clause, rel, target_node, join_op, all_ops
    )

    return current_op


def _process_remaining_pattern_parts(
    match_clause: MatchClause,
    rel: RelationshipEntity,
    target_node: NodeEntity,
    current_op: LogicalOperator,
    all_ops: list[LogicalOperator],
) -> LogicalOperator:
    """Process pattern parts after the variable-length relationship.

    Handles patterns like: (a)-[:KNOWS*1..2]-(b)-[:HAS_LOAN]->(l)
    where we need to continue processing [:HAS_LOAN]->(l) after the recursive path.

    Args:
        match_clause: The full MATCH clause
        rel: The variable-length relationship
        target_node: The target node of the VLP
        current_op: Current operator (join with target)
        all_ops: List to collect created operators

    Returns:
        The final operator after processing remaining parts
    """
    # Find the index of the VLP relationship, then target is at vlp_idx + 1
    vlp_idx = match_clause.pattern_parts.index(rel)
    target_idx = vlp_idx + 1
    remaining_parts = match_clause.pattern_parts[target_idx + 1:]

    if not remaining_parts:
        return current_op

    # First pass: assign aliases and update entity names
    auto_alias_counter = 0
    prev_node: NodeEntity | None = target_node
    for entity in remaining_parts:
        if not entity.alias:
            auto_alias_counter += 1
            entity.alias = f"_anon{auto_alias_counter}"

        if isinstance(entity, NodeEntity):
            prev_node = entity
        elif isinstance(entity, RelationshipEntity) and prev_node:
            entity.left_entity_name = prev_node.entity_name

    # Second pass: update right_entity_name for relationships
    prev_entity: Entity | None = None
    for entity in remaining_parts:
        if isinstance(entity, NodeEntity) and prev_entity:
            if isinstance(prev_entity, RelationshipEntity):
                prev_entity.right_entity_name = entity.entity_name
        prev_entity = entity

    # Third pass: create operators and join them
    prev_node_alias = target_node.alias

    for entity in remaining_parts:
        ds_op = DataSourceOperator(entity=entity)
        all_ops.append(ds_op)

        # Determine join type and create join pairs
        new_join_op = JoinOperator(join_type=JoinType.INNER)
        # Propagate recursive source alias through join chain
        if (isinstance(current_op, JoinOperator)
                and current_op.recursive_source_alias):
            new_join_op.recursive_source_alias = (
                current_op.recursive_source_alias
            )

        if isinstance(entity, RelationshipEntity):
            # Join target node to relationship
            if prev_node_alias:
                pair_type = _determine_join_pair_type(entity)
                new_join_op.add_join_pair(JoinKeyPair(
                    node_alias=prev_node_alias,
                    relationship_or_node_alias=entity.alias,
                    pair_type=pair_type,
                ))
        elif isinstance(entity, NodeEntity):
            # Find the previous relationship to join with
            for prev_ent in remaining_parts:
                if isinstance(prev_ent, RelationshipEntity):
                    if prev_ent.right_entity_name == entity.entity_name:
                        pair_type = _determine_sink_join_type(prev_ent)
                        new_join_op.add_join_pair(
                            JoinKeyPair(
                                node_alias=entity.alias,
                                relationship_or_node_alias=prev_ent.alias,
                                pair_type=pair_type,
                            )
                        )
            prev_node_alias = entity.alias

        new_join_op.set_in_operators(current_op, ds_op)
        all_ops.append(new_join_op)
        current_op = new_join_op

    return current_op


def extract_source_node_filter(
    where_expr: QueryExpression | None,
    source_alias: str,
) -> tuple[QueryExpression | None, QueryExpression | None]:
    """Extract filters on a node variable from WHERE clause.

    This optimization pushes filters like `p.name = 'Alice'` into the
    recursive CTE's base case, dramatically reducing the number of paths
    explored.

    Handles arbitrarily nested AND trees by flattening all conjuncts first,
    then partitioning them into source-only and remaining predicates.

    Args:
        where_expr: The WHERE clause expression
        source_alias: The node alias to extract filters for

    Returns:
        Tuple of (extracted_filter, remaining_expression)

    Examples:
        - WHERE p.name = 'Alice' -> (p.name = 'Alice', None)
        - WHERE p.name = 'Alice' AND f.age > 25 -> (p.name = 'Alice', f.age > 25)
        - WHERE f.age > 25 -> (None, f.age > 25)
        - WHERE p.name = 'Alice' AND f.age > 25 AND g(x) -> (p.name = 'Alice', f.age > 25 AND g(x))
    """
    if not where_expr:
        return None, None

    # Check if the entire expression references only the source node
    # (but never extract anything containing is_terminator() — it's a
    # barrier directive, possibly NOT-wrapped)
    if (
        _references_only_variable(where_expr, source_alias)
        and not _contains_is_terminator(where_expr)
    ):
        return where_expr, None

    # Flatten nested AND tree into a list of conjuncts, then partition
    conjuncts = _flatten_and_tree(where_expr)
    if len(conjuncts) <= 1:
        # Not an AND expression
        return None, where_expr

    source_parts: list[QueryExpression] = []
    remaining_parts: list[QueryExpression] = []

    for conjunct in conjuncts:
        if (
            _references_only_variable(conjunct, source_alias)
            and not _contains_is_terminator(conjunct)
        ):
            source_parts.append(conjunct)
        else:
            remaining_parts.append(conjunct)

    if not source_parts:
        return None, where_expr

    extracted = _rebuild_and_tree(source_parts)
    remaining = _rebuild_and_tree(remaining_parts) if remaining_parts else None
    return extracted, remaining


def _match_terminator_conjunct(
    conjunct: QueryExpression,
    target_alias: str,
) -> QueryExpression | None:
    """Match a conjunct as a barrier directive; return its predicate.

    Accepts two forms:
    - `is_terminator(P)` -> returns P
    - `NOT is_terminator(P)` -> returns NOT(P); the barrier fires where
      P is false (NOT is_terminator(P) == is_terminator(NOT P))

    Returns None when the conjunct is not a barrier directive or its
    inner predicate does not reference only `target_alias`.
    """
    negated = False
    call = conjunct
    if (
        isinstance(call, QueryExpressionFunction)
        and call.function == Function.NOT
        and len(call.parameters) == 1
    ):
        negated = True
        call = call.parameters[0]

    if not (
        isinstance(call, QueryExpressionFunction)
        and call.function == Function.IS_TERMINATOR
        and len(call.parameters) == 1
    ):
        return None

    inner = call.parameters[0]
    if not _references_only_variable(inner, target_alias):
        return None

    if negated:
        return QueryExpressionFunction(
            function=Function.NOT,
            parameters=[inner],
            data_type=bool,
        )
    return inner


def extract_barrier_filter(
    where_expr: QueryExpression | None,
    target_alias: str,
) -> tuple[QueryExpression | None, QueryExpression | None]:
    """Extract is_terminator() directive from WHERE clause.

    Finds `is_terminator(<predicate>)` — or its negated form
    `NOT is_terminator(<predicate>)` — in the AND-flattened WHERE
    conjuncts, extracts the barrier predicate, and returns it separately.
    For the negated form the barrier predicate is `NOT(<predicate>)`:
    the barrier fires where the inner predicate is false.

    The inner predicate MUST reference only `target_alias`.

    Args:
        where_expr: The WHERE clause expression (after source/sink extraction)
        target_alias: The target node alias (e.g., 'b')

    Returns:
        Tuple of (barrier_predicate, remaining_expression)
        barrier_predicate is the INNER expression (e.g., b.is_hub = true),
        NOT the is_terminator() wrapper.
    """
    if not where_expr:
        return None, None

    # Single (possibly NOT-wrapped) is_terminator() call, no AND wrapper
    single_pred = _match_terminator_conjunct(where_expr, target_alias)
    if single_pred is not None:
        return single_pred, None
    if _is_terminator_call(where_expr):
        # is_terminator referencing the wrong variable: leave in WHERE so
        # the caller's validation raises a descriptive error.
        return None, where_expr

    # Flatten AND tree and search conjuncts
    conjuncts = _flatten_and_tree(where_expr)
    if len(conjuncts) <= 1:
        return None, where_expr

    barrier_pred: QueryExpression | None = None
    remaining_parts: list[QueryExpression] = []

    for conjunct in conjuncts:
        matched = (
            _match_terminator_conjunct(conjunct, target_alias)
            if barrier_pred is None
            else None
        )
        if matched is not None:
            barrier_pred = matched
        else:
            remaining_parts.append(conjunct)

    if barrier_pred is None:
        return None, where_expr

    remaining = _rebuild_and_tree(remaining_parts) if remaining_parts else None
    return barrier_pred, remaining


def _flatten_and_tree(expr: QueryExpression) -> list[QueryExpression]:
    """Flatten a nested AND tree into a list of conjuncts.

    Example: (A AND B) AND C  ->  [A, B, C]
    Non-AND expressions return a single-element list.
    """
    if (
        isinstance(expr, QueryExpressionBinary)
        and expr.operator
        and expr.operator.name.name == "AND"
    ):
        parts: list[QueryExpression] = []
        if expr.left_expression is not None:
            parts.extend(_flatten_and_tree(expr.left_expression))
        if expr.right_expression is not None:
            parts.extend(_flatten_and_tree(expr.right_expression))
        return parts
    return [expr]


def _rebuild_and_tree(parts: list[QueryExpression]) -> QueryExpression:
    """Rebuild a left-associative AND tree from a list of conjuncts.

    [A, B, C]  ->  (A AND B) AND C
    """
    from gsql2rsql.parser.operators import (
        BinaryOperator,
        BinaryOperatorInfo,
        BinaryOperatorType,
    )

    assert parts, "Cannot rebuild AND tree from empty list"
    result = parts[0]
    and_op = BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL)
    for i in range(1, len(parts)):
        result = QueryExpressionBinary(
            operator=and_op,
            left_expression=result,
            right_expression=parts[i],
        )
    return result


def _is_terminator_call(expr: QueryExpression) -> bool:
    """Check if expression is an is_terminator() function call."""
    return (
        isinstance(expr, QueryExpressionFunction)
        and expr.function == Function.IS_TERMINATOR
    )


def _references_only_variable(
    expr: QueryExpression,
    variable: str,
) -> bool:
    """Check if expression references only the specified variable.

    Returns True if ALL property references in the expression use the
    given variable name.

    Examples with variable='p':
        - p.name = 'Alice'              -> True
        - p.age > 30 AND p.active       -> True
        - p.name = f.name               -> False
        - f.age > 25                    -> False
        - 1 = 1                         -> False (no property references)
    """
    properties = _collect_property_references(expr)

    if not properties:
        return False

    return all(prop.variable_name == variable for prop in properties)


def _collect_property_references(
    expr: QueryExpression,
) -> list[QueryExpressionProperty]:
    """Collect all property references from an expression tree."""
    properties: list[QueryExpressionProperty] = []

    if isinstance(expr, QueryExpressionProperty):
        properties.append(expr)
    elif isinstance(expr, QueryExpressionBinary):
        if expr.left_expression is not None:
            properties.extend(_collect_property_references(expr.left_expression))
        if expr.right_expression is not None:
            properties.extend(_collect_property_references(expr.right_expression))
    elif hasattr(expr, 'children'):
        for child in expr.children:
            if isinstance(child, QueryExpression):
                properties.extend(_collect_property_references(child))

    return properties


def _determine_join_pair_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's source node."""
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SOURCE
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SINK
    else:
        return JoinKeyPairType.EITHER_AS_SOURCE


def _determine_sink_join_type(rel: RelationshipEntity) -> JoinKeyPairType:
    """Determine the join pair type for a relationship's sink node."""
    if rel.direction == RelationshipDirection.FORWARD:
        return JoinKeyPairType.SINK
    elif rel.direction == RelationshipDirection.BACKWARD:
        return JoinKeyPairType.SOURCE
    else:
        return JoinKeyPairType.EITHER_AS_SINK
