"""Path Expression Analyzer for Recursive CTE Optimization.

This module analyzes Cypher path expressions to determine:
1. Whether edge collection is needed in the recursive CTE
2. Which edge predicates can be pushed down into the CTE for early filtering

WHY THIS IS NEEDED:
==================

Problem 1: Unnecessary Edge Collection
--------------------------------------
Without analysis, the transpiler collects ALL edge properties in the CTE
whenever a path variable exists, even when they're never used:

    MATCH path = (a)-[:TRANSFER*1..3]->(b)
    RETURN SIZE(path)  -- Only uses path length, NOT edge properties!

Current behavior: Collects ARRAY(NAMED_STRUCT(...)) for every edge
Optimal behavior: Skip edge collection entirely (saves memory/CPU)

Problem 2: Late Predicate Evaluation
------------------------------------
Edge predicates in ALL/FORALL are evaluated AFTER the CTE completes:

    MATCH path = (a)-[:TRANSFER*1..5]->(b)
    WHERE ALL(rel IN relationships(path) WHERE rel.amount > 1000)

Current SQL (inefficient):
    WITH RECURSIVE paths AS (
      SELECT ... FROM Transfer e  -- Collects ALL edges
      UNION ALL
      SELECT ... FROM paths p JOIN Transfer e ...  -- Explores ALL paths
    )
    SELECT ... WHERE FORALL(path_edges, r -> r.amount > 1000)  -- Filters at END

Optimal SQL (with pushdown):
    WITH RECURSIVE paths AS (
      SELECT ... FROM Transfer e WHERE e.amount > 1000  -- Filter EARLY
      UNION ALL
      SELECT ... FROM paths p JOIN Transfer e ...
        WHERE e.amount > 1000  -- Filter during recursion
    )
    SELECT ...  -- No FORALL needed, already filtered!

HOW THIS ANALYZER WORKS:
========================

1. Traverses the AST (WHERE clause + RETURN expressions)
2. Detects usage of `relationships(path)` function
3. Extracts predicates from ALL/ANY/NONE expressions for potential pushdown
4. Returns PathUsageInfo with collection requirements and pushable predicates

USAGE EXAMPLE:
==============

    analyzer = PathExpressionAnalyzer()
    info = analyzer.analyze(
        path_variable="path",
        where_expr=match_clause.where_expression,
        return_exprs=[ret.inner_expression for ret in part.return_body],
    )

    # Use results to configure RecursiveTraversalOperator
    recursive_op = RecursiveTraversalOperator(
        collect_edges=info.needs_edge_collection,
        edge_filter=info.combined_edge_predicate,  # For pushdown
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gsql2rsql.parser.ast import (
    QueryExpression,
    QueryExpressionBinary,
    QueryExpressionFunction,
    QueryExpressionListComprehension,
    QueryExpressionListPredicate,
    QueryExpressionProperty,
    QueryExpressionReduce,
)
from gsql2rsql.parser.operators import (
    BinaryOperator,
    BinaryOperatorInfo,
    BinaryOperatorType,
    Function,
    ListPredicateType,
)

if TYPE_CHECKING:
    pass


@dataclass
class PathUsageInfo:
    """Information about how a path variable is used in the query.

    This dataclass captures the analysis results that determine:
    - Whether to collect edge properties in the recursive CTE
    - Whether to collect node IDs (for nodes(path) function)
    - Which predicates can be pushed into the CTE for early filtering

    Attributes:
        path_variable: The name of the path variable (e.g., "path" in MATCH path = ...)

        needs_edge_collection: True if relationships(path) is used anywhere.
            When True, the CTE must accumulate edge properties in an array:
            ARRAY(NAMED_STRUCT('amount', e.amount, ...)) AS path_edges

        needs_node_collection: True if nodes(path) is used anywhere.
            Currently, node IDs are always collected in the 'path' array,
            so this is informational only.

        edge_predicates: List of predicates extracted from expressions like:
            - ALL(rel IN relationships(path) WHERE rel.amount > 1000)
            - [rel IN relationships(path) WHERE rel.valid | rel.id]
            These predicates can potentially be pushed into the CTE's WHERE clause
            to filter edges DURING recursion instead of AFTER.

        edge_lambda_variable: The variable name used in edge predicates (e.g., "rel").
            Needed to rewrite "rel.amount" as "e.amount" when pushing down.
    """

    path_variable: str
    needs_edge_collection: bool = False
    needs_node_collection: bool = False
    edge_predicates: list[QueryExpression] = field(default_factory=list)
    edge_lambda_variable: str = ""
    # Track the original ALL expressions that were pushed down.
    # These should be REMOVED from the WHERE clause since they're now
    # handled by the CTE's WHERE clause (predicate pushdown).
    #
    # Example: If we push down ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    # into the CTE, we should NOT also apply FORALL(path_edges, rel -> rel.amount > 1000)
    # at the end - that would be redundant.
    pushed_all_expressions: list[QueryExpression] = field(default_factory=list)

    @property
    def has_pushable_predicates(self) -> bool:
        """Check if there are predicates that can be pushed into the CTE.

        A predicate is pushable if:
        1. It comes from an ALL() expression (must hold for EVERY edge)
        2. It references only edge properties (not path-level aggregations)

        Note: ANY() predicates cannot be pushed because they only require
        ONE edge to match, which changes semantics if applied per-edge.
        """
        return len(self.edge_predicates) > 0

    @property
    def combined_edge_predicate(self) -> QueryExpression | None:
        """Combine all edge predicates with AND for pushdown.

        If we have multiple predicates from:
            ALL(r IN relationships(path) WHERE r.amount > 1000)
            ALL(r IN relationships(path) WHERE r.timestamp > date('2024-01-01'))

        We combine them into:
            r.amount > 1000 AND r.timestamp > date('2024-01-01')

        This combined predicate is then rewritten to use 'e.' prefix
        and applied in the CTE's WHERE clause.

        Returns:
            Combined predicate expression, or None if no predicates exist.
        """
        if not self.edge_predicates:
            return None

        if len(self.edge_predicates) == 1:
            return self.edge_predicates[0]

        # Combine with AND: pred1 AND pred2 AND pred3 ...
        # BinaryOperatorInfo contains the operator name and type
        and_operator = BinaryOperatorInfo(
            name=BinaryOperator.AND,
            operator_type=BinaryOperatorType.LOGICAL,
        )

        result = self.edge_predicates[0]
        for pred in self.edge_predicates[1:]:
            result = QueryExpressionBinary(
                left_expression=result,
                right_expression=pred,
                operator=and_operator,
            )
        return result


class PathExpressionAnalyzer:
    """Analyzes path expressions to optimize recursive CTE generation.

    This analyzer traverses the AST to determine:

    1. EDGE COLLECTION DECISION
       --------------------------
       Only collect edge properties if relationships(path) is actually used.

       Examples where collection IS needed:
       - ALL(rel IN relationships(path) WHERE rel.amount > 0)
       - REDUCE(sum = 0, r IN relationships(path) | sum + r.amount)
       - [r IN relationships(path) | r.timestamp]

       Examples where collection is NOT needed:
       - SIZE(path)           -- Uses path length only
       - nodes(path)          -- Uses node IDs only
       - path                 -- Returns path array of IDs

    2. PREDICATE PUSHDOWN EXTRACTION
       ------------------------------
       Extract predicates from ALL() expressions that can be pushed
       into the CTE for early filtering.

       Pushable (ALL - must hold for every edge):
           ALL(r IN relationships(path) WHERE r.amount > 1000)
           -> Push: WHERE e.amount > 1000 in base AND recursive case

       NOT pushable (ANY - only one edge needs to match):
           ANY(r IN relationships(path) WHERE r.flagged = true)
           -> Cannot push; semantics would change

       NOT pushable (aggregations over path):
           SIZE(relationships(path)) > 3
           -> Cannot push; requires complete path

    IMPLEMENTATION NOTES:
    --------------------
    - The analyzer is stateless and can be reused across queries
    - It performs a single traversal of each expression tree
    - Results are cached in PathUsageInfo for the planner to use
    """

    def analyze(
        self,
        path_variable: str,
        where_expr: QueryExpression | None,
        return_exprs: list[QueryExpression] | None = None,
    ) -> PathUsageInfo:
        """Analyze expressions to determine path usage and optimization opportunities.

        This is the main entry point for path analysis. It examines:
        1. The WHERE clause for path predicates
        2. The RETURN expressions for path function usage

        Args:
            path_variable: The path variable name (e.g., "path" from MATCH path = ...)
            where_expr: The WHERE clause expression (may be None)
            return_exprs: List of RETURN clause expressions (may be None or empty)

        Returns:
            PathUsageInfo containing:
            - needs_edge_collection: Whether to generate path_edges array in CTE
            - needs_node_collection: Whether nodes(path) is used
            - edge_predicates: Predicates that can be pushed into CTE
            - edge_lambda_variable: Variable name for predicate rewriting

        Example:
            >>> analyzer = PathExpressionAnalyzer()
            >>> info = analyzer.analyze(
            ...     path_variable="p",
            ...     where_expr=parse("ALL(r IN relationships(p) WHERE r.x > 1)"),
            ...     return_exprs=[parse("SIZE(p)")],
            ... )
            >>> info.needs_edge_collection
            True  # Because relationships(p) is used in ALL()
            >>> info.edge_predicates
            [<QueryExpressionBinary: r.x > 1>]  # Can be pushed down
        """
        info = PathUsageInfo(path_variable=path_variable)

        # Analyze WHERE clause
        if where_expr:
            self._analyze_expression(where_expr, path_variable, info)

        # Analyze RETURN expressions
        if return_exprs:
            for expr in return_exprs:
                self._analyze_expression(expr, path_variable, info)

        return info

    def _analyze_expression(
        self,
        expr: QueryExpression,
        path_var: str,
        info: PathUsageInfo,
    ) -> None:
        """Recursively analyze an expression for path variable usage.

        This method performs a depth-first traversal of the expression tree,
        looking for patterns that indicate path usage:

        1. relationships(path) function calls -> needs_edge_collection = True
        2. nodes(path) function calls -> needs_node_collection = True
        3. ALL(var IN relationships(path) WHERE pred) -> extract pred for pushdown
        4. List comprehensions with relationships(path) -> extract filter for pushdown

        The traversal is exhaustive - we check ALL subexpressions to ensure
        we don't miss any path usage (e.g., nested in complex boolean expressions).

        Args:
            expr: The expression to analyze
            path_var: The path variable name to look for
            info: PathUsageInfo to update with findings
        """
        if expr is None:
            return

        # -----------------------------------------------------------------
        # Case 1: Function calls - check for relationships(path) or nodes(path)
        # -----------------------------------------------------------------
        # -----------------------------------------------------------------
        # Case 0: Bare path variable reference (e.g., RETURN p, SIZE(p))
        #
        # Any direct reference to the path variable (outside nodes()/
        # relationships()) means the node-ID path array is needed.
        # -----------------------------------------------------------------
        if isinstance(expr, QueryExpressionProperty):
            if expr.variable_name == path_var and not expr.property_name:
                info.needs_node_collection = True

        if isinstance(expr, QueryExpressionFunction):
            if expr.function and expr.function == Function.RELATIONSHIPS:
                # relationships(path) is called - we need edge collection
                if self._references_path_variable(expr.parameters, path_var):
                    info.needs_edge_collection = True
                    return  # Don't recurse into path variable parameter

            elif expr.function and expr.function == Function.NODES:
                # nodes(path) is called - we need node collection
                if self._references_path_variable(expr.parameters, path_var):
                    info.needs_node_collection = True
                    return  # Don't recurse into path variable parameter

        # -----------------------------------------------------------------
        # Case 2: List predicates - ALL/ANY/NONE(var IN list WHERE pred)
        #
        # ALL(rel IN relationships(path) WHERE rel.amount > 1000)
        #      ^^^                              ^^^^^^^^^^^^^^^^
        #      |                                |
        #      edge_lambda_variable             edge_predicate (pushable!)
        #
        # Only ALL predicates can be pushed down because they must hold
        # for EVERY edge. ANY/NONE have different semantics.
        # -----------------------------------------------------------------
        elif isinstance(expr, QueryExpressionListPredicate):
            if self._is_relationships_of_path(expr.list_expression, path_var):
                info.needs_edge_collection = True

                # Extract predicate for pushdown (only for ALL)
                if (expr.predicate_type == ListPredicateType.ALL
                    and expr.filter_expression is not None):
                    # Store the predicate and variable name for rewriting
                    info.edge_predicates.append(expr.filter_expression)
                    if not info.edge_lambda_variable:
                        info.edge_lambda_variable = expr.variable_name
                    # Also store the ORIGINAL ALL expression so it can be
                    # removed from the WHERE clause after pushdown
                    # (it becomes redundant - the CTE already filters)
                    info.pushed_all_expressions.append(expr)

        # -----------------------------------------------------------------
        # Case 3: REDUCE expressions - REDUCE(acc = init, var IN list | expr)
        #
        # REDUCE(total = 0, r IN relationships(path) | total + r.amount)
        #
        # Cannot push down the reducer, but we need edge collection.
        # -----------------------------------------------------------------
        elif isinstance(expr, QueryExpressionReduce):
            if self._is_relationships_of_path(expr.list_expression, path_var):
                info.needs_edge_collection = True
                # Note: REDUCE expressions cannot be pushed down as simple filters
                # They require the full path to be available for aggregation

        # -----------------------------------------------------------------
        # Case 4: List comprehensions - [var IN list WHERE pred | map_expr]
        #
        # [r IN relationships(path) WHERE r.valid | r.id]
        #                                 ^^^^^^^
        #                                 filter_expression (pushable!)
        #
        # The filter part can be pushed down similar to ALL predicates.
        # -----------------------------------------------------------------
        elif isinstance(expr, QueryExpressionListComprehension):
            if self._is_relationships_of_path(expr.list_expression, path_var):
                info.needs_edge_collection = True

                # Extract filter predicate for pushdown
                if expr.filter_expression is not None:
                    info.edge_predicates.append(expr.filter_expression)
                    if not info.edge_lambda_variable:
                        info.edge_lambda_variable = expr.variable_name

        # -----------------------------------------------------------------
        # Recurse into all child expressions
        #
        # We must check ALL children because path usage might be nested:
        #   WHERE a.x > 1 AND ALL(r IN relationships(path) WHERE r.y > 2)
        #                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #                     Nested inside an AND expression
        # -----------------------------------------------------------------
        for child in expr.children:
            if isinstance(child, QueryExpression):
                self._analyze_expression(child, path_var, info)

    def _references_path_variable(
        self,
        params: list[QueryExpression],
        path_var: str,
    ) -> bool:
        """Check if any parameter references the path variable.

        Used to detect patterns like:
            relationships(path)  -> True if path_var == "path"
            relationships(p)     -> True if path_var == "p"
            relationships(other) -> False if path_var != "other"

        Args:
            params: List of function parameters
            path_var: The path variable name to look for

        Returns:
            True if any parameter is a property reference to path_var
        """
        # Types that have variable_name attribute
        types_with_variable_name = (
            QueryExpressionProperty,
            QueryExpressionListPredicate,
            QueryExpressionListComprehension,
            QueryExpressionReduce,
        )
        for param in params:
            if isinstance(param, types_with_variable_name):
                # Check if variable_name matches the path variable
                if param.variable_name == path_var:
                    return True
        return False

    def _is_relationships_of_path(
        self,
        expr: QueryExpression,
        path_var: str,
    ) -> bool:
        """Check if expression is specifically relationships(path_var).

        This is a more specific check than _references_path_variable,
        used to identify the exact pattern:
            relationships(path)

        Args:
            expr: Expression to check
            path_var: Path variable name to match

        Returns:
            True if expr is relationships(path_var)
        """
        if isinstance(expr, QueryExpressionFunction):
            if expr.function and expr.function == Function.RELATIONSHIPS:
                return self._references_path_variable(expr.parameters, path_var)
        return False


def rewrite_predicate_for_edge_alias(
    predicate: QueryExpression,
    lambda_var: str,
    edge_alias: str = "e",
) -> QueryExpression:
    """Rewrite a lambda predicate to use the edge table alias.

    When pushing down predicates from:
        ALL(rel IN relationships(path) WHERE rel.amount > 1000)

    We need to rewrite "rel.amount" to "e.amount" for use in the CTE:
        WHERE e.amount > 1000

    This function performs that rewriting by traversing the predicate
    and replacing all property accesses on lambda_var with edge_alias.

    Args:
        predicate: The predicate expression to rewrite
        lambda_var: The lambda variable name (e.g., "rel")
        edge_alias: The edge table alias in the CTE (default: "e")

    Returns:
        A new expression with variable names rewritten

    Example:
        >>> pred = parse("rel.amount > 1000 AND rel.timestamp > now()")
        >>> rewritten = rewrite_predicate_for_edge_alias(pred, "rel", "e")
        >>> str(rewritten)
        "e.amount > 1000 AND e.timestamp > now()"

    IMPORTANT: This creates a shallow copy - nested expressions are shared.
    For the current use case (SQL rendering), this is safe because we
    only read the expressions, never modify them.
    """
    from copy import copy

    if isinstance(predicate, QueryExpressionProperty):
        if predicate.variable_name == lambda_var:
            # Rewrite: rel.amount -> e.amount
            new_prop = copy(predicate)
            new_prop.variable_name = edge_alias
            return new_prop
        return predicate

    elif isinstance(predicate, QueryExpressionBinary):
        # Recursively rewrite both sides of binary expressions
        new_left = (
            rewrite_predicate_for_edge_alias(
                predicate.left_expression, lambda_var, edge_alias
            )
            if predicate.left_expression is not None
            else None
        )
        new_right = (
            rewrite_predicate_for_edge_alias(
                predicate.right_expression, lambda_var, edge_alias
            )
            if predicate.right_expression is not None
            else None
        )

        # Only create new expression if something changed
        if new_left is predicate.left_expression and new_right is predicate.right_expression:
            return predicate

        new_binary = copy(predicate)
        new_binary.left_expression = new_left
        new_binary.right_expression = new_right
        return new_binary

    elif isinstance(predicate, QueryExpressionFunction):
        # Recursively rewrite function parameters
        new_params = [
            rewrite_predicate_for_edge_alias(p, lambda_var, edge_alias)
            for p in predicate.parameters
        ]

        # Only create new expression if something changed
        if all(new_params[i] is predicate.parameters[i] for i in range(len(new_params))):
            return predicate

        new_func = copy(predicate)
        new_func.parameters = new_params
        return new_func

    # For other expression types, return as-is
    # (values, literals, etc. don't need rewriting)
    return predicate


def remove_pushed_predicates(
    where_expr: QueryExpression | None,
    pushed_expressions: list[QueryExpression],
) -> QueryExpression | None:
    """Remove pushed-down predicates from the WHERE clause.

    When we push ALL() predicates into the CTE, they become redundant in the
    final WHERE clause. This function removes them to avoid unnecessary
    FORALL evaluations.

    BEFORE (redundant):
    ┌─────────────────────────────────────────────────────────────────┐
    │  WITH RECURSIVE paths AS (                                      │
    │    ...WHERE e.amount > 1000...      ← Predicate in CTE          │
    │  )                                                              │
    │  SELECT ... WHERE FORALL(path_edges, r -> r.amount > 1000)      │
    │                   ↑ REDUNDANT! CTE already filtered this        │
    └─────────────────────────────────────────────────────────────────┘

    AFTER (optimized):
    ┌─────────────────────────────────────────────────────────────────┐
    │  WITH RECURSIVE paths AS (                                      │
    │    ...WHERE e.amount > 1000...      ← Predicate in CTE          │
    │  )                                                              │
    │  SELECT ... WHERE (other conditions only)                       │
    │             ↑ No redundant FORALL - paths already filtered!     │
    └─────────────────────────────────────────────────────────────────┘

    Args:
        where_expr: The original WHERE clause expression
        pushed_expressions: List of QueryExpressionListPredicate that were pushed

    Returns:
        Modified WHERE expression with pushed predicates removed,
        or None if all predicates were removed.

    Example:
        Input:  a.x > 1 AND ALL(r IN relationships(path) WHERE r.amount > 1000)
        Pushed: [ALL(r IN relationships(path) WHERE r.amount > 1000)]
        Output: a.x > 1
    """
    if where_expr is None or not pushed_expressions:
        return where_expr

    # Check if the entire expression is a pushed predicate
    for pushed in pushed_expressions:
        if where_expr is pushed:
            return None  # Entire WHERE was just the pushed predicate

    # Handle binary expressions (AND/OR)
    if isinstance(where_expr, QueryExpressionBinary):
        if where_expr.operator and where_expr.operator.name == BinaryOperator.AND:
            # Recursively remove from both sides
            left = remove_pushed_predicates(where_expr.left_expression, pushed_expressions)
            right = remove_pushed_predicates(where_expr.right_expression, pushed_expressions)

            # If both sides were removed, return None
            if left is None and right is None:
                return None
            # If only left was removed, return right
            if left is None:
                return right
            # If only right was removed, return left
            if right is None:
                return left
            # If nothing was removed, return as-is
            if left is where_expr.left_expression and right is where_expr.right_expression:
                return where_expr
            # Otherwise, create a new AND with the remaining parts
            from copy import copy
            new_and = copy(where_expr)
            new_and.left_expression = left
            new_and.right_expression = right
            return new_and

    return where_expr
