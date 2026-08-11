"""ANTLR4 visitor for converting parse tree to AST."""

from __future__ import annotations

from typing import Any

from gsql2rsql.common.exceptions import (
    TranspilerNotSupportedException,
    TranspilerSyntaxErrorException,
)
from gsql2rsql.common.logging import ILoggable
from gsql2rsql.parser.ast import (
    Entity,
    InfixOperator,
    InfixQueryNode,
    LimitClause,
    MatchClause,
    NodeEntity,
    PartialQueryNode,
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
    QueryExpressionParameter,
    QueryExpressionProperty,
    QueryExpressionReduce,
    QueryExpressionValue,
    QueryExpressionWithAlias,
    QueryNode,
    RelationshipDirection,
    RelationshipEntity,
    SingleQueryNode,
    SortItem,
    SortOrder,
    UnwindClause,
)
from gsql2rsql.parser.operators import (
    AggregationFunction,
    Function,
    ListPredicateType,
    try_get_function,
    try_get_operator,
    try_parse_aggregation_function,
)


def unescape_symbolic_name(name: str) -> str:
    """Strip backtick delimiters from an openCypher EscapedSymbolicName.

    openCypher allows any identifier — label, relationship type, variable,
    property key — to be escaped with backticks so it may contain characters
    that are otherwise illegal::

        MATCH (n:`My Label`) RETURN n.`first name`

    The backticks are delimiters, not part of the name, so ``:`Person``` and
    ``:Person`` must both bind to the node type ``Person``.  A doubled
    backtick inside the escape is the literal escape for one backtick.

    Non-escaped names are returned unchanged.
    """
    if len(name) >= 2 and name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    return name


def split_escaped_labels(labels_text: str) -> list[str]:
    """Split a label/type list on ``:`` respecting backtick escapes.

    ``oC_NodeLabels``/``oC_RelationshipTypes`` arrive as raw text such as
    ``":Person:Employee"`` or ``":`My Label`:Other"``.  A naive
    ``lstrip(':')`` + ``split(':')`` corrupts escaped names that contain a
    colon (``:`a:b``` would become ``a``), so the separator is only honoured
    outside backticks.

    Returns the unescaped names in order, with empty segments dropped.
    """
    names: list[str] = []
    current: list[str] = []
    in_backticks = False

    for char in labels_text:
        if char == "`":
            in_backticks = not in_backticks
            current.append(char)
        elif char == ":" and not in_backticks:
            if current:
                names.append("".join(current))
                current = []
        else:
            current.append(char)

    if current:
        names.append("".join(current))

    return [unescape_symbolic_name(n) for n in names]


def sanitize_expression_alias(expression_text: str) -> str:
    """Derive a bare SQL identifier from raw Cypher expression text.

    openCypher names an unaliased projection after the expression itself, so
    ``RETURN count(DISTINCT d)`` yields a column called ``count(DISTINCT d)``.
    That name is not a legal bare SQL identifier, and the renderer emits
    aliases unquoted, so the punctuation is folded to underscores:

        ``count(DISTINCT d)``  -> ``count_DISTINCT_d``
        ``n.age + 1``          -> ``n_age_1``
        ``count(*)``           -> ``count_star``

    Returns an empty string if nothing usable survives, letting the caller
    fall back to its own default rather than emitting a broken alias.
    """
    if not expression_text:
        return ""

    # '*' carries meaning (count(*) vs count(x)) so name it rather than drop it
    text = expression_text.replace("*", "_star_")

    sanitized = "".join(
        char if char.isalnum() or char == "_" else "_" for char in text
    )
    sanitized = "_".join(part for part in sanitized.split("_") if part)

    # A leading digit is not a legal identifier start (e.g. `RETURN 42`)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"

    return sanitized


class CypherVisitor:
    """
    Visitor that converts ANTLR parse tree to openCypher AST.

    This class implements the visitor pattern to walk the ANTLR-generated
    parse tree and construct our custom AST representation.
    """

    def __init__(self, logger: ILoggable | None = None) -> None:
        self._logger = logger

    def visit(self, tree: Any) -> Any:
        """
        Visit a parse tree node and dispatch to the appropriate handler.

        This is the main entry point for visiting parse tree nodes.
        """
        if tree is None:
            return None

        # Get the visitor method based on the node type
        class_name = tree.__class__.__name__

        # Handle context names like 'OC_CypherContext' -> 'visit_oC_Cypher'
        if class_name.endswith("Context"):
            # Remove 'Context' suffix and convert 'OC_' to 'oC_'
            rule_name = class_name[:-7]
            if rule_name.startswith("OC_"):
                rule_name = "oC_" + rule_name[3:]
            method_name = f"visit_{rule_name}"
        else:
            method_name = f"visit_{class_name}"

        visitor_method = getattr(self, method_name, None)

        if visitor_method:
            return visitor_method(tree)

        # Default: visit children and return last result
        return self.visit_children(tree)

    def visit_children(self, tree: Any) -> Any:
        """Visit all children of a node and return the last result."""
        result: Any = None
        for i in range(tree.getChildCount()):
            child = tree.getChild(i)
            # Skip whitespace/terminal nodes
            if not hasattr(child, "getRuleIndex"):
                continue
            child_result = self.visit(child)
            if child_result is not None:
                result = child_result
        return result

    # =========================================================================
    # Top-level query visitors
    # =========================================================================

    def visit_oC_Cypher(self, ctx: Any) -> QueryNode:
        """Visit the root Cypher context."""
        return self.visit(ctx.oC_Statement())

    def visit_oC_Statement(self, ctx: Any) -> QueryNode:
        """Visit a statement context."""
        return self.visit(ctx.oC_Query())

    def visit_oC_Query(self, ctx: Any) -> QueryNode:
        """Visit a query context."""
        if ctx.oC_RegularQuery():
            return self.visit(ctx.oC_RegularQuery())
        return SingleQueryNode()

    def visit_oC_RegularQuery(self, ctx: Any) -> QueryNode:
        """Visit a regular query context."""
        result: QueryNode = self.visit(ctx.oC_SingleQuery())

        # Handle UNION clauses
        for union_ctx in ctx.oC_Union() or []:
            right = self.visit(union_ctx.oC_SingleQuery())
            has_all = any(
                str(child.getText()).upper() == "ALL"
                for i in range(union_ctx.getChildCount())
                if hasattr((child := union_ctx.getChild(i)), "getText")
            )
            operator = InfixOperator.UNION_ALL if has_all else InfixOperator.UNION
            result = InfixQueryNode(
                operator=operator,
                left_query=result,
                right_query=right,
            )

        return result

    def visit_oC_SingleQuery(self, ctx: Any) -> SingleQueryNode:
        """Visit a single query context."""
        parts: list[PartialQueryNode] = []

        # Visit single part query if present
        if ctx.oC_SinglePartQuery():
            part = self.visit(ctx.oC_SinglePartQuery())
            if isinstance(part, PartialQueryNode):
                parts.append(part)

        # Visit multi-part query if present
        if ctx.oC_MultiPartQuery():
            multi_parts = self.visit(ctx.oC_MultiPartQuery())
            if isinstance(multi_parts, list):
                parts.extend(multi_parts)
            elif isinstance(multi_parts, PartialQueryNode):
                parts.append(multi_parts)

        return SingleQueryNode(parts=parts)

    def visit_oC_MultiPartQuery(self, ctx: Any) -> list[PartialQueryNode]:
        """Visit a multi-part query context.

        Grammar:
        oC_MultiPartQuery
            : ( ( oC_ReadingClause SP? )* ( oC_UpdatingClause SP? )* oC_With SP? )+
              oC_SinglePartQuery ;

        Multi-part queries have WITH clauses that produce intermediate results.
        Each group of reading clauses followed by a WITH forms one PartialQueryNode.

        IMPORTANT: We must iterate through children in order to correctly group
        reading clauses with their corresponding WITH clause. The grammar structure
        means that reading clauses belong to the NEXT WITH clause, not the previous one.
        """
        parts: list[PartialQueryNode] = []

        # Collect reading clauses until we hit a WITH, then create a part
        current_reading_clauses: list[Any] = []

        # Iterate through children in order to properly group
        # reading clauses with their corresponding WITH
        for child in ctx.children or []:
            child_class = child.__class__.__name__

            if child_class == "OC_ReadingClauseContext":
                # Accumulate reading clauses until we hit a WITH
                current_reading_clauses.append(child)

            elif child_class == "OC_WithContext":
                # Create a PartialQueryNode with the accumulated reading clauses
                part = PartialQueryNode()

                # Add all accumulated reading clauses to this part
                for reading_ctx in current_reading_clauses:
                    reading = self.visit(reading_ctx)
                    if isinstance(reading, MatchClause):
                        part.match_clauses.append(reading)
                    elif isinstance(reading, UnwindClause):
                        part.unwind_clauses.append(reading)

                # Process the WITH clause
                with_result = self.visit(child)
                if isinstance(with_result, dict):
                    self._apply_return_result(part, with_result)

                parts.append(part)

                # Clear reading clauses - they've been consumed by this WITH
                current_reading_clauses = []

            elif child_class == "OC_SinglePartQueryContext":
                # Final single part query - should have no reading clauses before it
                # (they should have been consumed by WITH clauses)
                final_part = self.visit(child)
                if isinstance(final_part, PartialQueryNode):
                    # If there are leftover reading clauses, add them to the final part
                    # This shouldn't normally happen with well-formed queries
                    for reading_ctx in current_reading_clauses:
                        reading = self.visit(reading_ctx)
                        if isinstance(reading, MatchClause):
                            final_part.match_clauses.insert(0, reading)
                        elif isinstance(reading, UnwindClause):
                            final_part.unwind_clauses.insert(0, reading)
                    parts.append(final_part)
                current_reading_clauses = []

            elif child_class == "OC_UpdatingClauseContext":
                self._reject_updating_clauses([child])

            # Skip other children like whitespace (SP tokens)

        return parts

    def visit_oC_With(self, ctx: Any) -> dict[str, Any]:
        """Visit a WITH clause context.

        Grammar:
        oC_With: WITH oC_ProjectionBody ( SP? oC_Where )? ;

        WITH is like RETURN but passes results to the next part of the query.
        The optional WHERE clause acts like HAVING when filtering aggregated columns.
        """
        result: dict[str, Any] = {}

        # Process the projection body (same as RETURN)
        if ctx.oC_ProjectionBody():
            body_result = self.visit(ctx.oC_ProjectionBody())
            if isinstance(body_result, dict):
                result.update(body_result)

        # Process the optional WHERE clause (acts as HAVING for aggregations)
        if ctx.oC_Where():
            where_expr = self.visit(ctx.oC_Where())
            if isinstance(where_expr, QueryExpression):
                # Store the WHERE/HAVING expression
                # This will be used to generate HAVING clause in SQL
                result["having"] = where_expr

        return result

    def visit_oC_SinglePartQuery(self, ctx: Any) -> PartialQueryNode:
        """Visit a single part query context."""
        # Reject write operations early — this transpiler only supports read queries
        if ctx.oC_UpdatingClause():
            self._reject_updating_clauses(ctx.oC_UpdatingClause())

        part = PartialQueryNode()

        # Visit reading clauses (MATCH and UNWIND)
        for reading_ctx in ctx.oC_ReadingClause() or []:
            reading = self.visit(reading_ctx)
            if isinstance(reading, MatchClause):
                part.match_clauses.append(reading)
            elif isinstance(reading, UnwindClause):
                part.unwind_clauses.append(reading)

        # Visit return clause
        if ctx.oC_Return():
            return_result = self.visit(ctx.oC_Return())
            if return_result:
                self._apply_return_result(part, return_result)

        return part

    def _apply_return_result(self, part: PartialQueryNode, result: dict[str, Any]) -> None:
        """Apply return clause results to a partial query node."""
        if "body" in result:
            part.return_body = result["body"]
        if "distinct" in result:
            part.is_distinct = result["distinct"]
        if "order" in result:
            part.order_by = result["order"]
        if "limit" in result:
            part.limit_clause = result["limit"]
        if "having" in result:
            part.having_expression = result["having"]

    def _reject_updating_clauses(self, updating_clauses: list[Any]) -> None:
        """Reject write operations — this transpiler only supports read queries."""
        clause_names = []
        for clause_ctx in updating_clauses:
            for child in clause_ctx.children or []:
                name = child.__class__.__name__
                if "Create" in name:
                    clause_names.append("CREATE")
                elif "Merge" in name:
                    clause_names.append("MERGE")
                elif "Delete" in name:
                    clause_names.append("DELETE")
                elif "Set" in name:
                    clause_names.append("SET")
                elif "Remove" in name:
                    clause_names.append("REMOVE")
        clauses_str = ", ".join(clause_names) if clause_names else "write operations"
        raise TranspilerNotSupportedException(
            f"{clauses_str} not supported. "
            f"This transpiler only handles read queries (MATCH ... RETURN)."
        )

    # =========================================================================
    # Reading clause visitors
    # =========================================================================

    def visit_oC_ReadingClause(self, ctx: Any) -> MatchClause | UnwindClause | None:
        """Visit a reading clause context.

        Grammar:
        oC_ReadingClause: oC_Match | oC_Unwind | oC_InQueryCall ;
        """
        if ctx.oC_Match():
            return self.visit(ctx.oC_Match())
        if ctx.oC_Unwind():
            return self.visit(ctx.oC_Unwind())
        return None

    def visit_oC_Unwind(self, ctx: Any) -> UnwindClause:
        """Visit an UNWIND clause context.

        Grammar:
        oC_Unwind: UNWIND SP oC_Expression SP AS SP oC_Variable ;

        Example: UNWIND a.tags AS tag
        """
        # Get the expression to unwind
        list_expression = self.visit(ctx.oC_Expression())

        # Get the variable name
        variable_name = unescape_symbolic_name(ctx.oC_Variable().getText())

        return UnwindClause(
            list_expression=list_expression,
            variable_name=variable_name,
        )

    def visit_oC_Match(self, ctx: Any) -> MatchClause:
        """Visit a MATCH clause context."""
        is_optional = any(
            str(child.getText()).upper() == "OPTIONAL"
            for i in range(ctx.getChildCount())
            if hasattr((child := ctx.getChild(i)), "getText")
            and not hasattr(child, "getRuleIndex")
        )

        # Extract pattern and path variable
        entities: list[Entity] = []
        path_variable = ""

        if ctx.oC_Pattern():
            for part_ctx in ctx.oC_Pattern().oC_PatternPart() or []:
                # Check for path variable assignment: path = (a)-[]->(b)
                if part_ctx.oC_Variable():
                    path_variable = unescape_symbolic_name(
                        part_ctx.oC_Variable().getText()
                    )

                # Get the pattern elements
                if part_ctx.oC_AnonymousPatternPart():
                    part = self.visit(part_ctx.oC_AnonymousPatternPart())
                    if isinstance(part, list):
                        entities.extend(part)
                    elif isinstance(part, Entity):
                        entities.append(part)

        where_expr = self.visit(ctx.oC_Where()) if ctx.oC_Where() else None

        return MatchClause(
            pattern_parts=entities,
            is_optional=is_optional,
            where_expression=where_expr,
            path_variable=path_variable,
        )

    def visit_oC_Pattern(self, ctx: Any) -> list[Entity]:
        """Visit a pattern context."""
        entities: list[Entity] = []
        for part_ctx in ctx.oC_PatternPart() or []:
            part = self.visit(part_ctx)
            if isinstance(part, list):
                entities.extend(part)
            elif isinstance(part, Entity):
                entities.append(part)
        return entities

    def visit_oC_PatternPart(self, ctx: Any) -> list[Entity]:
        """Visit a pattern part context.

        Note: Path variable extraction is handled in visit_oC_Match for MatchClause.
        This method is used for other contexts that don't need path variables.
        """
        if ctx.oC_AnonymousPatternPart():
            return self.visit(ctx.oC_AnonymousPatternPart())
        return []

    def visit_oC_AnonymousPatternPart(self, ctx: Any) -> list[Entity]:
        """Visit an anonymous pattern part context."""
        return self.visit(ctx.oC_PatternElement())

    def visit_oC_PatternElement(self, ctx: Any) -> list[Entity]:
        """Visit a pattern element context."""
        entities: list[Entity] = []

        # Get the node pattern
        if ctx.oC_NodePattern():
            node = self.visit(ctx.oC_NodePattern())
            if isinstance(node, NodeEntity):
                entities.append(node)

        # Get the chain elements
        for chain_ctx in ctx.oC_PatternElementChain() or []:
            chain = self.visit(chain_ctx)
            if isinstance(chain, list):
                entities.extend(chain)

        # Handle nested pattern element
        if ctx.oC_PatternElement():
            nested = self.visit(ctx.oC_PatternElement())
            if isinstance(nested, list):
                entities.extend(nested)

        return entities

    def visit_oC_NodePattern(self, ctx: Any) -> NodeEntity:
        """Visit a node pattern context.

        Extracts node alias, labels, and inline property filters.

        Grammar:
            oC_NodePattern: '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )?
                           ( oC_Properties SP? )? ')' ;

        Example patterns:
            (n)                          # Just variable
            (n:Person)                   # Variable + label
            (n:Person {name: 'Alice'})   # Variable + label + properties
            (:Person {age: 30})          # Label + properties, no variable
        """
        alias = ""
        entity_name = ""
        inline_properties = None

        if ctx.oC_Variable():
            alias = unescape_symbolic_name(ctx.oC_Variable().getText())

        if ctx.oC_NodeLabels():
            labels = ctx.oC_NodeLabels().getText()
            # Backtick-aware split; take first label if multiple
            parsed_labels = split_escaped_labels(labels)
            if parsed_labels:
                entity_name = parsed_labels[0]

        # NEW: Extract inline properties if present
        if ctx.oC_Properties():
            # Visit oC_Properties (which can be oC_MapLiteral or oC_Parameter)
            props = self.visit(ctx.oC_Properties())
            # Only store if it's a map literal (not a parameter)
            if isinstance(props, QueryExpressionMapLiteral):
                inline_properties = props

        return NodeEntity(
            alias=alias,
            entity_name=entity_name,
            inline_properties=inline_properties
        )

    def visit_oC_PatternElementChain(self, ctx: Any) -> list[Entity]:
        """Visit a pattern element chain context."""
        entities: list[Entity] = []

        if ctx.oC_RelationshipPattern():
            rel = self.visit(ctx.oC_RelationshipPattern())
            if isinstance(rel, RelationshipEntity):
                entities.append(rel)

        if ctx.oC_NodePattern():
            node = self.visit(ctx.oC_NodePattern())
            if isinstance(node, NodeEntity):
                # Update relationship's connected entities
                if entities and isinstance(entities[-1], RelationshipEntity):
                    entities[-1].right_entity_name = node.entity_name
                entities.append(node)

        return entities

    def visit_oC_RelationshipPattern(self, ctx: Any) -> RelationshipEntity:
        """Visit a relationship pattern context.

        Extracts relationship alias, type, direction, variable-length path
        info, and inline property filters.

        Grammar:
            oC_RelationshipPattern: oC_LeftArrowHead? SP? oC_Dash
                                   oC_RelationshipDetail? oC_Dash SP?
                                   oC_RightArrowHead? ;
            oC_RelationshipDetail: '[' SP? oC_Variable? SP?
                                  oC_RelationshipTypes? SP?
                                  oC_RangeLiteral? SP?
                                  oC_Properties? SP? ']' ;

        Example patterns:
            -[r:KNOWS]->                              # Basic relationship
            -[r:KNOWS {since: 2020}]->                # With properties
            -[r:KNOWS*1..3]->                         # Variable-length
            -[r:KNOWS*1..3 {weight: 1.0}]->          # Var-length + props
        """
        # Determine direction
        text = ctx.getText()
        if "->" in text:
            direction = RelationshipDirection.FORWARD
        elif "<-" in text:
            direction = RelationshipDirection.BACKWARD
        else:
            direction = RelationshipDirection.BOTH

        # Get details
        detail_ctx = ctx.oC_RelationshipDetail() if ctx.oC_RelationshipDetail() else None

        alias = ""
        entity_name = ""
        min_hops = None
        max_hops = None
        inline_properties = None

        if detail_ctx:
            if detail_ctx.oC_Variable():
                alias = unescape_symbolic_name(detail_ctx.oC_Variable().getText())
            if detail_ctx.oC_RelationshipTypes():
                types_text = detail_ctx.oC_RelationshipTypes().getText()
                # Multi-type OR syntax (:A|B|:C) is kept joined by '|' — the
                # planner splits it — but each type is unescaped individually.
                entity_name = "|".join(
                    name
                    for part in types_text.split("|")
                    for name in split_escaped_labels(part)
                )

            # Extract variable-length path information (e.g., *1..5)
            if detail_ctx.oC_RangeLiteral():
                range_ctx = detail_ctx.oC_RangeLiteral()
                # Parse the range literal - format: *[min][..[max]]
                integers = range_ctx.oC_IntegerLiteral()
                if integers:
                    if len(integers) >= 1:
                        min_hops = int(integers[0].getText())
                    if len(integers) >= 2:
                        max_hops = int(integers[1].getText())
                    elif ".." in range_ctx.getText():
                        # *1.. means min_hops only, unlimited max
                        pass
                    else:
                        # *3 means exactly 3 hops (min=max=3)
                        max_hops = min_hops
                else:
                    # Just * means any number of hops (default 1..unlimited)
                    min_hops = 1

            # NEW: Extract inline properties if present
            if detail_ctx.oC_Properties():
                props = self.visit(detail_ctx.oC_Properties())
                if isinstance(props, QueryExpressionMapLiteral):
                    inline_properties = props

        return RelationshipEntity(
            alias=alias,
            entity_name=entity_name,
            direction=direction,
            min_hops=min_hops,
            max_hops=max_hops,
            inline_properties=inline_properties,
        )

    # =========================================================================
    # WHERE clause visitor
    # =========================================================================

    def visit_oC_Where(self, ctx: Any) -> QueryExpression | None:
        """Visit a WHERE clause context."""
        if ctx.oC_Expression():
            return self.visit(ctx.oC_Expression())
        return None

    # =========================================================================
    # RETURN clause visitors
    # =========================================================================

    def visit_oC_Return(self, ctx: Any) -> dict[str, Any]:
        """Visit a RETURN clause context."""
        result: dict[str, Any] = {}

        # New grammar uses oC_ProjectionBody directly in oC_Return
        if hasattr(ctx, "oC_ProjectionBody") and ctx.oC_ProjectionBody():
            body_result = self.visit(ctx.oC_ProjectionBody())
            if isinstance(body_result, dict):
                result.update(body_result)
        # Fallback for old grammar structure
        elif hasattr(ctx, "oC_ReturnBody") and ctx.oC_ReturnBody():
            body_result = self.visit(ctx.oC_ReturnBody())
            if isinstance(body_result, dict):
                result.update(body_result)

        return result

    def visit_oC_ProjectionBody(self, ctx: Any) -> dict[str, Any]:
        """Visit a projection body context (new grammar)."""
        result: dict[str, Any] = {}

        # Check for DISTINCT keyword
        text_children = [
            ctx.getChild(i).getText()
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
            and not hasattr(ctx.getChild(i), "getRuleIndex")
        ]
        result["distinct"] = any(t.upper() == "DISTINCT" for t in text_children)

        if ctx.oC_ProjectionItems():
            items = self.visit(ctx.oC_ProjectionItems())
            if isinstance(items, list):
                result["body"] = items

        if ctx.oC_Order():
            order = self.visit(ctx.oC_Order())
            if isinstance(order, list):
                result["order"] = order

        if ctx.oC_Limit():
            limit = self.visit(ctx.oC_Limit())
            result["limit"] = LimitClause(limit_expression=limit)

        if ctx.oC_Skip():
            skip = self.visit(ctx.oC_Skip())
            if "limit" in result:
                result["limit"].skip_expression = skip
            else:
                result["limit"] = LimitClause(
                    limit_expression=QueryExpressionValue(
                        value=None, value_type=type(None)
                    ),
                    skip_expression=skip,
                )

        return result

    def visit_oC_ReturnBody(self, ctx: Any) -> dict[str, Any]:
        """Visit a return body context (legacy)."""
        result: dict[str, Any] = {}

        if ctx.oC_ReturnItems():
            items = self.visit(ctx.oC_ReturnItems())
            if isinstance(items, list):
                result["body"] = items

        if ctx.oC_Order():
            order = self.visit(ctx.oC_Order())
            if isinstance(order, list):
                result["order"] = order

        if ctx.oC_Limit():
            limit = self.visit(ctx.oC_Limit())
            result["limit"] = LimitClause(limit_expression=limit)

        if ctx.oC_Skip():
            skip = self.visit(ctx.oC_Skip())
            if "limit" in result:
                result["limit"].skip_expression = skip
            else:
                result["limit"] = LimitClause(
                    limit_expression=QueryExpressionValue(
                        value=None, value_type=type(None)
                    ),
                    skip_expression=skip,
                )

        return result

    def visit_oC_ProjectionItems(self, ctx: Any) -> list[QueryExpressionWithAlias]:
        """Visit projection items context."""
        items: list[QueryExpressionWithAlias] = []

        for item_ctx in ctx.oC_ProjectionItem() or []:
            item = self.visit(item_ctx)
            if isinstance(item, QueryExpressionWithAlias):
                items.append(item)

        return items

    def visit_oC_ProjectionItem(self, ctx: Any) -> QueryExpressionWithAlias | None:
        """Visit a projection item context."""
        expr = self.visit(ctx.oC_Expression())

        if not isinstance(expr, QueryExpression):
            return None

        alias = None
        if ctx.oC_Variable():
            alias = unescape_symbolic_name(ctx.oC_Variable().getText())
        elif isinstance(expr, QueryExpressionProperty):
            alias = expr.property_name or expr.variable_name
        else:
            # No explicit AS and not a property access (e.g. `RETURN count(*)`).
            # openCypher names such a column after the expression text; an
            # empty alias would emit `SELECT COUNT(*) AS  FROM ...`, which is a
            # SQL syntax error surfacing only at execution time.
            alias = sanitize_expression_alias(ctx.oC_Expression().getText())

        return QueryExpressionWithAlias(inner_expression=expr, alias=alias or "")

    def visit_oC_ReturnItems(self, ctx: Any) -> list[QueryExpressionWithAlias]:
        """Visit return items context (legacy)."""
        items: list[QueryExpressionWithAlias] = []

        for item_ctx in ctx.oC_ReturnItem() or []:
            item = self.visit(item_ctx)
            if isinstance(item, QueryExpressionWithAlias):
                items.append(item)

        return items

    def visit_oC_ReturnItem(self, ctx: Any) -> QueryExpressionWithAlias | None:
        """Visit a return item context (legacy)."""
        expr = self.visit(ctx.oC_Expression())

        if not isinstance(expr, QueryExpression):
            return None

        # Get alias if present
        alias = ""
        if ctx.oC_Variable():
            alias = unescape_symbolic_name(ctx.oC_Variable().getText())
        elif isinstance(expr, QueryExpressionProperty):
            alias = expr.property_name or expr.variable_name
        else:
            alias = str(expr)

        return QueryExpressionWithAlias(inner_expression=expr, alias=alias)

    def visit_oC_Order(self, ctx: Any) -> list[SortItem]:
        """Visit an ORDER BY clause context."""
        items: list[SortItem] = []

        for sort_ctx in ctx.oC_SortItem() or []:
            item = self.visit(sort_ctx)
            if isinstance(item, SortItem):
                items.append(item)

        return items

    def visit_oC_SortItem(self, ctx: Any) -> SortItem:
        """Visit a sort item context."""
        expr = self.visit(ctx.oC_Expression())

        # Check for DESC
        text_children = [
            ctx.getChild(i).getText()
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
        ]
        order = (
            SortOrder.DESC
            if any(t.upper() in ("DESC", "DESCENDING") for t in text_children)
            else SortOrder.ASC
        )

        if not isinstance(expr, QueryExpression):
            expr = QueryExpressionValue(value=None, value_type=type(None))

        return SortItem(expression=expr, order=order)

    def visit_oC_Limit(self, ctx: Any) -> QueryExpression:
        """Visit a LIMIT clause context."""
        return self.visit(ctx.oC_Expression())

    def visit_oC_Skip(self, ctx: Any) -> QueryExpression:
        """Visit a SKIP clause context."""
        return self.visit(ctx.oC_Expression())

    # =========================================================================
    # Expression visitors
    # =========================================================================

    def visit_oC_Expression(self, ctx: Any) -> QueryExpression:
        """Visit an expression context."""
        return self.visit(ctx.oC_OrExpression())

    def visit_oC_OrExpression(self, ctx: Any) -> QueryExpression:
        """Visit an OR expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_XorExpression(), "or")

    def visit_oC_XorExpression(self, ctx: Any) -> QueryExpression:
        """Visit an XOR expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_AndExpression(), "xor")

    def visit_oC_AndExpression(self, ctx: Any) -> QueryExpression:
        """Visit an AND expression context."""
        return self._visit_binary_expression(ctx, ctx.oC_NotExpression(), "and")

    def visit_oC_NotExpression(self, ctx: Any) -> QueryExpression:
        """Visit a NOT expression context."""
        # Check if NOT is present
        text = ctx.getText()
        has_not = text.upper().startswith("NOT")

        inner = self.visit(ctx.oC_ComparisonExpression())

        if has_not and isinstance(inner, QueryExpression):
            return QueryExpressionFunction(
                function=Function.NOT,
                parameters=[inner],
                data_type=bool,
            )

        return inner

    def visit_oC_ComparisonExpression(self, ctx: Any) -> QueryExpression:
        """Visit a comparison expression context."""
        # New grammar uses oC_StringListNullPredicateExpression
        if hasattr(ctx, "oC_StringListNullPredicateExpression"):
            left = self.visit(ctx.oC_StringListNullPredicateExpression())
        else:
            # Fallback for old grammar
            left = self.visit(ctx.oC_AddOrSubtractExpression())

        # Handle partial comparison expressions
        if ctx.oC_PartialComparisonExpression():
            for partial_ctx in ctx.oC_PartialComparisonExpression():
                op_text = ""
                for i in range(partial_ctx.getChildCount()):
                    child = partial_ctx.getChild(i)
                    if not hasattr(child, "getRuleIndex"):
                        op_text = child.getText()
                        break

                # Get the right side from partial comparison
                if hasattr(partial_ctx, "oC_StringListNullPredicateExpression"):
                    right = self.visit(
                        partial_ctx.oC_StringListNullPredicateExpression()
                    )
                else:
                    right = self.visit(partial_ctx.oC_AddOrSubtractExpression())

                op_info = try_get_operator(op_text)

                if op_info and isinstance(right, QueryExpression):
                    left = QueryExpressionBinary(
                        operator=op_info,
                        left_expression=(
                            left if isinstance(left, QueryExpression) else None
                        ),
                        right_expression=right,
                    )

        return left

    def visit_oC_StringListNullPredicateExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a string/list/null predicate expression context."""
        result = self.visit(ctx.oC_AddOrSubtractExpression())

        # Handle IS NULL, IS NOT NULL, IN, etc.
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if hasattr(child, "getRuleIndex"):
                child_name = child.__class__.__name__
                if "NullPredicateExpression" in child_name:
                    # IS NULL or IS NOT NULL
                    text = child.getText().upper()
                    if "NOT" in text:
                        result = QueryExpressionFunction(
                            function=Function.IS_NOT_NULL,
                            parameters=(
                                [result]
                                if isinstance(result, QueryExpression)
                                else []
                            ),
                            data_type=bool,
                        )
                    else:
                        result = QueryExpressionFunction(
                            function=Function.IS_NULL,
                            parameters=(
                                [result]
                                if isinstance(result, QueryExpression)
                                else []
                            ),
                            data_type=bool,
                        )
                elif "StringPredicateExpression" in child_name:
                    # STARTS WITH, ENDS WITH, CONTAINS
                    # Grammar: ( SP STARTS SP WITH | SP ENDS SP WITH | SP CONTAINS ) SP? oC_AddOrSubtractExpression
                    right_expr = self.visit(child.oC_AddOrSubtractExpression())
                    text = child.getText().upper()
                    if "STARTSWITH" in text or "STARTS" in text:
                        func = Function.STRING_STARTS_WITH
                    elif "ENDSWITH" in text or "ENDS" in text:
                        func = Function.STRING_ENDS_WITH
                    else:
                        func = Function.STRING_CONTAINS
                    params: list[QueryExpression] = []
                    if isinstance(result, QueryExpression):
                        params.append(result)
                    if isinstance(right_expr, QueryExpression):
                        params.append(right_expr)
                    result = QueryExpressionFunction(
                        function=func,
                        parameters=params,
                        data_type=bool,
                    )
                elif "ListPredicateExpression" in child_name:
                    # IN operator: a.id IN $watchlist or a.id IN [1, 2, 3]
                    # The child contains: SP IN SP? oC_AddOrSubtractExpression
                    right_expr = self.visit(child.oC_AddOrSubtractExpression())
                    if isinstance(right_expr, QueryExpression):
                        op_info = try_get_operator("in")
                        result = QueryExpressionBinary(
                            operator=op_info,
                            left_expression=(
                                result
                                if isinstance(result, QueryExpression)
                                else None
                            ),
                            right_expression=right_expr,
                        )

        return result

    def visit_oC_AddOrSubtractExpression(self, ctx: Any) -> QueryExpression:
        """Visit an add/subtract expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_MultiplyDivideModuloExpression(), ["+", "-"]
        )

    def visit_oC_MultiplyDivideModuloExpression(self, ctx: Any) -> QueryExpression:
        """Visit a multiply/divide/modulo expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_PowerOfExpression(), ["*", "/", "%"]
        )

    def visit_oC_PowerOfExpression(self, ctx: Any) -> QueryExpression:
        """Visit a power expression context."""
        return self._visit_chained_binary_expression(
            ctx, ctx.oC_UnaryAddOrSubtractExpression(), ["^"]
        )

    def visit_oC_UnaryAddOrSubtractExpression(self, ctx: Any) -> QueryExpression:
        """Visit a unary add/subtract expression context."""
        # New grammar uses oC_NonArithmeticOperatorExpression
        if hasattr(ctx, "oC_NonArithmeticOperatorExpression"):
            inner = self.visit(ctx.oC_NonArithmeticOperatorExpression())
        # Old grammar uses oC_StringListNullOperatorExpression
        elif hasattr(ctx, "oC_StringListNullOperatorExpression"):
            inner = self.visit(ctx.oC_StringListNullOperatorExpression())
        else:
            inner = self.visit_children(ctx)

        # Check for unary operator
        text = ctx.getText()
        if text.startswith("-") and isinstance(inner, QueryExpression):
            return QueryExpressionFunction(
                function=Function.NEGATIVE,
                parameters=[inner],
            )

        return inner

    def visit_oC_NonArithmeticOperatorExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a non-arithmetic operator expression context (new grammar).

        Grammar:
            oC_Atom ( oC_ListOperatorExpression | oC_PropertyLookup )*

        Postfix operators are applied left-to-right in source order:
        ``nodes(p)[0..-1]`` wraps the nodes() call in a LIST_SLICE,
        ``xs[0].name`` indexes first, then looks up the property.
        """
        result = self.visit(ctx.oC_Atom())

        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            rule_name = type(child).__name__
            if rule_name == "OC_PropertyLookupContext":
                prop_name = self.visit(child)
                if isinstance(result, QueryExpressionProperty) and prop_name:
                    result.property_name = prop_name
            elif rule_name == "OC_ListOperatorExpressionContext":
                result = self._apply_list_operator(result, child)

        return result

    def _apply_list_operator(
        self, target: QueryExpression, ctx: Any
    ) -> QueryExpression:
        """Wrap *target* in LIST_INDEX / LIST_SLICE from a ``[...]`` postfix.

        Grammar:
            ( '[' oC_Expression ']' )                      -> index
            | ( '[' oC_Expression? '..' oC_Expression? ']' ) -> slice

        Slices always carry exactly 3 parameters (list, start, end);
        omitted bounds become NULL literals so the renderer can
        distinguish ``[..b]`` / ``[a..]`` / ``[a..b]`` positionally.
        """
        exprs = [self.visit(e) for e in ctx.oC_Expression()]
        is_slice = ".." in ctx.getText()

        if not is_slice:
            return QueryExpressionFunction(
                function=Function.LIST_INDEX,
                parameters=[target, exprs[0]],
            )

        # Positional bounds: figure out which side of '..' each expr is on.
        start: QueryExpression = QueryExpressionValue(
            value=None, value_type=type(None)
        )
        end: QueryExpression = QueryExpressionValue(
            value=None, value_type=type(None)
        )
        dotdot_seen = False
        expr_iter = iter(exprs)
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            text = child.getText() if hasattr(child, "getText") else ""
            if text == "..":
                dotdot_seen = True
            elif type(child).__name__ == "OC_ExpressionContext":
                value = next(expr_iter)
                if dotdot_seen:
                    end = value
                else:
                    start = value

        return QueryExpressionFunction(
            function=Function.LIST_SLICE,
            parameters=[target, start, end],
        )

    def visit_oC_PropertyLookup(self, ctx: Any) -> str | None:
        """Visit a property lookup context."""
        if ctx.oC_PropertyKeyName():
            return unescape_symbolic_name(ctx.oC_PropertyKeyName().getText())
        return None

    def visit_oC_StringListNullOperatorExpression(
        self, ctx: Any
    ) -> QueryExpression:
        """Visit a string/list/null operator expression context (old grammar)."""
        result = self.visit(ctx.oC_PropertyOrLabelsExpression())

        # Handle IS NULL / IS NOT NULL
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if hasattr(child, "getText"):
                text = child.getText().upper()
                if "IS" in text and "NULL" in text:
                    if "NOT" in text:
                        result = QueryExpressionFunction(
                            function=Function.IS_NOT_NULL,
                            parameters=[result] if isinstance(result, QueryExpression) else [],
                        )
                    else:
                        result = QueryExpressionFunction(
                            function=Function.IS_NULL,
                            parameters=[result] if isinstance(result, QueryExpression) else [],
                        )

        return result

    def visit_oC_PropertyOrLabelsExpression(self, ctx: Any) -> QueryExpression:
        """Visit a property or labels expression context."""
        result = self.visit(ctx.oC_Atom())

        # Handle property lookups
        for lookup_ctx in ctx.oC_PropertyLookup() or []:
            prop_name = unescape_symbolic_name(
                lookup_ctx.oC_PropertyKeyName().getText()
            )
            if isinstance(result, QueryExpressionProperty):
                result.property_name = prop_name
            elif isinstance(result, QueryExpression):
                result = QueryExpressionProperty(
                    variable_name=str(result),
                    property_name=prop_name,
                )

        return result

    # =========================================================================
    # Atom visitors
    # =========================================================================

    def visit_oC_Atom(self, ctx: Any) -> QueryExpression:
        """Visit an atom context.

        Grammar:
        oC_Atom
            :  oC_Literal
            | oC_Parameter
            | oC_CaseExpression
            | ( COUNT SP? '(' SP? '*' SP? ')' )
            | oC_ListComprehension
            | oC_PatternComprehension
            | oC_Quantifier
            | oC_Reduce
            | oC_PatternPredicate
            | oC_ParenthesizedExpression
            | oC_FunctionInvocation
            | oC_ExistentialSubquery
            | oC_Variable
            ;
        """
        if ctx.oC_Literal():
            return self.visit(ctx.oC_Literal())
        if ctx.oC_Parameter():
            return self.visit(ctx.oC_Parameter())
        if ctx.oC_Variable():
            return QueryExpressionProperty(
                variable_name=unescape_symbolic_name(ctx.oC_Variable().getText())
            )
        if ctx.oC_FunctionInvocation():
            return self.visit(ctx.oC_FunctionInvocation())
        if ctx.oC_ParenthesizedExpression():
            return self.visit(ctx.oC_ParenthesizedExpression())
        if ctx.oC_CaseExpression():
            return self.visit(ctx.oC_CaseExpression())
        if ctx.oC_ListComprehension():
            return self.visit(ctx.oC_ListComprehension())
        if ctx.oC_Quantifier():
            return self.visit(ctx.oC_Quantifier())
        if ctx.oC_Reduce():
            return self.visit(ctx.oC_Reduce())
        if ctx.oC_ExistentialSubquery():
            return self.visit(ctx.oC_ExistentialSubquery())
        if ctx.oC_PatternPredicate():
            return self.visit_oC_PatternPredicate(ctx.oC_PatternPredicate())
        # Check for COUNT(*) - it's a token, not a rule
        if hasattr(ctx, "COUNT") and ctx.COUNT():
            return QueryExpressionAggregationFunction(
                aggregation_function=try_parse_aggregation_function("count")
                or AggregationFunction.COUNT,
            )

        # Default: return a property with the text
        return QueryExpressionProperty(variable_name=ctx.getText())

    def visit_oC_Literal(self, ctx: Any) -> QueryExpressionValue:
        """Visit a literal context."""
        if ctx.oC_NumberLiteral():
            return self.visit(ctx.oC_NumberLiteral())
        if ctx.StringLiteral():
            text = ctx.StringLiteral().getText()
            # Remove quotes
            if (text.startswith("'") and text.endswith("'")) or (
                text.startswith('"') and text.endswith('"')
            ):
                text = text[1:-1]
            return QueryExpressionValue(value=text, value_type=str)
        if ctx.oC_BooleanLiteral():
            text = ctx.oC_BooleanLiteral().getText().lower()
            return QueryExpressionValue(value=text == "true", value_type=bool)
        if ctx.getText().upper() == "NULL":
            return QueryExpressionValue(value=None, value_type=type(None))
        if ctx.oC_ListLiteral():
            return self.visit(ctx.oC_ListLiteral())
        if ctx.oC_MapLiteral():
            return self.visit(ctx.oC_MapLiteral())

        return QueryExpressionValue(value=ctx.getText(), value_type=str)

    def visit_oC_NumberLiteral(self, ctx: Any) -> QueryExpressionValue:
        """Visit a number literal context."""
        if ctx.oC_IntegerLiteral():
            text = ctx.oC_IntegerLiteral().getText()
            int_value = int(text)
            return QueryExpressionValue(value=int_value, value_type=int)
        if ctx.oC_DoubleLiteral():
            text = ctx.oC_DoubleLiteral().getText()
            float_value = float(text)
            return QueryExpressionValue(value=float_value, value_type=float)

        return QueryExpressionValue(value=0, value_type=int)

    def visit_oC_ListLiteral(self, ctx: Any) -> QueryExpressionList:
        """Visit a list literal context."""
        items: list[QueryExpression] = []
        for expr_ctx in ctx.oC_Expression() or []:
            expr = self.visit(expr_ctx)
            if isinstance(expr, QueryExpression):
                items.append(expr)
        return QueryExpressionList(items=items)

    def visit_oC_MapLiteral(self, ctx: Any) -> QueryExpressionMapLiteral:
        """Visit a map literal context.

        Grammar:
        oC_MapLiteral
            :  '{' SP? ( oC_PropertyKeyName SP? ':' SP? oC_Expression SP?
               ( ',' SP? oC_PropertyKeyName SP? ':' SP? oC_Expression SP? )* )? '}' ;

        Examples:
        - {name: 'John', age: 30}
        - {year: 2024, month: 1, day: 15}
        - {}  (empty map)
        """
        entries: list[tuple[str, QueryExpression]] = []

        # Get all property key names and expressions
        key_ctxs = ctx.oC_PropertyKeyName() or []
        expr_ctxs = ctx.oC_Expression() or []

        # Pair them up
        for key_ctx, expr_ctx in zip(key_ctxs, expr_ctxs):
            key_name = unescape_symbolic_name(key_ctx.getText())
            value_expr = self.visit(expr_ctx)
            if isinstance(value_expr, QueryExpression):
                entries.append((key_name, value_expr))

        return QueryExpressionMapLiteral(entries=entries)

    def visit_oC_FunctionInvocation(self, ctx: Any) -> QueryExpression:
        """Visit a function invocation context.

        Grammar:
        oC_FunctionInvocation
            :  oC_FunctionName SP? '(' SP? ( DISTINCT SP? )?
               ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )?
               ( SP? ORDER SP BY SP oC_SortItem ( SP? ',' SP? oC_SortItem )* )? ')' ;
        """
        func_name = ctx.oC_FunctionName().getText()

        # Check for DISTINCT
        is_distinct = any(
            ctx.getChild(i).getText().upper() == "DISTINCT"
            for i in range(ctx.getChildCount())
            if hasattr(ctx.getChild(i), "getText")
        )

        # Get parameters
        params: list[QueryExpression] = []
        for expr_ctx in ctx.oC_Expression() or []:
            expr = self.visit(expr_ctx)
            if isinstance(expr, QueryExpression):
                params.append(expr)

        # Parse ORDER BY for ordered aggregation (e.g., COLLECT(x ORDER BY y DESC))
        order_by: list[tuple[QueryExpression, bool]] = []
        sort_items = ctx.oC_SortItem() if hasattr(ctx, "oC_SortItem") else None
        if sort_items:
            for sort_item in sort_items if isinstance(sort_items, list) else [sort_items]:
                sort_expr = self.visit(sort_item.oC_Expression())
                is_desc = False
                # Check for DESC keyword
                if sort_item.DESC() or sort_item.DESCENDING():
                    is_desc = True
                if isinstance(sort_expr, QueryExpression):
                    order_by.append((sort_expr, is_desc))

        # Check if it's an aggregation function
        agg_func = try_parse_aggregation_function(func_name)
        if agg_func:
            return QueryExpressionAggregationFunction(
                aggregation_function=agg_func,
                is_distinct=is_distinct,
                inner_expression=params[0] if params else None,
                order_by=order_by,
            )

        # Check if it's a regular function
        func_info = try_get_function(func_name)
        if func_info:
            return QueryExpressionFunction(
                function=func_info.function_name,
                parameters=params,
            )

        # Unknown function - return as generic function
        return QueryExpressionFunction(
            function=Function.INVALID,
            parameters=params,
        )

    def visit_oC_ParenthesizedExpression(self, ctx: Any) -> QueryExpression:
        """Visit a parenthesized expression context."""
        return self.visit(ctx.oC_Expression())

    def visit_oC_CaseExpression(self, ctx: Any) -> QueryExpressionCaseExpression:
        """Visit a CASE expression context.

        Grammar:
        oC_CaseExpression:
            ( CASE ( oC_CaseAlternative )+ )                    # Simple CASE
            | ( CASE oC_Expression ( oC_CaseAlternative )+ )    # Searched CASE
            ( ELSE oC_Expression )? END

        oC_CaseAlternative: WHEN oC_Expression THEN oC_Expression
        """
        test_expr = None
        alternatives: list[tuple[QueryExpression, QueryExpression]] = []
        else_expr = None

        # Get all expressions - first one might be test expression
        exprs = ctx.oC_Expression() or []

        # Get all CASE alternatives (WHEN/THEN pairs)
        case_alternatives = ctx.oC_CaseAlternative() or []

        # If we have expressions AND case alternatives, first expression is test
        # Otherwise it's a simple CASE (no test expression)
        if exprs and case_alternatives:
            # Check if first expression is test expression or ELSE
            # ELSE expression comes after all alternatives, so if we have
            # more alternatives than expressions, first expr is test
            if len(exprs) == 1:
                # Only one expression - could be ELSE
                else_expr = self.visit(exprs[0])
            elif len(exprs) == 2:
                # Two expressions - first is test, second is ELSE
                test_expr = self.visit(exprs[0])
                else_expr = self.visit(exprs[1])
            else:
                # Multiple - first is test, last is ELSE
                test_expr = self.visit(exprs[0])
                else_expr = self.visit(exprs[-1])
        elif exprs and not case_alternatives:
            # No alternatives - this shouldn't happen in valid CASE
            pass

        # Process WHEN/THEN pairs from oC_CaseAlternative
        for alt in case_alternatives:
            alt_exprs = alt.oC_Expression() or []
            if len(alt_exprs) >= 2:
                when_expr = self.visit(alt_exprs[0])
                then_expr = self.visit(alt_exprs[1])
                if isinstance(when_expr, QueryExpression) and isinstance(
                    then_expr, QueryExpression
                ):
                    alternatives.append((when_expr, then_expr))

        return QueryExpressionCaseExpression(
            test_expression=test_expr,
            alternatives=alternatives,
            else_expression=else_expr,
        )

    def visit_oC_ExistentialSubquery(self, ctx: Any) -> QueryExpressionExists:
        """Visit an existential subquery context.

        Grammar:
        oC_ExistentialSubquery
            :  EXISTS SP? '{' SP? ( oC_RegularQuery | ( oC_Pattern ( SP? oC_Where )? ) ) SP? '}' ;

        This represents EXISTS { pattern } or EXISTS { MATCH ... RETURN ... } constructs.
        """
        pattern_entities: list[Entity] = []
        where_expression: QueryExpression | None = None
        subquery: QueryNode | None = None

        # Check for full subquery (oC_RegularQuery)
        if ctx.oC_RegularQuery():
            subquery = self.visit(ctx.oC_RegularQuery())
        # Check for pattern-based EXISTS
        elif ctx.oC_Pattern():
            pattern_entities = self.visit(ctx.oC_Pattern())
            if isinstance(pattern_entities, Entity):
                pattern_entities = [pattern_entities]
            elif not isinstance(pattern_entities, list):
                pattern_entities = []

            # Check for optional WHERE clause
            if ctx.oC_Where():
                where_result = self.visit(ctx.oC_Where())
                if isinstance(where_result, QueryExpression):
                    where_expression = where_result

        return QueryExpressionExists(
            pattern_entities=pattern_entities,
            where_expression=where_expression,
            is_negated=False,  # NOT is handled in expression visitor
            subquery=subquery,
        )

    def visit_oC_PatternPredicate(self, ctx: Any) -> QueryExpressionExists:
        """Visit a pattern predicate context.

        Grammar:
        oC_PatternPredicate: oC_RelationshipsPattern ;

        In OpenCypher, a pattern in a boolean context (e.g., WHERE clause) represents
        an implicit EXISTS check. For example:
            WHERE (c)-[:HAS_LOAN]->(:Loan)
        is equivalent to:
            WHERE EXISTS { (c)-[:HAS_LOAN]->(:Loan) }

        This method converts the pattern into a QueryExpressionExists node.
        """
        pattern_entities: list[Entity] = []

        # Visit the relationships pattern to extract entities
        # oC_RelationshipsPattern: oC_NodePattern ( SP? oC_PatternElementChain )+
        if ctx.oC_RelationshipsPattern():
            rel_ctx = ctx.oC_RelationshipsPattern()

            # Get the starting node pattern
            if rel_ctx.oC_NodePattern():
                node = self.visit(rel_ctx.oC_NodePattern())
                if isinstance(node, NodeEntity):
                    pattern_entities.append(node)

            # Get the chain elements (relationships and nodes)
            for chain_ctx in rel_ctx.oC_PatternElementChain() or []:
                chain = self.visit(chain_ctx)
                if isinstance(chain, list):
                    pattern_entities.extend(chain)
                elif isinstance(chain, Entity):
                    pattern_entities.append(chain)

        # Create an EXISTS expression with the pattern
        # The NOT operator (if present) is handled by the parent expression visitor
        return QueryExpressionExists(
            pattern_entities=pattern_entities,
            where_expression=None,
            is_negated=False,
            subquery=None,
        )

    def visit_oC_Parameter(self, ctx: Any) -> QueryExpressionParameter:
        """Visit a parameter context.

        Grammar:
        oC_Parameter: '$' ( oC_SymbolicName | DecimalInteger ) ;

        Handles parameter syntax like $watchlist, $1, etc.
        """
        # Get the full text and remove the '$' prefix
        text = ctx.getText()
        param_name = text[1:] if text.startswith("$") else text
        return QueryExpressionParameter(parameter_name=param_name)

    def visit_oC_Quantifier(self, ctx: Any) -> QueryExpressionListPredicate:
        """Visit a quantifier expression context.

        Grammar:
        oC_Quantifier: ( ALL SP? '(' SP? oC_FilterExpression SP? ')' )
                     | ( ANY SP? '(' SP? oC_FilterExpression SP? ')' )
                     | ( NONE SP? '(' SP? oC_FilterExpression SP? ')' )
                     | ( SINGLE SP? '(' SP? oC_FilterExpression SP? ')' );

        oC_FilterExpression: oC_IdInColl ( SP? oC_Where )? ;
        oC_IdInColl: oC_Variable SP IN SP oC_Expression ;

        Examples:
        - ALL(x IN list WHERE x > 0)
        - ANY(x IN accounts WHERE x.balance < 0)
        - NONE(x IN transactions WHERE x.suspicious)
        - SINGLE(x IN matches WHERE x.valid)
        """
        # Determine predicate type from the keyword
        text = ctx.getText().upper()
        if text.startswith("ALL"):
            predicate_type = ListPredicateType.ALL
        elif text.startswith("ANY"):
            predicate_type = ListPredicateType.ANY
        elif text.startswith("NONE"):
            predicate_type = ListPredicateType.NONE
        elif text.startswith("SINGLE"):
            predicate_type = ListPredicateType.SINGLE
        else:
            predicate_type = ListPredicateType.ANY  # fallback

        # Get the filter expression
        filter_expr_ctx = ctx.oC_FilterExpression()
        if not filter_expr_ctx:
            raise TranspilerSyntaxErrorException(
                "Quantifier requires a filter expression"
            )

        # Parse oC_IdInColl: oC_Variable SP IN SP oC_Expression
        id_in_coll_ctx = filter_expr_ctx.oC_IdInColl()
        if not id_in_coll_ctx:
            raise TranspilerSyntaxErrorException(
                "Quantifier requires variable IN list syntax"
            )

        variable_name = id_in_coll_ctx.oC_Variable().getText()
        list_expression = self.visit(id_in_coll_ctx.oC_Expression())

        # Parse optional WHERE clause
        filter_expression: QueryExpression | None = None
        where_ctx = filter_expr_ctx.oC_Where()
        if where_ctx:
            filter_expression = self.visit(where_ctx)

        return QueryExpressionListPredicate(
            predicate_type=predicate_type,
            variable_name=variable_name,
            list_expression=list_expression,
            filter_expression=filter_expression,
        )

    def visit_oC_ListComprehension(self, ctx: Any) -> QueryExpressionListComprehension:
        """Visit a list comprehension context.

        Grammar:
        oC_ListComprehension
            :  '[' SP? oC_FilterExpression ( SP? '|' SP? oC_Expression )? SP? ']' ;

        oC_FilterExpression: oC_IdInColl ( SP? oC_Where )? ;
        oC_IdInColl: oC_Variable SP IN SP oC_Expression ;

        Examples:
        - [x IN list] -> just the list (identity)
        - [x IN list WHERE x > 0] -> FILTER(list, x -> x > 0)
        - [x IN list | x * 2] -> TRANSFORM(list, x -> x * 2)
        - [x IN list WHERE x > 0 | x * 2] -> TRANSFORM(FILTER(list, x -> x > 0), x -> x * 2)
        """
        # Get the filter expression (contains variable IN list WHERE pred)
        filter_expr_ctx = ctx.oC_FilterExpression()
        if not filter_expr_ctx:
            raise TranspilerSyntaxErrorException(
                "List comprehension requires a filter expression"
            )

        # Parse oC_IdInColl: oC_Variable SP IN SP oC_Expression
        id_in_coll_ctx = filter_expr_ctx.oC_IdInColl()
        if not id_in_coll_ctx:
            raise TranspilerSyntaxErrorException(
                "List comprehension requires variable IN list syntax"
            )

        variable_name = id_in_coll_ctx.oC_Variable().getText()
        list_expression = self.visit(id_in_coll_ctx.oC_Expression())

        # Parse optional WHERE clause (filter)
        filter_expression: QueryExpression | None = None
        where_ctx = filter_expr_ctx.oC_Where()
        if where_ctx:
            filter_expression = self.visit(where_ctx)

        # Parse optional map expression (after |)
        map_expression: QueryExpression | None = None
        # The map expression is the oC_Expression directly under oC_ListComprehension
        # (not the one inside oC_FilterExpression)
        exprs = ctx.oC_Expression()
        if exprs:
            # The grammar has oC_Expression after the |
            map_expression = self.visit(exprs)

        return QueryExpressionListComprehension(
            variable_name=variable_name,
            list_expression=list_expression,
            filter_expression=filter_expression,
            map_expression=map_expression,
        )

    def visit_oC_Reduce(self, ctx: Any) -> QueryExpressionReduce:
        """Visit a REDUCE expression context.

        Grammar:
        oC_Reduce
            :  REDUCE SP? '(' SP? oC_Variable SP? '=' SP? oC_Expression SP? ','
               SP? oC_IdInColl SP? '|' SP? oC_Expression SP? ')' ;

        oC_IdInColl: oC_Variable SP IN SP oC_Expression ;

        Example:
        REDUCE(total = 0, x IN amounts | total + x)
        -> AGGREGATE(amounts, 0, (total, x) -> total + x)
        """
        # Get accumulator variable (the first oC_Variable in the rule)
        acc_var_ctx = ctx.oC_Variable()
        if not acc_var_ctx:
            raise TranspilerSyntaxErrorException(
                "REDUCE requires an accumulator variable"
            )
        accumulator_name = unescape_symbolic_name(acc_var_ctx.getText())

        # Get all expressions - there are 2 in the main rule:
        # 1. Initial value (after =)
        # 2. Reducer expression (after |)
        # The list expression is inside oC_IdInColl
        expressions = ctx.oC_Expression()
        if not expressions or len(expressions) < 2:
            raise TranspilerSyntaxErrorException(
                "REDUCE requires initial value and reducer expression"
            )

        initial_value = self.visit(expressions[0])
        reducer_expression = self.visit(expressions[1])

        # Get oC_IdInColl for variable IN list
        id_in_coll_ctx = ctx.oC_IdInColl()
        if not id_in_coll_ctx:
            raise TranspilerSyntaxErrorException(
                "REDUCE requires variable IN list syntax"
            )

        variable_name = id_in_coll_ctx.oC_Variable().getText()
        list_expression = self.visit(id_in_coll_ctx.oC_Expression())

        return QueryExpressionReduce(
            accumulator_name=accumulator_name,
            initial_value=initial_value,
            variable_name=variable_name,
            list_expression=list_expression,
            reducer_expression=reducer_expression,
        )

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _visit_binary_expression(
        self,
        ctx: Any,
        sub_expressions: list[Any],
        operator_keyword: str,
    ) -> QueryExpression:
        """Visit a binary expression with a single operator type."""
        if not sub_expressions:
            return QueryExpressionValue(value=None, value_type=type(None))

        result = self.visit(sub_expressions[0])

        if len(sub_expressions) > 1:
            op_info = try_get_operator(operator_keyword)
            for sub_expr in sub_expressions[1:]:
                right = self.visit(sub_expr)
                if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                    result = QueryExpressionBinary(
                        operator=op_info,
                        left_expression=result,
                        right_expression=right,
                    )

        return result

    def _visit_chained_binary_expression(
        self,
        ctx: Any,
        sub_expressions: list[Any],
        operators: list[str],
    ) -> QueryExpression:
        """Visit a chained binary expression with multiple operator types."""
        if not sub_expressions:
            return QueryExpressionValue(value=None, value_type=type(None))

        result = self.visit(sub_expressions[0])

        # Find operators in the children
        op_index = 0
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if not hasattr(child, "getRuleIndex"):
                text = child.getText()
                if text in operators and op_index < len(sub_expressions) - 1:
                    op_index += 1
                    right = self.visit(sub_expressions[op_index])
                    op_info = try_get_operator(text)
                    if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                        result = QueryExpressionBinary(
                            operator=op_info,
                            left_expression=result,
                            right_expression=right,
                        )

        # If we didn't find operators in children, just visit the rest
        if op_index == 0:
            for i, sub_expr in enumerate(sub_expressions[1:], 1):
                right = self.visit(sub_expr)
                if isinstance(right, QueryExpression) and isinstance(result, QueryExpression):
                    result = QueryExpressionBinary(
                        operator=try_get_operator(operators[0] if operators else "+"),
                        left_expression=result,
                        right_expression=right,
                    )

        return result
