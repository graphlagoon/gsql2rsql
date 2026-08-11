"""Expression renderer — all expression-to-SQL translation logic.

Handles rendering of QueryExpression subtypes to Databricks SQL strings:
literal values, property access, binary operators, function calls,
aggregations, list predicates, CASE, REDUCE, EXISTS sub-queries,
entity-as-struct rendering, date/time/duration helpers, etc.
"""

from __future__ import annotations


from gsql2rsql.common.exceptions import (
    TranspilerInternalErrorException,
    TranspilerNotSupportedException,
    TranspilerSyntaxErrorException,
)
from gsql2rsql.parser.ast import (
    NodeEntity,
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
    RelationshipEntity,
)
from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    Function,
    ListPredicateType,
)
from gsql2rsql.planner.column_ref import ResolvedProjection
from gsql2rsql.planner.operators import (
    LogicalOperator,
    ProjectionOperator,
)
from gsql2rsql.planner.schema import EntityField, EntityType
from gsql2rsql.renderer.dialect import (
    AGGREGATION_PATTERNS,
    FUNCTION_TEMPLATES,
    OPERATOR_PATTERNS,
    render_from_template,
)

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gsql2rsql.renderer.render_context import RenderContext
    from gsql2rsql.renderer.sql_enrichment import EnrichedNodesListPredicate


class ExpressionRenderer:
    """Renders QueryExpression trees to Databricks SQL strings.

    Receives a ``RenderContext`` for access to shared state (schema provider,
    resolution result, column-pruning sets, counters).  All methods that were
    previously on ``SQLRenderer`` and belonged to the *expression cluster* now
    live here.
    """

    def __init__(self, ctx: "RenderContext") -> None:
        self._ctx = ctx

    def render_order_by_expression(
        self,
        expr: QueryExpression,
        context_op: LogicalOperator,
        entities_rendered_as_struct: set[str],
        resolved_projections_map: dict[str, "ResolvedProjection"],
        projections: list[tuple[str, QueryExpression]] | None = None,
    ) -> str:
        """Render an ORDER BY expression, handling struct field access for entity returns.

        When an entity is returned as NAMED_STRUCT (e.g., RETURN a renders as
        NAMED_STRUCT(...) AS a), ORDER BY expressions referencing that entity's
        properties need to use struct field access syntax.

        For example:
            RETURN DISTINCT a ORDER BY a.id
            ->
            SELECT DISTINCT NAMED_STRUCT(...) AS a ... ORDER BY a.id

        Instead of:
            ORDER BY _gsql2rsql_a_id  (wrong - column not available after STRUCT wrapping)

        Also handles ORDER BY expressions that match projection expressions - in this case
        we use the projection alias instead of the rendered expression. This is needed
        when ORDER BY references struct fields (e.g., r.src) that were extracted in the
        projection (e.g., r.src AS src).

        Args:
            expr: The ORDER BY expression
            context_op: The operator context
            entities_rendered_as_struct: Set of entity variables rendered as NAMED_STRUCT
            resolved_projections_map: Map of alias -> ResolvedProjection
            projections: List of (alias, expr) tuples from the projection operator

        Returns:
            SQL string for the ORDER BY expression
        """
        # First, check if the ORDER BY expression matches a projection expression.
        # If so, use the projection alias. This handles cases like:
        #   RETURN r.src AS src ORDER BY r.src  -->  SELECT r.src AS src ORDER BY src
        # Without this, ORDER BY r.src would fail because r is not visible at outer level.
        if projections:
            expr_str = str(expr)
            for alias, proj_expr in projections:
                if str(proj_expr) == expr_str:
                    return alias

        # Check if this is a property access on an entity rendered as struct
        if isinstance(expr, QueryExpressionProperty) and entities_rendered_as_struct:
            entity_var = expr.variable_name
            property_name = expr.property_name

            if entity_var in entities_rendered_as_struct:
                # Find the output alias for this entity
                # It's typically the same as the entity variable, but could be aliased
                output_alias = entity_var
                for alias, resolved_proj in resolved_projections_map.items():
                    if resolved_proj.is_entity_ref:
                        refs = list(resolved_proj.expression.all_refs())
                        if refs and refs[0].original_variable == entity_var:
                            output_alias = alias
                            break

                if property_name is None:
                    # Bare entity reference in ORDER BY (e.g., ORDER BY a)
                    # This doesn't really make sense, but return the alias
                    return output_alias
                else:
                    # Property access - use struct field access syntax
                    return f"{output_alias}.{property_name}"

        # Default: use normal expression rendering
        return self.render_expression(expr, context_op)

    def render_expression(
        self, expr: QueryExpression, context_op: LogicalOperator
    ) -> str:
        """Render an expression to SQL."""
        if isinstance(expr, QueryExpressionValue):
            return self.render_value(expr)
        elif isinstance(expr, QueryExpressionParameter):
            return self.render_parameter(expr)
        elif isinstance(expr, QueryExpressionProperty):
            return self._render_property(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            return self._render_binary(expr, context_op)
        elif isinstance(expr, QueryExpressionFunction):
            return self._render_function(expr, context_op)
        elif isinstance(expr, QueryExpressionAggregationFunction):
            return self._render_aggregation(expr, context_op)
        elif isinstance(expr, QueryExpressionList):
            return self._render_list(expr, context_op)
        elif isinstance(expr, QueryExpressionCaseExpression):
            return self._render_case(expr, context_op)
        elif isinstance(expr, QueryExpressionExists):
            return self._render_exists(expr, context_op)
        elif isinstance(expr, QueryExpressionListPredicate):
            return self._render_list_predicate(expr, context_op)
        elif isinstance(expr, QueryExpressionListComprehension):
            return self._render_list_comprehension(expr, context_op)
        elif isinstance(expr, QueryExpressionReduce):
            return self._render_reduce(expr, context_op)
        elif isinstance(expr, QueryExpressionMapLiteral):
            return self._render_map_literal(expr, context_op)
        else:
            return str(expr)

    def render_value(self, expr: QueryExpressionValue) -> str:
        """Render a literal value (Databricks SQL syntax)."""
        if expr.value is None:
            return "NULL"
        if isinstance(expr.value, str):
            escaped = expr.value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(expr.value, bool):
            return "TRUE" if expr.value else "FALSE"
        return str(expr.value)

    def render_parameter(self, expr: QueryExpressionParameter) -> str:
        """Render a parameter expression (Databricks SQL syntax).

        Uses :param_name syntax for named parameters in Databricks SQL.
        """
        return f":{expr.parameter_name}"

    def _render_property(
        self, expr: QueryExpressionProperty, context_op: LogicalOperator
    ) -> str:
        """Render a property access using pre-resolved column references.

        The renderer is now "stupid and safe" - it uses pre-resolved column
        references from ColumnResolver and doesn't perform any semantic resolution.

        Args:
            expr: The property expression (e.g., p.name or p)
            context_op: The operator context

        Returns:
            SQL column reference (e.g., "_gsql2rsql_p_name")

        Raises:
            ValueError: If the reference was not resolved (indicates a bug in ColumnResolver)

        Trade-offs:
            - No guessing or schema lookups - uses pre-validated references
            - Fails fast if resolution is incomplete (better than silent bugs)
            - Simpler, more maintainable code
        """
        # Use resolution - guaranteed to be available after render_plan check
        resolved_ref = self._ctx.get_resolved_ref(
            expr.variable_name, expr.property_name, context_op
        )

        if resolved_ref is None:
            # This should never happen if ColumnResolver is working correctly
            prop_text = f"{expr.variable_name}.{expr.property_name}" if expr.property_name else expr.variable_name
            raise ValueError(
                f"Unresolved column reference: {prop_text} in operator {context_op.operator_debug_id}. "
                f"This indicates a bug in ColumnResolver - all references should be resolved before rendering."
            )

        return resolved_ref.full_sql_reference

    def _render_binary(
        self, expr: QueryExpressionBinary, context_op: LogicalOperator
    ) -> str:
        """Render a binary expression."""
        if not expr.operator or not expr.left_expression or not expr.right_expression:
            return "NULL"

        # Special handling for IN with parameter: use ARRAY_CONTAINS
        if (
            expr.operator.name == BinaryOperator.IN
            and isinstance(expr.right_expression, QueryExpressionParameter)
        ):
            left = self.render_expression(expr.left_expression, context_op)
            right = self.render_expression(expr.right_expression, context_op)
            return f"ARRAY_CONTAINS({right}, {left})"

        # Special handling for comparisons with duration when timestamps are involved
        # This is needed because UNIX_TIMESTAMP returns seconds (BIGINT),
        # while DURATION returns INTERVAL which can't be compared directly
        if expr.operator.name in (BinaryOperator.LT, BinaryOperator.GT,
                                   BinaryOperator.LEQ, BinaryOperator.GEQ):
            # Check if one side is a DURATION function
            duration_expr = None
            other_expr = None
            if self._is_duration_expression(expr.right_expression):
                duration_expr = expr.right_expression
                other_expr = expr.left_expression
            elif self._is_duration_expression(expr.left_expression):
                duration_expr = expr.left_expression
                other_expr = expr.right_expression

            if duration_expr and other_expr and self._contains_timestamp_subtraction(other_expr):
                # Render the duration as seconds for comparison with UNIX_TIMESTAMP diff
                if expr.left_expression is None or expr.right_expression is None:
                    raise TranspilerInternalErrorException(
                        "Binary expression has None operand"
                    )
                left = self.render_expression(expr.left_expression, context_op)
                right = self.render_expression(expr.right_expression, context_op)

                # Convert the INTERVAL to seconds
                if duration_expr == expr.right_expression and isinstance(
                    duration_expr, QueryExpressionFunction
                ):
                    right = self._duration_to_seconds(duration_expr)
                elif isinstance(duration_expr, QueryExpressionFunction):
                    left = self._duration_to_seconds(duration_expr)

                pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
                return pattern.format(left, right)

        left = self.render_expression(expr.left_expression, context_op)
        right = self.render_expression(expr.right_expression, context_op)

        # String concatenation: Cypher + on strings → Spark CONCAT()
        # List concatenation: Cypher + on lists → Spark CONCAT() too; Spark
        # has no `+` overload for arrays, so falling through to arithmetic
        # `+` was a DATATYPE_MISMATCH error at execution time.
        if expr.operator.name == BinaryOperator.PLUS:
            if self._is_array_concat(
                expr.left_expression, expr.right_expression, context_op
            ):
                # List literals render as `(a, b)` for IN clauses; as a
                # CONCAT operand they must be a real ARRAY value.
                left = self._render_as_array_value(
                    expr.left_expression, context_op
                )
                right = self._render_as_array_value(
                    expr.right_expression, context_op
                )
                return f"CONCAT({left}, {right})"
            if self._is_string_concat(
                expr.left_expression, expr.right_expression, context_op
            ):
                return f"CONCAT({left}, {right})"

        # Special handling for timestamp subtraction in Databricks SQL
        # Spark doesn't support direct timestamp - timestamp, need UNIX_TIMESTAMP
        # BUT: timestamp - DURATION should use direct subtraction (INTERVAL)
        if expr.operator.name == BinaryOperator.MINUS:
            # If one side is a DURATION expression, use direct subtraction
            # because DURATION renders as INTERVAL which works with direct subtraction
            if (self._is_duration_expression(expr.left_expression) or
                self._is_duration_expression(expr.right_expression)):
                # Direct subtraction: date - INTERVAL or INTERVAL - date
                pass  # Use default pattern below
            # Check if operands might be timestamps (heuristic: contains 'timestamp' in name)
            elif self._might_be_timestamp_subtraction(expr.left_expression, expr.right_expression):
                return f"(UNIX_TIMESTAMP({left}) - UNIX_TIMESTAMP({right}))"

        pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
        return pattern.format(left, right)

    def _is_string_concat(
        self,
        left: QueryExpression,
        right: QueryExpression,
        context_op: LogicalOperator | None = None,
    ) -> bool:
        """Check if a + operation is string concatenation (not numeric)."""
        return self._has_string_type(left, context_op) or self._has_string_type(
            right, context_op
        )

    def _is_array_concat(
        self,
        left: QueryExpression,
        right: QueryExpression,
        context_op: LogicalOperator | None = None,
    ) -> bool:
        """Check if a + operation is list concatenation (not numeric)."""
        return self._has_array_type(left, context_op) or self._has_array_type(
            right, context_op
        )

    def _has_array_type(
        self,
        expr: QueryExpression,
        context_op: LogicalOperator | None = None,
    ) -> bool:
        """Check if an expression is list-typed (literal or schema type)."""
        if isinstance(expr, QueryExpressionList):
            return True
        if expr.evaluate_type() is list:
            return True
        return self._schema_property_type(expr, context_op) is list

    def _schema_property_type(
        self,
        expr: QueryExpression,
        context_op: LogicalOperator | None,
    ) -> type[Any] | None:
        """Look up a property's declared type in the operator's schema.

        AST nodes often reach the renderer with ``data_type`` unset, so
        ``evaluate_type()`` alone cannot drive type dispatch (Cypher
        ``size()``/``+`` are polymorphic). The operator's input schema is
        the planner's authoritative view: entity fields carry each
        property's declared Python type from the graph schema.
        """
        if (
            context_op is None
            or not isinstance(expr, QueryExpressionProperty)
            or not expr.property_name
        ):
            return None
        schema = getattr(context_op, "input_schema", None)
        if not schema:
            return None
        entity = schema.get_field(expr.variable_name)
        if isinstance(entity, EntityField):
            for value_field in entity.encapsulated_fields:
                if value_field.field_alias == expr.property_name:
                    return value_field.data_type
        return None

    def _render_as_array_value(
        self, expr: QueryExpression, context_op: LogicalOperator
    ) -> str:
        """Render an expression as an ARRAY *value*.

        ``_render_list`` emits ``(a, b)`` because its main consumer is SQL
        ``IN`` lists; contexts that need a first-class array (CONCAT
        operands) must wrap the elements in ``ARRAY(...)`` instead.
        """
        if isinstance(expr, QueryExpressionList):
            items = [
                self.render_expression(item, context_op)
                for item in expr.items
            ]
            return f"ARRAY({', '.join(items)})"
        return self.render_expression(expr, context_op)

    def _has_string_type(
        self,
        expr: QueryExpression,
        context_op: LogicalOperator | None = None,
    ) -> bool:
        """Check if an expression is string-typed (recursively for + chains)."""
        expr_type = expr.evaluate_type()
        if expr_type is str:
            return True
        if isinstance(expr, QueryExpressionValue) and isinstance(expr.value, str):
            return True
        if self._schema_property_type(expr, context_op) is str:
            return True
        # Recurse into + chains: if (a + 'x') is the left operand,
        # the inner 'x' makes the whole chain string concatenation
        if isinstance(expr, QueryExpressionBinary):
            if expr.operator and expr.operator.name == BinaryOperator.PLUS:
                left = expr.left_expression
                right = expr.right_expression
                if left and self._has_string_type(left, context_op):
                    return True
                if right and self._has_string_type(right, context_op):
                    return True
        return False

    def _is_duration_expression(self, expr: QueryExpression) -> bool:
        """Check if an expression is a DURATION function call."""
        if isinstance(expr, QueryExpressionFunction):
            return expr.function == Function.DURATION
        return False

    def _contains_timestamp_subtraction(self, expr: QueryExpression) -> bool:
        """Check if expression contains timestamp subtraction (recursively)."""
        if isinstance(expr, QueryExpressionBinary):
            left = expr.left_expression
            right = expr.right_expression
            if expr.operator and expr.operator.name == BinaryOperator.MINUS:
                if left and right and self._might_be_timestamp_subtraction(left, right):
                    return True
            # Check children
            left_has = left is not None and self._contains_timestamp_subtraction(left)
            right_has = right is not None and self._contains_timestamp_subtraction(right)
            return left_has or right_has
        elif isinstance(expr, QueryExpressionFunction):
            # Check if any parameter contains timestamp subtraction
            return any(self._contains_timestamp_subtraction(p) for p in expr.parameters)
        return False

    def _duration_to_seconds(self, expr: QueryExpressionFunction) -> str:
        """Convert a DURATION expression to seconds (as a numeric literal)."""
        if not expr.parameters:
            return "0"

        first_param = expr.parameters[0]
        if isinstance(first_param, QueryExpressionValue) and isinstance(first_param.value, str):
            return str(self._parse_iso8601_to_seconds(first_param.value))
        return "0"

    def _parse_iso8601_to_seconds(self, duration_str: str) -> int:
        """Parse ISO 8601 duration string to total seconds.

        Examples:
        - PT5M -> 300 (5 minutes = 300 seconds)
        - PT1H -> 3600 (1 hour = 3600 seconds)
        - P1D -> 86400 (1 day = 86400 seconds)
        """
        import re

        if not duration_str:
            return 0

        s = duration_str.strip().upper()
        if not s.startswith("P"):
            return 0
        s = s[1:]

        total_seconds = 0

        # Split by 'T' to separate date and time parts
        if "T" in s:
            date_part, time_part = s.split("T", 1)
        else:
            date_part = s
            time_part = ""

        # Parse date part
        date_pattern = re.compile(r"(\d+)([YMWD])")
        for match in date_pattern.finditer(date_part):
            value = int(match.group(1))
            unit = match.group(2)
            if unit == "Y":
                total_seconds += value * 365 * 24 * 3600
            elif unit == "M":
                total_seconds += value * 30 * 24 * 3600  # Approximate
            elif unit == "W":
                total_seconds += value * 7 * 24 * 3600
            elif unit == "D":
                total_seconds += value * 24 * 3600

        # Parse time part
        time_pattern = re.compile(r"(\d+(?:\.\d+)?)([HMS])")
        for match in time_pattern.finditer(time_part):
            time_value = float(match.group(1))
            time_unit = match.group(2)
            if time_unit == "H":
                total_seconds += int(time_value * 3600)
            elif time_unit == "M":
                total_seconds += int(time_value * 60)
            elif time_unit == "S":
                total_seconds += int(time_value)

        return total_seconds

    def _might_be_timestamp_subtraction(
        self, left: QueryExpression, right: QueryExpression
    ) -> bool:
        """Check if a subtraction might involve timestamps (heuristic-based)."""
        # Check if either operand is a property access with 'timestamp' in the name
        def is_timestamp_property(expr: QueryExpression) -> bool:
            if isinstance(expr, QueryExpressionProperty):
                prop = expr.property_name or ""
                return "timestamp" in prop.lower() or "date" in prop.lower()
            return False

        return is_timestamp_property(left) or is_timestamp_property(right)

    def _render_function(
        self,
        expr: QueryExpressionFunction,
        context_op: LogicalOperator,
        pre_rendered_params: list[str] | None = None,
    ) -> str:
        """Render a function call (Databricks SQL syntax).

        Simple functions (55 of 67) are dispatched via the declarative
        FUNCTION_TEMPLATES registry in dialect.py.  Complex functions that
        need AST inspection (DATE, DATETIME, DURATION, etc.) remain here.

        Args:
            pre_rendered_params: If provided, use these instead of rendering
                expr.parameters via render_expression. Used by list predicate
                filter rendering to pass lambda-aware parameter renderings.
        """
        params = pre_rendered_params if pre_rendered_params is not None else [
            self.render_expression(p, context_op) for p in expr.parameters
        ]

        func = expr.function

        # Cypher size() is polymorphic: element count for lists/maps,
        # character count for strings. Spark's SIZE() is array/map-only, so
        # a string operand must become LENGTH() — the blanket template used
        # to emit SIZE(<string>), a type error at execution time.
        if (
            func == Function.SIZE
            and expr.parameters
            and self._has_string_type(expr.parameters[0], context_op)
        ):
            return f"LENGTH({params[0]})"

        # --- Data-driven dispatch for simple functions ---
        tmpl = FUNCTION_TEMPLATES.get(func)
        if tmpl is not None:
            return render_from_template(tmpl, params)

        # --- Complex handlers requiring AST inspection ---
        if func == Function.LIST_INDEX:
            # Cypher list[i]: 0-based, negative counts from the end,
            # out-of-range -> NULL. GET() is 0-based and NULL-safe.
            arr, idx = params[0], params[1]
            return (
                f"GET({arr}, CASE WHEN ({idx}) < 0 "
                f"THEN SIZE({arr}) + ({idx}) ELSE ({idx}) END)"
            )
        elif func == Function.LIST_SLICE:
            # Cypher list[a..b]: 0-based, end-exclusive, negatives from
            # the end, bounds clamped, omitted bound = open end (the
            # visitor passes NULL literals for omitted bounds).
            return self._render_list_slice(params[0], params[1], params[2])

        if func == Function.TYPE:
            # type(r) is resolved to r.<type column> by enrichment ONLY in
            # the pushed-down VLP edge lambda. Anywhere else (RETURN type(r),
            # single-hop WHERE) reaching the renderer means it was never
            # rewritten — fail clearly instead of with an internal error.
            raise TranspilerNotSupportedException(
                "type(<rel>) is currently supported only inside a "
                "variable-length ALL(r IN relationships(p) WHERE ...) "
                "predicate. Elsewhere, reference the relationship-type "
                "column directly (e.g. r.relationship_type = 'KNOWS')."
            )

        if func == Function.NODES:
            # nodes(path) → pass-through path column
            return params[0] if params else "ARRAY()"
        elif func == Function.RELATIONSHIPS:
            # relationships(path) → derive edges column from path column
            if params:
                path_col = params[0]
                if path_col.endswith("_id"):
                    return path_col[:-3] + "_edges"
                elif "_path" in path_col and not path_col.endswith("_edges"):
                    return path_col + "_edges"
                else:
                    return path_col + "_edges"
            return "ARRAY()"
        elif func == Function.DATE:
            if not params:
                return "CURRENT_DATE()"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_date_from_map(first_param, context_op)
            return f"TO_DATE({params[0]})"
        elif func == Function.DATETIME:
            if not params:
                return "CURRENT_TIMESTAMP()"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_datetime_from_map(first_param, context_op)
            return f"TO_TIMESTAMP({params[0]})"
        elif func == Function.LOCALDATETIME:
            if not params:
                return "CURRENT_TIMESTAMP()"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_datetime_from_map(first_param, context_op)
            return f"TO_TIMESTAMP({params[0]})"
        elif func == Function.TIME:
            if not params:
                return "DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')"
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_time_from_map(first_param, context_op)
            return f"DATE_FORMAT(TO_TIMESTAMP({params[0]}), 'HH:mm:ss')"
        elif func == Function.LOCALTIME:
            if not params:
                return "DATE_FORMAT(CURRENT_TIMESTAMP(), 'HH:mm:ss')"
            return f"DATE_FORMAT(TO_TIMESTAMP({params[0]}), 'HH:mm:ss')"
        elif func == Function.DURATION:
            first_param = expr.parameters[0] if expr.parameters else None
            if isinstance(first_param, QueryExpressionMapLiteral):
                return self._render_duration_from_map(first_param, context_op)
            if isinstance(first_param, QueryExpressionValue) and isinstance(
                first_param.value, str
            ):
                return self._parse_iso8601_duration(first_param.value)
            if params:
                rendered = params[0]
                if rendered.startswith("'") and rendered.endswith("'"):
                    return self._parse_iso8601_duration(rendered[1:-1])
            return "INTERVAL '0' DAY"
        else:
            # Library error, never NotImplementedError: this is reachable by
            # any user query calling a function without a translation, and
            # the message must name what they wrote (raw_name), not the
            # parser's INVALID placeholder.
            user_name = expr.raw_name or func.name.lower()
            raise TranspilerNotSupportedException(
                f"Function '{user_name}()' is not supported by the "
                f"transpiler. See docs/limitations.md for the list of "
                f"supported functions and workarounds."
            )

    def _render_aggregation(
        self, expr: QueryExpressionAggregationFunction, context_op: LogicalOperator
    ) -> str:
        """Render an aggregation function.

        Supports ordered aggregation for COLLECT:
        COLLECT(x ORDER BY y DESC) ->
            TRANSFORM(
                ARRAY_SORT(
                    COLLECT_LIST(STRUCT(_sort_key, _value)),
                    (a, b) -> CASE WHEN a._sort_key > b._sort_key THEN -1 ELSE 1 END
                ),
                s -> s._value
            )

        Also supports collecting entities as STRUCT:
        COLLECT(a) where a is a node -> COLLECT_LIST(NAMED_STRUCT('prop1', col1, ...))
        """
        # Handle ordered COLLECT specially
        if (
            expr.order_by
            and expr.aggregation_function == AggregationFunction.COLLECT
            and expr.inner_expression
        ):
            return self._render_ordered_collect(expr, context_op)

        # Check if this is COLLECT of an entity (node or edge)
        if (
            expr.aggregation_function == AggregationFunction.COLLECT
            and expr.inner_expression
            and isinstance(expr.inner_expression, QueryExpressionProperty)
            and expr.inner_expression.property_name is None  # Bare entity reference
        ):
            entity_var = expr.inner_expression.variable_name
            # Generate NAMED_STRUCT for the entity
            entity_struct = self._render_entity_in_collect_as_struct(entity_var, context_op)
            return f"COLLECT_LIST({entity_struct})"

        inner = (
            self.render_expression(expr.inner_expression, context_op)
            if expr.inner_expression
            else "*"
        )

        if expr.is_distinct:
            inner = f"DISTINCT {inner}"

        pattern = AGGREGATION_PATTERNS.get(expr.aggregation_function)
        if pattern is None:
            # A silent "{0}" fallback here once emitted the bare operand for
            # percentileCont(), turning an aggregate into a per-row
            # projection — a wrong answer with no warning. Fail loudly.
            raise TranspilerNotSupportedException(
                f"Aggregation function "
                f"'{expr.aggregation_function.name.lower()}' has no SQL "
                f"translation. Add it to AGGREGATION_PATTERNS in dialect.py."
            )

        extras = [
            self.render_expression(p, context_op)
            for p in expr.extra_parameters
        ]
        if "{1}" in pattern and not extras:
            raise TranspilerSyntaxErrorException(
                f"{expr.aggregation_function.name.lower()} requires a "
                f"second argument (e.g. percentileCont(expr, 0.5))."
            )
        return pattern.format(inner, *extras)

    def _render_ordered_collect(
        self, expr: QueryExpressionAggregationFunction, context_op: LogicalOperator
    ) -> str:
        """Render an ordered COLLECT using ARRAY_SORT.

        COLLECT(x ORDER BY y DESC) becomes:
        TRANSFORM(
            ARRAY_SORT(
                COLLECT_LIST(STRUCT(y AS _sort_key, x AS _value)),
                (a, b) -> CASE WHEN a._sort_key > b._sort_key THEN -1 ELSE 1 END
            ),
            s -> s._value
        )
        """
        if not expr.inner_expression:
            return "COLLECT_LIST(NULL)"
        value_sql = self.render_expression(expr.inner_expression, context_op)

        # Build STRUCT with sort keys and value
        struct_parts = []
        sort_comparisons = []

        for i, (sort_expr, is_desc) in enumerate(expr.order_by):
            sort_key_sql = self.render_expression(sort_expr, context_op)
            key_name = f"_sk{i}"
            struct_parts.append(f"{sort_key_sql} AS {key_name}")

            # Build comparison for this sort key
            if is_desc:
                # DESC: a > b -> -1 (a comes first)
                sort_comparisons.append(
                    f"CASE WHEN a.{key_name} > b.{key_name} THEN -1 "
                    f"WHEN a.{key_name} < b.{key_name} THEN 1 ELSE 0 END"
                )
            else:
                # ASC: a < b -> -1 (a comes first)
                sort_comparisons.append(
                    f"CASE WHEN a.{key_name} < b.{key_name} THEN -1 "
                    f"WHEN a.{key_name} > b.{key_name} THEN 1 ELSE 0 END"
                )

        struct_parts.append(f"{value_sql} AS _value")
        struct_sql = f"STRUCT({', '.join(struct_parts)})"

        # Combine sort comparisons with COALESCE-like logic
        # For multiple sort keys, check each in order
        if len(sort_comparisons) == 1:
            comparator = sort_comparisons[0]
        else:
            # Chain comparisons: if first is 0, use second, etc.
            comparator = sort_comparisons[-1]
            for comp in reversed(sort_comparisons[:-1]):
                comparator = f"CASE WHEN ({comp}) = 0 THEN ({comparator}) ELSE ({comp}) END"

        return (
            f"TRANSFORM("
            f"ARRAY_SORT("
            f"COLLECT_LIST({struct_sql}), "
            f"(a, b) -> {comparator}"
            f"), "
            f"s -> s._value"
            f")"
        )

    def _render_list(
        self, expr: QueryExpressionList, context_op: LogicalOperator
    ) -> str:
        """Render a list expression."""
        items = [self.render_expression(item, context_op) for item in expr.items]
        return f"({', '.join(items)})"

    def _render_list_predicate(
        self, expr: QueryExpressionListPredicate, context_op: LogicalOperator
    ) -> str:
        """Render a list predicate expression (ALL/ANY/NONE/SINGLE).

        Databricks SQL translation (optimized for 17.x):

        OPTIMIZATION 1: Simple equality checks use ARRAY_CONTAINS (O(1) with bloom filter)
        - ANY(x IN list WHERE x = val) -> ARRAY_CONTAINS(list, val)
        - NONE(x IN list WHERE x = val) -> NOT ARRAY_CONTAINS(list, val)

        OPTIMIZATION 2: Use EXISTS/FORALL HOFs when available (Databricks 17.x)
        - ANY(x IN list WHERE cond) -> EXISTS(list, x -> cond)
        - ALL(x IN list WHERE cond) -> FORALL(list, x -> cond)

        FALLBACK: Complex predicates use FILTER + SIZE
        - ALL(x IN list WHERE cond) -> SIZE(FILTER(list, x -> NOT (cond))) = 0
        - ANY(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) > 0
        - NONE(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) = 0
        - SINGLE(x IN list WHERE cond) -> SIZE(FILTER(list, x -> cond)) = 1
        """
        var_name = expr.variable_name
        list_sql = self.render_expression(expr.list_expression, context_op)

        # nodes(path) with property access: the path array holds scalar node
        # IDs, so a HOF lambda (FORALL(path, n -> n.prop)) is a Spark type
        # error. Enrichment pre-resolves the node table for these; render a
        # correlated subquery against it instead.
        enriched_np = (
            self._ctx.enriched.nodes_predicates.get(id(expr))
            if self._ctx.enriched
            else None
        )
        if enriched_np is not None:
            return self._render_nodes_predicate_subquery(
                expr, enriched_np, list_sql, context_op
            )

        # Check for simple equality optimization: ANY(x IN list WHERE x = value)
        equality_value = self._extract_equality_value(
            expr.filter_expression, var_name, context_op
        )

        if equality_value is not None:
            # Use ARRAY_CONTAINS optimization (O(1) with bloom filter in Databricks)
            if expr.predicate_type == ListPredicateType.ANY:
                return f"ARRAY_CONTAINS({list_sql}, {equality_value})"
            elif expr.predicate_type == ListPredicateType.NONE:
                return f"NOT ARRAY_CONTAINS({list_sql}, {equality_value})"
            # For ALL/SINGLE with equality, fall through to standard approach

        # Build the lambda body for complex predicates
        if expr.filter_expression:
            filter_sql = self._render_list_predicate_filter(
                expr.filter_expression, var_name, context_op
            )
        else:
            filter_sql = "TRUE"

        # Use EXISTS/FORALL HOFs for ANY/ALL (Databricks 17.x optimization)
        if expr.predicate_type == ListPredicateType.ALL:
            if expr.filter_expression:
                # FORALL is cleaner but equivalent to SIZE(FILTER(NOT cond)) = 0
                return f"FORALL({list_sql}, {var_name} -> {filter_sql})"
            else:
                # ALL(x IN list) without filter - check all not null
                return f"FORALL({list_sql}, {var_name} -> {var_name} IS NOT NULL)"
        elif expr.predicate_type == ListPredicateType.ANY:
            # EXISTS is cleaner and may short-circuit
            return f"EXISTS({list_sql}, {var_name} -> {filter_sql})"
        elif expr.predicate_type == ListPredicateType.NONE:
            # NONE = NOT EXISTS
            return f"NOT EXISTS({list_sql}, {var_name} -> {filter_sql})"
        elif expr.predicate_type == ListPredicateType.SINGLE:
            # SINGLE: Exactly one element matches (no HOF equivalent)
            return f"SIZE(FILTER({list_sql}, {var_name} -> {filter_sql})) = 1"
        else:
            return f"EXISTS({list_sql}, {var_name} -> {filter_sql})"

    def _render_list_slice(self, arr: str, start: str, end: str) -> str:
        """Render Cypher ``list[a..b]`` as a Spark SLICE call.

        Cypher slice: 0-based, end-exclusive, negative bounds count from
        the end, out-of-range bounds clamp, omitted bounds (rendered as
        the literal NULL by the visitor) mean open start/end.

        General form:
            SLICE(arr, norm(start)+1, GREATEST(0, norm(end)-norm(start)))

        Common literal bounds are simplified so the dominant patterns
        (``[0..-1]``, ``[1..]``) stay readable.
        """
        def _literal_int(sql: str) -> int | None:
            # Bounds render through the value renderer, so a negative
            # literal arrives as a unary-minus string like "-(1)". Strip
            # surrounding whitespace/parens and a leading '-' so the
            # readable negative-literal branch below is actually reached.
            text = sql.strip()
            try:
                return int(text.strip("() "))
            except ValueError:
                pass
            compact = text.replace(" ", "")
            if compact.startswith("-"):
                try:
                    return -int(compact[1:].strip("()"))
                except ValueError:
                    return None
            return None

        def _norm(bound: str, default_sql: str) -> str:
            if bound.strip().upper() == "NULL":
                return default_sql
            lit = _literal_int(bound)
            if lit is not None:
                if lit == 0:
                    return "0"
                if lit > 0:
                    return f"LEAST({lit}, SIZE({arr}))"
                return f"GREATEST(0, SIZE({arr}) - {-lit})"
            return (
                f"CASE WHEN ({bound}) < 0 "
                f"THEN GREATEST(0, SIZE({arr}) + ({bound})) "
                f"ELSE LEAST(({bound}), SIZE({arr})) END"
            )

        norm_start = _norm(start, "0")
        norm_end = _norm(end, f"SIZE({arr})")

        if norm_start == "0":
            length = norm_end
            return f"SLICE({arr}, 1, {length})"
        return (
            f"SLICE({arr}, ({norm_start}) + 1, "
            f"GREATEST(0, ({norm_end}) - ({norm_start})))"
        )

    def _render_nodes_predicate_subquery(
        self,
        expr: QueryExpressionListPredicate,
        enriched_np: EnrichedNodesListPredicate,
        list_sql: str,
        context_op: LogicalOperator,
    ) -> str:
        """Render ALL/ANY/NONE/SINGLE over nodes(path) with property access.

        The path array contains scalar node IDs; node properties live in the
        node table. Emits a subquery correlated only through
        ``ARRAY_CONTAINS(<path column>, <alias>.<id>)``, using the Cypher
        lambda variable as the table alias so the lambda body renders
        unchanged (``n.status`` -> ``n.status``).

        Cypher WHERE semantics (rows survive only on TRUE):
        - all():    no path node may FAIL or NULL-evaluate the predicate
        - any():    at least one node must satisfy it (TRUE)
        - none():   no node may satisfy it, and none may NULL-evaluate it
        - single(): exactly one node satisfies it (NULL elements approximate
          to non-matching; path nodes are distinct thanks to the traversal's
          visited check, so COUNT over the id-join is exact)

        In a projection context (RETURN all(...)), cases where Cypher yields
        NULL are returned as FALSE/TRUE by these forms — acceptable for
        boolean filtering, documented deviation for projection.
        """
        if self._ctx.vlp_rendering_mode == "procedural":
            # The procedural BFS final view emits CAST(NULL AS ARRAY) for the
            # path column (paths are never collected), so ARRAY_CONTAINS
            # would be vacuously NULL and the filter silently dropped.
            # The ONLY supported form is a conjunctive ALL whose predicate
            # was extracted into the traversal's node_filter: the procedural
            # renderer then enforces it via its frontier filter (every
            # frontier/seed node must pass), making the residual here
            # trivially TRUE. Both sides consult the same eligibility rule
            # (vlp_node_pushdown_active) so neither can silently diverge.
            from gsql2rsql.renderer.sql_enrichment import (
                vlp_node_pushdown_active,
            )

            op_id = enriched_np.enforced_by_traversal_op_id
            enriched_rec = (
                self._ctx.enriched.recursive_ops.get(op_id)
                if self._ctx.enriched and op_id is not None
                else None
            )
            if vlp_node_pushdown_active(enriched_rec, self._ctx.config):
                return "TRUE /* enforced by VLP node pushdown */"
            raise TranspilerNotSupportedException(
                "List predicates over nodes(<path>) that access node "
                "properties are only supported with "
                "vlp_rendering_mode='procedural' when they are a "
                "conjunctive all(...) whose predicate references only the "
                "lambda variable (enforced via the BFS frontier filter). "
                "This predicate cannot be enforced that way — use the "
                "default CTE rendering mode for this query."
            )

        var = expr.variable_name
        tbl = enriched_np.node_table_name
        id_col = enriched_np.node_id_column
        member = f"ARRAY_CONTAINS({list_sql}, {var}.{id_col})"
        base_parts = [member]
        if enriched_np.node_type_filter:
            base_parts.insert(0, f"{var}.{enriched_np.node_type_filter}")
        base = " AND ".join(base_parts)

        if expr.filter_expression is not None:
            pred = self._render_list_predicate_filter(
                expr.filter_expression, var, context_op
            )
        else:
            pred = f"{var}.{id_col} IS NOT NULL"

        ptype = expr.predicate_type
        if ptype == ListPredicateType.ALL:
            violation = f"(NOT ({pred}) OR ({pred}) IS NULL)"
            return (
                f"NOT EXISTS (SELECT 1 FROM {tbl} {var} "
                f"WHERE {base} AND {violation})"
            )
        elif ptype == ListPredicateType.ANY:
            return (
                f"EXISTS (SELECT 1 FROM {tbl} {var} "
                f"WHERE {base} AND ({pred}))"
            )
        elif ptype == ListPredicateType.NONE:
            match_or_null = f"(({pred}) OR ({pred}) IS NULL)"
            return (
                f"NOT EXISTS (SELECT 1 FROM {tbl} {var} "
                f"WHERE {base} AND {match_or_null})"
            )
        else:  # SINGLE
            return (
                f"(SELECT COUNT(*) FROM {tbl} {var} "
                f"WHERE {base} AND ({pred})) = 1"
            )

    def _extract_equality_value(
        self,
        expr: QueryExpression | None,
        var_name: str,
        context_op: LogicalOperator,
    ) -> str | None:
        """Extract value from simple equality expression like 'x = value'.

        Returns the rendered value SQL if expr is a simple equality where one side
        is just the variable (var_name) and the other is a constant/expression.
        Returns None if not a simple equality pattern.

        This enables ARRAY_CONTAINS optimization which is O(1) with bloom filter.
        """
        if expr is None:
            return None

        if not isinstance(expr, QueryExpressionBinary):
            return None

        # Check if it's an equality operator
        if expr.operator is None or expr.operator.name != BinaryOperator.EQ:
            return None

        left = expr.left_expression
        right = expr.right_expression

        # Check if left is just the variable and right is the value
        if (
            isinstance(left, QueryExpressionProperty)
            and left.variable_name == var_name
            and not left.property_name
            and right is not None
        ):
            # Pattern: x = value
            return self.render_expression(right, context_op)

        # Check if right is just the variable and left is the value
        if (
            isinstance(right, QueryExpressionProperty)
            and right.variable_name == var_name
            and not right.property_name
            and left is not None
        ):
            # Pattern: value = x
            return self.render_expression(left, context_op)

        return None

    def _render_list_predicate_filter(
        self, expr: QueryExpression, var_name: str, context_op: LogicalOperator
    ) -> str:
        """Render a filter expression for list predicate, handling variable references.

        In list predicates like ALL(x IN list WHERE x > 0), references to 'x'
        should be rendered as just 'x' (the lambda parameter), not as a
        field lookup in the context operator's schema.
        """
        if isinstance(expr, QueryExpressionExists):
            # Pattern predicates like size((n)--()) or exists((n)--())
            # inside a lambda would need a per-element correlated pattern
            # count — not supported. The alternative is a materialized
            # degree/count column on the node table.
            raise TranspilerNotSupportedException(
                "Graph pattern predicates (e.g. size((n)--()), "
                "exists((n)--())) are not supported inside list-predicate "
                "lambdas such as all(n IN nodes(p) WHERE ...). "
                "Materialize the count as a node property (e.g. a "
                "'degree' column) and filter on it instead: "
                "all(n IN nodes(p) WHERE n.degree <= 15)."
            )
        if isinstance(expr, QueryExpressionFunction) and (
            expr.function == Function.TYPE
        ):
            # type(r) is rewritten to r.<type column> by enrichment when
            # the edge schema declares one. Reaching here means either the
            # schema has no type column or type() was used outside a
            # pushed-down edge lambda.
            raise TranspilerNotSupportedException(
                "type(<var>) could not be resolved to a relationship-type "
                "column here. Either the edge schema declares no type "
                "column, or type() was used in a position that is not a "
                "conjunctive all(r IN relationships(p) WHERE ...). "
                "Reference the type column directly instead (e.g. "
                "r.relationship_type = 'FILIAL_DE')."
            )
        if isinstance(expr, QueryExpressionProperty):
            # If it's just the variable (no property), return it as-is
            if expr.variable_name == var_name and not expr.property_name:
                return var_name
            # If it has a property, it's accessing a field on the lambda variable
            if expr.variable_name == var_name and expr.property_name:
                return f"{var_name}.{expr.property_name}"
            # Otherwise it's a reference to an outer variable
            return self._render_property(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            left_expr = expr.left_expression
            right_expr = expr.right_expression
            if left_expr is None or right_expr is None:
                return "NULL"
            left = self._render_list_predicate_filter(left_expr, var_name, context_op)
            right = self._render_list_predicate_filter(right_expr, var_name, context_op)
            if expr.operator is None:
                return f"({left}) ? ({right})"
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)
        elif isinstance(expr, QueryExpressionFunction):
            params = [
                self._render_list_predicate_filter(p, var_name, context_op)
                for p in expr.parameters
            ]
            # Re-use the existing function rendering logic
            func = expr.function
            if func == Function.NOT:
                return f"NOT ({params[0]})" if params else "NOT (NULL)"
            elif func == Function.IS_NULL:
                return f"({params[0]}) IS NULL" if params else "NULL IS NULL"
            elif func == Function.IS_NOT_NULL:
                return f"({params[0]}) IS NOT NULL" if params else "NULL IS NOT NULL"
            # For all other functions, reuse _render_function with the
            # lambda-aware pre-rendered params so that loop variable property
            # accesses (e.g., rel.status inside coalesce()) render correctly
            # as struct field access instead of prefixed column names.
            return self._render_function(expr, context_op, pre_rendered_params=params)
        else:
            # For other expression types, use default rendering
            return self.render_expression(expr, context_op)

    def _render_case(
        self, expr: QueryExpressionCaseExpression, context_op: LogicalOperator
    ) -> str:
        """Render a CASE expression."""
        parts = ["CASE"]

        if expr.test_expression:
            parts.append(self.render_expression(expr.test_expression, context_op))

        for when_expr, then_expr in expr.alternatives:
            when_rendered = self.render_expression(when_expr, context_op)
            then_rendered = self.render_expression(then_expr, context_op)
            parts.append(f"WHEN {when_rendered} THEN {then_rendered}")

        if expr.else_expression:
            else_rendered = self.render_expression(expr.else_expression, context_op)
            parts.append(f"ELSE {else_rendered}")

        parts.append("END")
        return " ".join(parts)

    def _render_list_comprehension(
        self, expr: QueryExpressionListComprehension, context_op: LogicalOperator
    ) -> str:
        """Render a list comprehension expression.

        Cypher: [x IN list WHERE predicate | expression]
        Databricks SQL:
        - FILTER(list, x -> predicate) for WHERE only
        - TRANSFORM(list, x -> expression) for map only
        - TRANSFORM(FILTER(list, x -> predicate), x -> expression) for both

        Examples:
        - [x IN arr WHERE x > 0] -> FILTER(arr, x -> x > 0)
        - [x IN arr | x * 2] -> TRANSFORM(arr, x -> x * 2)
        - [x IN arr WHERE x > 0 | x * 2] -> TRANSFORM(FILTER(arr, x -> x > 0), x -> x * 2)

        OPTIMIZATION: Path Node ID Extraction
        -------------------------------------
        [node IN nodes(path) | node.id] is a common pattern that should NOT
        generate TRANSFORM because:
        - nodes(path) returns `path` which is already an array of node IDs
        - Applying `node.id` to an integer ID doesn't make sense

        This optimization detects this pattern and returns `path` directly:
        - [node IN nodes(path) | node.id] -> path (not TRANSFORM(path, node -> node.id))
        """
        var = expr.variable_name
        list_sql = self.render_expression(expr.list_expression, context_op)

        # =====================================================================
        # OPTIMIZATION: Detect [node IN nodes(path) | node.id] pattern
        # =====================================================================
        # When iterating over nodes(path) and extracting .id, the TRANSFORM
        # is redundant because `path` already contains node IDs.
        #
        # Pattern to detect:
        # - list_expression is Function.NODES(path_var)
        # - map_expression is var.id (where var matches the comprehension variable)
        # - no filter_expression
        #
        # Result: Return path directly instead of TRANSFORM(path, node -> node.id)
        # =====================================================================
        if self._is_nodes_id_extraction_pattern(expr):
            # Return the path array directly - it already contains node IDs
            return list_sql

        # Start with the list
        result = list_sql

        # Apply FILTER if there's a WHERE predicate
        if expr.filter_expression:
            filter_sql = self._render_list_predicate_filter(
                expr.filter_expression, var, context_op
            )
            result = f"FILTER({result}, {var} -> {filter_sql})"

        # Apply TRANSFORM if there's a map expression
        if expr.map_expression:
            map_sql = self._render_list_predicate_filter(
                expr.map_expression, var, context_op
            )
            result = f"TRANSFORM({result}, {var} -> {map_sql})"

        return result

    def _is_nodes_id_extraction_pattern(
        self, expr: QueryExpressionListComprehension
    ) -> bool:
        """Check if this is a [node IN nodes(path) | node.id] pattern.

        This pattern should NOT generate TRANSFORM because:
        - nodes(path) returns `path` which already contains node IDs
        - Applying `node.id` to integers doesn't make sense

        Returns:
            True if this is the nodes ID extraction pattern that should
            be optimized to just return `path` directly.
        """
        # Must have a map expression and no filter
        if not expr.map_expression or expr.filter_expression:
            return False

        # Check if list_expression is nodes(path_var)
        if not isinstance(expr.list_expression, QueryExpressionFunction):
            return False
        if expr.list_expression.function != Function.NODES:
            return False

        # Check if map_expression is var.id (extracting id from the variable)
        if not isinstance(expr.map_expression, QueryExpressionProperty):
            return False
        map_expr = expr.map_expression
        if map_expr.variable_name != expr.variable_name:
            return False
        if map_expr.property_name != "id":
            return False

        return True

    def _render_reduce(
        self, expr: QueryExpressionReduce, context_op: LogicalOperator
    ) -> str:
        """Render a REDUCE expression.

        Cypher: REDUCE(acc = initial, x IN list | acc_expr)
        Databricks SQL: AGGREGATE(list, initial, (acc, x) -> acc_expr)

        Example:
        - REDUCE(total = 0, x IN amounts | total + x)
          -> AGGREGATE(amounts, 0, (total, x) -> total + x)
        """
        acc_name = expr.accumulator_name
        var_name = expr.variable_name
        list_sql = self.render_expression(expr.list_expression, context_op)
        initial_sql = self.render_expression(expr.initial_value, context_op)

        # Cast numeric initial value to DOUBLE if the reducer involves field access
        # This avoids type mismatch when the reducer returns DOUBLE (e.g., amount fields)
        if self._reducer_might_return_double(expr.reducer_expression, var_name):
            if isinstance(expr.initial_value, QueryExpressionValue):
                if isinstance(expr.initial_value.value, (int, float)):
                    initial_sql = f"CAST({initial_sql} AS DOUBLE)"

        # Render the reducer expression, replacing accumulator and variable references
        reducer_sql = self._render_reduce_expression(
            expr.reducer_expression, acc_name, var_name, context_op
        )

        return f"AGGREGATE({list_sql}, {initial_sql}, ({acc_name}, {var_name}) -> {reducer_sql})"

    def _reducer_might_return_double(
        self, expr: QueryExpression, var_name: str
    ) -> bool:
        """Check if a reducer expression might return DOUBLE (heuristic).

        Returns True if the expression accesses properties of the variable,
        which might be numeric fields like 'amount', 'balance', etc.
        """
        if isinstance(expr, QueryExpressionProperty):
            # If it's a property access on the variable, might be a numeric field
            if expr.variable_name == var_name and expr.property_name:
                return True
        elif isinstance(expr, QueryExpressionBinary):
            # Check both sides
            left_check = (
                expr.left_expression is not None
                and self._reducer_might_return_double(expr.left_expression, var_name)
            )
            right_check = (
                expr.right_expression is not None
                and self._reducer_might_return_double(expr.right_expression, var_name)
            )
            return left_check or right_check
        elif isinstance(expr, QueryExpressionFunction):
            # Check parameters
            return any(
                self._reducer_might_return_double(p, var_name)
                for p in expr.parameters
            )
        return False

    def _render_reduce_expression(
        self,
        expr: QueryExpression,
        acc_name: str,
        var_name: str,
        context_op: LogicalOperator,
    ) -> str:
        """Render an expression within a REDUCE, handling accumulator and variable references."""
        if isinstance(expr, QueryExpressionProperty):
            # Check if it's the accumulator or variable
            if expr.property_name is None:
                if expr.variable_name == acc_name:
                    return acc_name
                elif expr.variable_name == var_name:
                    return var_name
            elif expr.variable_name == var_name:
                # Property access on the lambda variable (e.g., rel.amount)
                # For edge structs in path_edges, use struct field access
                return f"{var_name}.{expr.property_name}"
            elif expr.variable_name == acc_name:
                # Property access on accumulator (if it's a struct)
                return f"{acc_name}.{expr.property_name}"
            return self.render_expression(expr, context_op)
        elif isinstance(expr, QueryExpressionBinary):
            if not expr.left_expression or not expr.right_expression or not expr.operator:
                return "NULL"
            left = self._render_reduce_expression(
                expr.left_expression, acc_name, var_name, context_op
            )
            right = self._render_reduce_expression(
                expr.right_expression, acc_name, var_name, context_op
            )
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)
        elif isinstance(expr, QueryExpressionFunction):
            params = [
                self._render_reduce_expression(p, acc_name, var_name, context_op)
                for p in expr.parameters
            ]
            # Use the same function rendering logic
            return self._render_function_with_params(expr.function, params)
        else:
            return self.render_expression(expr, context_op)

    def _render_function_with_params(self, func: Function, params: list[str]) -> str:
        """Render a function with pre-rendered parameters.

        Delegates to the FUNCTION_TEMPLATES registry for simple functions.
        Falls back to _render_function (which handles complex handlers) for
        functions not in the registry.
        """
        tmpl = FUNCTION_TEMPLATES.get(func)
        if tmpl is not None:
            return render_from_template(tmpl, params)
        raise TranspilerNotSupportedException(
            f"Function '{func.name.lower()}()' is not supported inside "
            f"list predicates. See docs/limitations.md."
        )

    def _render_exists(
        self, expr: QueryExpressionExists, context_op: LogicalOperator
    ) -> str:
        """Render an EXISTS subquery expression.

        Generates a correlated subquery for EXISTS patterns:
        EXISTS { (p)-[:ACTED_IN]->(:Movie) }
        becomes:
        EXISTS (
          SELECT 1
          FROM graph.ActedIn r
          JOIN graph.Movie m ON r.target_id = m.id
          WHERE r.source_id = outer_query.p_id
        )

        All SQL metadata (table names, column names) is read from
        ``EnrichedExistsExpr`` produced by the enrichment pass, keyed by
        ``id(expr)``.
        """
        prefix = "NOT " if expr.is_negated else ""

        if expr.subquery:
            return f"{prefix}EXISTS (SELECT 1)"

        if not expr.pattern_entities:
            return f"{prefix}EXISTS (SELECT 1)"

        # Parse pattern entities for source/relationship/target
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
            return f"{prefix}EXISTS (SELECT 1)"

        # Read pre-resolved SQL metadata from enrichment
        enriched = (
            self._ctx.enriched.exists_exprs.get(id(expr))
            if self._ctx.enriched
            else None
        )
        if not enriched:
            rel_name = relationship.entity_name
            return f"{prefix}EXISTS (SELECT 1 /* unknown relationship: {rel_name} */)"

        rel_table = enriched.rel_table_name
        source_id_col = enriched.source_id_col
        target_id_col = enriched.target_id_col

        # Build subquery parts
        lines = []
        lines.append("SELECT 1")
        lines.append(f"FROM {rel_table} _exists_rel")

        # If target node exists and has a type, join to target table
        if target_node and target_node.entity_name and enriched.target_table_name:
            target_node_id_col = enriched.target_node_id_col
            if expr.target_join_uses_sink:
                edge_join_col = target_id_col
            else:
                edge_join_col = source_id_col
            lines.append(
                f"JOIN {enriched.target_table_name} _exists_target "
                f"ON _exists_rel.{edge_join_col} = _exists_target.{target_node_id_col}"
            )

        # Correlate with outer query using source node's ID
        source_alias = source_node.alias or "_src"
        source_node_id_col = enriched.source_node_id_col
        outer_field = f"{self._ctx.COLUMN_PREFIX}{source_alias}_{source_node_id_col}"

        if expr.correlation_uses_source:
            correlation = f"_exists_rel.{source_id_col} = {outer_field}"
        else:
            correlation = f"_exists_rel.{target_id_col} = {outer_field}"

        where_parts = [correlation]

        if expr.where_expression:
            where_rendered = self.render_expression(expr.where_expression, context_op)
            where_parts.append(where_rendered)

        lines.append("WHERE " + " AND ".join(where_parts))

        subquery = " ".join(lines)
        return f"{prefix}EXISTS ({subquery})"

    def has_aggregation(self, expr: QueryExpression) -> bool:
        """Check if an expression contains an aggregation function."""
        if isinstance(expr, QueryExpressionAggregationFunction):
            return True
        if isinstance(expr, QueryExpressionBinary):
            left_has = (
                self.has_aggregation(expr.left_expression)
                if expr.left_expression
                else False
            )
            right_has = (
                self.has_aggregation(expr.right_expression)
                if expr.right_expression
                else False
            )
            return left_has or right_has
        if isinstance(expr, QueryExpressionFunction):
            return any(self.has_aggregation(p) for p in expr.parameters)
        return False

    def references_aliases(self, expr: QueryExpression, aliases: set[str]) -> bool:
        """Check if an expression references any of the given aliases.

        This is used to detect expressions like `shared_cards + shared_merchants`
        where `shared_merchants` is an aggregate alias. Such expressions should
        not be included in GROUP BY because they reference aggregates.
        """
        if isinstance(expr, QueryExpressionProperty):
            # Bare variable reference (e.g., shared_merchants)
            if expr.variable_name and not expr.property_name:
                return expr.variable_name in aliases
            return False
        if isinstance(expr, QueryExpressionBinary):
            left_refs = (
                self.references_aliases(expr.left_expression, aliases)
                if expr.left_expression
                else False
            )
            right_refs = (
                self.references_aliases(expr.right_expression, aliases)
                if expr.right_expression
                else False
            )
            return left_refs or right_refs
        if isinstance(expr, QueryExpressionFunction):
            return any(self.references_aliases(p, aliases) for p in expr.parameters)
        if isinstance(expr, QueryExpressionAggregationFunction):
            # Aggregation functions don't reference aliases in the same sense
            return False
        return False

    def _collect_entity_props_from_schema(
        self,
        entity_var: str,
        context_op: "LogicalOperator",
    ) -> tuple[EntityField | None, list[tuple[str, str]]]:
        """Collect (prop_name, sql_col) pairs from an entity's schema.

        Shared core of render_entity_as_struct and _render_entity_in_collect_as_struct.

        Returns:
            (entity_field_or_None, props_list)
        """
        prefix = f"{self._ctx.COLUMN_PREFIX}{entity_var}_"
        props: list[tuple[str, str]] = []

        entity_field: EntityField | None = None
        if hasattr(context_op, 'input_schema') and context_op.input_schema:
            for field in context_op.input_schema:
                if isinstance(field, EntityField) and field.field_alias == entity_var:
                    entity_field = field
                    break

        if not entity_field:
            return None, props

        def add_prop(pn: str) -> None:
            # DEFENSIVE: after WITH boundaries (e.g. post-VLP), field_names may
            # already be fully prefixed (e.g. _gsql2rsql_b_vertex_key). Use them
            # directly and strip the prefix to recover the property key —
            # re-prefixing would produce a dangling double-prefixed column.
            if pn.startswith(prefix):
                props.append((pn[len(prefix):], pn))
            elif pn.startswith(self._ctx.COLUMN_PREFIX):
                props.append((pn, pn))
            else:
                props.append((pn, f"{prefix}{pn}"))

        if entity_field.entity_type == EntityType.NODE:
            if entity_field.node_join_field:
                add_prop(entity_field.node_join_field.field_name)
            for pf in entity_field.encapsulated_fields:
                add_prop(pf.field_name)
        else:
            # Derive key/column from the schema's real source/sink column names
            # (mirrors the NODE branch). Hardcoding "src"/"dst" only works when
            # the schema happens to use those names; with custom columns
            # (e.g. source_node_id) it produced references to columns never
            # materialized in the inner projection (UNRESOLVED_COLUMN).
            if entity_field.rel_source_join_field:
                add_prop(entity_field.rel_source_join_field.field_name)
            if entity_field.rel_sink_join_field:
                add_prop(entity_field.rel_sink_join_field.field_name)
            for pf in entity_field.encapsulated_fields:
                add_prop(pf.field_name)

        return entity_field, props

    @staticmethod
    def _build_named_struct(
        props: list[tuple[str, str]], fallback_id_col: str
    ) -> str:
        """Deduplicate, sort, and format as NAMED_STRUCT."""
        seen: set[str] = set()
        sorted_props = []
        for prop_name, sql_col in sorted(props, key=lambda x: x[0]):
            if prop_name not in seen:
                seen.add(prop_name)
                sorted_props.append((prop_name, sql_col))

        if sorted_props:
            parts = [f"'{pn}', {sc}" for pn, sc in sorted_props]
            return f"NAMED_STRUCT({', '.join(parts)})"
        return f"NAMED_STRUCT('id', {fallback_id_col})"

    def render_entity_as_struct(
        self,
        resolved_proj: "ResolvedProjection",
        entity_var: str,
        context_op: "LogicalOperator",
    ) -> str | None:
        """Render an entity reference as NAMED_STRUCT with all properties.

        Returns None if the variable is not an entity (e.g., UNWIND variable).
        """
        entity_field, props = self._collect_entity_props_from_schema(
            entity_var, context_op
        )
        if not entity_field:
            return None

        prefix = f"{self._ctx.COLUMN_PREFIX}{entity_var}_"
        fallback = resolved_proj.entity_id_column or f"{prefix}id"
        return self._build_named_struct(props, fallback)

    def _render_entity_in_collect_as_struct(
        self,
        entity_var: str,
        context_op: "LogicalOperator",
    ) -> str:
        """Render an entity inside collect() as NAMED_STRUCT."""
        prefix = f"{self._ctx.COLUMN_PREFIX}{entity_var}_"
        _, props = self._collect_entity_props_from_schema(
            entity_var, context_op
        )

        # Fallback 1: Try required columns
        if not props and self._ctx.required_columns:
            for col in sorted(self._ctx.required_columns):
                if col.startswith(prefix):
                    props.append((col[len(prefix):], col))

        # Fallback 2: Try resolved expressions
        if not props and self._ctx.resolution_result is not None:
            op_id = context_op.operator_debug_id
            if op_id in self._ctx.resolution_result.resolved_projections:
                for rp in self._ctx.resolution_result.resolved_projections[op_id]:
                    for _key, ref in rp.expression.column_refs.items():
                        if ref.original_variable == entity_var:
                            pn = ref.original_property if ref.original_property else "id"
                            props.append((pn, ref.sql_column_name))

        return self._build_named_struct(props, f"{prefix}id")

    def get_entity_properties_for_aggregation(
        self, op: ProjectionOperator
    ) -> list[str]:
        """Get required entity property columns that need to be projected through GROUP BY.

        When a node variable (e.g., 'c') is projected through a GROUP BY, only its
        ID column (_gsql2rsql_c_id) is normally included. But if downstream operators need
        other properties (e.g., c.name -> _gsql2rsql_c_name), those must also be projected.

        This method identifies entity variables in the projections and returns
        any required property columns for those entities that aren't already projected.

        Args:
            op: The ProjectionOperator with aggregations.

        Returns:
            List of column aliases (e.g., ['_gsql2rsql_c_name']) to add to the SELECT list.
        """
        extra_columns: list[str] = []
        already_projected: set[str] = set()

        # Find entity variables in the projections (bare entity references)
        # and also track which entity ID columns need to be preserved
        entity_vars: set[str] = set()
        entity_id_columns: dict[str, str] = {}  # entity_var -> id_column_alias

        # Collect all columns that are already projected
        for _, expr in op.projections:
            if isinstance(expr, QueryExpressionProperty):
                if expr.property_name:
                    # Property access - already projected as _gsql2rsql_entity_prop
                    col = self._ctx.get_field_name(expr.variable_name, expr.property_name)
                    already_projected.add(col)
                else:
                    # Bare entity reference - the ID is projected under a different alias
                    entity_vars.add(expr.variable_name)
                    # Use resolution to get the ID column
                    resolved_ref = self._ctx.get_resolved_ref(expr.variable_name, None, op)
                    if resolved_ref:
                        id_col = resolved_ref.sql_column_name
                        entity_id_columns[expr.variable_name] = id_col
                        # Mark ID as already projected to avoid duplicating it in extra_columns
                        # The ID is already in the main projection as "_gsql2rsql_p_id AS p"
                        already_projected.add(id_col)

        # For bare entity references, we generally DON'T need to add their ID column
        # because the bare entity reference already provides the ID value.
        # For example, if we project `a` (rendering as `_gsql2rsql_a_id AS a`), downstream
        # code accessing `a.id` should use the alias `a`, not `_gsql2rsql_a_id`.
        #
        # We only add the ID column separately if there are OTHER properties
        # of that entity that need to be projected (e.g., _gsql2rsql_a_name for a.name).
        # In that case, we need _gsql2rsql_a_id as a grouping key alongside _gsql2rsql_a_name.
        for entity_var, id_col in entity_id_columns.items():
            if id_col not in already_projected:
                # Only add if we're adding OTHER properties for this entity
                # (not just the ID column itself)
                has_other_properties = any(
                    req_col.startswith(f"{self._ctx.COLUMN_PREFIX}{entity_var}_") and req_col != id_col
                    for req_col in self._ctx.required_columns
                )
                if has_other_properties:
                    extra_columns.append(id_col)
                    already_projected.add(id_col)

        # Check required columns for properties of these entities
        for required_col in self._ctx.required_columns:
            if required_col in already_projected:
                continue
            # Check if this column belongs to one of our entity variables
            # Pattern: _gsql2rsql_{entity}_{property}
            for entity_var in entity_vars:
                prefix = f"{self._ctx.COLUMN_PREFIX}{entity_var}_"
                if required_col.startswith(prefix):
                    # Skip the entity's ID column - it's already provided by the bare entity
                    # reference (e.g., _gsql2rsql_a_id is already available via the alias 'a')
                    entity_id_col = entity_id_columns.get(entity_var)
                    if entity_id_col is not None and required_col == entity_id_col:
                        break

                    # Defensive workaround: skip double-prefixed columns.
                    # Root cause is in column_resolver — when var name matches
                    # a schema role name (e.g., "sink"), property "sink_id"
                    # produces _gsql2rsql_sink_sink_id instead of _sink_id.
                    # Fix: filter in column_resolver when expanding entity
                    # properties for bare RETURN clauses.
                    suffix = required_col[len(prefix):]
                    if suffix.startswith(f"{entity_var}_"):
                        break

                    # This is a property of an entity variable we're projecting
                    extra_columns.append(required_col)
                    already_projected.add(required_col)
                    break

        # Sort to ensure deterministic output (avoid non-determinism from set iteration)
        return sorted(extra_columns)

    def render_edge_filter_expression(self, expr: QueryExpression) -> str:
        """Render an edge filter expression using DIRECT column references.

        This method is specifically for rendering predicates that have been
        pushed down into the recursive CTE. Unlike render_expression, this
        renders property accesses as direct SQL column references (e.g., e.amount)
        rather than entity-prefixed aliases (e.g., _gsql2rsql_e_amount).

        This is necessary because inside the CTE, we reference edge columns
        directly from the edge table (aliased as 'e'), not through the
        entity field aliasing system used for JOINs.

        Example:
            Input: QueryExpressionProperty(variable_name="e", property_name="amount")
            render_expression output: "_gsql2rsql_e_amount" (wrong for CTE)
            This method output: "e.amount" (correct for CTE)

        Args:
            expr: The expression to render (already rewritten with 'e' prefix)

        Returns:
            SQL string with direct column references
        """
        if isinstance(expr, QueryExpressionProperty):
            # Render as direct column reference: e.amount, e.timestamp
            if expr.property_name:
                return f"{expr.variable_name}.{expr.property_name}"
            return expr.variable_name

        elif isinstance(expr, QueryExpressionBinary):
            # Recursively render binary expressions
            if not expr.operator or not expr.left_expression or not expr.right_expression:
                return "NULL"
            left = self.render_edge_filter_expression(expr.left_expression)
            right = self.render_edge_filter_expression(expr.right_expression)
            pattern = OPERATOR_PATTERNS.get(expr.operator.name, "({0}) ? ({1})")
            return pattern.format(left, right)

        elif isinstance(expr, QueryExpressionFunction):
            # Render function calls with recursive parameter rendering
            params = [self.render_edge_filter_expression(p) for p in expr.parameters]
            func = expr.function

            # CTE-specific overrides: these functions have different semantics
            # inside the recursive CTE (direct column refs, simpler DATE handling).
            if func == Function.DATETIME:
                return "CURRENT_TIMESTAMP()"
            elif func == Function.DATE:
                if params:
                    return f"DATE({params[0]})"
                return "CURRENT_DATE()"
            elif func == Function.DURATION:
                if params:
                    duration_str = params[0].strip("'\"")
                    return self._convert_duration_to_interval(duration_str)
                return "INTERVAL 0 DAY"

            # Data-driven dispatch for simple functions
            tmpl = FUNCTION_TEMPLATES.get(func)
            if tmpl is not None:
                return render_from_template(tmpl, params)

            raise TranspilerNotSupportedException(
                f"Function '{func.name.lower()}()' is not supported inside "
                f"a variable-length path edge filter. See "
                f"docs/limitations.md."
            )

        elif isinstance(expr, QueryExpressionValue):
            # Render literal values
            if expr.value is None:
                return "NULL"
            elif isinstance(expr.value, str):
                escaped = expr.value.replace("'", "''")
                return f"'{escaped}'"
            elif isinstance(expr.value, bool):
                return "TRUE" if expr.value else "FALSE"
            else:
                return str(expr.value)

        elif isinstance(expr, QueryExpressionList):
            # Render list literals for IN operator: [1, 2, 3] -> (1, 2, 3)
            # Use recursive call to render_edge_filter_expression for items
            # to maintain consistent rendering within the CTE context.
            # List items in IN clauses are typically values (strings, numbers),
            # which render_edge_filter_expression handles correctly.
            items = [self.render_edge_filter_expression(item) for item in expr.items]
            return f"({', '.join(items)})"

        # Fallback: try the regular renderer (shouldn't normally reach here)
        # This is a safety net for expression types we haven't handled
        return str(expr)

    def _convert_duration_to_interval(self, duration_str: str) -> str:
        """Convert ISO 8601 duration string to Databricks INTERVAL.

        Examples:
            P7D -> INTERVAL 7 DAY
            P1M -> INTERVAL 1 MONTH
            PT1H -> INTERVAL 1 HOUR
        """
        import re

        # Simple patterns for common durations
        if match := re.match(r"P(\d+)D", duration_str):
            return f"INTERVAL {match.group(1)} DAY"
        elif match := re.match(r"P(\d+)M", duration_str):
            return f"INTERVAL {match.group(1)} MONTH"
        elif match := re.match(r"P(\d+)Y", duration_str):
            return f"INTERVAL {match.group(1)} YEAR"
        elif match := re.match(r"PT(\d+)H", duration_str):
            return f"INTERVAL {match.group(1)} HOUR"
        elif match := re.match(r"PT(\d+)M", duration_str):
            return f"INTERVAL {match.group(1)} MINUTE"
        elif match := re.match(r"PT(\d+)S", duration_str):
            return f"INTERVAL {match.group(1)} SECOND"
        else:
            # Default to days if format not recognized
            return "INTERVAL 1 DAY"

    def _render_map_literal(
        self, expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render a map literal as a Databricks SQL STRUCT.

        Cypher: {name: 'John', age: 30}
        Databricks SQL: STRUCT('John' AS name, 30 AS age)
        """
        if not expr.entries:
            return "STRUCT()"

        parts = []
        for key, value in expr.entries:
            value_sql = self.render_expression(value, context_op)
            parts.append(f"{value_sql} AS {key}")

        return f"STRUCT({', '.join(parts)})"

    def _render_date_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render date({year: y, month: m, day: d}) as MAKE_DATE.

        Cypher: date({year: 2024, month: 1, day: 15})
        Databricks SQL: MAKE_DATE(2024, 1, 15)
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self.render_expression(
                value, context_op
            )

        year = entries_dict.get("year", "1970")
        month = entries_dict.get("month", "1")
        day = entries_dict.get("day", "1")

        return f"MAKE_DATE({year}, {month}, {day})"

    def _render_datetime_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render datetime({...}) as MAKE_TIMESTAMP.

        Cypher: datetime({year: 2024, month: 1, day: 15, hour: 10})
        Databricks SQL: MAKE_TIMESTAMP(2024, 1, 15, 10, 0, 0)
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self.render_expression(
                value, context_op
            )

        year = entries_dict.get("year", "1970")
        month = entries_dict.get("month", "1")
        day = entries_dict.get("day", "1")
        hour = entries_dict.get("hour", "0")
        minute = entries_dict.get("minute", "0")
        second = entries_dict.get("second", "0")

        return f"MAKE_TIMESTAMP({year}, {month}, {day}, {hour}, {minute}, {second})"

    def _render_time_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render time({hour: h, minute: m, second: s}) as time string.

        Cypher: time({hour: 10, minute: 30, second: 0})
        Databricks SQL: '10:30:00'
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self.render_expression(
                value, context_op
            )

        hour = entries_dict.get("hour", "0")
        minute = entries_dict.get("minute", "0")
        second = entries_dict.get("second", "0")

        # Build time as formatted string
        return f"CONCAT({hour}, ':', {minute}, ':', {second})"

    def _render_duration_from_map(
        self, map_expr: QueryExpressionMapLiteral, context_op: LogicalOperator
    ) -> str:
        """Render duration({...}) as Databricks INTERVAL.

        Cypher: duration({days: 7, hours: 12})
        Databricks SQL: INTERVAL '7' DAY + INTERVAL '12' HOUR
        """
        entries_dict = {}
        for key, value in map_expr.entries:
            entries_dict[key.lower()] = self.render_expression(
                value, context_op
            )

        # Map Cypher duration components to Databricks INTERVAL units
        interval_parts = []
        unit_mapping = {
            "years": "YEAR",
            "months": "MONTH",
            "weeks": "WEEK",
            "days": "DAY",
            "hours": "HOUR",
            "minutes": "MINUTE",
            "seconds": "SECOND",
        }

        for cypher_unit, sql_unit in unit_mapping.items():
            if cypher_unit in entries_dict:
                val = entries_dict[cypher_unit]
                interval_parts.append(f"INTERVAL {val} {sql_unit}")

        if not interval_parts:
            return "INTERVAL '0' DAY"

        return " + ".join(interval_parts)

    def _parse_iso8601_duration(self, duration_str: str) -> str:
        """Parse an ISO 8601 duration string to Databricks INTERVAL expression.

        ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
        Examples:
        - P7D -> INTERVAL 7 DAY
        - PT1H -> INTERVAL 1 HOUR
        - P30D -> INTERVAL 30 DAY
        - PT5M -> INTERVAL 5 MINUTE
        - P1Y2M3D -> INTERVAL 1 YEAR + INTERVAL 2 MONTH + INTERVAL 3 DAY
        - PT1H30M -> INTERVAL 1 HOUR + INTERVAL 30 MINUTE
        - P1DT12H -> INTERVAL 1 DAY + INTERVAL 12 HOUR

        Args:
            duration_str: ISO 8601 duration string (e.g., 'P7D', 'PT1H')

        Returns:
            Databricks SQL INTERVAL expression
        """
        import re

        if not duration_str:
            return "INTERVAL '0' DAY"

        # Remove leading 'P'
        s = duration_str.strip().upper()
        if not s.startswith("P"):
            return "INTERVAL '0' DAY"
        s = s[1:]

        interval_parts = []

        # Split by 'T' to separate date and time parts
        if "T" in s:
            date_part, time_part = s.split("T", 1)
        else:
            date_part = s
            time_part = ""

        # Parse date part: [n]Y[n]M[n]W[n]D
        date_pattern = re.compile(r"(\d+)([YMWD])")
        for match in date_pattern.finditer(date_part):
            value = match.group(1)
            unit = match.group(2)
            unit_map = {"Y": "YEAR", "M": "MONTH", "W": "WEEK", "D": "DAY"}
            if unit in unit_map:
                interval_parts.append(f"INTERVAL {value} {unit_map[unit]}")

        # Parse time part: [n]H[n]M[n]S
        time_pattern = re.compile(r"(\d+(?:\.\d+)?)([HMS])")
        for match in time_pattern.finditer(time_part):
            value = match.group(1)
            unit = match.group(2)
            unit_map = {"H": "HOUR", "M": "MINUTE", "S": "SECOND"}
            if unit in unit_map:
                interval_parts.append(f"INTERVAL {value} {unit_map[unit]}")

        if not interval_parts:
            return "INTERVAL '0' DAY"

        return " + ".join(interval_parts)
