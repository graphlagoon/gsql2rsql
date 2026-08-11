"""Abstract Syntax Tree (AST) nodes for openCypher queries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, TypeVar

from gsql2rsql.common.utils import change_indentation
from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperatorInfo,
    Function,
    ListPredicateType,
)

# TypeVars for generic methods (Python 3.11 compatible, replaces PEP 695 syntax)
_TTreeNode = TypeVar("_TTreeNode", bound="TreeNode")
_TQueryExpr = TypeVar("_TQueryExpr", bound="QueryExpression")


class TreeNode(ABC):
    """Base class for all AST nodes."""

    @property
    @abstractmethod
    def children(self) -> list[TreeNode]:
        """Return the children of this node."""
        ...

    def dump_tree(self, depth: int = 0) -> str:
        """
        Dump the tree in textual format for debugging.

        Args:
            depth: Current depth for indentation.

        Returns:
            String representation of the tree.
        """
        lines = [
            change_indentation(f"+{self.__class__.__name__}", depth),
            change_indentation(f"|{self}", depth),
        ]
        for child in self.children:
            lines.append(child.dump_tree(depth + 1))
        return "\n".join(lines)

    def get_children_of_type(
        self, node_type: type[_TTreeNode]
    ) -> Iterator[_TTreeNode]:
        """
        Get all descendants of a specific type.

        Args:
            node_type: The type of nodes to find.

        Yields:
            Nodes of the specified type.
        """
        if isinstance(self, node_type):
            yield self
        for child in self.children:
            yield from child.get_children_of_type(node_type)


# ==============================================================================
# Query Expression Nodes
# ==============================================================================


@dataclass
class QueryExpression(TreeNode, ABC):
    """Base class for all query expressions."""

    @property
    def children(self) -> list[TreeNode]:
        return []

    @abstractmethod
    def evaluate_type(self) -> type[Any] | None:
        """
        Static evaluation of the returned data type.

        Returns:
            Python type equivalent of the data type.
        """
        ...

    def get_children_query_expression_type(
        self, expr_type: type[_TQueryExpr]
    ) -> Iterator[_TQueryExpr]:
        """Get all descendant expressions of a specific type."""
        return self.get_children_of_type(expr_type)


@dataclass
class QueryExpressionValue(QueryExpression):
    """A literal value expression."""

    value: Any
    value_type: type[Any]

    @property
    def children(self) -> list[TreeNode]:
        return []

    def evaluate_type(self) -> type[Any] | None:
        return self.value_type

    def __str__(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        if isinstance(self.value, bool):
            return str(self.value).lower()
        return str(self.value)


@dataclass
class QueryExpressionProperty(QueryExpression):
    """A property access expression (e.g., n.name)."""

    variable_name: str
    property_name: str | None = None
    data_type: type[Any] | None = None

    @property
    def children(self) -> list[TreeNode]:
        return []

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        if self.property_name:
            return f"{self.variable_name}.{self.property_name}"
        return self.variable_name


@dataclass
class QueryExpressionBinary(QueryExpression):
    """A binary expression (e.g., a + b)."""

    operator: BinaryOperatorInfo | None = None
    left_expression: QueryExpression | None = None
    right_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = []
        if self.left_expression:
            result.append(self.left_expression)
        if self.right_expression:
            result.append(self.right_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        if not self.operator:
            return None
        from gsql2rsql.parser.operators import BinaryOperatorType

        if self.operator.operator_type == BinaryOperatorType.LOGICAL:
            return bool
        if self.operator.operator_type == BinaryOperatorType.COMPARISON:
            return bool
        # For value operators, return the type of the operands
        if self.left_expression:
            return self.left_expression.evaluate_type()
        return None

    def __str__(self) -> str:
        op_str = self.operator.name.name if self.operator else "?"
        return f"({self.left_expression} {op_str} {self.right_expression})"


@dataclass
class QueryExpressionFunction(QueryExpression):
    """A function call expression."""

    function: Function
    parameters: list[QueryExpression] = field(default_factory=list)
    data_type: type[Any] | None = None
    # The name as the user wrote it. Set for Function.INVALID so error
    # messages can name the actual function instead of 'INVALID'.
    raw_name: str = ""

    @property
    def children(self) -> list[TreeNode]:
        return list(self.parameters)

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        params = ", ".join(str(p) for p in self.parameters)
        return f"{self.function.name}({params})"


@dataclass
class QueryExpressionAggregationFunction(QueryExpression):
    """An aggregation function call expression.

    Supports ordered aggregation for COLLECT:
    COLLECT(x ORDER BY y DESC) -> ARRAY_SORT(COLLECT_LIST(STRUCT(y, x)), ...), s -> s.x
    """

    aggregation_function: AggregationFunction
    is_distinct: bool = False
    inner_expression: QueryExpression | None = None
    data_type: type[Any] | None = None
    # Order by for ordered aggregation (e.g., COLLECT(x ORDER BY y DESC))
    # List of (expression, is_descending) tuples
    order_by: list[tuple[QueryExpression, bool]] = field(default_factory=list)
    # Arguments beyond the aggregated expression, e.g. the percentile in
    # percentileCont(x, 0.5). Kept separate from inner_expression because
    # they parameterise the aggregate rather than being aggregated.
    extra_parameters: list[QueryExpression] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = []
        if self.inner_expression:
            result.append(self.inner_expression)
        result.extend(self.extra_parameters)
        for expr, _ in self.order_by:
            result.append(expr)
        return result

    def evaluate_type(self) -> type[Any] | None:
        return self.data_type

    def __str__(self) -> str:
        distinct = "DISTINCT " if self.is_distinct else ""
        inner = str(self.inner_expression) if self.inner_expression else "*"
        extras = "".join(f", {p}" for p in self.extra_parameters)
        order = ""
        if self.order_by:
            items = ", ".join(
                f"{expr} {'DESC' if desc else 'ASC'}"
                for expr, desc in self.order_by
            )
            order = f" ORDER BY {items}"
        return (
            f"{self.aggregation_function.name}"
            f"({distinct}{inner}{extras}{order})"
        )


@dataclass
class QueryExpressionList(QueryExpression):
    """A list expression (e.g., [1, 2, 3])."""

    items: list[QueryExpression] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        return list(self.items)

    def evaluate_type(self) -> type[Any] | None:
        return list

    def __str__(self) -> str:
        items = ", ".join(str(i) for i in self.items)
        return f"[{items}]"


@dataclass
class QueryExpressionMapLiteral(QueryExpression):
    """A map literal expression (e.g., {name: 'John', age: 30}).

    In Cypher, map literals use key: value syntax.
    Databricks SQL: STRUCT(value1 AS key1, value2 AS key2, ...)

    Can be used for:
    - Creating structured data: {name: 'John', age: 30}
    - Date/time construction: date({year: 2024, month: 1, day: 15})
    - Duration specification: duration({days: 7, hours: 12})
    """

    # Map entries as list of (key, value) tuples
    entries: list[tuple[str, QueryExpression]] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        return [value for _, value in self.entries]

    def evaluate_type(self) -> type[Any] | None:
        return dict

    def __str__(self) -> str:
        entries = ", ".join(f"{key}: {value}" for key, value in self.entries)
        return f"{{{entries}}}"


@dataclass
class QueryExpressionCaseExpression(QueryExpression):
    """A CASE expression."""

    test_expression: QueryExpression | None = None
    alternatives: list[tuple[QueryExpression, QueryExpression]] = field(
        default_factory=list
    )
    else_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = []
        if self.test_expression:
            result.append(self.test_expression)
        for when_expr, then_expr in self.alternatives:
            result.extend([when_expr, then_expr])
        if self.else_expression:
            result.append(self.else_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        if self.alternatives:
            return self.alternatives[0][1].evaluate_type()
        if self.else_expression:
            return self.else_expression.evaluate_type()
        return None

    def __str__(self) -> str:
        parts = ["CASE"]
        if self.test_expression:
            parts.append(str(self.test_expression))
        for when_expr, then_expr in self.alternatives:
            parts.append(f"WHEN {when_expr} THEN {then_expr}")
        if self.else_expression:
            parts.append(f"ELSE {self.else_expression}")
        parts.append("END")
        return " ".join(parts)


@dataclass
class QueryExpressionWithAlias(QueryExpression):
    """An expression with an alias (e.g., n.name AS name)."""

    inner_expression: QueryExpression
    alias: str

    @property
    def children(self) -> list[TreeNode]:
        return [self.inner_expression]

    def evaluate_type(self) -> type[Any] | None:
        return self.inner_expression.evaluate_type()

    def __str__(self) -> str:
        return f"{self.inner_expression} AS {self.alias}"


@dataclass
class QueryExpressionParameter(QueryExpression):
    """A parameter expression (e.g., $watchlist, $1).

    Used for parameterized queries where values are provided at runtime.
    """

    parameter_name: str

    @property
    def children(self) -> list[TreeNode]:
        return []

    def evaluate_type(self) -> type[Any] | None:
        # Type unknown at parse time
        return None

    def __str__(self) -> str:
        return f"${self.parameter_name}"


@dataclass
class QueryExpressionListPredicate(QueryExpression):
    """A list predicate expression (ALL, ANY, NONE, SINGLE).

    Represents quantifier expressions like:
    - ALL(x IN list WHERE predicate)
    - ANY(x IN list WHERE predicate)
    - NONE(x IN list WHERE predicate)
    - SINGLE(x IN list WHERE predicate)
    """

    predicate_type: ListPredicateType
    variable_name: str
    list_expression: QueryExpression
    filter_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = [self.list_expression]
        if self.filter_expression:
            result.append(self.filter_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        return bool

    def __str__(self) -> str:
        filter_part = f" WHERE {self.filter_expression}" if self.filter_expression else ""
        return f"{self.predicate_type.name}({self.variable_name} IN {self.list_expression}{filter_part})"


@dataclass
class QueryExpressionListComprehension(QueryExpression):
    """A list comprehension expression.

    Represents [x IN list WHERE predicate | expression]:
    - FILTER: [x IN list WHERE x > 0] -> FILTER(list, x -> x > 0)
    - MAP: [x IN list | x * 2] -> TRANSFORM(list, x -> x * 2)
    - FILTER + MAP: [x IN list WHERE x > 0 | x * 2] -> TRANSFORM(FILTER(list, x -> x > 0), x -> x * 2)

    Databricks SQL: TRANSFORM(FILTER(list, x -> pred), x -> expr)
    """

    variable_name: str
    list_expression: QueryExpression
    filter_expression: QueryExpression | None = None
    map_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = [self.list_expression]
        if self.filter_expression:
            result.append(self.filter_expression)
        if self.map_expression:
            result.append(self.map_expression)
        return result

    def evaluate_type(self) -> type[Any] | None:
        return list

    def __str__(self) -> str:
        filter_part = f" WHERE {self.filter_expression}" if self.filter_expression else ""
        map_part = f" | {self.map_expression}" if self.map_expression else ""
        return f"[{self.variable_name} IN {self.list_expression}{filter_part}{map_part}]"


@dataclass
class QueryExpressionReduce(QueryExpression):
    """A REDUCE expression.

    Represents REDUCE(accumulator = initial, x IN list | expression):
    - REDUCE(total = 0, x IN amounts | total + x)

    Databricks SQL: AGGREGATE(list, initial, (acc, x) -> expression)
    """

    accumulator_name: str
    initial_value: QueryExpression
    variable_name: str
    list_expression: QueryExpression
    reducer_expression: QueryExpression

    @property
    def children(self) -> list[TreeNode]:
        return [
            self.initial_value,
            self.list_expression,
            self.reducer_expression,
        ]

    def evaluate_type(self) -> type[Any] | None:
        return self.initial_value.evaluate_type()

    def __str__(self) -> str:
        return (
            f"REDUCE({self.accumulator_name} = {self.initial_value}, "
            f"{self.variable_name} IN {self.list_expression} | {self.reducer_expression})"
        )


@dataclass
class QueryExpressionExists(QueryExpression):
    """An EXISTS subquery expression.

    Represents EXISTS { pattern } or EXISTS { MATCH pattern WHERE cond RETURN ... }
    Used for semi-join semantics in WHERE clauses.

    Resolved Context Fields (set by planner during resolution):
        These fields are populated by the planner to encode direction-dependent
        decisions. The renderer uses these pre-resolved values directly, avoiding
        direction checks and maintaining Separation of Concerns.
    """

    # Pattern entities forming the EXISTS pattern (for pattern-based EXISTS)
    pattern_entities: list[Entity] = field(default_factory=list)
    # Optional WHERE clause within the EXISTS
    where_expression: QueryExpression | None = None
    # Whether this is NOT EXISTS
    is_negated: bool = False
    # For full subquery EXISTS (EXISTS { MATCH ... RETURN ... })
    subquery: QueryNode | None = None

    # Pre-resolved context for rendering (populated by planner during resolution)
    # These remove the need for the renderer to check relationship direction.
    # correlation_uses_source: True = use source_id for outer correlation,
    #                          False = use sink_id (for BACKWARD direction)
    correlation_uses_source: bool = True
    # target_join_uses_sink: True = join target node on sink_id,
    #                        False = join on source_id (for BACKWARD direction)
    target_join_uses_sink: bool = True

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = list(self.pattern_entities)
        if self.where_expression:
            result.append(self.where_expression)
        if self.subquery:
            result.append(self.subquery)
        return result

    def evaluate_type(self) -> type[Any] | None:
        return bool

    def __str__(self) -> str:
        neg = "NOT " if self.is_negated else ""
        if self.subquery:
            return f"{neg}EXISTS {{ {self.subquery} }}"
        pattern = ", ".join(str(e) for e in self.pattern_entities)
        where_part = f" WHERE {self.where_expression}" if self.where_expression else ""
        return f"{neg}EXISTS {{ {pattern}{where_part} }}"

    def resolve_direction_context(
        self, direction: RelationshipDirection
    ) -> None:
        """Resolve direction-dependent fields based on relationship direction.

        This method is called by the planner to pre-compute direction-based
        decisions. The renderer uses these resolved values directly, maintaining
        Separation of Concerns (planner decides semantics, renderer just generates SQL).

        Args:
            direction: The relationship direction from the EXISTS pattern.

        Side Effects:
            Sets correlation_uses_source and target_join_uses_sink fields.
        """
        # For BACKWARD direction (<-): source and sink are swapped from the
        # pattern's perspective. The "source" node in the pattern actually
        # correlates with the edge's sink column.
        if direction == RelationshipDirection.BACKWARD:
            self.correlation_uses_source = False  # Use sink_id for outer correlation
            self.target_join_uses_sink = False  # Join target on source_id
        else:
            # FORWARD or BOTH: use standard direction
            self.correlation_uses_source = True  # Use source_id for outer correlation
            self.target_join_uses_sink = True  # Join target on sink_id


# ==============================================================================
# Query Structure Nodes
# ==============================================================================


class SortOrder(Enum):
    """Sort order for ORDER BY clauses."""

    ASC = auto()
    DESC = auto()


@dataclass
class SortItem(TreeNode):
    """A single item in an ORDER BY clause."""

    expression: QueryExpression
    order: SortOrder = SortOrder.ASC

    @property
    def children(self) -> list[TreeNode]:
        return [self.expression]

    def __str__(self) -> str:
        order_str = "DESC" if self.order == SortOrder.DESC else "ASC"
        return f"{self.expression} {order_str}"


@dataclass
class LimitClause(TreeNode):
    """A LIMIT clause."""

    limit_expression: QueryExpression
    skip_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = [self.limit_expression]
        if self.skip_expression:
            result.append(self.skip_expression)
        return result

    def __str__(self) -> str:
        skip_part = f"SKIP {self.skip_expression} " if self.skip_expression else ""
        return f"{skip_part}LIMIT {self.limit_expression}"


# ==============================================================================
# Entity Nodes (Nodes and Relationships)
# ==============================================================================


@dataclass
class Entity(TreeNode, ABC):
    """Base class for graph entities (nodes and relationships)."""

    alias: str
    entity_name: str

    @property
    def children(self) -> list[TreeNode]:
        return []

    def __str__(self) -> str:
        return f"{self.alias}:{self.entity_name}"


@dataclass
class NodeEntity(Entity):
    """A node entity in a MATCH pattern.

    Supports inline property filters: (p:Person {name: 'Alice', age: 30})

    The inline_properties field stores property filters specified directly
    in the node pattern, which will be converted to WHERE predicates by
    the planner.
    """

    # Inline property filters from pattern: (n:Person {name: 'Alice'})
    inline_properties: QueryExpressionMapLiteral | None = None

    @property
    def children(self) -> list[TreeNode]:
        """Return child nodes for AST traversal."""
        if self.inline_properties:
            return [self.inline_properties]
        return []


class RelationshipDirection(Enum):
    """Direction of a relationship."""

    FORWARD = auto()  # (a)-[r]->(b)
    BACKWARD = auto()  # (a)<-[r]-(b)
    BOTH = auto()  # (a)-[r]-(b)


@dataclass
class RelationshipEntity(Entity):
    """A relationship entity in a MATCH pattern.

    Supports inline property filters: -[r:KNOWS {since: 2020}]->

    The inline_properties field stores property filters specified directly
    in the relationship pattern, which will be converted to WHERE predicates
    by the planner.
    """

    direction: RelationshipDirection = RelationshipDirection.BOTH
    left_entity_name: str = ""
    right_entity_name: str = ""
    # Variable-length path support
    min_hops: int | None = None  # None means fixed (not variable length)
    max_hops: int | None = None  # None means unlimited

    # Inline property filters from pattern: -[r:KNOWS {weight: 1.0}]->
    inline_properties: QueryExpressionMapLiteral | None = None

    @property
    def is_variable_length(self) -> bool:
        """Check if this is a variable-length relationship pattern."""
        return self.min_hops is not None or self.max_hops is not None

    @property
    def children(self) -> list[TreeNode]:
        """Return child nodes for AST traversal."""
        if self.inline_properties:
            return [self.inline_properties]
        return []

    def __str__(self) -> str:
        dir_char = {
            RelationshipDirection.FORWARD: "->",
            RelationshipDirection.BACKWARD: "<-",
            RelationshipDirection.BOTH: "-",
        }[self.direction]
        hops = ""
        if self.is_variable_length:
            if self.min_hops is not None and self.max_hops is not None:
                hops = f"*{self.min_hops}..{self.max_hops}"
            elif self.min_hops is not None:
                hops = f"*{self.min_hops}.."
            elif self.max_hops is not None:
                hops = f"*..{self.max_hops}"
            else:
                hops = "*"
        return f"[{self.alias}:{self.entity_name}{hops}]{dir_char}"


# ==============================================================================
# Clause Nodes
# ==============================================================================


@dataclass
class MatchClause(TreeNode):
    """A MATCH clause.

    Supports named paths like: MATCH path = (a)-[*1..3]->(b)
    The path_variable stores the variable name (e.g., 'path') for later use
    in functions like relationships(path) or nodes(path).
    """

    pattern_parts: list[Entity] = field(default_factory=list)
    is_optional: bool = False
    where_expression: QueryExpression | None = None
    path_variable: str = ""  # Named path variable, e.g., "path" in "MATCH path = ..."

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = list(self.pattern_parts)
        if self.where_expression:
            result.append(self.where_expression)
        return result

    def __str__(self) -> str:
        optional = "OPTIONAL " if self.is_optional else ""
        path_assign = f"{self.path_variable} = " if self.path_variable else ""
        pattern = ", ".join(str(p) for p in self.pattern_parts)
        where_part = f" WHERE {self.where_expression}" if self.where_expression else ""
        return f"{optional}MATCH {path_assign}{pattern}{where_part}"


@dataclass
class UnwindClause(TreeNode):
    """An UNWIND clause that expands a list into rows.

    UNWIND expression AS variable

    In Databricks SQL, this becomes LATERAL EXPLODE:
    FROM ..., LATERAL EXPLODE(expression) AS t(variable)
    """

    list_expression: QueryExpression | None = None
    variable_name: str = ""

    @property
    def children(self) -> list[TreeNode]:
        if self.list_expression:
            return [self.list_expression]
        return []

    def __str__(self) -> str:
        return f"UNWIND {self.list_expression} AS {self.variable_name}"


# ==============================================================================
# Query Nodes
# ==============================================================================


@dataclass
class QueryNode(TreeNode, ABC):
    """Base class for query nodes."""

    pass


@dataclass
class PartialQueryNode(QueryNode):
    """A partial query (single reading/updating clause)."""

    match_clauses: list[MatchClause] = field(default_factory=list)
    unwind_clauses: list[UnwindClause] = field(default_factory=list)
    where_expression: QueryExpression | None = None
    return_body: list[QueryExpressionWithAlias] = field(default_factory=list)
    order_by: list[SortItem] = field(default_factory=list)
    limit_clause: LimitClause | None = None
    is_distinct: bool = False
    # HAVING expression (from WITH ... WHERE on aggregated columns)
    having_expression: QueryExpression | None = None

    @property
    def children(self) -> list[TreeNode]:
        result: list[TreeNode] = list(self.match_clauses)
        result.extend(self.unwind_clauses)
        if self.where_expression:
            result.append(self.where_expression)
        result.extend(self.return_body)
        result.extend(self.order_by)
        if self.limit_clause:
            result.append(self.limit_clause)
        if self.having_expression:
            result.append(self.having_expression)
        return result

    def __str__(self) -> str:
        parts: list[str] = []
        for match in self.match_clauses:
            parts.append(str(match))
        for unwind in self.unwind_clauses:
            parts.append(str(unwind))
        if self.where_expression:
            parts.append(f"WHERE {self.where_expression}")
        if self.return_body:
            distinct = "DISTINCT " if self.is_distinct else ""
            returns = ", ".join(str(r) for r in self.return_body)
            parts.append(f"RETURN {distinct}{returns}")
        if self.order_by:
            order = ", ".join(str(o) for o in self.order_by)
            parts.append(f"ORDER BY {order}")
        if self.limit_clause:
            parts.append(str(self.limit_clause))
        return " ".join(parts)


@dataclass
class SingleQueryNode(QueryNode):
    """A single complete query."""

    parts: list[PartialQueryNode] = field(default_factory=list)

    @property
    def children(self) -> list[TreeNode]:
        return list(self.parts)

    def __str__(self) -> str:
        return " ".join(str(p) for p in self.parts)


class InfixOperator(Enum):
    """Set operators for combining queries."""

    UNION = auto()
    UNION_ALL = auto()


@dataclass
class InfixQueryNode(QueryNode):
    """A query combining two queries with a set operator."""

    operator: InfixOperator
    left_query: QueryNode
    right_query: QueryNode

    @property
    def children(self) -> list[TreeNode]:
        return [self.left_query, self.right_query]

    def __str__(self) -> str:
        op = "UNION ALL" if self.operator == InfixOperator.UNION_ALL else "UNION"
        return f"({self.left_query}) {op} ({self.right_query})"
