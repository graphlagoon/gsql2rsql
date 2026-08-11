"""Databricks SQL dialect constants.

Type mappings, operator patterns, aggregation patterns, and function
templates for the Databricks SQL target dialect.  These are pure data —
no logic — so that the renderer stays "stupid and safe".

The **Function Registry Pattern** (``FUNCTION_TEMPLATES``) replaces the
260-line ``elif`` chain that previously lived in ``ExpressionRenderer._render_function``.
Every simple Cypher → SQL function mapping is declared once here and consumed
by three dispatch sites:

1. ``ExpressionRenderer._render_function`` — main expression rendering
2. ``ExpressionRenderer._render_function_with_params`` — REDUCE lambda context
3. ``ExpressionRenderer.render_edge_filter_expression`` — CTE edge filter context

Complex functions that need AST inspection (DATE, DATETIME, DURATION, etc.)
are NOT in the registry — they remain as explicit handlers in the renderer.

Adding a new simple function mapping requires a single line in ``FUNCTION_TEMPLATES``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from gsql2rsql.parser.operators import (
    AggregationFunction,
    BinaryOperator,
    Function,
)


class DatabricksSqlType(Enum):
    """Databricks SQL data types."""

    INT = auto()
    SMALLINT = auto()
    BIGINT = auto()
    DOUBLE = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    BINARY = auto()
    DECIMAL = auto()
    DATE = auto()
    TIMESTAMP = auto()
    ARRAY = auto()
    MAP = auto()
    STRUCT = auto()


# Mapping from Python types to Databricks SQL types
TYPE_TO_SQL_TYPE: dict[type[Any], DatabricksSqlType] = {
    int: DatabricksSqlType.BIGINT,
    float: DatabricksSqlType.DOUBLE,
    str: DatabricksSqlType.STRING,
    bool: DatabricksSqlType.BOOLEAN,
    bytes: DatabricksSqlType.BINARY,
    list: DatabricksSqlType.ARRAY,
    dict: DatabricksSqlType.MAP,
}

# Mapping from Databricks SQL types to their string representations
SQL_TYPE_RENDERING: dict[DatabricksSqlType, str] = {
    DatabricksSqlType.INT: "INT",
    DatabricksSqlType.SMALLINT: "SMALLINT",
    DatabricksSqlType.BIGINT: "BIGINT",
    DatabricksSqlType.DOUBLE: "DOUBLE",
    DatabricksSqlType.FLOAT: "FLOAT",
    DatabricksSqlType.STRING: "STRING",
    DatabricksSqlType.BOOLEAN: "BOOLEAN",
    DatabricksSqlType.BINARY: "BINARY",
    DatabricksSqlType.DECIMAL: "DECIMAL(38, 10)",
    DatabricksSqlType.DATE: "DATE",
    DatabricksSqlType.TIMESTAMP: "TIMESTAMP",
    DatabricksSqlType.ARRAY: "ARRAY<STRING>",
    DatabricksSqlType.MAP: "MAP<STRING, STRING>",
    DatabricksSqlType.STRUCT: "STRUCT<>",
}

# Mapping from binary operators to SQL patterns (Databricks compatible)
OPERATOR_PATTERNS: dict[BinaryOperator, str] = {
    BinaryOperator.PLUS: "({0}) + ({1})",
    BinaryOperator.MINUS: "({0}) - ({1})",
    BinaryOperator.MULTIPLY: "({0}) * ({1})",
    BinaryOperator.DIVIDE: "({0}) / ({1})",
    BinaryOperator.MODULO: "({0}) % ({1})",
    BinaryOperator.EXPONENTIATION: "POWER({0}, {1})",
    BinaryOperator.AND: "({0}) AND ({1})",
    BinaryOperator.OR: "({0}) OR ({1})",
    BinaryOperator.XOR: "(({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1}))",
    BinaryOperator.LT: "({0}) < ({1})",
    BinaryOperator.LEQ: "({0}) <= ({1})",
    BinaryOperator.GT: "({0}) > ({1})",
    BinaryOperator.GEQ: "({0}) >= ({1})",
    BinaryOperator.EQ: "({0}) = ({1})",
    BinaryOperator.NEQ: "({0}) != ({1})",
    BinaryOperator.REGMATCH: "({0}) RLIKE ({1})",
    BinaryOperator.IN: "({0}) IN {1}",
}

# Mapping from aggregation functions to SQL patterns (Databricks compatible)
AGGREGATION_PATTERNS: dict[AggregationFunction, str] = {
    AggregationFunction.AVG: "AVG(CAST({0} AS DOUBLE))",
    AggregationFunction.SUM: "SUM({0})",
    AggregationFunction.MIN: "MIN({0})",
    AggregationFunction.MAX: "MAX({0})",
    AggregationFunction.FIRST: "FIRST({0})",
    AggregationFunction.LAST: "LAST({0})",
    AggregationFunction.STDEV: "STDDEV({0})",
    AggregationFunction.STDEVP: "STDDEV_POP({0})",
    AggregationFunction.COUNT: "COUNT({0})",
    AggregationFunction.COLLECT: "COLLECT_LIST({0})",
    # {1} is the percentile argument (percentileCont(x, 0.5) -> 0.5).
    AggregationFunction.PERCENTILE_CONT: (
        "PERCENTILE_CONT({1}) WITHIN GROUP (ORDER BY {0})"
    ),
    AggregationFunction.PERCENTILE_DISC: (
        "PERCENTILE_DISC({1}) WITHIN GROUP (ORDER BY {0})"
    ),
}


# ---------------------------------------------------------------------------
# Function templates — declarative Cypher Function → Databricks SQL mapping
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FunctionTemplate:
    """Declarative template for rendering a Cypher Function to Databricks SQL.

    Attributes:
        template: Format string with ``{0}``, ``{1}``, … for positional params,
            or ``{*}`` for variadic (``', '.join(params)``).
        min_params: Minimum parameter count.  Below this, *fallback* is returned.
        max_params: Maximum parameter count (``None`` = variadic).
        fallback: SQL string returned when fewer than *min_params* are provided.
        zero_param_literal: If set, always returns this literal (ignores params).
    """

    template: str = ""
    min_params: int = 1
    max_params: int | None = 1
    fallback: str = "NULL"
    zero_param_literal: str | None = None


def render_from_template(tmpl: FunctionTemplate, params: list[str]) -> str:
    """Apply a *FunctionTemplate* to pre-rendered parameters.

    Returns the rendered SQL string, or *tmpl.fallback* when there are
    fewer parameters than *tmpl.min_params*.
    """
    if tmpl.zero_param_literal is not None:
        return tmpl.zero_param_literal
    if len(params) < tmpl.min_params:
        return tmpl.fallback
    if "{*}" in tmpl.template:
        return tmpl.template.replace("{*}", ", ".join(params))
    return tmpl.template.format(*params)


# 55 simple functions — complex handlers (DATE, DATETIME, DURATION, NODES,
# RELATIONSHIPS, LOCALDATETIME, TIME, LOCALTIME, INVALID) stay in the renderer.
FUNCTION_TEMPLATES: dict[Function, FunctionTemplate] = {
    # -- Unary operators / null checks --
    Function.NOT:         FunctionTemplate("NOT ({0})", 1, 1, "NOT (NULL)"),
    Function.NEGATIVE:    FunctionTemplate("-({0})", 1, 1, "-NULL"),
    Function.POSITIVE:    FunctionTemplate("+({0})", 1, 1, "+NULL"),
    Function.IS_NULL:     FunctionTemplate("({0}) IS NULL", 1, 1, "NULL IS NULL"),
    Function.IS_NOT_NULL: FunctionTemplate("({0}) IS NOT NULL", 1, 1, "NULL IS NOT NULL"),
    # -- Type casts --
    Function.TO_STRING:  FunctionTemplate("CAST({0} AS STRING)", 1, 1),
    Function.TO_INTEGER: FunctionTemplate("CAST({0} AS BIGINT)", 1, 1),
    Function.TO_FLOAT:   FunctionTemplate("CAST({0} AS DOUBLE)", 1, 1),
    Function.TO_BOOLEAN: FunctionTemplate("CAST({0} AS BOOLEAN)", 1, 1),
    Function.TO_LONG:    FunctionTemplate("CAST({0} AS BIGINT)", 1, 1),
    Function.TO_DOUBLE:  FunctionTemplate("CAST({0} AS DOUBLE)", 1, 1),
    # -- String functions (unary) --
    Function.STRING_TO_UPPER: FunctionTemplate("UPPER({0})", 1, 1),
    Function.STRING_TO_LOWER: FunctionTemplate("LOWER({0})", 1, 1),
    Function.STRING_TRIM:     FunctionTemplate("TRIM({0})", 1, 1),
    Function.STRING_LTRIM:    FunctionTemplate("LTRIM({0})", 1, 1),
    Function.STRING_RTRIM:    FunctionTemplate("RTRIM({0})", 1, 1),
    Function.STRING_SIZE:     FunctionTemplate("LENGTH({0})", 1, 1),
    # -- String functions (binary) --
    Function.STRING_LEFT:        FunctionTemplate("LEFT({0}, {1})", 2, 2),
    Function.STRING_RIGHT:       FunctionTemplate("RIGHT({0}, {1})", 2, 2),
    Function.STRING_STARTS_WITH: FunctionTemplate("STARTSWITH({0}, {1})", 2, 2),
    Function.STRING_ENDS_WITH:   FunctionTemplate("ENDSWITH({0}, {1})", 2, 2),
    Function.STRING_CONTAINS:    FunctionTemplate("CONTAINS({0}, {1})", 2, 2),
    # -- Variadic --
    Function.COALESCE: FunctionTemplate("COALESCE({*})", 1, None, "NULL"),
    Function.RANGE:    FunctionTemplate("SEQUENCE({*})", 2, None, "ARRAY()"),
    # -- Collection / path --
    Function.SIZE:   FunctionTemplate("SIZE({0})", 1, 1, "0"),
    Function.LENGTH: FunctionTemplate("(SIZE({0}) - 1)", 1, 1, "0"),
    # -- Math (unary) --
    Function.ABS:     FunctionTemplate("ABS({0})", 1, 1),
    Function.CEIL:    FunctionTemplate("CEIL({0})", 1, 1),
    Function.FLOOR:   FunctionTemplate("FLOOR({0})", 1, 1),
    Function.SQRT:    FunctionTemplate("SQRT({0})", 1, 1),
    Function.SIGN:    FunctionTemplate("SIGN({0})", 1, 1),
    Function.LOG:     FunctionTemplate("LN({0})", 1, 1),
    Function.LOG10:   FunctionTemplate("LOG10({0})", 1, 1),
    Function.EXP:     FunctionTemplate("EXP({0})", 1, 1),
    Function.SIN:     FunctionTemplate("SIN({0})", 1, 1),
    Function.COS:     FunctionTemplate("COS({0})", 1, 1),
    Function.TAN:     FunctionTemplate("TAN({0})", 1, 1),
    Function.ASIN:    FunctionTemplate("ASIN({0})", 1, 1),
    Function.ACOS:    FunctionTemplate("ACOS({0})", 1, 1),
    Function.ATAN:    FunctionTemplate("ATAN({0})", 1, 1),
    Function.DEGREES: FunctionTemplate("DEGREES({0})", 1, 1),
    Function.RADIANS: FunctionTemplate("RADIANS({0})", 1, 1),
    # -- Math (binary) --
    Function.ATAN2: FunctionTemplate("ATAN2({0}, {1})", 2, 2),
    # -- Math (1 or 2 params) --
    Function.ROUND: FunctionTemplate("ROUND({*})", 1, 2),
    # -- Zero-param constants --
    Function.RAND: FunctionTemplate(zero_param_literal="RAND()"),
    Function.PI:   FunctionTemplate(zero_param_literal="PI()"),
    Function.E:    FunctionTemplate(zero_param_literal="E()"),
    # -- Date component extraction --
    Function.DATE_YEAR:      FunctionTemplate("YEAR({0})", 1, 1),
    Function.DATE_MONTH:     FunctionTemplate("MONTH({0})", 1, 1),
    Function.DATE_DAY:       FunctionTemplate("DAY({0})", 1, 1),
    Function.DATE_HOUR:      FunctionTemplate("HOUR({0})", 1, 1),
    Function.DATE_MINUTE:    FunctionTemplate("MINUTE({0})", 1, 1),
    Function.DATE_SECOND:    FunctionTemplate("SECOND({0})", 1, 1),
    Function.DATE_WEEK:      FunctionTemplate("WEEKOFYEAR({0})", 1, 1),
    Function.DATE_DAYOFWEEK: FunctionTemplate("DAYOFWEEK({0})", 1, 1),
    Function.DATE_QUARTER:   FunctionTemplate("QUARTER({0})", 1, 1),
    # -- Date arithmetic --
    Function.DATE_TRUNCATE:    FunctionTemplate("DATE_TRUNC({0}, {1})", 2, 2),
    Function.DURATION_BETWEEN: FunctionTemplate("DATEDIFF({1}, {0})", 2, 2, "0"),
}
