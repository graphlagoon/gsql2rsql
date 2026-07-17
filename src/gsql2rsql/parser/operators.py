"""Binary and unary operators for openCypher expressions."""

from enum import Enum, auto
from typing import NamedTuple


class BinaryOperator(Enum):
    """Binary operators supported by the transpiler."""

    INVALID = auto()

    # Numerical operators
    PLUS = auto()
    MINUS = auto()
    MULTIPLY = auto()
    DIVIDE = auto()
    MODULO = auto()
    EXPONENTIATION = auto()

    # Logical operators
    AND = auto()
    OR = auto()
    XOR = auto()

    # Comparison operators
    LT = auto()
    LEQ = auto()
    GT = auto()
    GEQ = auto()
    EQ = auto()
    NEQ = auto()
    REGMATCH = auto()
    IN = auto()


class BinaryOperatorType(Enum):
    """Classification of binary operators by their type semantics."""

    INVALID = auto()
    VALUE = auto()  # Takes value type (string/numeric) and outputs value type
    LOGICAL = auto()  # Takes logical type (bool) and outputs logical type
    COMPARISON = auto()  # Takes value type (string/numeric) and outputs logical type


class BinaryOperatorInfo(NamedTuple):
    """Information about a binary operator."""

    name: BinaryOperator
    operator_type: BinaryOperatorType

    def __str__(self) -> str:
        return self.name.name


# Mapping from operator symbols to their info
OPERATORS: dict[str, BinaryOperatorInfo] = {
    "+": BinaryOperatorInfo(BinaryOperator.PLUS, BinaryOperatorType.VALUE),
    "-": BinaryOperatorInfo(BinaryOperator.MINUS, BinaryOperatorType.VALUE),
    "*": BinaryOperatorInfo(BinaryOperator.MULTIPLY, BinaryOperatorType.VALUE),
    "/": BinaryOperatorInfo(BinaryOperator.DIVIDE, BinaryOperatorType.VALUE),
    "%": BinaryOperatorInfo(BinaryOperator.MODULO, BinaryOperatorType.VALUE),
    "^": BinaryOperatorInfo(BinaryOperator.EXPONENTIATION, BinaryOperatorType.VALUE),
    "<>": BinaryOperatorInfo(BinaryOperator.NEQ, BinaryOperatorType.COMPARISON),
    "=": BinaryOperatorInfo(BinaryOperator.EQ, BinaryOperatorType.COMPARISON),
    "<": BinaryOperatorInfo(BinaryOperator.LT, BinaryOperatorType.COMPARISON),
    ">": BinaryOperatorInfo(BinaryOperator.GT, BinaryOperatorType.COMPARISON),
    "<=": BinaryOperatorInfo(BinaryOperator.LEQ, BinaryOperatorType.COMPARISON),
    ">=": BinaryOperatorInfo(BinaryOperator.GEQ, BinaryOperatorType.COMPARISON),
    "=~": BinaryOperatorInfo(BinaryOperator.REGMATCH, BinaryOperatorType.COMPARISON),
    "in": BinaryOperatorInfo(BinaryOperator.IN, BinaryOperatorType.COMPARISON),
    "and": BinaryOperatorInfo(BinaryOperator.AND, BinaryOperatorType.LOGICAL),
    "or": BinaryOperatorInfo(BinaryOperator.OR, BinaryOperatorType.LOGICAL),
    "xor": BinaryOperatorInfo(BinaryOperator.XOR, BinaryOperatorType.LOGICAL),
}


def try_get_operator(op: str) -> BinaryOperatorInfo | None:
    """
    Try to get operator info for the given operator symbol.

    Args:
        op: The operator symbol (e.g., "+", "and").

    Returns:
        BinaryOperatorInfo if found, None otherwise.
    """
    return OPERATORS.get(op.lower() if op else "")


class AggregationFunction(Enum):
    """Aggregation functions supported by the transpiler."""

    INVALID = auto()
    NONE = auto()
    SUM = auto()
    AVG = auto()
    COUNT = auto()
    MAX = auto()
    MIN = auto()
    FIRST = auto()
    LAST = auto()
    PERCENTILE_CONT = auto()
    PERCENTILE_DISC = auto()
    STDEV = auto()
    STDEVP = auto()
    COLLECT = auto()


def try_parse_aggregation_function(function_name: str) -> AggregationFunction | None:
    """
    Try to parse a function name into an aggregation function.

    Args:
        function_name: The function name to parse.

    Returns:
        AggregationFunction if it's an aggregation function, None otherwise.
    """
    mapping = {
        "avg": AggregationFunction.AVG,
        "sum": AggregationFunction.SUM,
        "count": AggregationFunction.COUNT,
        "max": AggregationFunction.MAX,
        "min": AggregationFunction.MIN,
        "first": AggregationFunction.FIRST,
        "last": AggregationFunction.LAST,
        "percentilecont": AggregationFunction.PERCENTILE_CONT,
        "percentiledisc": AggregationFunction.PERCENTILE_DISC,
        "stdev": AggregationFunction.STDEV,
        "stddev": AggregationFunction.STDEV,  # Alias with double D
        "stdevp": AggregationFunction.STDEVP,
        "stddevp": AggregationFunction.STDEVP,  # Alias with double D
        "collect": AggregationFunction.COLLECT,
    }
    return mapping.get(function_name.lower())


class ListPredicateType(Enum):
    """Types of list predicate expressions (quantifiers)."""

    ALL = auto()  # ALL(x IN list WHERE pred)
    ANY = auto()  # ANY(x IN list WHERE pred)
    NONE = auto()  # NONE(x IN list WHERE pred)
    SINGLE = auto()  # SINGLE(x IN list WHERE pred)


class Function(Enum):
    """Functions supported by the transpiler."""

    INVALID = auto()

    # Unary operators
    POSITIVE = auto()
    NEGATIVE = auto()
    NOT = auto()

    # Type conversion functions
    TO_FLOAT = auto()
    TO_STRING = auto()
    TO_BOOLEAN = auto()
    TO_INTEGER = auto()
    TO_LONG = auto()
    TO_DOUBLE = auto()

    # String functions
    STRING_STARTS_WITH = auto()
    STRING_ENDS_WITH = auto()
    STRING_CONTAINS = auto()
    STRING_LEFT = auto()
    STRING_RIGHT = auto()
    STRING_TRIM = auto()
    STRING_LTRIM = auto()
    STRING_RTRIM = auto()
    STRING_TO_UPPER = auto()
    STRING_TO_LOWER = auto()
    STRING_SIZE = auto()

    # Misc functions
    IS_NULL = auto()
    IS_NOT_NULL = auto()

    # Null handling functions
    COALESCE = auto()

    # List/Array functions
    RANGE = auto()  # Cypher: RANGE(start, end[, step]) -> Databricks: SEQUENCE
    SIZE = auto()  # Cypher: SIZE(list) -> Databricks: SIZE(list)

    # Path functions
    NODES = auto()  # nodes(path) -> returns array of node IDs
    RELATIONSHIPS = auto()  # relationships(path) -> returns array of edge structs
    LENGTH = auto()  # length(path) -> number of relationships (edges) in path
    IS_TERMINATOR = auto()  # is_terminator(predicate) — traversal barrier directive
    TYPE = auto()  # type(r) -> relationship type name

    # List access (built by the visitor from '[' ... ']', not by name)
    LIST_INDEX = auto()  # list[i] -> params: (list, index)
    LIST_SLICE = auto()  # list[a..b] -> params: (list, start|None, end|None)

    # Math functions
    ABS = auto()
    CEIL = auto()
    FLOOR = auto()
    ROUND = auto()
    SQRT = auto()
    SIGN = auto()
    LOG = auto()  # Natural log
    LOG10 = auto()
    EXP = auto()
    SIN = auto()
    COS = auto()
    TAN = auto()
    ASIN = auto()
    ACOS = auto()
    ATAN = auto()
    ATAN2 = auto()
    DEGREES = auto()
    RADIANS = auto()
    RAND = auto()
    PI = auto()
    E = auto()

    # Date/Time functions
    DATE = auto()  # date() or date({...})
    DATETIME = auto()  # datetime() or datetime({...})
    LOCALDATETIME = auto()  # localdatetime()
    TIME = auto()  # time()
    LOCALTIME = auto()  # localtime()
    DURATION = auto()  # duration({...})
    DURATION_BETWEEN = auto()  # duration.between(d1, d2)

    # Date/Time component extraction
    DATE_YEAR = auto()  # date.year or datetime.year
    DATE_MONTH = auto()  # date.month or datetime.month
    DATE_DAY = auto()  # date.day or datetime.day
    DATE_HOUR = auto()  # datetime.hour or time.hour
    DATE_MINUTE = auto()  # datetime.minute or time.minute
    DATE_SECOND = auto()  # datetime.second or time.second
    DATE_WEEK = auto()  # date.week
    DATE_DAYOFWEEK = auto()  # date.dayOfWeek
    DATE_QUARTER = auto()  # date.quarter

    # Date/Time arithmetic and manipulation
    DATE_TRUNCATE = auto()  # date.truncate('unit', d)


class FunctionInfo(NamedTuple):
    """Information about a function."""

    function_name: Function
    required_parameters: int
    optional_parameters: int = 0


# Mapping from function names to their info
FUNCTIONS: dict[str, FunctionInfo] = {
    "+": FunctionInfo(Function.POSITIVE, 1),
    "-": FunctionInfo(Function.NEGATIVE, 1),
    "not": FunctionInfo(Function.NOT, 1),
    "tofloat": FunctionInfo(Function.TO_FLOAT, 1),
    "todouble": FunctionInfo(Function.TO_DOUBLE, 1),
    "tostring": FunctionInfo(Function.TO_STRING, 1),
    "toboolean": FunctionInfo(Function.TO_BOOLEAN, 1),
    "tointeger": FunctionInfo(Function.TO_INTEGER, 1),
    "tolong": FunctionInfo(Function.TO_LONG, 1),
    "startswith": FunctionInfo(Function.STRING_STARTS_WITH, 2),
    "endswith": FunctionInfo(Function.STRING_ENDS_WITH, 2),
    "contains": FunctionInfo(Function.STRING_CONTAINS, 2),
    "left": FunctionInfo(Function.STRING_LEFT, 2),
    "right": FunctionInfo(Function.STRING_RIGHT, 2),
    "trim": FunctionInfo(Function.STRING_TRIM, 1),
    "ltrim": FunctionInfo(Function.STRING_LTRIM, 1),
    "rtrim": FunctionInfo(Function.STRING_RTRIM, 1),
    "toupper": FunctionInfo(Function.STRING_TO_UPPER, 1),
    "tolower": FunctionInfo(Function.STRING_TO_LOWER, 1),
    # Null handling - COALESCE takes 1+ args (variadic)
    "coalesce": FunctionInfo(Function.COALESCE, 1, 99),
    # List/Array functions
    "range": FunctionInfo(Function.RANGE, 2, 1),  # RANGE(start, end[, step])
    "size": FunctionInfo(Function.SIZE, 1),  # SIZE works for both strings and arrays
    "length": FunctionInfo(Function.LENGTH, 1),  # LENGTH(path) = number of edges (hops)
    # Path functions
    "nodes": FunctionInfo(Function.NODES, 1),  # nodes(path) -> array of node IDs
    "relationships": FunctionInfo(Function.RELATIONSHIPS, 1),  # rels(path) -> array of edges
    "rels": FunctionInfo(Function.RELATIONSHIPS, 1),  # alias for relationships
    # Traversal barrier directive
    "is_terminator": FunctionInfo(Function.IS_TERMINATOR, 1),
    "type": FunctionInfo(Function.TYPE, 1),
    # Math functions
    "abs": FunctionInfo(Function.ABS, 1),
    "ceil": FunctionInfo(Function.CEIL, 1),
    "ceiling": FunctionInfo(Function.CEIL, 1),  # alias
    "floor": FunctionInfo(Function.FLOOR, 1),
    "round": FunctionInfo(Function.ROUND, 1, 1),  # ROUND(x) or ROUND(x, precision)
    "sqrt": FunctionInfo(Function.SQRT, 1),
    "sign": FunctionInfo(Function.SIGN, 1),
    "log": FunctionInfo(Function.LOG, 1),
    "ln": FunctionInfo(Function.LOG, 1),  # alias
    "log10": FunctionInfo(Function.LOG10, 1),
    "exp": FunctionInfo(Function.EXP, 1),
    "sin": FunctionInfo(Function.SIN, 1),
    "cos": FunctionInfo(Function.COS, 1),
    "tan": FunctionInfo(Function.TAN, 1),
    "asin": FunctionInfo(Function.ASIN, 1),
    "acos": FunctionInfo(Function.ACOS, 1),
    "atan": FunctionInfo(Function.ATAN, 1),
    "atan2": FunctionInfo(Function.ATAN2, 2),
    "degrees": FunctionInfo(Function.DEGREES, 1),
    "radians": FunctionInfo(Function.RADIANS, 1),
    "rand": FunctionInfo(Function.RAND, 0),
    "random": FunctionInfo(Function.RAND, 0),  # alias
    "pi": FunctionInfo(Function.PI, 0),
    "e": FunctionInfo(Function.E, 0),
    # Date/Time functions
    "date": FunctionInfo(Function.DATE, 0, 1),  # date() or date({...})
    "datetime": FunctionInfo(Function.DATETIME, 0, 1),  # datetime() or datetime({...})
    "timestamp": FunctionInfo(Function.DATETIME, 0, 1),  # timestamp() -> same as datetime()
    "localdatetime": FunctionInfo(Function.LOCALDATETIME, 0, 1),
    "time": FunctionInfo(Function.TIME, 0, 1),  # time() or time({...})
    "localtime": FunctionInfo(Function.LOCALTIME, 0, 1),
    "duration": FunctionInfo(Function.DURATION, 1),  # duration({...})
    "duration.between": FunctionInfo(Function.DURATION_BETWEEN, 2),
    # Date component extraction (used as methods on date/datetime values)
    "year": FunctionInfo(Function.DATE_YEAR, 1),
    "month": FunctionInfo(Function.DATE_MONTH, 1),
    "day": FunctionInfo(Function.DATE_DAY, 1),
    "hour": FunctionInfo(Function.DATE_HOUR, 1),
    "minute": FunctionInfo(Function.DATE_MINUTE, 1),
    "second": FunctionInfo(Function.DATE_SECOND, 1),
    "week": FunctionInfo(Function.DATE_WEEK, 1),
    "dayofweek": FunctionInfo(Function.DATE_DAYOFWEEK, 1),
    "quarter": FunctionInfo(Function.DATE_QUARTER, 1),
    # Date truncation
    "date.truncate": FunctionInfo(Function.DATE_TRUNCATE, 2),
}


def try_get_function(function_name: str) -> FunctionInfo | None:
    """
    Try to get function info for the given function name.

    Args:
        function_name: The function name.

    Returns:
        FunctionInfo if found, None otherwise.
    """
    return FUNCTIONS.get(function_name.lower() if function_name else "")
