"""Tests for the Function Registry Pattern in dialect.py.

The FUNCTION_TEMPLATES registry maps Cypher Function enum values to
Databricks SQL templates.  These tests validate the registry data,
the render_from_template function, and that every entry produces the
expected SQL output (matching the old elif chain behavior).
"""

import pytest

from gsql2rsql.parser.operators import Function
from gsql2rsql.renderer.dialect import (
    FUNCTION_TEMPLATES,
    FunctionTemplate,
    render_from_template,
)

# Functions that require AST inspection or custom logic — NOT in the registry.
COMPLEX_HANDLERS = {
    Function.INVALID,
    Function.NODES,
    Function.RELATIONSHIPS,
    Function.DATE,
    Function.DATETIME,
    Function.LOCALDATETIME,
    Function.TIME,
    Function.LOCALTIME,
    Function.DURATION,
    Function.IS_TERMINATOR,  # Directive, not a SQL function
    Function.TYPE,  # Rewritten to a type-column property by enrichment
    Function.LIST_INDEX,  # Explicit handler (GET with index normalization)
    Function.LIST_SLICE,  # Explicit handler (_render_list_slice)
}


class TestFunctionTemplateData:
    """Validate completeness and structure of FUNCTION_TEMPLATES."""

    def test_all_simple_functions_are_registered(self) -> None:
        """Every non-complex Function enum member must be in the registry."""
        for func in Function:
            if func in COMPLEX_HANDLERS:
                assert func not in FUNCTION_TEMPLATES, (
                    f"{func} is a complex handler and should NOT be in FUNCTION_TEMPLATES"
                )
            else:
                assert func in FUNCTION_TEMPLATES, (
                    f"{func} missing from FUNCTION_TEMPLATES — add it or mark as complex"
                )

    def test_template_min_max_params_consistent(self) -> None:
        for func, tmpl in FUNCTION_TEMPLATES.items():
            if tmpl.zero_param_literal is not None:
                continue
            assert tmpl.min_params >= 0, f"{func}: min_params must be >= 0"
            if tmpl.max_params is not None:
                assert tmpl.max_params >= tmpl.min_params, (
                    f"{func}: max_params ({tmpl.max_params}) < min_params ({tmpl.min_params})"
                )

    def test_registry_count(self) -> None:
        """Sanity check: all non-complex Function enum values are registered."""
        expected = len(Function) - len(COMPLEX_HANDLERS)
        assert len(FUNCTION_TEMPLATES) == expected


class TestRenderFromTemplate:
    """Unit tests for the render_from_template free function."""

    def test_zero_param_literal(self) -> None:
        tmpl = FunctionTemplate(zero_param_literal="RAND()")
        assert render_from_template(tmpl, []) == "RAND()"
        assert render_from_template(tmpl, ["ignored"]) == "RAND()"

    def test_unary_template(self) -> None:
        tmpl = FunctionTemplate("ABS({0})", 1, 1)
        assert render_from_template(tmpl, ["x"]) == "ABS(x)"

    def test_unary_fallback(self) -> None:
        tmpl = FunctionTemplate("ABS({0})", 1, 1, "NULL")
        assert render_from_template(tmpl, []) == "NULL"

    def test_binary_template(self) -> None:
        tmpl = FunctionTemplate("ATAN2({0}, {1})", 2, 2)
        assert render_from_template(tmpl, ["a", "b"]) == "ATAN2(a, b)"

    def test_binary_fallback(self) -> None:
        tmpl = FunctionTemplate("ATAN2({0}, {1})", 2, 2)
        assert render_from_template(tmpl, ["only_one"]) == "NULL"

    def test_variadic(self) -> None:
        tmpl = FunctionTemplate("COALESCE({*})", 1, None, "NULL")
        assert render_from_template(tmpl, ["a"]) == "COALESCE(a)"
        assert render_from_template(tmpl, ["a", "b", "c"]) == "COALESCE(a, b, c)"

    def test_variadic_fallback(self) -> None:
        tmpl = FunctionTemplate("COALESCE({*})", 1, None, "NULL")
        assert render_from_template(tmpl, []) == "NULL"

    def test_postfix_is_null(self) -> None:
        tmpl = FUNCTION_TEMPLATES[Function.IS_NULL]
        assert render_from_template(tmpl, ["col"]) == "(col) IS NULL"

    def test_param_swap_duration_between(self) -> None:
        """DURATION_BETWEEN swaps: duration.between(d1,d2) → DATEDIFF(d2,d1)."""
        tmpl = FUNCTION_TEMPLATES[Function.DURATION_BETWEEN]
        assert render_from_template(tmpl, ["d1", "d2"]) == "DATEDIFF(d2, d1)"

    def test_round_one_param(self) -> None:
        tmpl = FUNCTION_TEMPLATES[Function.ROUND]
        assert render_from_template(tmpl, ["x"]) == "ROUND(x)"

    def test_round_two_params(self) -> None:
        tmpl = FUNCTION_TEMPLATES[Function.ROUND]
        assert render_from_template(tmpl, ["x", "2"]) == "ROUND(x, 2)"

    def test_custom_fallback(self) -> None:
        tmpl = FunctionTemplate("SIZE({0})", 1, 1, "0")
        assert render_from_template(tmpl, []) == "0"


# Parametrized regression tests: each entry must match the old elif chain output.
_REGISTRY_CASES = [
    # Unary / null
    (Function.NOT, ["x > 1"], "NOT (x > 1)"),
    (Function.NOT, [], "NOT (NULL)"),
    (Function.NEGATIVE, ["5"], "-(5)"),
    (Function.NEGATIVE, [], "-NULL"),
    (Function.POSITIVE, ["5"], "+(5)"),
    (Function.POSITIVE, [], "+NULL"),
    (Function.IS_NULL, ["col"], "(col) IS NULL"),
    (Function.IS_NULL, [], "NULL IS NULL"),
    (Function.IS_NOT_NULL, ["col"], "(col) IS NOT NULL"),
    (Function.IS_NOT_NULL, [], "NULL IS NOT NULL"),
    # Type casts
    (Function.TO_STRING, ["42"], "CAST(42 AS STRING)"),
    (Function.TO_INTEGER, ["'5'"], "CAST('5' AS BIGINT)"),
    (Function.TO_FLOAT, ["'3.14'"], "CAST('3.14' AS DOUBLE)"),
    (Function.TO_BOOLEAN, ["'true'"], "CAST('true' AS BOOLEAN)"),
    (Function.TO_LONG, ["42"], "CAST(42 AS BIGINT)"),
    (Function.TO_DOUBLE, ["42"], "CAST(42 AS DOUBLE)"),
    # String (unary)
    (Function.STRING_TO_UPPER, ["'hello'"], "UPPER('hello')"),
    (Function.STRING_TO_LOWER, ["'HELLO'"], "LOWER('HELLO')"),
    (Function.STRING_TRIM, ["' x '"], "TRIM(' x ')"),
    (Function.STRING_LTRIM, ["' x'"], "LTRIM(' x')"),
    (Function.STRING_RTRIM, ["'x '"], "RTRIM('x ')"),
    (Function.STRING_SIZE, ["'hello'"], "LENGTH('hello')"),
    # String (binary)
    (Function.STRING_LEFT, ["'hello'", "3"], "LEFT('hello', 3)"),
    (Function.STRING_RIGHT, ["'hello'", "3"], "RIGHT('hello', 3)"),
    (Function.STRING_STARTS_WITH, ["'hello'", "'he'"], "STARTSWITH('hello', 'he')"),
    (Function.STRING_ENDS_WITH, ["'hello'", "'lo'"], "ENDSWITH('hello', 'lo')"),
    (Function.STRING_CONTAINS, ["'hello'", "'ell'"], "CONTAINS('hello', 'ell')"),
    # Variadic
    (Function.COALESCE, ["a", "b"], "COALESCE(a, b)"),
    (Function.COALESCE, [], "NULL"),
    (Function.RANGE, ["1", "10"], "SEQUENCE(1, 10)"),
    (Function.RANGE, ["1", "10", "2"], "SEQUENCE(1, 10, 2)"),
    (Function.RANGE, ["1"], "ARRAY()"),
    # Collection / path
    (Function.SIZE, ["arr"], "SIZE(arr)"),
    (Function.SIZE, [], "0"),
    (Function.LENGTH, ["path"], "(SIZE(path) - 1)"),
    (Function.LENGTH, [], "0"),
    # Math (unary)
    (Function.ABS, ["-5"], "ABS(-5)"),
    (Function.CEIL, ["3.2"], "CEIL(3.2)"),
    (Function.FLOOR, ["3.8"], "FLOOR(3.8)"),
    (Function.SQRT, ["16"], "SQRT(16)"),
    (Function.SIGN, ["-5"], "SIGN(-5)"),
    (Function.LOG, ["10"], "LN(10)"),
    (Function.LOG10, ["100"], "LOG10(100)"),
    (Function.EXP, ["1"], "EXP(1)"),
    (Function.SIN, ["0"], "SIN(0)"),
    (Function.COS, ["0"], "COS(0)"),
    (Function.TAN, ["0"], "TAN(0)"),
    (Function.ASIN, ["0.5"], "ASIN(0.5)"),
    (Function.ACOS, ["0.5"], "ACOS(0.5)"),
    (Function.ATAN, ["1"], "ATAN(1)"),
    (Function.DEGREES, ["3.14"], "DEGREES(3.14)"),
    (Function.RADIANS, ["180"], "RADIANS(180)"),
    # Math (binary)
    (Function.ATAN2, ["1", "0"], "ATAN2(1, 0)"),
    (Function.ATAN2, ["1"], "NULL"),
    # Math (1-2 params)
    (Function.ROUND, ["3.14"], "ROUND(3.14)"),
    (Function.ROUND, ["3.14159", "2"], "ROUND(3.14159, 2)"),
    (Function.ROUND, [], "NULL"),
    # Zero-param constants
    (Function.RAND, [], "RAND()"),
    (Function.PI, [], "PI()"),
    (Function.E, [], "E()"),
    # Date extraction
    (Function.DATE_YEAR, ["d"], "YEAR(d)"),
    (Function.DATE_MONTH, ["d"], "MONTH(d)"),
    (Function.DATE_DAY, ["d"], "DAY(d)"),
    (Function.DATE_HOUR, ["d"], "HOUR(d)"),
    (Function.DATE_MINUTE, ["d"], "MINUTE(d)"),
    (Function.DATE_SECOND, ["d"], "SECOND(d)"),
    (Function.DATE_WEEK, ["d"], "WEEKOFYEAR(d)"),
    (Function.DATE_DAYOFWEEK, ["d"], "DAYOFWEEK(d)"),
    (Function.DATE_QUARTER, ["d"], "QUARTER(d)"),
    # Date arithmetic
    (Function.DATE_TRUNCATE, ["'month'", "d"], "DATE_TRUNC('month', d)"),
    (Function.DATE_TRUNCATE, ["'month'"], "NULL"),
    (Function.DURATION_BETWEEN, ["d1", "d2"], "DATEDIFF(d2, d1)"),
    (Function.DURATION_BETWEEN, ["d1"], "0"),
]


class TestFunctionRegistryIntegration:
    """Parametrized: render_from_template must match the old elif chain."""

    @pytest.mark.parametrize(
        "func,params,expected",
        _REGISTRY_CASES,
        ids=[f"{c[0].name}_{i}" for i, c in enumerate(_REGISTRY_CASES)],
    )
    def test_render_matches_expected(
        self, func: Function, params: list[str], expected: str
    ) -> None:
        tmpl = FUNCTION_TEMPLATES[func]
        assert render_from_template(tmpl, params) == expected
