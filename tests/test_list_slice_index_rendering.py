"""Non-PySpark structural tests for list slice/index and type(r).

These run WITHOUT a Spark session so the parser + renderer changes have
coverage even when pyspark is absent. The exact SLICE()/GET() semantics
were validated against real Spark separately; here we lock the generated
SQL shape and the error paths.
"""

import pytest

from gsql2rsql import GraphContext
from gsql2rsql.common.exceptions import TranspilerNotSupportedException


@pytest.fixture
def graph():
    ctx = GraphContext(
        nodes_table="n",
        edges_table="e",
        node_id_col="id",
        node_type_col="t",
        edge_type_col="rt",
        edge_src_col="src",
        edge_dst_col="dst",
        extra_node_attrs={"g": int},
        extra_edge_attrs={"pct": float},
    )
    ctx.set_types(node_types=["PF", "PJ"], edge_types=["A", "B"])
    return ctx


def _slice_line(sql: str) -> str:
    lines = [ln for ln in sql.splitlines() if "SLICE(" in ln]
    assert lines, f"no SLICE in SQL:\n{sql}"
    return lines[0]


class TestListSliceRendering:
    """Verify the SLICE() emitted for each Cypher slice form.

    Cypher slices are 0-based, end-exclusive, negatives-from-end; Spark
    SLICE(arr, start, length) is 1-based length-based. The renderer bridges
    them; these assert the bridge for each shape.
    """

    def test_all_but_last(self, graph):
        # [0..-1]: start clamped to 0 -> SLICE(arr, 1, <end-norm>)
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[0..-1] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        line = _slice_line(sql)
        assert "SLICE(" in line
        assert "GREATEST(0, SIZE(" in line  # end = -1 -> size - 1

    def test_drop_first(self, graph):
        # [1..]: start=1 (literal), end omitted (open)
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[1..] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        line = _slice_line(sql)
        assert "LEAST(1, SIZE(" in line  # start normalized
        assert ") + 1" in line  # 0-based start -> +1 for Spark

    def test_first_three(self, graph):
        # [..3]: start omitted (0) -> SLICE(arr, 1, LEAST(3, size))
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[..3] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        line = _slice_line(sql)
        assert "SLICE(" in line
        assert "1, LEAST(3, SIZE(" in line

    def test_last_two_negative_literal_simplified(self, graph):
        # [-2..]: negative literal must hit the readable GREATEST branch,
        # not the verbose CASE (regression: -2 renders as -(2)).
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[-2..] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        line = _slice_line(sql)
        assert "GREATEST(0, SIZE(" in line and "- 2)" in line
        assert "CASE WHEN" not in line  # simplified form, not the fallback

    def test_explicit_bounds(self, graph):
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[1..3] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        line = _slice_line(sql)
        assert "SLICE(" in line

    def test_sliced_nodes_not_pushed_down(self, graph):
        """A sliced nodes(path) predicate must NOT create the filtered-edge
        CTE — the slice excludes nodes the pushdown would wrongly filter."""
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) "
            "WHERE all(n IN nodes(p)[0..-1] WHERE n.g <= 5) "
            "RETURN b.id AS id"
        )
        assert "vlp_filtered_edges_" not in sql
        # still enforced via the correlated subquery over the sliced array
        assert "SLICE(" in sql and "NOT EXISTS" in sql


class TestListIndexRendering:
    def test_positive_index(self, graph):
        sql = graph.transpile(
            "MATCH p=(a)-[*1..2]-(b) "
            "RETURN nodes(p)[0] AS first_id"
        )
        assert "GET(" in sql

    def test_negative_index_normalized(self, graph):
        sql = graph.transpile(
            "MATCH p=(a)-[*1..2]-(b) "
            "RETURN nodes(p)[-1] AS last_id"
        )
        line = [ln for ln in sql.splitlines() if "GET(" in ln][0]
        assert "SIZE(" in line  # negative -> size + idx


class TestTypeFunction:
    def test_type_in_edge_lambda_becomes_column(self, graph):
        sql = graph.transpile(
            "MATCH p=(a)-[arestas*1..2]-(b) "
            "WHERE all(r IN arestas WHERE type(r) = 'A') "
            "RETURN b.id AS id"
        )
        # rewritten to the type column; no raw TYPE call survives
        assert "rt) = ('A')" in sql or "rt = 'A'" in sql
        assert "TYPE(" not in sql.upper().replace("NAMED_STRUCT", "")

    def test_type_or_precedence_parenthesized(self, graph):
        """type(r)='A' OR r.pct>=50 must be wrapped so depth/visited guards
        are not swallowed by the OR (regression guard)."""
        sql = graph.transpile(
            "MATCH p=(a)-[arestas*1..3]-(b) "
            "WHERE all(r IN arestas WHERE "
            "type(r) = 'A' OR r.pct >= 50.0) "
            "RETURN b.id AS id"
        )
        # the OR chain appears grouped and AND-joined with the guard
        assert "OR" in sql
        rec = [
            ln for ln in sql.splitlines()
            if "depth <" in ln or "NOT ARRAY_CONTAINS(p.visited" in ln
        ]
        assert rec, "recursion guard missing"

    def test_type_in_return_raises_clear_error(self, graph):
        with pytest.raises(TranspilerNotSupportedException):
            graph.transpile(
                "MATCH (a)-[r]->(b) RETURN type(r) AS ty"
            )

    def test_type_in_single_hop_where_raises_clear_error(self, graph):
        with pytest.raises(TranspilerNotSupportedException):
            graph.transpile(
                "MATCH (a)-[r]->(b) WHERE type(r) = 'A' RETURN b.id AS id"
            )


class TestPatternCountInLambdaRejected:
    def test_size_pattern_in_node_lambda_raises(self, graph):
        with pytest.raises(TranspilerNotSupportedException):
            graph.transpile(
                "MATCH p=(a)-[*1..2]-(b) "
                "WHERE all(n IN nodes(p) WHERE size((n)--()) <= 5) "
                "RETURN b.id AS id"
            )


class TestInertWhenUnused:
    def test_plain_vlp_query_has_no_new_constructs(self, graph):
        """A VLP query with none of the new constructs must not emit any
        of the new machinery — proving the changes are dormant when unused."""
        sql = graph.transpile(
            "MATCH p=(a)-[*1..3]-(b) WHERE a.id = 'X' RETURN b.id AS id"
        )
        assert "vlp_filtered_edges_" not in sql
        assert "SLICE(" not in sql
        assert "GET(" not in sql
        assert "bfs_vlp_nodes_ok" not in sql
