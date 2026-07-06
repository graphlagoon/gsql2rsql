"""Render context — shared state and utilities for all sub-renderers.

This module provides the ``RenderContext`` class that acts as the "glue" between
the orchestrator (``SQLRenderer``) and the extracted sub-renderers
(expression, CTE, join).  Sub-renderers receive a ``RenderContext`` instance
instead of a back-reference to ``SQLRenderer``, which prevents circular imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gsql2rsql.planner.column_ref import ResolvedColumnRef
from gsql2rsql.planner.column_resolver import ResolutionResult
from gsql2rsql.planner.operators import LogicalOperator
from gsql2rsql.planner.schema import EntityField, EntityType, Schema, ValueField
from gsql2rsql.renderer.schema_provider import ISQLDBSchemaProvider

if TYPE_CHECKING:
    from gsql2rsql.renderer.procedural_bfs_renderer import (
        ProceduralBFSOptimizations,
    )
    from gsql2rsql.renderer.sql_enrichment import EnrichedPlanData


class RenderContext:
    """Shared rendering state passed to every sub-renderer.

    Holds mutable counters (CTE names, JOIN aliases), read-only references to
    the schema provider and resolution result, and small utility helpers that
    are used across multiple clusters.
    """

    COLUMN_PREFIX = "_gsql2rsql_"

    def __init__(
        self,
        db_schema: ISQLDBSchemaProvider,
        resolution_result: ResolutionResult | None,
        required_columns: set[str],
        required_value_fields: set[str],
        enable_column_pruning: bool,
        config: dict[str, object],
        enriched: EnrichedPlanData | None = None,
        vlp_rendering_mode: str = "cte",
        materialization_strategy: str = "temp_tables",
        procedural_optimizations: ProceduralBFSOptimizations | None = None,
    ) -> None:
        self.db_schema = db_schema
        self.resolution_result = resolution_result
        self.required_columns = required_columns
        self.required_value_fields = required_value_fields
        self.enable_column_pruning = enable_column_pruning
        self.config = config
        self.enriched = enriched
        self.vlp_rendering_mode = vlp_rendering_mode
        self.materialization_strategy = materialization_strategy
        if procedural_optimizations is None:
            # Local import to avoid a circular module dependency
            # (procedural_bfs_renderer imports RenderContext for typing).
            from gsql2rsql.renderer.procedural_bfs_renderer import (
                ProceduralBFSOptimizations,
            )
            procedural_optimizations = ProceduralBFSOptimizations()
        self.procedural_optimizations = procedural_optimizations
        self._cte_counter = 0
        self._join_alias_counter = 0

    # ------------------------------------------------------------------
    # Counters
    # ------------------------------------------------------------------

    def next_join_alias_pair(self) -> tuple[str, str]:
        """Generate unique ``(_left_N, _right_N)`` alias pair for JOINs."""
        alias_id = self._join_alias_counter
        self._join_alias_counter += 1
        return (f"_left_{alias_id}", f"_right_{alias_id}")

    @property
    def cte_counter(self) -> int:
        return self._cte_counter

    @cte_counter.setter
    def cte_counter(self, value: int) -> None:
        self._cte_counter = value

    @property
    def join_alias_counter(self) -> int:
        return self._join_alias_counter

    @join_alias_counter.setter
    def join_alias_counter(self, value: int) -> None:
        self._join_alias_counter = value

    # ------------------------------------------------------------------
    # Small utilities
    # ------------------------------------------------------------------

    def get_field_name(self, prefix: str, field_name: str) -> str:
        """Generate a column name with entity prefix: ``_gsql2rsql_{prefix}_{field}``."""
        clean_prefix = "".join(
            c if c.isalnum() or c == "_" else "" for c in prefix
        )
        return f"{self.COLUMN_PREFIX}{clean_prefix}_{field_name}"

    def resolve_field_key(self, field: ValueField, entity_alias: str) -> str:
        """Return the SQL column name for *field*, using pre-rendered name when available.

        Variable-length path fields already carry the full SQL column name in
        ``field.field_name`` (e.g. ``_gsql2rsql_peer_id``).  For normal entities
        the name is computed from *entity_alias* + ``field.field_alias``.
        """
        if (
            field.field_name
            and field.field_name.startswith(self.COLUMN_PREFIX)
        ):
            return field.field_name
        return self.get_field_name(entity_alias, field.field_alias)

    @staticmethod
    def indent(depth: int) -> str:
        """Return indentation string for *depth* nesting levels."""
        return "  " * depth

    def get_resolved_ref(
        self,
        variable: str,
        property_name: str | None,
        context_op: LogicalOperator,
    ) -> ResolvedColumnRef | None:
        """Look up a pre-resolved column reference for *variable.property_name*."""
        if self.resolution_result is None:
            return None

        op_id = context_op.operator_debug_id
        resolved_exprs = self.resolution_result.resolved_expressions.get(op_id, [])

        for resolved_expr in resolved_exprs:
            ref = resolved_expr.get_ref(variable, property_name)
            if ref is not None:
                return ref

        if op_id in self.resolution_result.resolved_projections:
            for resolved_proj in self.resolution_result.resolved_projections[op_id]:
                ref = resolved_proj.expression.get_ref(variable, property_name)
                if ref is not None:
                    return ref

        return None

    def get_entity_id_column_from_schema(
        self, schema: Schema, entity_alias: str
    ) -> str | None:
        """Get the rendered ID column name for *entity_alias* in *schema*."""
        for field in schema:
            if isinstance(field, EntityField) and field.field_alias == entity_alias:
                if field.entity_type == EntityType.NODE and field.node_join_field:
                    return self.get_field_name(
                        entity_alias, field.node_join_field.field_alias
                    )
                elif field.rel_source_join_field:
                    return self.get_field_name(
                        entity_alias, field.rel_source_join_field.field_alias
                    )
        return None
