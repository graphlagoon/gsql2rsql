"""DataSourceOperator for node and edge table data sources."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from gsql2rsql.common.schema import IGraphSchemaProvider
from gsql2rsql.parser.ast import (
    Entity,
    NodeEntity,
    QueryExpression,
    RelationshipDirection,
    RelationshipEntity,
)
from gsql2rsql.planner.operators.base import IBindable, StartLogicalOperator
from gsql2rsql.planner.schema import (
    EntityField,
    EntityType,
    Schema,
    ValueField,
)


def _describe_available_node_types(
    graph_definition: IGraphSchemaProvider,
    requested: str,
) -> str:
    """Build an actionable suffix for a failed node-type binding.

    A bare "failed to bind" says nothing about *why* the lookup missed.  The
    lookup is an exact, case-sensitive match against the registered type
    names, so the overwhelmingly common causes are a case difference or
    stray whitespace in the schema registration.  Surfacing the registered
    names — and any case-insensitive near-match — turns a dead end into a
    one-line fix.

    Best-effort only: providers are not required to enumerate their types,
    so any failure here degrades to an empty suffix rather than masking the
    original binding error.
    """
    try:
        get_all = getattr(graph_definition, "get_all_node_schemas", None)
        if get_all is None:
            return ""
        available = sorted(
            schema.name for schema in get_all() if getattr(schema, "name", None)
        )
    except Exception:  # pragma: no cover - defensive
        return ""

    if not available:
        return ""

    near = [
        name
        for name in available
        if name.strip().casefold() == requested.strip().casefold()
    ]
    hint = ""
    if near:
        hint = (
            f" Did you mean '{near[0]}'? Node type names are matched exactly "
            f"(case-sensitive, whitespace-sensitive)."
        )

    return f".{hint} Available node types: {', '.join(repr(n) for n in available)}"


@dataclass
class _BindingResult:
    """Result of binding an entity to a graph schema.

    This dataclass encapsulates all the information extracted during binding,
    making the data flow explicit and avoiding scattered variable assignments.
    """

    entity_unique_name: str
    properties: list[ValueField]
    source_entity_name: str = ""
    sink_entity_name: str = ""
    node_id_field: ValueField | None = None
    edge_src_id_field: ValueField | None = None
    edge_sink_id_field: ValueField | None = None
    resolved_edge_types: list[str] = field(default_factory=list)


@dataclass
class DataSourceOperator(StartLogicalOperator, IBindable):
    """Operator representing a data source (node or edge table).

    Attributes:
        entity: The graph entity (NodeEntity or RelationshipEntity) this source represents.
        filter_expression: Optional filter expression to apply to this data source.
            When set, the renderer should generate a WHERE clause for this source.
            This is populated by SelectionPushdownOptimizer when a predicate
            references only this entity's variable.
    """

    entity: Entity | None = None
    filter_expression: QueryExpression | None = None

    def __post_init__(self) -> None:
        if self.entity:
            # Initialize output schema with the entity
            entity_type = (
                EntityType.NODE
                if isinstance(self.entity, NodeEntity)
                else EntityType.RELATIONSHIP
            )
            self.output_schema = Schema([
                EntityField(
                    field_alias=self.entity.alias,
                    entity_name=self.entity.entity_name,
                    entity_type=entity_type,
                )
            ])

    def bind(self, graph_definition: IGraphSchemaProvider) -> None:
        """Bind this data source to a graph schema."""
        if not self.entity:
            return

        # Delegate to specific binding method based on entity type
        if isinstance(self.entity, NodeEntity):
            result = self._bind_node_entity(self.entity, graph_definition)
        elif isinstance(self.entity, RelationshipEntity):
            result = self._bind_relationship_entity(self.entity, graph_definition)
        else:
            return

        # Apply binding result to output schema
        self._apply_binding_result(result)

    def _bind_node_entity(
        self,
        node_entity: NodeEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> _BindingResult:
        """Bind a NodeEntity to its schema definition.

        Args:
            node_entity: The node entity to bind.
            graph_definition: The graph schema provider.

        Returns:
            _BindingResult with the binding information.

        Raises:
            TranspilerBindingException: If the node type cannot be found.
        """
        from gsql2rsql.common.exceptions import TranspilerBindingException

        entity_name = node_entity.entity_name

        if not entity_name:
            # No label provided - try wildcard support
            node_def = graph_definition.get_wildcard_node_definition()
            if not node_def:
                raise TranspilerBindingException(
                    f"No-label node '{node_entity.alias}' not supported. "
                    f"Specify a label or enable no_label_support."
                )
        else:
            node_def = graph_definition.get_node_definition(entity_name)
            if not node_def:
                raise TranspilerBindingException(
                    f"Failed to bind entity '{node_entity.alias}' "
                    f"of type '{entity_name}'"
                    + _describe_available_node_types(graph_definition, entity_name)
                )

        # Build node ID field if available
        node_id_field: ValueField | None = None
        if node_def.node_id_property:
            node_id_field = ValueField(
                field_alias=node_def.node_id_property.property_name,
                field_name=node_def.node_id_property.property_name,
                data_type=node_def.node_id_property.data_type,
            )

        # Build properties list
        properties: list[ValueField] = [
            ValueField(
                field_alias=prop.property_name,
                field_name=prop.property_name,
                data_type=prop.data_type,
            )
            for prop in node_def.properties
        ]
        if node_id_field:
            properties.append(node_id_field)

        return _BindingResult(
            entity_unique_name=node_def.id,
            properties=properties,
            node_id_field=node_id_field,
        )

    def _bind_relationship_entity(
        self,
        rel_entity: RelationshipEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> _BindingResult:
        """Bind a RelationshipEntity to its schema definition.

        Args:
            rel_entity: The relationship entity to bind.
            graph_definition: The graph schema provider.

        Returns:
            _BindingResult with the binding information.

        Raises:
            TranspilerBindingException: If the relationship type cannot be found.
        """
        from gsql2rsql.common.exceptions import TranspilerBindingException

        # Parse edge types (handle OR syntax like "KNOWS|WORKS_AT")
        raw_edge_types = [
            t.strip() for t in rel_entity.entity_name.split("|") if t.strip()
        ]

        # Determine source/sink based on direction
        source_type, sink_type = self._get_endpoint_types(rel_entity)

        # Try to bind each edge type and collect resolved types
        edge_def, resolved_edge_types = self._resolve_edge_types(
            raw_edge_types, source_type, sink_type, rel_entity, graph_definition
        )

        # Handle untyped edges or raise error
        if not edge_def:
            if not raw_edge_types:
                # Untyped edge (e.g., -[]- or --), use wildcard edge
                edge_def = graph_definition.get_wildcard_edge_definition()

            if not edge_def:
                raise TranspilerBindingException(
                    f"Failed to bind relationship '{rel_entity.alias}' "
                    f"of type '{rel_entity.entity_name}'"
                )

        # Build ID fields
        edge_src_id_field: ValueField | None = None
        edge_sink_id_field: ValueField | None = None

        if edge_def.source_id_property:
            edge_src_id_field = ValueField(
                field_alias=edge_def.source_id_property.property_name,
                field_name=edge_def.source_id_property.property_name,
                data_type=edge_def.source_id_property.data_type,
            )
        if edge_def.sink_id_property:
            edge_sink_id_field = ValueField(
                field_alias=edge_def.sink_id_property.property_name,
                field_name=edge_def.sink_id_property.property_name,
                data_type=edge_def.sink_id_property.data_type,
            )

        # Build properties list
        properties: list[ValueField] = [
            ValueField(
                field_alias=prop.property_name,
                field_name=prop.property_name,
                data_type=prop.data_type,
            )
            for prop in edge_def.properties
        ]
        if edge_src_id_field:
            properties.append(edge_src_id_field)
        if edge_sink_id_field:
            properties.append(edge_sink_id_field)

        return _BindingResult(
            entity_unique_name=edge_def.id,
            properties=properties,
            source_entity_name=edge_def.source_node_id,
            sink_entity_name=edge_def.sink_node_id,
            edge_src_id_field=edge_src_id_field,
            edge_sink_id_field=edge_sink_id_field,
            resolved_edge_types=resolved_edge_types,
        )

    def _get_endpoint_types(
        self, rel_entity: RelationshipEntity
    ) -> tuple[str | None, str | None]:
        """Determine source and sink types based on relationship direction."""
        if rel_entity.direction == RelationshipDirection.FORWARD:
            return (
                rel_entity.left_entity_name or None,
                rel_entity.right_entity_name or None,
            )
        elif rel_entity.direction == RelationshipDirection.BACKWARD:
            return (
                rel_entity.right_entity_name or None,
                rel_entity.left_entity_name or None,
            )
        else:
            return (
                rel_entity.left_entity_name or None,
                rel_entity.right_entity_name or None,
            )

    def _resolve_edge_types(
        self,
        raw_edge_types: list[str],
        source_type: str | None,
        sink_type: str | None,
        rel_entity: RelationshipEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> tuple[Any, list[str]]:
        """Resolve edge types from schema, returning the first edge definition found.

        Returns:
            Tuple of (edge_def or None, list of resolved edge type names).
        """
        from gsql2rsql.common.schema import EdgeSchema

        edge_def: EdgeSchema | None = None
        resolved_edge_types: list[str] = []

        for edge_type in raw_edge_types:
            found_edge = self._find_edge_definition(
                edge_type, source_type, sink_type, rel_entity, graph_definition
            )
            if found_edge:
                resolved_edge_types.append(edge_type)
                if edge_def is None:
                    edge_def = found_edge  # Use first for schema

        return edge_def, resolved_edge_types

    def _find_edge_definition(
        self,
        edge_type: str,
        source_type: str | None,
        sink_type: str | None,
        rel_entity: RelationshipEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> Any:
        """Find edge definition for a single edge type."""
        # Check if either endpoint is unknown (no label)
        if source_type is None or sink_type is None:
            return self._find_edge_partial_lookup(
                edge_type, source_type, sink_type, rel_entity, graph_definition
            )
        else:
            return self._find_edge_exact_lookup(
                edge_type, rel_entity, graph_definition
            )

    def _find_edge_partial_lookup(
        self,
        edge_type: str,
        source_type: str | None,
        sink_type: str | None,
        rel_entity: RelationshipEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> Any:
        """Find edge with partial endpoint information."""
        edges = graph_definition.find_edges_by_verb(
            edge_type,
            from_node_name=source_type,
            to_node_name=sink_type,
        )
        if edges:
            return edges[0]

        # If direction is BOTH, also try reverse
        if rel_entity.direction == RelationshipDirection.BOTH:
            edges = graph_definition.find_edges_by_verb(
                edge_type,
                from_node_name=sink_type,
                to_node_name=source_type,
            )
            if edges:
                return edges[0]

        return None

    def _find_edge_exact_lookup(
        self,
        edge_type: str,
        rel_entity: RelationshipEntity,
        graph_definition: IGraphSchemaProvider,
    ) -> Any:
        """Find edge with exact endpoint information."""
        if rel_entity.direction == RelationshipDirection.FORWARD:
            return graph_definition.get_edge_definition(
                edge_type,
                rel_entity.left_entity_name,
                rel_entity.right_entity_name,
            )
        elif rel_entity.direction == RelationshipDirection.BACKWARD:
            return graph_definition.get_edge_definition(
                edge_type,
                rel_entity.right_entity_name,
                rel_entity.left_entity_name,
            )
        else:
            # Try both directions
            found_edge = graph_definition.get_edge_definition(
                edge_type,
                rel_entity.left_entity_name,
                rel_entity.right_entity_name,
            )
            if not found_edge:
                found_edge = graph_definition.get_edge_definition(
                    edge_type,
                    rel_entity.right_entity_name,
                    rel_entity.left_entity_name,
                )
            return found_edge

    def _apply_binding_result(self, result: _BindingResult) -> None:
        """Apply binding result to the output schema's entity field."""
        if not self.output_schema:
            return

        entity_field = self.output_schema[0]
        if not isinstance(entity_field, EntityField):
            return

        entity_field.bound_entity_name = result.entity_unique_name
        entity_field.bound_source_entity_name = result.source_entity_name
        entity_field.bound_sink_entity_name = result.sink_entity_name
        entity_field.encapsulated_fields = result.properties
        entity_field.node_join_field = result.node_id_field
        entity_field.rel_source_join_field = result.edge_src_id_field
        entity_field.rel_sink_join_field = result.edge_sink_id_field

        # Store resolved edge types for OR syntax ([:KNOWS|WORKS_AT])
        if isinstance(self.entity, RelationshipEntity):
            entity_field.bound_edge_types = result.resolved_edge_types

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by this data source.

        DataSource introduces exactly one symbol: the entity alias.
        """
        if self.entity:
            return {self.entity.alias}
        return set()

    def __str__(self) -> str:
        base = super().__str__()
        filter_str = (
            f"\n  Filter: {self.filter_expression}"
            if self.filter_expression
            else ""
        )
        return f"{base}\n  DataSource: {self.entity}{filter_str}"
