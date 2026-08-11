"""SQL database schema provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from gsql2rsql.common.schema import (
    EdgeAccessStrategy,
    EdgeSchema,
    EntityProperty,
    IGraphSchemaProvider,
    NodeSchema,
)


@dataclass
class SQLTableDescriptor:
    """Describes how a graph entity maps to SQL table(s).

    Can be constructed in two ways:
    1. Direct: SQLTableDescriptor(table_or_view_name="Device", schema_name="dbo")
    2. Convenience: SQLTableDescriptor(
           entity_id="device",
           table_name="dbo.Device",
           node_id_columns=["id"]
       )

    When using the convenience form with table_name, the schema and table
    will be parsed automatically.
    """

    table_or_view_name: str = ""
    schema_name: str = "dbo"
    column_mappings: dict[str, str] = field(default_factory=dict)
    node_id_columns: list[str] = field(default_factory=list)
    entity_id: str | None = None
    table_name: str | None = field(default=None, repr=False)
    filter: str | None = None

    def __post_init__(self) -> None:
        """Parse table_name if provided and table_or_view_name is not set."""
        if self.table_name and not self.table_or_view_name:
            if "." in self.table_name:
                parts = self.table_name.rsplit(".", 1)
                # Use object.__setattr__ since dataclass may be frozen
                object.__setattr__(self, "schema_name", parts[0])
                object.__setattr__(self, "table_or_view_name", parts[1])
            else:
                object.__setattr__(self, "schema_name", "")
                object.__setattr__(self, "table_or_view_name", self.table_name)

    @classmethod
    def from_table_name(
        cls,
        entity_id: str,
        table_name: str,
        node_id_columns: list[str] | None = None,
        column_mappings: dict[str, str] | None = None,
    ) -> SQLTableDescriptor:
        """
        Create a SQLTableDescriptor from a fully qualified table name.

        Args:
            entity_id: The entity identifier
                (node name or edge id like "node1@verb@node2").
            table_name: Fully qualified table name
                (e.g., "dbo.Device" or "Device").
            node_id_columns: List of ID column names for the entity.
            column_mappings: Optional property to column mappings.

        Returns:
            A new SQLTableDescriptor instance.
        """
        if "." in table_name:
            parts = table_name.rsplit(".", 1)
            schema_name = parts[0]
            table_or_view_name = parts[1]
        else:
            schema_name = ""
            table_or_view_name = table_name

        return cls(
            table_or_view_name=table_or_view_name,
            schema_name=schema_name,
            column_mappings=column_mappings or {},
            node_id_columns=node_id_columns or [],
            entity_id=entity_id,
        )

    @property
    def full_table_name(self) -> str:
        """Get the fully qualified table name for Databricks SQL.

        For Databricks SQL, table names can be:
        - Simple: table_name
        - Two-part: schema.table
        - Three-part: catalog.schema.table

        Table names are returned exactly as provided by the user.
        If backticks are needed, they should be included by the user when
        creating the SQLTableDescriptor.
        """
        table_name = self.table_or_view_name

        # If table_name already contains dots (e.g., catalog.schema.table),
        # use it directly without adding schema prefix
        if "." in table_name:
            return table_name

        # Skip 'dbo' prefix - it's a SQL Server convention, not Databricks
        if self.schema_name and self.schema_name.lower() != "dbo":
            return f"{self.schema_name}.{table_name}"

        return table_name


class ISQLDBSchemaProvider(IGraphSchemaProvider, ABC):
    """
    Interface for SQL database schema providers.

    Extends IGraphSchemaProvider with SQL-specific mappings.
    """

    @abstractmethod
    def get_sql_table_descriptors(self, entity_name: str) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for a graph entity.

        Args:
            entity_name: The unique entity name (node name or edge id).

        Returns:
            SQLTableDescriptor if found, None otherwise.
        """
        ...

    @abstractmethod
    def get_wildcard_table_descriptor(self) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for wildcard nodes (no type filter).

        Returns:
            SQLTableDescriptor if wildcard support is enabled, None otherwise.
        """
        ...

    @abstractmethod
    def get_wildcard_edge_table_descriptor(self) -> SQLTableDescriptor | None:
        """
        Get the SQL table descriptor for wildcard edges (no type filter).

        Returns:
            SQLTableDescriptor if wildcard edge support is enabled, None otherwise.
        """
        ...

    def find_edge_by_verb(
        self, verb: str, target_node_name: str | None = None
    ) -> tuple[EdgeSchema, SQLTableDescriptor] | None:
        """
        Find an edge schema by verb (relationship name), optionally filtered by target node.

        This is useful when the source node type is unknown (e.g., in EXISTS patterns).

        Args:
            verb: The relationship type/verb (e.g., "ACTED_IN")
            target_node_name: Optional target node type to filter by

        Returns:
            Tuple of (EdgeSchema, SQLTableDescriptor) if found, None otherwise.
        """
        return None  # Default implementation - subclasses can override


class SimpleSQLSchemaProvider(ISQLDBSchemaProvider):
    """A simple in-memory SQL schema provider.

    Automatically supports no-label nodes and untyped edges by detecting
    the base table from the first node/edge added. When a query uses a node
    without a label or an edge without a type, the base table is used without
    any type filter.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, NodeSchema] = {}
        self._edges: dict[str, EdgeSchema] = {}
        self._table_descriptors: dict[str, SQLTableDescriptor] = {}
        # Explicit wildcard configuration
        self._wildcard_node: NodeSchema | None = None
        self._wildcard_table_desc: SQLTableDescriptor | None = None
        # Auto-detected base tables (from first node/edge added)
        self._base_node_table: SQLTableDescriptor | None = None
        self._base_node_schema: NodeSchema | None = None
        self._base_edge_table: SQLTableDescriptor | None = None
        self._base_edge_schema: EdgeSchema | None = None

    def add_node(
        self,
        schema: NodeSchema,
        table_descriptor: SQLTableDescriptor | None = None,
    ) -> None:
        """Add a node schema with its SQL table descriptor.

        Args:
            schema: The node schema to add.
            table_descriptor: SQL table descriptor. If None, a default one is
                created using the schema name as table name.

        The first node added is used as the base for no-label node support.
        """
        self._nodes[schema.name] = schema

        # Create default table descriptor if not provided
        if table_descriptor is None:
            table_descriptor = SQLTableDescriptor(
                entity_id=schema.name,
                table_name=schema.name,
                node_id_columns=["id"],
            )

        self._table_descriptors[schema.id] = table_descriptor
        # Auto-detect base node table from first node added
        if self._base_node_table is None:
            self._base_node_table = table_descriptor
            self._base_node_schema = schema

    def add_edge(
        self,
        schema: EdgeSchema,
        table_descriptor: SQLTableDescriptor | None = None,
    ) -> None:
        """Add an edge schema with its SQL table descriptor.

        Args:
            schema: The edge schema to add.
            table_descriptor: SQL table descriptor. If None, a default one is
                created using the edge id as table name.

        The first edge added is used as the base for untyped edge support.
        """
        # Create default table descriptor if not provided
        if table_descriptor is None:
            table_descriptor = SQLTableDescriptor(
                entity_id=schema.id,
                table_name=schema.name,
                node_id_columns=["source_id", "target_id"],
            )

        # An edge without id properties cannot be joined: the renderer would
        # prune the (undeclared) key columns out of the edge subquery while
        # still naming them in the ON clause — SQL that cannot execute.
        # Derive them from the descriptor's [source, sink] columns; reject
        # when even that is impossible.
        if schema.source_id_property is None or schema.sink_id_property is None:
            columns = table_descriptor.node_id_columns or []
            if len(columns) < 2:
                raise ValueError(
                    f"Edge '{schema.name}' has no source_id_property/"
                    f"sink_id_property and its table descriptor declares "
                    f"{len(columns)} node_id_columns — the join keys cannot "
                    f"be determined. Provide both id properties on the "
                    f"EdgeSchema or two node_id_columns ([source, sink]) on "
                    f"the SQLTableDescriptor."
                )
            if schema.source_id_property is None:
                schema.source_id_property = EntityProperty(columns[0], int)
            if schema.sink_id_property is None:
                schema.sink_id_property = EntityProperty(columns[1], int)

        self._edges[schema.id] = schema
        self._table_descriptors[schema.id] = table_descriptor
        # Auto-detect base edge table from first edge added
        if self._base_edge_table is None:
            self._base_edge_table = table_descriptor
            self._base_edge_schema = schema

    def set_wildcard_node(
        self,
        schema: NodeSchema,
        table_descriptor: SQLTableDescriptor,
    ) -> None:
        """Register wildcard node with its SQL descriptor.

        WARNING: No-label support causes full table scans for unlabeled nodes.
        Use labels whenever possible for production queries.

        Args:
            schema: Node schema for wildcard nodes.
            table_descriptor: SQL table descriptor with filter=None (no type filter).
        """
        self._wildcard_node = schema
        self._wildcard_table_desc = table_descriptor

    def enable_no_label_support(
        self,
        table_name: str,
        node_id_columns: list[str],
        properties: list[EntityProperty] | None = None,
    ) -> None:
        """Enable support for nodes without labels in MATCH patterns.

        This method configures the schema provider to handle queries like:
            MATCH (a)-[:REL]->(b:Label)
        where node 'a' has no label specified.

        WARNING: No-label support causes full table scans for unlabeled nodes.
        Use labels whenever possible for production queries.

        Args:
            table_name: The nodes table name (e.g., "catalog.schema.nodes").
            node_id_columns: List of node ID column names (e.g., ["node_id"]).
            properties: Optional list of node properties available on all nodes.

        Example:
            >>> provider = SimpleSQLSchemaProvider()
            >>> # Add node types...
            >>> provider.enable_no_label_support(
            ...     table_name="catalog.schema.nodes",
            ...     node_id_columns=["node_id"],
            ...     properties=[EntityProperty("name", str)],
            ... )
        """
        from gsql2rsql.common.schema import WILDCARD_NODE_TYPE

        # Create wildcard schema with provided properties
        wildcard_schema = NodeSchema(
            name=WILDCARD_NODE_TYPE,
            node_id_property=EntityProperty(
                property_name=node_id_columns[0] if node_id_columns else "id",
                data_type=str,
            ),
            properties=properties or [],
        )

        # Create table descriptor without type filter (matches all nodes)
        wildcard_desc = SQLTableDescriptor(
            table_name=table_name,
            node_id_columns=node_id_columns,
            filter=None,  # No type filter - matches all nodes
        )

        # Register the wildcard node
        self.set_wildcard_node(wildcard_schema, wildcard_desc)

    def get_wildcard_node_definition(self) -> NodeSchema | None:
        """Get the wildcard node schema.

        Returns explicitly configured wildcard, or auto-generates one from
        the base node table if available.
        """
        if self._wildcard_node is not None:
            return self._wildcard_node
        # Auto-generate from base node schema
        if self._base_node_schema is not None:
            from gsql2rsql.common.schema import WILDCARD_NODE_TYPE
            return NodeSchema(
                name=WILDCARD_NODE_TYPE,
                node_id_property=self._base_node_schema.node_id_property,
                properties=self._base_node_schema.properties,
            )
        return None

    def get_wildcard_table_descriptor(self) -> SQLTableDescriptor | None:
        """Get SQL descriptor for wildcard nodes (no type filter).

        Returns explicitly configured wildcard descriptor, or auto-generates
        one from the base node table (same table, no filter).
        """
        if self._wildcard_table_desc is not None:
            return self._wildcard_table_desc
        # Auto-generate from base node table (same table, no filter)
        if self._base_node_table is not None:
            return SQLTableDescriptor(
                table_name=self._base_node_table.table_name
                or self._base_node_table.full_table_name,
                node_id_columns=self._base_node_table.node_id_columns,
                filter=None,  # No type filter for wildcard
            )
        return None

    def get_wildcard_edge_definition(self) -> EdgeSchema | None:
        """Get the wildcard edge schema.

        Auto-generates one from the base edge table if available.
        """
        if self._base_edge_schema is not None:
            from gsql2rsql.common.schema import WILDCARD_EDGE_TYPE, WILDCARD_NODE_TYPE
            return EdgeSchema(
                name=WILDCARD_EDGE_TYPE,
                source_node_id=WILDCARD_NODE_TYPE,
                sink_node_id=WILDCARD_NODE_TYPE,
                source_id_property=self._base_edge_schema.source_id_property,
                sink_id_property=self._base_edge_schema.sink_id_property,
                type_property=self._base_edge_schema.type_property,
                properties=self._base_edge_schema.properties,
            )
        return None

    def get_wildcard_edge_table_descriptor(self) -> SQLTableDescriptor | None:
        """Get SQL descriptor for wildcard edges (no type filter).

        Auto-generates from the base edge table (same table, no filter).
        """
        if self._base_edge_table is not None:
            return SQLTableDescriptor(
                table_name=self._base_edge_table.table_name
                or self._base_edge_table.full_table_name,
                node_id_columns=self._base_edge_table.node_id_columns,
                filter=None,  # No type filter for wildcard
            )
        return None

    def get_node_definition(self, node_name: str) -> NodeSchema | None:
        """Get a node schema by name."""
        return self._nodes.get(node_name)

    def get_all_node_schemas(self) -> list[NodeSchema]:
        """Get all registered node schemas."""
        return list(self._nodes.values())

    def get_all_edge_schemas(self) -> list[EdgeSchema]:
        """Get all registered edge schemas."""
        return list(self._edges.values())

    def get_edge_definition(
        self, edge_verb: str, from_node_name: str, to_node_name: str
    ) -> EdgeSchema | None:
        """Get an edge schema by verb and connected node names."""
        edge_id = EdgeSchema.get_edge_id(edge_verb, from_node_name, to_node_name)
        return self._edges.get(edge_id)

    def find_edges_by_verb(
        self,
        edge_verb: str,
        from_node_name: str | None = None,
        to_node_name: str | None = None,
    ) -> list[EdgeSchema]:
        """Find edges matching verb and optionally source/sink types.

        Args:
            edge_verb: Relationship type (required, exact match).
            from_node_name: Source type (None/empty = match any).
            to_node_name: Target type (None/empty = match any).
        """
        results = []
        for edge_schema in self._edges.values():
            if edge_schema.name != edge_verb:
                continue
            if from_node_name and edge_schema.source_node_id != from_node_name:
                continue
            if to_node_name and edge_schema.sink_node_id != to_node_name:
                continue
            results.append(edge_schema)
        return results

    def get_sql_table_descriptors(self, entity_name: str) -> SQLTableDescriptor | None:
        """Get the SQL table descriptor for an entity.

        Also handles wildcard nodes and edges - if the entity_name contains
        the wildcard type, falls back to the wildcard table descriptor.
        """
        from gsql2rsql.common.schema import WILDCARD_EDGE_TYPE, WILDCARD_NODE_TYPE

        # Standard lookup
        result = self._table_descriptors.get(entity_name)
        if result is not None:
            return result

        # Fallback: check for wildcard edge (e.g., __wildcard_node__@__wildcard_edge__@__wildcard_node__)
        if WILDCARD_EDGE_TYPE in entity_name:
            return self.get_wildcard_edge_table_descriptor()

        # Fallback: check for wildcard node
        if entity_name == WILDCARD_NODE_TYPE:
            return self.get_wildcard_table_descriptor()

        return None

    def find_edge_by_verb(
        self, verb: str, target_node_name: str | None = None
    ) -> tuple[EdgeSchema, SQLTableDescriptor] | None:
        """Find an edge schema by verb, optionally filtered by target node."""
        for edge_id, edge_schema in self._edges.items():
            if edge_schema.name == verb:
                # Check target node if specified
                if target_node_name and edge_schema.sink_node_id != target_node_name:
                    continue
                # Found matching edge
                table_desc = self._table_descriptors.get(edge_id)
                if table_desc:
                    return (edge_schema, table_desc)
        return None

    def add_table(self, descriptor: SQLTableDescriptor) -> None:
        """
        Add a table descriptor directly using its entity_id.

        This is a convenience method that automatically creates the appropriate
        NodeSchema or EdgeSchema based on the entity_id format.

        Args:
            descriptor: A SQLTableDescriptor with entity_id set.
                For nodes: entity_id should be the node name (e.g., "device").
                For edges: entity_id should be "from@verb@to"
                    (e.g., "device@belongsTo@tenant").

        Raises:
            ValueError: If descriptor.entity_id is not set.
        """
        if not descriptor.entity_id:
            raise ValueError(
                "descriptor.entity_id must be set when using add_table()"
            )

        entity_id = descriptor.entity_id

        # Check if this is an edge (format: "from@verb@to")
        if "@" in entity_id:
            parts = entity_id.split("@")
            if len(parts) == 3:
                from_node, verb, to_node = parts
                edge_schema = EdgeSchema(
                    name=verb,
                    source_node_id=from_node,
                    sink_node_id=to_node,
                    properties=[],
                )
                self._edges[edge_schema.id] = edge_schema
                self._table_descriptors[edge_schema.id] = descriptor
            else:
                raise ValueError(
                    f"Invalid edge entity_id format: {entity_id}. "
                    "Expected 'from@verb@to'."
                )
        else:
            # It's a node
            node_schema = NodeSchema(name=entity_id, properties=[])
            self._nodes[node_schema.name] = node_schema
            self._table_descriptors[node_schema.id] = descriptor

    def get_edge_access_strategy(self) -> EdgeAccessStrategy:
        """Return the edge access strategy for this schema provider.

        Current implementation uses EDGE_LIST strategy (single table with directed edges).
        Undirected traversal requires UNION ALL to access edges in both directions.

        Returns:
            EdgeAccessStrategy.EDGE_LIST
        """
        return EdgeAccessStrategy.EDGE_LIST
