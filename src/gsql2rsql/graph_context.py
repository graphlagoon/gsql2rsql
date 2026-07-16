"""
GraphContext: Simplified API for Triple Store scenarios.

This module provides a high-level API for the most common use case:
a graph stored as two tables (nodes and edges) with type columns.

Example:
    >>> from gsql2rsql import GraphContext
    >>>
    >>> graph = GraphContext(
    ...     spark=spark,
    ...     nodes_table="catalog.schema.nodes",
    ...     edges_table="catalog.schema.edges"
    ... )
    >>>
    >>> # Transpile OpenCypher to SQL
    >>> sql = graph.transpile("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name")
    >>>
    >>> # Execute directly (returns DataFrame)
    >>> df = graph.execute("MATCH (n) RETURN n LIMIT 10")

This eliminates ~100 lines of boilerplate schema configuration code.
"""

from typing import TYPE_CHECKING, Literal

from gsql2rsql.common.schema import (
    EdgeSchema,
    EntityProperty,
    NodeSchema,
)
from gsql2rsql.parser.opencypher_parser import OpenCypherParser
from gsql2rsql.planner.bidirectional_optimizer import apply_bidirectional_optimization
from gsql2rsql.planner.logical_plan import LogicalPlan
from gsql2rsql.planner.pass_manager import optimize_plan
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.renderer.procedural_bfs_renderer import (
    ProceduralBFSOptimizations,
)
from gsql2rsql.renderer.sql_renderer import SQLRenderer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class GraphContext:
    """High-level API for Triple Store graph queries.

    Automatically discovers schema from node and edge tables, eliminating
    the need for manual schema configuration.

    Args:
        spark: PySpark SparkSession (optional, only needed for execute())
        nodes_table: Fully qualified path to nodes table (e.g., "`catalog`.`schema`.`nodes`")
        edges_table: Fully qualified path to edges table
        node_type_col: Column name for node type (default: "type")
        edge_type_col: Column name for edge type (default: "relationship_type")
        node_id_col: Column name for node ID (default: "node_id")
        edge_src_col: Column name for edge source (default: "src")
        edge_dst_col: Column name for edge destination (default: "dst")
        extra_node_attrs: Additional node properties with types (default: auto-discover)
        extra_edge_attrs: Additional edge properties with types (default: auto-discover)
        discover_edge_combinations: If True, queries DB to find actual edge
            combinations instead of creating all possible combinations. This can
            dramatically reduce schema size (e.g., 15 real edges vs 500 possible).
            Requires spark session. Default: False for backward compatibility.
        optimize_dead_tables: If True (default), removes unnecessary JOINs when
            nodes/entities are not used in RETURN/WHERE/ORDER BY clauses.
            For example: `MATCH (a)-[r]->(b) RETURN r` won't JOIN with nodes table.
            Set to False to preserve INNER JOINs that filter out orphan edges
            (edges pointing to non-existent nodes). See new_bugs/002_dead_table_elimination.md.

    Example:
        >>> # Basic usage (creates all possible edge combinations)
        >>> graph = GraphContext(
        ...     spark=spark,
        ...     nodes_table="catalog.schema.nodes",
        ...     edges_table="catalog.schema.edges"
        ... )
        >>>
        >>> # Efficient usage (discovers only real edge combinations)
        >>> graph = GraphContext(
        ...     spark=spark,
        ...     nodes_table="catalog.schema.nodes",
        ...     edges_table="catalog.schema.edges",
        ...     discover_edge_combinations=True  # ← Much faster for large schemas!
        ... )
        >>>
        >>> sql = graph.transpile("MATCH (p:Person) RETURN p.name")
        >>> df = graph.execute("MATCH (p:Person) RETURN p LIMIT 10")
    """

    def __init__(
        self,
        spark: "SparkSession | None" = None,
        nodes_table: str | None = None,
        edges_table: str | None = None,
        node_type_col: str = "node_type",
        edge_type_col: str = "relationship_type",
        node_id_col: str = "node_id",
        edge_src_col: str = "src",
        edge_dst_col: str = "dst",
        extra_node_attrs: dict[str, type] | None = None,
        extra_edge_attrs: dict[str, type] | None = None,
        discover_edge_combinations: bool = False,
        optimize_dead_tables: bool = True,
    ):
        """Initialize GraphContext with Triple Store tables.

        Args:
            discover_edge_combinations: If True, queries database to find actual
                edge combinations (source_type, edge_type, sink_type) instead of
                creating all possible combinations. Requires spark session.
            optimize_dead_tables: If True (default), enables Dead Table Elimination
                optimization which removes unnecessary JOINs when nodes/entities
                are not used in RETURN/WHERE/ORDER BY. Set to False if you need
                to filter out orphan edges (edges pointing to non-existent nodes).
                See new_bugs/002_dead_table_elimination.md for details.
        """
        self.spark = spark
        self.nodes_table = nodes_table
        self.edges_table = edges_table
        self.node_type_col = node_type_col
        self.edge_type_col = edge_type_col
        self.node_id_col = node_id_col
        self.edge_src_col = edge_src_col
        self.edge_dst_col = edge_dst_col
        self.extra_node_attrs = extra_node_attrs or {}
        self.extra_edge_attrs = extra_edge_attrs or {}
        self.discover_edge_combinations = discover_edge_combinations
        self.optimize_dead_tables = optimize_dead_tables

        # Validate required parameters
        if nodes_table is None:
            raise ValueError("nodes_table is required")
        if edges_table is None:
            raise ValueError("edges_table is required")

        # Auto-discover schema if spark is available
        self._node_types: list[str] | None = None
        self._edge_types: list[str] | None = None
        self._edge_combinations: list[tuple[str, str, str]] | None = None

        if spark is not None:
            self._discover_types()
            if discover_edge_combinations:
                self._discover_edge_combinations()

        # Setup internal schema (single provider for both planner and renderer)
        # SimpleSQLSchemaProvider implements ISQLDBSchemaProvider which extends IGraphSchemaProvider
        self._schema: SimpleSQLSchemaProvider | None = None
        self._setup_schemas()

        # Setup transpiler components
        self._parser = OpenCypherParser()
        self._renderer: SQLRenderer | None = None  # Lazy init after schemas are ready
        self._vlp_rendering_mode: str = "cte"
        self._materialization_strategy: str = "temp_tables"
        self._procedural_optimizations = ProceduralBFSOptimizations()

    def _discover_types(self) -> None:
        """Auto-discover node and edge types from tables."""
        if self.spark is None:
            raise RuntimeError("Spark session required for type discovery")

        # Discover node types
        node_types_query = (
            f"SELECT DISTINCT({self.node_type_col}) FROM {self.nodes_table}"
        )
        self._node_types = [
            getattr(row, self.node_type_col)
            for row in self.spark.sql(node_types_query).collect()
        ]

        # Discover edge types
        edge_types_query = (
            f"SELECT DISTINCT({self.edge_type_col}) FROM {self.edges_table}"
        )
        self._edge_types = [
            getattr(row, self.edge_type_col)
            for row in self.spark.sql(edge_types_query).collect()
        ]

    def _discover_edge_combinations(self) -> None:
        """Discover actual edge combinations (source_type, edge_type, sink_type).

        Queries database to find which combinations actually exist, avoiding
        creating unnecessary schemas for impossible edge combinations.

        Requires spark session.
        """
        if self.spark is None:
            raise RuntimeError("Spark session required for edge combination discovery")

        # Query to find actual (source_type, edge_type, sink_type) triplets
        query = f"""
            SELECT DISTINCT
                src_node.{self.node_type_col} AS source_type,
                e.{self.edge_type_col} AS edge_type,
                dst_node.{self.node_type_col} AS sink_type
            FROM {self.edges_table} e
            JOIN {self.nodes_table} src_node ON e.{self.edge_src_col} = src_node.{self.node_id_col}
            JOIN {self.nodes_table} dst_node ON e.{self.edge_dst_col} = dst_node.{self.node_id_col}
        """

        self._edge_combinations = [
            (row.source_type, row.edge_type, row.sink_type)
            for row in self.spark.sql(query).collect()
        ]

    def set_types(
        self,
        node_types: list[str],
        edge_types: list[str],
        edge_combinations: list[tuple[str, str, str]] | None = None,
    ) -> None:
        """Manually set node and edge types (for non-Spark usage).

        Args:
            node_types: List of node type names (e.g., ["Person", "Company"])
            edge_types: List of edge type names (e.g., ["KNOWS", "WORKS_AT"])
            edge_combinations: Optional list of actual edge combinations as
                (source_type, edge_type, sink_type) tuples. If not provided,
                all possible combinations will be created.

        Example:
            >>> # Simple usage (creates all combinations)
            >>> graph = GraphContext(
            ...     nodes_table="catalog.schema.nodes",
            ...     edges_table="catalog.schema.edges"
            ... )
            >>> graph.set_types(
            ...     node_types=["Person", "Company"],
            ...     edge_types=["KNOWS", "WORKS_AT"]
            ... )
            >>>
            >>> # Advanced usage (specify exact combinations)
            >>> graph.set_types(
            ...     node_types=["Person", "Company"],
            ...     edge_types=["KNOWS", "WORKS_AT"],
            ...     edge_combinations=[
            ...         ("Person", "KNOWS", "Person"),
            ...         ("Person", "WORKS_AT", "Company"),
            ...     ]
            ... )
        """
        self._node_types = node_types
        self._edge_types = edge_types
        self._edge_combinations = edge_combinations
        self._setup_schemas()

    def _setup_schemas(self) -> None:
        """Setup internal schema provider.

        Uses SimpleSQLSchemaProvider which implements both IGraphSchemaProvider
        (for the planner) and ISQLDBSchemaProvider (for the renderer).
        """
        if self._node_types is None or self._edge_types is None:
            # Can't setup without types
            return

        # Single schema provider for both planner and renderer
        self._schema = SimpleSQLSchemaProvider()

        # Reset renderer so it gets recreated with new schema
        self._renderer = None

        # Create node schemas
        for node_type in self._node_types:
            # Build properties list: system properties + user-defined properties
            node_properties = [
                # System properties (always available)
                EntityProperty(property_name=self.node_id_col, data_type=str),
                EntityProperty(property_name=self.node_type_col, data_type=str),
            ] + [
                # User-defined properties
                EntityProperty(property_name=prop_name, data_type=data_type)
                for prop_name, data_type in self.extra_node_attrs.items()
                # Skip if user already defined system columns (avoid duplicates)
                if prop_name not in (self.node_id_col, self.node_type_col)
            ]

            node_schema = NodeSchema(
                name=node_type,
                properties=node_properties,
                node_id_property=EntityProperty(
                    property_name=self.node_id_col, data_type=str
                )
            )
            self._schema.add_node(
                node_schema,
                SQLTableDescriptor(
                    table_name=self.nodes_table,
                    node_id_columns=[self.node_id_col],
                    filter=f"{self.node_type_col} = '{node_type}'"
                )
            )

        # Create edge schemas
        # Use discovered/manual combinations if available, otherwise create all combinations
        if self._edge_combinations is not None:
            # Use specific combinations (efficient)
            combinations = self._edge_combinations
        else:
            # Create all possible combinations (backward compatible, but inefficient)
            combinations = [
                (source_type, edge_type, sink_type)
                for edge_type in self._edge_types
                for source_type in self._node_types
                for sink_type in self._node_types
            ]

        for source_type, edge_type, sink_type in combinations:
            edge_id = f"{source_type}@{edge_type}@{sink_type}"

            # Build properties list: system properties + user-defined properties
            edge_properties = [
                # System properties (always available)
                EntityProperty(property_name=self.edge_src_col, data_type=str),
                EntityProperty(property_name=self.edge_dst_col, data_type=str),
                EntityProperty(property_name=self.edge_type_col, data_type=str),
            ] + [
                # User-defined properties
                EntityProperty(property_name=prop_name, data_type=data_type)
                for prop_name, data_type in self.extra_edge_attrs.items()
                # Skip if user already defined system columns (avoid duplicates)
                if prop_name not in (
                    self.edge_src_col, self.edge_dst_col, self.edge_type_col
                )
            ]

            edge_schema = EdgeSchema(
                name=edge_type,
                source_node_id=source_type,
                sink_node_id=sink_type,
                source_id_property=EntityProperty(
                    property_name=self.edge_src_col, data_type=str
                ),
                sink_id_property=EntityProperty(
                    property_name=self.edge_dst_col, data_type=str
                ),
                properties=edge_properties,
            )
            self._schema.add_edge(
                edge_schema,
                SQLTableDescriptor(
                    entity_id=edge_id,
                    table_name=self.edges_table,
                    filter=f"{self.edge_type_col} = '{edge_type}'",
                    node_id_columns=[self.edge_src_col, self.edge_dst_col]
                )
            )

        # No-label node support and untyped edge support are automatically
        # enabled by SimpleSQLSchemaProvider when nodes/edges are added.
        # The provider detects the base table and creates wildcards automatically.

    def transpile(
        self,
        query: str,
        optimize: bool = True,
        bidirectional_mode: Literal[
            "off", "recursive", "unrolling", "auto"
        ] = "recursive",
        vlp_rendering_mode: Literal["cte", "procedural"] = "cte",
        materialization_strategy: Literal[
            "temp_tables", "numbered_views"
        ] = "temp_tables",
        procedural_optimizations: "ProceduralBFSOptimizations | None" = None,
        config: dict[str, object] | None = None,
    ) -> str:
        """Transpile OpenCypher query to Databricks SQL.

        Args:
            query: OpenCypher query string
            optimize: Enable optimizations (predicate pushdown, flattening)
            bidirectional_mode: BFS bidirectional optimization mode:
                - "off": Disable bidirectional BFS
                - "recursive": Use WITH RECURSIVE forward/backward CTEs (default)
                - "unrolling": Use unrolled CTEs (fwd0, fwd1, bwd0, bwd1)
                - "auto": Auto-select based on query characteristics

                Bidirectional BFS is only applied when BOTH source AND target
                have equality filters on their ID columns. Enables large-scale
                queries that would exceed Spark's row limits.
            config: Optional renderer configuration dictionary. Supported keys:
                - 'enable_vlp_node_pushdown' (bool, default True): traverse a
                  pre-filtered edge set when the query has a conjunctive
                  all(n IN nodes(p) WHERE ...) predicate. Optimization only;
                  disabling it never changes results.

        Returns:
            Databricks SQL query string

        Example:
            >>> sql = graph.transpile("MATCH (p:Person) RETURN p.name")
            >>> print(sql)
            SELECT name AS name FROM ...

            >>> # With bidirectional optimization
            >>> sql = graph.transpile(
            ...     "MATCH (a)-[:KNOWS*1..5]->(b) WHERE a.id='X' AND b.id='Y' RETURN path",
            ...     bidirectional_mode="auto"
            ... )
        """
        if self._schema is None:
            raise RuntimeError(
                "Schema not initialized. Call set_types() or provide spark parameter."
            )

        if procedural_optimizations is None:
            procedural_optimizations = ProceduralBFSOptimizations()

        # Lazy initialize renderer (recreate when mode/strategy/options change)
        if (
            self._renderer is None
            or self._vlp_rendering_mode != vlp_rendering_mode
            or self._materialization_strategy != materialization_strategy
            or self._procedural_optimizations != procedural_optimizations
            or getattr(self, "_renderer_config", None) != config
        ):
            self._vlp_rendering_mode = vlp_rendering_mode
            self._materialization_strategy = materialization_strategy
            self._procedural_optimizations = procedural_optimizations
            self._renderer_config = config
            self._renderer = SQLRenderer(
                db_schema_provider=self._schema,
                config=config,
                vlp_rendering_mode=vlp_rendering_mode,
                materialization_strategy=materialization_strategy,
                procedural_optimizations=procedural_optimizations,
            )

        # Parse query
        ast = self._parser.parse(query)

        # Create logical plan (SimpleSQLSchemaProvider implements IGraphSchemaProvider)
        plan = LogicalPlan.process_query_tree(ast, self._schema)

        # Apply optimizations
        if optimize:
            optimize_plan(
                plan,
                enabled=True,
                pushdown_enabled=True,
                dead_table_elimination_enabled=self.optimize_dead_tables,
            )

        # Apply bidirectional BFS optimization
        # This sets flags on RecursiveTraversalOperator that the renderer uses
        apply_bidirectional_optimization(
            plan,
            graph_schema=self._schema,
            mode=bidirectional_mode,
        )

        # Resolve column references
        plan.resolve(original_query=query)

        # Render to SQL
        return self._renderer.render_plan(plan)

    def execute(self, query: str, optimize: bool = True) -> "DataFrame":
        """Execute OpenCypher query and return results as DataFrame.

        Args:
            query: OpenCypher query string
            optimize: Enable optimizations (predicate pushdown, flattening)

        Returns:
            PySpark DataFrame with query results

        Raises:
            RuntimeError: If spark session not provided

        Example:
            >>> df = graph.execute("MATCH (p:Person) RETURN p.name LIMIT 10")
            >>> df.show()
            +--------+
            |   name |
            +--------+
            |  Alice |
            |    Bob |
            +--------+
        """
        if self.spark is None:
            raise RuntimeError(
                "Spark session required for execute(). "
                "Pass spark parameter to GraphContext() or use transpile() instead."
            )

        # Transpile to SQL
        sql = self.transpile(query, optimize=optimize)

        # Execute SQL
        return self.spark.sql(sql)

    def __repr__(self) -> str:
        """String representation of GraphContext."""
        node_count = len(self._node_types) if self._node_types else 0
        edge_count = len(self._edge_types) if self._edge_types else 0

        return (
            f"GraphContext("
            f"nodes_table={self.nodes_table!r}, "
            f"edges_table={self.edges_table!r}, "
            f"node_types={node_count}, "
            f"edge_types={edge_count}"
            f")"
        )
