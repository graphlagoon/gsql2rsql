"""RecursiveTraversalOperator for variable-length path traversal."""

from __future__ import annotations

from gsql2rsql.parser.ast import (
    QueryExpression,
    RelationshipDirection,
)
from gsql2rsql.planner.operators.base import LogicalOperator
from gsql2rsql.planner.schema import (
    EntityField,
    EntityType,
    Field,
    Schema,
    ValueField,
)


class RecursiveTraversalOperator(LogicalOperator):
    r"""Operator for recursive traversal (BFS/DFS with variable-length paths).

    Supports path accumulation for nodes(path) and relationships(path) functions.
    When path_variable is set, the CTE accumulates:
    - path_nodes: ARRAY of node IDs in traversal order
    - path_edges: ARRAY of STRUCT with edge properties

    This enables HoF predicates like:
    - ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    - [n IN nodes(path) | n.id]

    PREDICATE PUSHDOWN OPTIMIZATION
    ================================

    The `edge_filter` field enables a critical optimization called "Predicate Pushdown"
    that can dramatically reduce memory usage and execution time for path queries.

    Problem: Exponential Path Growth
    --------------------------------

    Without pushdown, recursive CTEs explore ALL possible paths first, then filter:

                                        A
                                       /|\
                   depth=1 →  $100    $50    $2000
                               /|\     |       |
                  depth=2 →  $20 $30  $15    $5000
                              ...     ...     ...
                               ↓       ↓       ↓
                        ═══════════════════════════
                        AFTER CTE: 10,000+ paths
                        ═══════════════════════════
                               ↓
                        FORALL(edges, e -> e.amount > 1000)
                               ↓
                        ═══════════════════════════
                        FINAL: Only 2 paths survive!
                        ═══════════════════════════

    This is wasteful: we explored 10,000 paths but kept only 2.

    Solution: Push Filter INTO the CTE
    -----------------------------------

    With predicate pushdown, we filter DURING recursion:

                                        A
                                        |
                   depth=1 →         $2000  ← Only edges with amount > 1000
                                        |
                  depth=2 →          $5000
                                        |
                        ═══════════════════════════
                        AFTER CTE: Only 2 paths (already filtered!)
                        ═══════════════════════════

    SQL Comparison:

    BEFORE (no pushdown):
        WITH RECURSIVE paths AS (
          SELECT ... FROM Transfer e          -- ALL edges
          UNION ALL
          SELECT ... FROM paths p JOIN Transfer e ...  -- ALL paths
        )
        SELECT ... WHERE FORALL(path_edges, r -> r.amount > 1000)

    AFTER (with pushdown):
        WITH RECURSIVE paths AS (
          SELECT ... FROM Transfer e
            WHERE e.amount > 1000             ← PREDICATE IN BASE CASE
          UNION ALL
          SELECT ... FROM paths p JOIN Transfer e ...
            WHERE e.amount > 1000             ← PREDICATE IN RECURSIVE CASE
        )
        SELECT ...  -- No FORALL needed!

    When is Pushdown Safe?
    ----------------------

    Only ALL() predicates can be pushed down:
    - ALL(r IN relationships(path) WHERE r.amount > 1000)
      → "Every edge must satisfy" = filter each edge individually ✓

    ANY() predicates CANNOT be pushed:
    - ANY(r IN relationships(path) WHERE r.flagged)
      → "At least one edge must satisfy" = need complete path first ✗
    """

    def __init__(
        self,
        edge_types: list[str],
        source_node_type: str,
        target_node_type: str,
        min_hops: int,
        max_hops: int | None = None,
        source_id_column: str = "id",
        target_id_column: str = "id",
        start_node_filter: QueryExpression | None = None,
        sink_node_filter: QueryExpression | None = None,
        barrier_filter: QueryExpression | None = None,
        cte_name: str = "",
        source_alias: str = "",
        target_alias: str = "",
        path_variable: str = "",
        relationship_variable: str = "",
        collect_nodes: bool = False,
        collect_edges: bool = False,
        edge_properties: list[str] | None = None,
        edge_filter: QueryExpression | None = None,
        edge_filter_lambda_var: str = "",
        direction: RelationshipDirection = RelationshipDirection.FORWARD,
        use_internal_union_for_bidirectional: bool = False,
        swap_source_sink: bool = False,
        # BFS Bidirectional optimization fields
        bidirectional_bfs_mode: str = "off",  # "off", "recursive", "unrolling"
        bidirectional_depth_forward: int | None = None,
        bidirectional_depth_backward: int | None = None,
        bidirectional_target_value: str | None = None,
    ) -> None:
        super().__init__()
        self.edge_types = edge_types
        self.source_node_type = source_node_type
        self.target_node_type = target_node_type
        self.min_hops = min_hops
        self.max_hops = max_hops
        self.source_id_column = source_id_column
        self.target_id_column = target_id_column
        self.start_node_filter = start_node_filter
        self.sink_node_filter = sink_node_filter
        self.barrier_filter = barrier_filter
        self.cte_name = cte_name
        self.source_alias = source_alias
        self.target_alias = target_alias
        # Path accumulation support
        self.path_variable = path_variable
        # Relationship variable for VLP (e.g., 'r' in [r*1..3])
        # In Cypher, this represents the list of relationships traversed
        self.relationship_variable = relationship_variable
        self.collect_nodes = collect_nodes
        self.collect_edges = collect_edges or bool(path_variable) or bool(relationship_variable)
        self.edge_properties = edge_properties or []

        # Predicate pushdown for early path filtering
        # See class docstring for detailed explanation of this optimization
        self.edge_filter = edge_filter
        self.edge_filter_lambda_var = edge_filter_lambda_var

        # Direction for undirected traversal support
        # FORWARD: (a)-[:TYPE*]->(b) - follow edges in their direction
        # BACKWARD: (a)<-[:TYPE*]-(b) - follow edges in reverse
        # BOTH: (a)-[:TYPE*]-(b) - follow edges in both directions (undirected)
        self.direction = direction

        # Planner decision: whether to use UNION ALL inside the CTE for bidirectional traversal.
        # This is set by the planner based on direction + EdgeAccessStrategy.
        # When True: renderer generates CTE with internal UNION ALL (forward + backward)
        # When False: renderer generates single-direction CTE
        # This moves the semantic decision out of the renderer (SoC principle).
        self.use_internal_union_for_bidirectional = use_internal_union_for_bidirectional

        # Planner decision: whether to swap source/sink columns in the CTE.
        # True for BACKWARD direction: edges are traversed in reverse
        # This moves the direction interpretation out of the renderer (SoC principle).
        self.swap_source_sink = swap_source_sink

        # BFS Bidirectional optimization
        # ===============================
        # When both source AND target have equality filters on their ID columns,
        # bidirectional BFS can enable large-scale queries that would hit row limits.
        #
        # Modes:
        # - "off": Disable bidirectional BFS (default, safest)
        # - "recursive": Use WITH RECURSIVE forward/backward CTEs
        # - "unrolling": Use unrolled CTEs (fwd0, fwd1, bwd0, bwd1)
        #
        # The optimizer sets these fields; the renderer uses them.
        self.bidirectional_bfs_mode = bidirectional_bfs_mode
        self.bidirectional_depth_forward = bidirectional_depth_forward
        self.bidirectional_depth_backward = bidirectional_depth_backward
        self.bidirectional_target_value = bidirectional_target_value

    @property
    def depth(self) -> int:
        if not self.graph_in_operators:
            return 1
        return max(op.depth for op in self.graph_in_operators) + 1

    @property
    def is_circular(self) -> bool:
        """Check if this is a circular path (source and target are the same variable)."""
        return bool(self.source_alias and self.source_alias == self.target_alias)

    def __str__(self) -> str:
        edge_str = "|".join(self.edge_types)
        hops_str = f"*{self.min_hops}..{self.max_hops}" if self.max_hops else f"*{self.min_hops}.."
        path_str = f", path={self.path_variable}" if self.path_variable else ""
        circular_str = ", circular=True" if self.is_circular else ""
        dir_str = f", direction={self.direction.name}" if self.direction != RelationshipDirection.FORWARD else ""
        return f"RecursiveTraversal({edge_str}{hops_str}{path_str}{circular_str}{dir_str})"

    def propagate_data_types_for_in_schema(self) -> None:
        """Propagate data types from upstream operators to input schema.

        RecursiveTraversal's input schema is the merged output of all input operators
        (typically the source node's DataSourceOperator).
        """
        if self.graph_in_operators:
            merged_fields: list[Field] = []
            for op in self.graph_in_operators:
                if op.output_schema:
                    merged_fields.extend(op.output_schema.fields)
            self.input_schema = Schema(merged_fields)

    def propagate_data_types_for_out_schema(self) -> None:
        """Propagate data types to output schema.

        RecursiveTraversal output includes:
        1. All fields from input (source node)
        2. Target node as EntityField
        3. Path variable if specified (AUTHORITATIVE ArrayType with structured element)

        AUTHORITATIVE SCHEMA DECLARATION
        ---------------------------------
        This method is the source of truth for the path variable's type.
        The path is declared as ARRAY<STRUCT<id: INT, ...>> where the struct
        contains at minimum the node ID field. This enables downstream components
        (ColumnResolver, Renderer) to correctly resolve expressions like:
            [n IN nodes(path) | n.id]

        The resolver MUST trust this declaration and NOT infer the type.
        The renderer MUST use this type information and NOT guess.
        """
        fields: list[Field] = []

        # Copy input fields (source node)
        if self.input_schema:
            fields.extend(self.input_schema.fields)

        # Add target node as EntityField
        if self.target_alias:
            # Use target_id_column to match what the renderer generates
            # The renderer uses the actual node ID column name (e.g., "node_id")
            # to create columns like "_gsql2rsql_other_node_id"
            target_field = EntityField(
                field_alias=self.target_alias,
                entity_name=self.target_alias,
                entity_type=EntityType.NODE,
                bound_entity_name=self.target_node_type,
                node_join_field=ValueField(
                    field_alias=f"{self.target_alias}_{self.target_id_column}",
                    field_name=f"_gsql2rsql_{self.target_alias}_{self.target_id_column}",
                    data_type=int,
                ),
                encapsulated_fields=[],
            )
            fields.append(target_field)

        # Add path variable if specified (with AUTHORITATIVE structured type)
        # Only add the node-ID path field when collect_nodes is True
        # (i.e., when nodes(path) or bare path reference is actually used)
        if self.path_variable and self.collect_nodes:
            path_field = self._create_authoritative_path_field()
            fields.append(path_field)

        # Add path_edges field when collect_edges is enabled
        # This is needed for relationships(path) function even without
        # a named relationship variable (e.g., MATCH p = ()-[*1..3]->())
        if self.path_variable and self.collect_edges:
            path_edges_field = self._create_path_edges_field()
            fields.append(path_edges_field)

        # Add relationship variable if specified (e.g., 'e' in [e*1..3])
        # The relationship variable represents the list of edges traversed
        # and maps to the path_edges column in the CTE output
        if self.relationship_variable:
            rel_var_field = self._create_authoritative_relationship_variable_field()
            fields.append(rel_var_field)

        self.output_schema = Schema(fields)

    def _create_authoritative_path_field(self) -> ValueField:
        """Create an authoritative path field with structured type.

        This method creates a ValueField for the path variable with a fully
        specified ArrayType(StructType(...)) that enables proper resolution
        of expressions like [n IN nodes(path) | n.id].

        DESIGN NOTE:
        ------------
        The path contains node IDs (not full node objects), so when we iterate
        over nodes(path), we're iterating over integers. However, since Cypher
        semantics allow n.id on path elements, we model the element as a struct
        with an 'id' field.

        For now, we use a minimal struct with just the ID field. If we need
        additional node properties in the future, we can extend this.

        TODO: If multi-label nodes are traversed, the struct should include
              only fields guaranteed to exist on all possible node types.

        Returns:
            ValueField with authoritative ArrayType(StructType) type
        """
        from gsql2rsql.planner.column_ref import compute_sql_column_name
        from gsql2rsql.planner.data_types import (
            ArrayType,
            PrimitiveType,
            StructField,
            StructType,
        )

        # Build the struct fields for path elements
        # At minimum, we guarantee the 'id' field exists
        struct_fields: list[StructField] = [
            StructField(
                name="id",
                data_type=PrimitiveType.INT,
                sql_name=compute_sql_column_name("node", "id"),
            ),
        ]

        # Create the element struct type
        # TODO: Add 'label' field if needed for multi-label traversals
        element_struct = StructType(
            name=f"PathElement_{self.path_variable}",
            fields=tuple(struct_fields),
        )

        # Create the array type
        path_type = ArrayType(element_type=element_struct)

        # Create the ValueField with authoritative type
        # The field_name uses _id suffix to match renderer output (path array of node IDs)
        return ValueField(
            field_alias=self.path_variable,
            field_name=f"_gsql2rsql_{self.path_variable}_id",
            data_type=list,  # Legacy type for backward compatibility
            structured_type=path_type,  # AUTHORITATIVE type declaration
        )

    def _create_path_edges_field(self) -> ValueField:
        """Create a field for path_edges when using relationships(path).

        When there's a path variable but no named relationship variable,
        we still need the edges column for relationships(path) function.
        This field uses a synthetic alias to avoid conflicts.

        The renderer generates the column as _gsql2rsql_{path_variable}_edges.

        Returns:
            ValueField for the path edges column
        """
        from gsql2rsql.planner.data_types import (
            ArrayType,
            PrimitiveType,
            StructField,
            StructType,
        )

        # Build the struct fields for edge elements (same as relationship variable)
        struct_fields: list[StructField] = [
            StructField(name="src", data_type=PrimitiveType.STRING, sql_name="src"),
            StructField(name="dst", data_type=PrimitiveType.STRING, sql_name="dst"),
        ]

        # Add edge properties if available
        if self.edge_properties:
            for prop in self.edge_properties:
                struct_fields.append(
                    StructField(name=prop, data_type=PrimitiveType.STRING, sql_name=prop)
                )

        element_struct = StructType(
            name=f"PathEdge_{self.path_variable}",
            fields=tuple(struct_fields),
        )
        edges_type = ArrayType(element_type=element_struct)

        # Use synthetic alias to distinguish from path_id field
        # The alias is internal and not exposed to users
        return ValueField(
            field_alias=f"_path_edges_{self.path_variable}",
            field_name=f"_gsql2rsql_{self.path_variable}_edges",
            data_type=list,
            structured_type=edges_type,
        )

    def _create_authoritative_relationship_variable_field(self) -> ValueField:
        """Create an authoritative field for the relationship variable.

        The relationship variable (e.g., 'e' in [e*1..3]) represents the list
        of edges traversed in a variable-length path. This maps to the
        path_edges column in the CTE output.

        The renderer generates this column as:
        - Internal CTE column: path_edges
        - Final aliased column: _gsql2rsql_{relationship_variable}_edges

        The struct type includes all edge properties (src, dst, and any
        additional edge attributes from the schema) so that property access
        like r.weight works after UNWIND e AS r.

        Returns:
            ValueField with authoritative ArrayType(StructType) for edges
        """
        from gsql2rsql.planner.data_types import (
            ArrayType,
            PrimitiveType,
            StructField,
            StructType,
        )

        # Build the struct fields for edge elements
        # The field names must match the NAMED_STRUCT keys generated by the renderer.
        # The renderer gets edge src/dst column names from the schema at runtime.
        # For the struct type, we use the conceptual names "src" and "dst" which
        # map to the first two fields of the NAMED_STRUCT.
        # NOTE: The actual SQL column names may differ (e.g., "source_id", "target_id")
        # but the struct field names here must match what the renderer uses as keys.
        # Since the renderer uses edge_schema.source_id_property.property_name as both
        # the key and value column name, we need to match that.
        # TODO: Pass actual edge column names from schema through the operator.
        # For now, we rely on the most common convention: "src" and "dst".
        struct_fields: list[StructField] = [
            StructField(
                name="src",
                data_type=PrimitiveType.STRING,
                sql_name="src",
            ),
            StructField(
                name="dst",
                data_type=PrimitiveType.STRING,
                sql_name="dst",
            ),
        ]

        # Add all additional edge properties from the schema
        # These are the properties the renderer includes in the NAMED_STRUCT
        if self.edge_properties:
            for prop in self.edge_properties:
                struct_fields.append(
                    StructField(
                        name=prop,
                        data_type=PrimitiveType.STRING,  # Default to string, actual type doesn't affect SQL
                        sql_name=prop,
                    )
                )

        # Create the element struct type for edges
        element_struct = StructType(
            name=f"EdgeElement_{self.relationship_variable}",
            fields=tuple(struct_fields),
        )

        # Create the array type
        edges_type = ArrayType(element_type=element_struct)

        # Create the ValueField with authoritative type
        # The field_name uses _edges suffix to match renderer output
        return ValueField(
            field_alias=self.relationship_variable,
            field_name=f"_gsql2rsql_{self.relationship_variable}_edges",
            data_type=list,  # Legacy type for backward compatibility
            structured_type=edges_type,  # AUTHORITATIVE type declaration
        )

    def introduced_symbols(self) -> set[str]:
        """Return symbols introduced by this traversal.

        RecursiveTraversal introduces:
        - target_alias (if specified)
        - path_variable (if specified)
        - relationship_variable (if specified)
        """
        introduced: set[str] = set()
        if self.target_alias:
            introduced.add(self.target_alias)
        if self.path_variable:
            introduced.add(self.path_variable)
        if self.relationship_variable:
            introduced.add(self.relationship_variable)
        return introduced
