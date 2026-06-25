"""Unit tests for schema propagation across all operators.

This test file implements TDD for the schema propagation initiative.
See docs/development/schema-propagation.md for design details.
"""

import pytest

from gsql2rsql.planner.operators import (
    DataSourceOperator,
    JoinOperator,
    JoinType,
    SelectionOperator,
    ProjectionOperator,
    AggregationBoundaryOperator,
    SetOperator,
    SetOperationType,
    RecursiveTraversalOperator,
    UnwindOperator,
)
from gsql2rsql.planner.schema import (
    EntityField,
    EntityType,
)
from gsql2rsql.parser.ast import (
    NodeEntity,
    RelationshipEntity,
    QueryExpressionProperty,
    QueryExpressionValue,
    QueryExpressionAggregationFunction,
)
from gsql2rsql.parser.operators import AggregationFunction
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def simple_schema_provider() -> SimpleSQLSchemaProvider:
    """Create a schema provider with Person, Company, and WORKS_AT."""
    provider = SimpleSQLSchemaProvider()

    # Add Person node
    person_schema = NodeSchema(
        name="Person",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="age", data_type=int),
            EntityProperty(property_name="city", data_type=str),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        person_schema,
        SQLTableDescriptor(table_or_view_name="Person"),
    )

    # Add Company node
    company_schema = NodeSchema(
        name="Company",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="industry", data_type=str),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        company_schema,
        SQLTableDescriptor(table_or_view_name="Company"),
    )

    # Add WORKS_AT relationship
    works_at_schema = EdgeSchema(
        name="WORKS_AT",
        properties=[
            EntityProperty(property_name="since", data_type=int),
            EntityProperty(property_name="role", data_type=str),
        ],
        source_node_id="Person",
        sink_node_id="Company",
        source_id_property=EntityProperty(property_name="person_id", data_type=int),
        sink_id_property=EntityProperty(property_name="company_id", data_type=int),
    )
    provider.add_edge(
        works_at_schema,
        SQLTableDescriptor(table_or_view_name="WorksAt"),
    )

    return provider


@pytest.fixture
def person_entity() -> NodeEntity:
    """Create a Person node entity."""
    return NodeEntity(alias="p", entity_name="Person")


@pytest.fixture
def company_entity() -> NodeEntity:
    """Create a Company node entity."""
    return NodeEntity(alias="c", entity_name="Company")


@pytest.fixture
def works_at_entity() -> RelationshipEntity:
    """Create a WORKS_AT relationship entity."""
    return RelationshipEntity(
        alias="r",
        entity_name="WORKS_AT",
        left_entity_name="Person",
        right_entity_name="Company",
    )


# ============================================================================
# Iteration 1: DataSource Scope Tests
# ============================================================================


class TestDataSourceScope:
    """Tests for DataSourceOperator schema propagation."""

    def test_scan_output_scope_has_entity(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """DataSource produces EntityField for scanned entity."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        output_scope = ds.get_output_scope()

        # Assert
        assert len(output_scope) == 1
        field = output_scope[0]
        assert isinstance(field, EntityField)
        assert field.field_alias == "p"
        assert field.entity_name == "Person"
        assert field.entity_type == EntityType.NODE

    def test_scan_entity_has_all_properties(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """EntityField.encapsulated_fields contains all properties from schema."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        output_scope = ds.get_output_scope()
        entity_field = output_scope[0]

        # Assert
        assert isinstance(entity_field, EntityField)
        property_names = {f.field_alias for f in entity_field.encapsulated_fields}
        assert "name" in property_names
        assert "age" in property_names
        assert "city" in property_names

    def test_scan_entity_has_join_field(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """EntityField.node_join_field is populated for nodes."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        output_scope = ds.get_output_scope()
        entity_field = output_scope[0]

        # Assert
        assert isinstance(entity_field, EntityField)
        assert entity_field.node_join_field is not None
        assert entity_field.node_join_field.data_type == int

    def test_scan_introduced_symbols(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """DataSource introduces exactly one symbol (entity alias)."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        introduced = ds.introduced_symbols()

        # Assert
        assert introduced == {"p"}

    def test_scan_required_symbols_empty(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """DataSource requires no input symbols (it's a start operator)."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        required = ds.required_input_symbols()

        # Assert
        assert required == set()

    def test_scan_relationship_has_source_sink_fields(
        self,
        simple_schema_provider: SimpleSQLSchemaProvider,
        works_at_entity: RelationshipEntity,
    ) -> None:
        """Relationship EntityField has source and sink join fields."""
        # Arrange
        ds = DataSourceOperator(entity=works_at_entity)
        ds.bind(simple_schema_provider)

        # Act
        output_scope = ds.get_output_scope()
        entity_field = output_scope[0]

        # Assert
        assert isinstance(entity_field, EntityField)
        assert entity_field.entity_type == EntityType.RELATIONSHIP
        assert entity_field.rel_source_join_field is not None
        assert entity_field.rel_sink_join_field is not None


# ============================================================================
# Iteration 2: Join Scope Tests (No Conflict)
# ============================================================================


class TestJoinScope:
    """Tests for JoinOperator schema propagation."""

    def test_join_merges_scopes_no_conflict(
        self,
        simple_schema_provider: SimpleSQLSchemaProvider,
        person_entity: NodeEntity,
        company_entity: NodeEntity,
    ) -> None:
        """Join merges left and right schemas."""
        # Arrange
        ds_person = DataSourceOperator(entity=person_entity)
        ds_person.bind(simple_schema_provider)

        ds_company = DataSourceOperator(entity=company_entity)
        ds_company.bind(simple_schema_provider)

        join_op = JoinOperator(join_type=JoinType.CROSS)
        join_op.set_in_operators(ds_person, ds_company)
        join_op.propagate_data_types_for_in_schema()
        join_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = join_op.get_output_scope()

        # Assert
        assert len(output_scope) == 2
        aliases = {f.field_alias for f in output_scope}
        assert aliases == {"p", "c"}

    def test_join_all_fields_available(
        self,
        simple_schema_provider: SimpleSQLSchemaProvider,
        person_entity: NodeEntity,
        company_entity: NodeEntity,
    ) -> None:
        """All fields from both inputs available in output."""
        # Arrange
        ds_person = DataSourceOperator(entity=person_entity)
        ds_person.bind(simple_schema_provider)

        ds_company = DataSourceOperator(entity=company_entity)
        ds_company.bind(simple_schema_provider)

        join_op = JoinOperator(join_type=JoinType.CROSS)
        join_op.set_in_operators(ds_person, ds_company)
        join_op.propagate_data_types_for_in_schema()
        join_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = join_op.get_output_scope()
        p_field = output_scope.get_field("p")
        c_field = output_scope.get_field("c")

        # Assert
        assert p_field is not None
        assert c_field is not None
        assert isinstance(p_field, EntityField)
        assert isinstance(c_field, EntityField)

        # Check Person properties available
        p_props = {f.field_alias for f in p_field.encapsulated_fields}
        assert "name" in p_props
        assert "age" in p_props

        # Check Company properties available
        c_props = {f.field_alias for f in c_field.encapsulated_fields}
        assert "name" in c_props
        assert "industry" in c_props

    def test_join_introduced_symbols_empty(
        self,
        simple_schema_provider: SimpleSQLSchemaProvider,
        person_entity: NodeEntity,
        company_entity: NodeEntity,
    ) -> None:
        """Join introduces no new symbols (just passes through)."""
        # Arrange
        ds_person = DataSourceOperator(entity=person_entity)
        ds_person.bind(simple_schema_provider)

        ds_company = DataSourceOperator(entity=company_entity)
        ds_company.bind(simple_schema_provider)

        join_op = JoinOperator(join_type=JoinType.CROSS)
        join_op.set_in_operators(ds_person, ds_company)

        # Act
        introduced = join_op.introduced_symbols()

        # Assert
        assert introduced == set()


# ============================================================================
# Iteration 3: Join with Conflicts (Disambiguation)
# ============================================================================


class TestJoinDisambiguation:
    """Tests for JoinOperator handling duplicate aliases."""

    def test_join_same_entity_type_both_sides(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Join of same entity type (self-join) has different aliases from Cypher."""
        # In Cypher: MATCH (p1:Person)-[:KNOWS]->(p2:Person)
        # The aliases p1 and p2 are different, so no disambiguation needed
        # Arrange
        person1 = NodeEntity(alias="p1", entity_name="Person")
        person2 = NodeEntity(alias="p2", entity_name="Person")

        ds1 = DataSourceOperator(entity=person1)
        ds1.bind(simple_schema_provider)

        ds2 = DataSourceOperator(entity=person2)
        ds2.bind(simple_schema_provider)

        join_op = JoinOperator(join_type=JoinType.CROSS)
        join_op.set_in_operators(ds1, ds2)
        join_op.propagate_data_types_for_in_schema()
        join_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = join_op.get_output_scope()

        # Assert
        assert len(output_scope) == 2
        aliases = {f.field_alias for f in output_scope}
        assert aliases == {"p1", "p2"}


# ============================================================================
# Iteration 4: Projection Scope Tests
# ============================================================================


class TestProjectionScope:
    """Tests for ProjectionOperator schema propagation."""

    def test_projection_transforms_scope(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Projection output contains only projected fields."""
        # Arrange: MATCH (p:Person) RETURN p.name AS name
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(ds)
        proj.propagate_data_types_for_in_schema()
        proj.propagate_data_types_for_out_schema()

        # Act
        output_scope = proj.get_output_scope()

        # Assert
        assert len(output_scope) == 1
        field = output_scope[0]
        assert field.field_alias == "name"
        # Original entity 'p' should NOT be in output scope
        assert output_scope.get_field("p") is None

    def test_projection_with_alias_renames(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Projection with alias creates new field name."""
        # Arrange: MATCH (p:Person) RETURN p.name AS person_name
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                (
                    "person_name",
                    QueryExpressionProperty(variable_name="p", property_name="name"),
                )
            ]
        )
        proj.set_in_operator(ds)
        proj.propagate_data_types_for_in_schema()
        proj.propagate_data_types_for_out_schema()

        # Act
        output_scope = proj.get_output_scope()

        # Assert
        assert len(output_scope) == 1
        field = output_scope[0]
        assert field.field_alias == "person_name"

    def test_projection_bare_entity_keeps_entity(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """WITH p keeps entity for downstream operations."""
        # Arrange: MATCH (p:Person) WITH p
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[("p", QueryExpressionProperty(variable_name="p", property_name=None))]
        )
        proj.set_in_operator(ds)
        proj.propagate_data_types_for_in_schema()
        proj.propagate_data_types_for_out_schema()

        # Act
        output_scope = proj.get_output_scope()

        # Assert
        assert len(output_scope) == 1
        field = output_scope[0]
        # For WITH p (not RETURN p), we should keep the EntityField
        # so downstream MATCH can use p.properties
        assert field.field_alias == "p"

    def test_projection_input_schema_populated(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Projection input schema contains upstream fields."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(ds)
        proj.propagate_data_types_for_in_schema()

        # Act
        input_scope = proj.input_schema

        # Assert
        assert len(input_scope) == 1
        assert input_scope.get_field("p") is not None

    def test_projection_introduced_symbols(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Projection introduces new aliases."""
        # Arrange: RETURN p.name AS name
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(ds)

        # Act
        introduced = proj.introduced_symbols()

        # Assert
        # 'name' is introduced (it didn't exist before)
        assert "name" in introduced

    def test_projection_required_symbols(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Projection requires symbols referenced in expressions."""
        # Arrange: RETURN p.name AS name
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(ds)

        # Act
        required = proj.required_input_symbols()

        # Assert
        assert "p" in required


# ============================================================================
# Iteration 5: Aggregation Scope Tests
# ============================================================================


class TestAggregationScope:
    """Tests for AggregationBoundaryOperator schema propagation."""

    def test_aggregation_introduces_aggregate_symbols(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Aggregation creates symbols for group keys + aggregates."""
        # Arrange: WITH p.city AS city, COUNT(*) AS cnt
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        agg = AggregationBoundaryOperator(
            group_keys=[
                ("city", QueryExpressionProperty(variable_name="p", property_name="city"))
            ],
            aggregates=[("cnt", QueryExpressionAggregationFunction(
                        aggregation_function=AggregationFunction.COUNT
                    ))],
        )
        agg.set_in_operator(ds)
        agg.propagate_data_types_for_in_schema()
        agg.propagate_data_types_for_out_schema()

        # Act
        output_scope = agg.get_output_scope()

        # Assert
        assert len(output_scope) == 2
        aliases = {f.field_alias for f in output_scope}
        assert aliases == {"city", "cnt"}

    def test_aggregation_clears_upstream_scope(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Non-projected symbols are NOT in output after aggregation."""
        # Arrange: MATCH (p:Person) WITH p.city AS city, COUNT(*) AS cnt
        # After this, 'p' should NOT be accessible
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        agg = AggregationBoundaryOperator(
            group_keys=[
                ("city", QueryExpressionProperty(variable_name="p", property_name="city"))
            ],
            aggregates=[("cnt", QueryExpressionAggregationFunction(
                        aggregation_function=AggregationFunction.COUNT
                    ))],
        )
        agg.set_in_operator(ds)
        agg.propagate_data_types_for_in_schema()
        agg.propagate_data_types_for_out_schema()

        # Act
        output_scope = agg.get_output_scope()

        # Assert
        # 'p' should NOT be in output scope (scope boundary)
        assert output_scope.get_field("p") is None
        # Only projected fields available
        aliases = {f.field_alias for f in output_scope}
        assert aliases == {"city", "cnt"}

    def test_aggregation_with_bare_entity(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """WITH p, COUNT(*) AS cnt keeps p as value (ID), not entity."""
        # Arrange: MATCH (p:Person) WITH p, COUNT(*) AS cnt
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        agg = AggregationBoundaryOperator(
            group_keys=[("p", QueryExpressionProperty(variable_name="p", property_name=None))],
            aggregates=[("cnt", QueryExpressionAggregationFunction(
                        aggregation_function=AggregationFunction.COUNT
                    ))],
        )
        agg.set_in_operator(ds)
        agg.propagate_data_types_for_in_schema()
        agg.propagate_data_types_for_out_schema()

        # Act
        output_scope = agg.get_output_scope()

        # Assert
        assert len(output_scope) == 2
        p_field = output_scope.get_field("p")
        assert p_field is not None
        # After aggregation, 'p' is a value (the ID), not the full entity
        # This is different from WITH p without aggregation


# ============================================================================
# Iteration 6: RecursiveTraversal Scope Tests
# ============================================================================


class TestRecursiveTraversalScope:
    """Tests for RecursiveTraversalOperator schema propagation."""

    def test_recursive_traversal_adds_target(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """RecursiveTraversal adds target node to scope."""
        # Arrange: MATCH (p:Person)-[:WORKS_AT*1..3]->(c:Company)
        ds_source = DataSourceOperator(entity=person_entity)
        ds_source.bind(simple_schema_provider)

        rec_op = RecursiveTraversalOperator(
            edge_types=["WORKS_AT"],
            source_node_type="Person",
            target_node_type="Company",
            min_hops=1,
            max_hops=3,
            source_alias="p",
            target_alias="c",
        )
        rec_op.add_in_operator(ds_source)

        # Propagate
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = rec_op.get_output_scope()

        # Assert
        # Should have both source (p) and target (c)
        aliases = {f.field_alias for f in output_scope}
        assert "p" in aliases
        assert "c" in aliases

    def test_recursive_traversal_path_variable(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Path variable added to scope if specified."""
        # Arrange: MATCH path = (p:Person)-[:WORKS_AT*1..3]->(c:Company)
        ds_source = DataSourceOperator(entity=person_entity)
        ds_source.bind(simple_schema_provider)

        rec_op = RecursiveTraversalOperator(
            edge_types=["WORKS_AT"],
            source_node_type="Person",
            target_node_type="Company",
            min_hops=1,
            max_hops=3,
            source_alias="p",
            target_alias="c",
            path_variable="path",
            collect_nodes=True,
        )
        rec_op.add_in_operator(ds_source)

        # Propagate
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = rec_op.get_output_scope()

        # Assert
        aliases = {f.field_alias for f in output_scope}
        assert "path" in aliases

    def test_visited_not_in_output_scope(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Internal _visited array NOT exposed in output scope."""
        # Arrange
        ds_source = DataSourceOperator(entity=person_entity)
        ds_source.bind(simple_schema_provider)

        rec_op = RecursiveTraversalOperator(
            edge_types=["WORKS_AT"],
            source_node_type="Person",
            target_node_type="Company",
            min_hops=1,
            max_hops=3,
            source_alias="p",
            target_alias="c",
        )
        rec_op.add_in_operator(ds_source)
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = rec_op.get_output_scope()

        # Assert
        aliases = {f.field_alias for f in output_scope}
        assert "_visited" not in aliases
        assert "visited" not in aliases


# ============================================================================
# Iteration 7: Subquery/CTE Scoping Tests
# ============================================================================


class TestSubqueryScopeTests:
    """Tests for scope handling with subqueries and CTEs."""

    def test_with_creates_scope_boundary(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """WITH clause creates scope boundary - only projected vars visible."""
        # Arrange: MATCH (p:Person) WITH p.name AS name RETURN name
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # WITH p.name AS name (projection)
        proj1 = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj1.set_in_operator(ds)
        proj1.propagate_data_types_for_in_schema()
        proj1.propagate_data_types_for_out_schema()

        # RETURN name
        proj2 = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="name", property_name=None))
            ]
        )
        proj2.set_in_operator(proj1)
        proj2.propagate_data_types_for_in_schema()
        proj2.propagate_data_types_for_out_schema()

        # Act
        final_scope = proj2.get_output_scope()
        intermediate_scope = proj1.get_output_scope()

        # Assert
        # After WITH p.name AS name, only 'name' is available, not 'p'
        assert intermediate_scope.get_field("p") is None
        assert intermediate_scope.get_field("name") is not None

        # Final RETURN name works
        assert final_scope.get_field("name") is not None


# ============================================================================
# Iteration 8: SetOperator, Unwind Tests
# ============================================================================


class TestSetOperatorScope:
    """Tests for SetOperator (UNION) schema propagation."""

    def test_union_output_is_left_schema(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """UNION output uses left operand schema."""
        # Arrange: MATCH (p:Person) RETURN p.name UNION MATCH (p:Person) RETURN p.name
        ds1 = DataSourceOperator(entity=person_entity)
        ds1.bind(simple_schema_provider)
        proj1 = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj1.set_in_operator(ds1)
        proj1.propagate_data_types_for_in_schema()
        proj1.propagate_data_types_for_out_schema()

        ds2 = DataSourceOperator(entity=NodeEntity(alias="p", entity_name="Person"))
        ds2.bind(simple_schema_provider)
        proj2 = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj2.set_in_operator(ds2)
        proj2.propagate_data_types_for_in_schema()
        proj2.propagate_data_types_for_out_schema()

        union_op = SetOperator(set_operation=SetOperationType.UNION)
        union_op.set_in_operators(proj1, proj2)
        union_op.propagate_data_types_for_in_schema()
        union_op.propagate_data_types_for_out_schema()

        # Act
        output_scope = union_op.get_output_scope()

        # Assert
        assert len(output_scope) == 1
        assert output_scope[0].field_alias == "name"


class TestUnwindScope:
    """Tests for UnwindOperator schema propagation."""

    def test_unwind_adds_variable(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """UNWIND adds new variable to scope."""
        # Arrange: MATCH (p:Person) UNWIND [1,2,3] AS x RETURN p, x
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        unwind = UnwindOperator(
            list_expression=QueryExpressionValue(value=[1, 2, 3], value_type=list),
            variable_name="x",
        )
        unwind.set_in_operator(ds)
        unwind.propagate_data_types_for_in_schema()
        unwind.propagate_data_types_for_out_schema()

        # Act
        output_scope = unwind.get_output_scope()

        # Assert
        aliases = {f.field_alias for f in output_scope}
        assert "p" in aliases  # Upstream preserved
        assert "x" in aliases  # New from UNWIND

    def test_unwind_preserves_upstream(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """UNWIND preserves all upstream fields."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        unwind = UnwindOperator(
            list_expression=QueryExpressionValue(value=[1, 2, 3], value_type=list),
            variable_name="x",
        )
        unwind.set_in_operator(ds)
        unwind.propagate_data_types_for_in_schema()
        unwind.propagate_data_types_for_out_schema()

        # Act
        output_scope = unwind.get_output_scope()

        # Assert
        # Original entity 'p' should still be available with all properties
        p_field = output_scope.get_field("p")
        assert p_field is not None
        assert isinstance(p_field, EntityField)


# ============================================================================
# Iteration 9: Selection Tests
# ============================================================================


class TestSelectionScope:
    """Tests for SelectionOperator schema propagation."""

    def test_selection_preserves_scope(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Selection (WHERE) does not change scope - pass through."""
        # Arrange: MATCH (p:Person) WHERE p.age > 30
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        sel = SelectionOperator(
            filter_expression=QueryExpressionProperty(variable_name="p", property_name="age")
        )
        sel.set_in_operator(ds)
        sel.propagate_data_types_for_in_schema()
        sel.propagate_data_types_for_out_schema()

        # Act
        output_scope = sel.get_output_scope()
        input_scope = ds.get_output_scope()

        # Assert
        assert len(output_scope) == len(input_scope)
        assert output_scope.get_field("p") is not None

    def test_selection_introduced_symbols_empty(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Selection introduces no new symbols."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        sel = SelectionOperator(
            filter_expression=QueryExpressionProperty(variable_name="p", property_name="age")
        )
        sel.set_in_operator(ds)

        # Act
        introduced = sel.introduced_symbols()

        # Assert
        assert introduced == set()

    def test_selection_required_symbols(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """Selection requires symbols from filter expression."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        sel = SelectionOperator(
            filter_expression=QueryExpressionProperty(variable_name="p", property_name="age")
        )
        sel.set_in_operator(ds)

        # Act
        required = sel.required_input_symbols()

        # Assert
        assert "p" in required


# ============================================================================
# End-to-End Integration Tests
# ============================================================================


class TestEndToEndScopePropagation:
    """End-to-end tests for complete query scope propagation."""

    def test_simple_match_return(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """MATCH (p:Person) RETURN p.name"""
        # Build plan manually
        ds = DataSourceOperator(entity=NodeEntity(alias="p", entity_name="Person"))
        ds.bind(simple_schema_provider)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(ds)

        # Propagate
        ds.propagate_data_types_for_out_schema()
        proj.propagate_data_types_for_in_schema()
        proj.propagate_data_types_for_out_schema()

        # Assert final scope
        final_scope = proj.get_output_scope()
        assert len(final_scope) == 1
        assert final_scope[0].field_alias == "name"

    def test_match_where_return(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """MATCH (p:Person) WHERE p.age > 30 RETURN p.name"""
        # Build plan
        ds = DataSourceOperator(entity=NodeEntity(alias="p", entity_name="Person"))
        ds.bind(simple_schema_provider)

        sel = SelectionOperator(
            filter_expression=QueryExpressionProperty(variable_name="p", property_name="age")
        )
        sel.set_in_operator(ds)

        proj = ProjectionOperator(
            projections=[
                ("name", QueryExpressionProperty(variable_name="p", property_name="name"))
            ]
        )
        proj.set_in_operator(sel)

        # Propagate through chain
        ds.propagate_data_types_for_out_schema()
        sel.propagate_data_types_for_in_schema()
        sel.propagate_data_types_for_out_schema()
        proj.propagate_data_types_for_in_schema()
        proj.propagate_data_types_for_out_schema()

        # Assert
        assert sel.get_output_scope().get_field("p") is not None
        assert proj.get_output_scope().get_field("name") is not None
        assert proj.get_output_scope().get_field("p") is None


# ============================================================================
# Debug Helper Tests
# ============================================================================


class TestDebugHelpers:
    """Tests for debugging helper methods."""

    def test_dump_scope_readable(
        self, simple_schema_provider: SimpleSQLSchemaProvider, person_entity: NodeEntity
    ) -> None:
        """dump_scope() returns human-readable scope info."""
        # Arrange
        ds = DataSourceOperator(entity=person_entity)
        ds.bind(simple_schema_provider)

        # Act
        dump = ds.dump_scope()

        # Assert
        assert "p" in dump
        assert "Person" in dump
        assert "name" in dump or "age" in dump  # Properties mentioned
