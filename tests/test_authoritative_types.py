"""Tests for authoritative type system and schema propagation.

This test file validates that:
1. Operators produce AUTHORITATIVE schema declarations (not descriptive)
2. Types are explicit and structural (ArrayType, StructType, etc.)
3. Lambda variable bindings use schema-derived types
4. The system fails fast when type information is missing

Design Philosophy Being Tested:
-------------------------------
- Operators DEFINE what exists (authoritative)
- Resolver TRUSTS operators (no inference)
- Renderer USES resolved facts (no guessing)

See docs/development/schema-propagation.md for design details.
"""

import pytest

from gsql2rsql.planner.data_types import (
    ArrayType,
    PrimitiveKind,
    PrimitiveType,
    StructField,
    StructType,
)
from gsql2rsql.planner.operators import (
    DataSourceOperator,
    RecursiveTraversalOperator,
)
from gsql2rsql.planner.schema import ValueField
from gsql2rsql.parser.ast import NodeEntity
from gsql2rsql.renderer.schema_provider import (
    SimpleSQLSchemaProvider,
    SQLTableDescriptor,
)
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def simple_schema_provider() -> SimpleSQLSchemaProvider:
    """Create a schema provider with Person and KNOWS for path traversal tests."""
    provider = SimpleSQLSchemaProvider()

    # Add Person node
    person_schema = NodeSchema(
        name="Person",
        properties=[
            EntityProperty(property_name="name", data_type=str),
            EntityProperty(property_name="age", data_type=int),
        ],
        node_id_property=EntityProperty(property_name="id", data_type=int),
    )
    provider.add_node(
        person_schema,
        SQLTableDescriptor(table_or_view_name="Person"),
    )

    # Add KNOWS relationship
    knows_schema = EdgeSchema(
        name="KNOWS",
        properties=[
            EntityProperty(property_name="since", data_type=int),
        ],
        source_node_id="Person",
        sink_node_id="Person",
        source_id_property=EntityProperty(property_name="person_id", data_type=int),
        sink_id_property=EntityProperty(property_name="friend_id", data_type=int),
    )
    provider.add_edge(
        knows_schema,
        SQLTableDescriptor(table_or_view_name="Knows"),
    )

    return provider


# =============================================================================
# Test 1: test_recursive_path_element_struct
# Validates that RecursiveTraversalOperator creates authoritative path type
# =============================================================================


class TestRecursivePathElementStruct:
    """Test that RecursiveTraversalOperator produces authoritative path schema.

    The path variable MUST have an ArrayType(StructType(...)) that enables
    downstream resolution of expressions like [n IN nodes(path) | n.id].
    """

    def test_path_field_has_structured_type(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Path field MUST have structured_type set (not just data_type=list)."""
        # Arrange: Create a recursive traversal with path variable
        rec_op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
            source_alias="a",
            target_alias="b",
            path_variable="path",
            collect_nodes=True,
        )

        # Set up input schema (simulating source node)
        source_ds = DataSourceOperator(entity=NodeEntity(alias="a", entity_name="Person"))
        source_ds.bind(simple_schema_provider)
        rec_op.add_in_operator(source_ds)

        # Act: Propagate types
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Assert: Path field has authoritative structured type
        output_schema = rec_op.get_output_scope()
        path_field = output_schema.get_field("path")

        assert path_field is not None, "Path field must exist in output schema"
        assert isinstance(path_field, ValueField), "Path must be a ValueField"
        assert path_field.structured_type is not None, (
            "Path MUST have structured_type set (authoritative declaration). "
            "Having only data_type=list is DESCRIPTIVE, not AUTHORITATIVE."
        )

    def test_path_structured_type_is_array(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Path structured_type MUST be an ArrayType."""
        # Arrange
        rec_op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
            path_variable="path",
            collect_nodes=True,
        )
        source_ds = DataSourceOperator(entity=NodeEntity(alias="a", entity_name="Person"))
        source_ds.bind(simple_schema_provider)
        rec_op.add_in_operator(source_ds)
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        path_field = rec_op.get_output_scope().get_field("path")
        assert isinstance(path_field, ValueField)

        # Assert
        assert isinstance(path_field.structured_type, ArrayType), (
            "Path type MUST be ArrayType, not just 'list'. "
            "ArrayType is authoritative; 'list' is descriptive."
        )

    def test_path_element_type_is_struct(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Path array elements MUST be StructType (not primitive)."""
        # Arrange
        rec_op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
            path_variable="path",
            collect_nodes=True,
        )
        source_ds = DataSourceOperator(entity=NodeEntity(alias="a", entity_name="Person"))
        source_ds.bind(simple_schema_provider)
        rec_op.add_in_operator(source_ds)
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        path_field = rec_op.get_output_scope().get_field("path")
        assert isinstance(path_field, ValueField)
        assert isinstance(path_field.structured_type, ArrayType)

        element_type = path_field.structured_type.element_type

        # Assert
        assert isinstance(element_type, StructType), (
            "Path element type MUST be StructType. "
            "This enables resolution of expressions like n.id in [n IN nodes(path) | n.id]"
        )

    def test_path_element_struct_has_id_field(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Path element StructType MUST have 'id' field for n.id access."""
        # Arrange
        rec_op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
            path_variable="path",
            collect_nodes=True,
        )
        source_ds = DataSourceOperator(entity=NodeEntity(alias="a", entity_name="Person"))
        source_ds.bind(simple_schema_provider)
        rec_op.add_in_operator(source_ds)
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        path_field = rec_op.get_output_scope().get_field("path")
        assert isinstance(path_field, ValueField)
        element_struct = path_field.get_element_struct()

        # Assert
        assert element_struct is not None, "get_element_struct() must return StructType"
        assert element_struct.has_field("id"), (
            "Path element struct MUST have 'id' field. "
            "This is AUTHORITATIVE: operator guarantees this field exists. "
            "Resolver and renderer TRUST this declaration."
        )

    def test_path_id_field_is_int_type(
        self, simple_schema_provider: SimpleSQLSchemaProvider
    ) -> None:
        """Path element 'id' field MUST have INT type."""
        # Arrange
        rec_op = RecursiveTraversalOperator(
            edge_types=["KNOWS"],
            source_node_type="Person",
            target_node_type="Person",
            min_hops=1,
            max_hops=3,
            path_variable="path",
            collect_nodes=True,
        )
        source_ds = DataSourceOperator(entity=NodeEntity(alias="a", entity_name="Person"))
        source_ds.bind(simple_schema_provider)
        rec_op.add_in_operator(source_ds)
        rec_op.propagate_data_types_for_in_schema()
        rec_op.propagate_data_types_for_out_schema()

        # Act
        path_field = rec_op.get_output_scope().get_field("path")
        assert isinstance(path_field, ValueField)
        element_struct = path_field.get_element_struct()
        assert element_struct is not None
        id_field = element_struct.get_field("id")

        # Assert
        assert id_field is not None
        assert isinstance(id_field.data_type, PrimitiveType)
        assert id_field.data_type.kind == PrimitiveKind.INT


# =============================================================================
# DataType Unit Tests
# =============================================================================


class TestDataTypes:
    """Unit tests for DataType classes."""

    def test_primitive_type_sql_name(self) -> None:
        """PrimitiveType returns correct SQL type name."""
        assert PrimitiveType.INT.sql_type_name() == "INT"
        assert PrimitiveType.STRING.sql_type_name() == "STRING"
        assert PrimitiveType.BOOLEAN.sql_type_name() == "BOOLEAN"

    def test_struct_type_sql_name(self) -> None:
        """StructType returns correct SQL STRUCT declaration."""
        struct = StructType(
            name="TestStruct",
            fields=(
                StructField("id", PrimitiveType.INT),
                StructField("name", PrimitiveType.STRING),
            )
        )
        assert struct.sql_type_name() == "STRUCT<id: INT, name: STRING>"

    def test_array_type_sql_name(self) -> None:
        """ArrayType returns correct SQL ARRAY declaration."""
        array = ArrayType(element_type=PrimitiveType.INT)
        assert array.sql_type_name() == "ARRAY<INT>"

    def test_nested_array_struct_sql_name(self) -> None:
        """Nested ArrayType(StructType) returns correct SQL declaration."""
        struct = StructType(
            fields=(StructField("id", PrimitiveType.INT),)
        )
        array = ArrayType(element_type=struct)
        assert array.sql_type_name() == "ARRAY<STRUCT<id: INT>>"

    def test_struct_type_get_field(self) -> None:
        """StructType.get_field() returns correct field."""
        struct = StructType(
            fields=(
                StructField("id", PrimitiveType.INT),
                StructField("name", PrimitiveType.STRING),
            )
        )
        id_field = struct.get_field("id")
        assert id_field is not None
        assert id_field.name == "id"
        assert id_field.data_type == PrimitiveType.INT

    def test_struct_type_has_field(self) -> None:
        """StructType.has_field() returns correct boolean."""
        struct = StructType(
            fields=(StructField("id", PrimitiveType.INT),)
        )
        assert struct.has_field("id") is True
        assert struct.has_field("nonexistent") is False

    def test_array_type_get_element_struct(self) -> None:
        """ArrayType.get_element_struct() returns struct if element is StructType."""
        struct = StructType(fields=(StructField("id", PrimitiveType.INT),))
        array = ArrayType(element_type=struct)

        element = array.get_element_struct()
        assert element is struct

    def test_array_type_get_element_struct_returns_none_for_primitive(self) -> None:
        """ArrayType.get_element_struct() returns None for primitive elements."""
        array = ArrayType(element_type=PrimitiveType.INT)
        assert array.get_element_struct() is None

    def test_data_type_clone(self) -> None:
        """DataType.clone() creates independent copy."""
        struct = StructType(
            name="Test",
            fields=(StructField("id", PrimitiveType.INT),)
        )
        array = ArrayType(element_type=struct)

        cloned = array.clone()
        assert cloned is not array
        assert cloned.element_type is not struct
        assert cloned.sql_type_name() == array.sql_type_name()
