package com.thunderduck.expression;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for MapLiteralExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("MapLiteralExpression Tests")
public class MapLiteralExpressionTest extends TestBase {

    private Expression lit(int value) {
        return Literal.of(value);
    }

    private Expression lit(String value) {
        return Literal.of(value);
    }

    @Nested
    @DisplayName("Construction")
    class Construction {

        @Test
        @DisplayName("Creates map with entries")
        void testBasicConstruction() {
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a"), lit("b")),
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(map.size()).isEqualTo(2);
            assertThat(map.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Creates empty map")
        void testEmptyMap() {
            MapLiteralExpression map = new MapLiteralExpression(
                Collections.emptyList(),
                Collections.emptyList()
            );

            assertThat(map.size()).isEqualTo(0);
            assertThat(map.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Throws on mismatched key/value sizes")
        void testMismatchedSizes() {
            assertThatThrownBy(() -> new MapLiteralExpression(
                Arrays.asList(lit("a"), lit("b")),
                Arrays.asList(lit(1))  // Only one value
            )).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Keys and values lists are unmodifiable")
        void testUnmodifiableLists() {
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );

            assertThatThrownBy(() -> map.keys().add(lit("b")))
                .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> map.values().add(lit(2)))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Returns MapType with inferred key/value types")
        void testBasicTypeInference() {
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a"), lit("b")),
                Arrays.asList(lit(1), lit(2))
            );

            DataType type = map.dataType();
            assertThat(type).isInstanceOf(MapType.class);
            MapType mapType = (MapType) type;
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Empty map returns MapType with StringType defaults")
        void testEmptyMapType() {
            MapLiteralExpression map = new MapLiteralExpression(
                Collections.emptyList(),
                Collections.emptyList()
            );

            DataType type = map.dataType();
            assertThat(type).isInstanceOf(MapType.class);
            MapType mapType = (MapType) type;
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Schema-aware type resolution")
        void testSchemaAwareResolution() {
            StructType schema = new StructType(Arrays.asList(
                new StructField("decimal_col", new DecimalType(10, 2), true)
            ));

            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("key1")),
                Arrays.asList(new UnresolvedColumn("decimal_col"))
            );

            DataType type = TypeInferenceEngine.resolveType(map, schema);
            assertThat(type).isInstanceOf(MapType.class);
            MapType mapType = (MapType) type;
            assertThat(mapType.valueType()).isInstanceOf(DecimalType.class);
        }
    }

    @Nested
    @DisplayName("Nullability")
    class Nullability {

        @Test
        @DisplayName("Map literal is not nullable")
        void testMapNotNullable() {
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );

            assertThat(map.nullable()).isFalse();
        }

        @Test
        @DisplayName("Contains nullable values detection")
        void testValueContainsNull() {
            // Literals are not nullable
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );

            assertThat(map.valueContainsNull()).isFalse();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("Generates correct SQL for map")
        void testMapSql() {
            MapLiteralExpression map = new MapLiteralExpression(
                Arrays.asList(lit("a"), lit("b")),
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(map.toSQL()).isEqualTo("MAP(['a', 'b'], [1, 2])");
        }

        @Test
        @DisplayName("Generates correct SQL for empty map")
        void testEmptyMapSql() {
            MapLiteralExpression map = new MapLiteralExpression(
                Collections.emptyList(),
                Collections.emptyList()
            );

            assertThat(map.toSQL()).isEqualTo("MAP([], [])");
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal maps are equal")
        void testEquality() {
            MapLiteralExpression map1 = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );
            MapLiteralExpression map2 = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );

            assertThat(map1).isEqualTo(map2);
            assertThat(map1.hashCode()).isEqualTo(map2.hashCode());
        }

        @Test
        @DisplayName("Different maps are not equal")
        void testInequality() {
            MapLiteralExpression map1 = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(1))
            );
            MapLiteralExpression map2 = new MapLiteralExpression(
                Arrays.asList(lit("a")),
                Arrays.asList(lit(2))
            );

            assertThat(map1).isNotEqualTo(map2);
        }
    }
}
