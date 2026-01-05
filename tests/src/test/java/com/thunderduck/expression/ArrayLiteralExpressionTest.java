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
 * Test suite for ArrayLiteralExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("ArrayLiteralExpression Tests")
public class ArrayLiteralExpressionTest extends TestBase {

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
        @DisplayName("Creates array with elements")
        void testBasicConstruction() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2), lit(3))
            );

            assertThat(array.size()).isEqualTo(3);
            assertThat(array.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("Creates empty array")
        void testEmptyArray() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(Collections.emptyList());

            assertThat(array.size()).isEqualTo(0);
            assertThat(array.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Elements list is unmodifiable")
        void testUnmodifiableElements() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );

            assertThatThrownBy(() -> array.elements().add(lit(3)))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Returns ArrayType with IntegerType elements")
        void testIntegerElements() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2), lit(3))
            );

            DataType type = array.dataType();
            assertThat(type).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) type).elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Returns ArrayType with StringType elements")
        void testStringElements() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit("a"), lit("b"), lit("c"))
            );

            DataType type = array.dataType();
            assertThat(type).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) type).elementType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Empty array returns ArrayType with StringType")
        void testEmptyArrayType() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(Collections.emptyList());

            DataType type = array.dataType();
            assertThat(type).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) type).elementType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Schema-aware type resolution")
        void testSchemaAwareResolution() {
            StructType schema = new StructType(Arrays.asList(
                new StructField("decimal_col", new DecimalType(10, 2), true)
            ));

            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(new UnresolvedColumn("decimal_col"), lit(100))
            );

            DataType type = TypeInferenceEngine.resolveType(array, schema);
            assertThat(type).isInstanceOf(ArrayType.class);
            ArrayType arrayType = (ArrayType) type;
            // Should unify Decimal(10,2) with Integer -> Decimal type
            assertThat(arrayType.elementType()).isInstanceOf(DecimalType.class);
        }
    }

    @Nested
    @DisplayName("Nullability")
    class Nullability {

        @Test
        @DisplayName("Array literal is not nullable")
        void testArrayNotNullable() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(array.nullable()).isFalse();
        }

        @Test
        @DisplayName("Contains nullable elements detection")
        void testContainsNullableElements() {
            // Literals are not nullable
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(array.containsNullableElements()).isFalse();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("Generates correct SQL for integer array")
        void testIntegerArraySql() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2), lit(3))
            );

            assertThat(array.toSQL()).isEqualTo("[1, 2, 3]");
        }

        @Test
        @DisplayName("Generates correct SQL for string array")
        void testStringArraySql() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(
                Arrays.asList(lit("a"), lit("b"))
            );

            assertThat(array.toSQL()).isEqualTo("['a', 'b']");
        }

        @Test
        @DisplayName("Generates correct SQL for empty array")
        void testEmptyArraySql() {
            ArrayLiteralExpression array = new ArrayLiteralExpression(Collections.emptyList());

            assertThat(array.toSQL()).isEqualTo("[]");
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal arrays are equal")
        void testEquality() {
            ArrayLiteralExpression array1 = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );
            ArrayLiteralExpression array2 = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(array1).isEqualTo(array2);
            assertThat(array1.hashCode()).isEqualTo(array2.hashCode());
        }

        @Test
        @DisplayName("Different arrays are not equal")
        void testInequality() {
            ArrayLiteralExpression array1 = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(2))
            );
            ArrayLiteralExpression array2 = new ArrayLiteralExpression(
                Arrays.asList(lit(1), lit(3))
            );

            assertThat(array1).isNotEqualTo(array2);
        }
    }
}
