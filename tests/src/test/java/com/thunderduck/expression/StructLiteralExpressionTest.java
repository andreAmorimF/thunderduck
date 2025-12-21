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
 * Test suite for StructLiteralExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("StructLiteralExpression Tests")
public class StructLiteralExpressionTest extends TestBase {

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
        @DisplayName("Creates struct with fields")
        void testBasicConstruction() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("name", "age"),
                Arrays.asList(lit("John"), lit(30))
            );

            assertThat(struct.size()).isEqualTo(2);
            assertThat(struct.fieldNames()).containsExactly("name", "age");
        }

        @Test
        @DisplayName("Throws on mismatched field names and values")
        void testMismatchedSizes() {
            assertThatThrownBy(() -> new StructLiteralExpression(
                Arrays.asList("name", "age"),
                Arrays.asList(lit("John"))  // Only one value
            )).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Throws on empty struct")
        void testEmptyStruct() {
            assertThatThrownBy(() -> new StructLiteralExpression(
                Collections.emptyList(),
                Collections.emptyList()
            )).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Lists are unmodifiable")
        void testUnmodifiableLists() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("John"))
            );

            assertThatThrownBy(() -> struct.fieldNames().add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> struct.fieldValues().add(lit(1)))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Returns StructType with inferred field types")
        void testBasicTypeInference() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("name", "age"),
                Arrays.asList(lit("John"), lit(30))
            );

            DataType type = struct.dataType();
            assertThat(type).isInstanceOf(StructType.class);
            StructType structType = (StructType) type;

            assertThat(structType.size()).isEqualTo(2);
            assertThat(structType.fieldAt(0).name()).isEqualTo("name");
            assertThat(structType.fieldAt(0).dataType()).isInstanceOf(StringType.class);
            assertThat(structType.fieldAt(1).name()).isEqualTo("age");
            assertThat(structType.fieldAt(1).dataType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Schema-aware type resolution")
        void testSchemaAwareResolution() {
            StructType schema = new StructType(Arrays.asList(
                new StructField("decimal_col", new DecimalType(10, 2), true)
            ));

            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("amount"),
                Arrays.asList(new UnresolvedColumn("decimal_col"))
            );

            DataType type = TypeInferenceEngine.resolveType(struct, schema);
            assertThat(type).isInstanceOf(StructType.class);
            StructType structType = (StructType) type;
            assertThat(structType.fieldAt(0).dataType()).isInstanceOf(DecimalType.class);
        }
    }

    @Nested
    @DisplayName("Nullability")
    class Nullability {

        @Test
        @DisplayName("Struct literal is not nullable")
        void testStructNotNullable() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("John"))
            );

            assertThat(struct.nullable()).isFalse();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("Generates correct SQL for struct")
        void testStructSql() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("name", "age"),
                Arrays.asList(lit("John"), lit(30))
            );

            assertThat(struct.toSQL()).isEqualTo("{'name': 'John', 'age': 30}");
        }

        @Test
        @DisplayName("Escapes single quotes in field names")
        void testQuoteEscaping() {
            StructLiteralExpression struct = new StructLiteralExpression(
                Arrays.asList("field'name"),
                Arrays.asList(lit(1))
            );

            assertThat(struct.toSQL()).isEqualTo("{'field''name': 1}");
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal structs are equal")
        void testEquality() {
            StructLiteralExpression struct1 = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("John"))
            );
            StructLiteralExpression struct2 = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("John"))
            );

            assertThat(struct1).isEqualTo(struct2);
            assertThat(struct1.hashCode()).isEqualTo(struct2.hashCode());
        }

        @Test
        @DisplayName("Different structs are not equal")
        void testInequality() {
            StructLiteralExpression struct1 = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("John"))
            );
            StructLiteralExpression struct2 = new StructLiteralExpression(
                Arrays.asList("name"),
                Arrays.asList(lit("Jane"))
            );

            assertThat(struct1).isNotEqualTo(struct2);
        }
    }
}
