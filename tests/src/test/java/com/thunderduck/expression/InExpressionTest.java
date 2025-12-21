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
 * Test suite for InExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("InExpression Tests")
public class InExpressionTest extends TestBase {

    private Expression lit(int value) {
        return Literal.of(value);
    }

    private Expression lit(String value) {
        return Literal.of(value);
    }

    private Expression col(String name) {
        return new UnresolvedColumn(name);
    }

    @Nested
    @DisplayName("Construction")
    class Construction {

        @Test
        @DisplayName("Creates IN expression")
        void testBasicConstruction() {
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2), lit(3))
            );

            assertThat(in.testExpr()).isInstanceOf(UnresolvedColumn.class);
            assertThat(in.values()).hasSize(3);
            assertThat(in.isNegated()).isFalse();
        }

        @Test
        @DisplayName("Creates NOT IN expression")
        void testNotInConstruction() {
            InExpression notIn = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2)),
                true  // negated
            );

            assertThat(notIn.isNegated()).isTrue();
        }

        @Test
        @DisplayName("Throws on empty values")
        void testEmptyValues() {
            assertThatThrownBy(() -> new InExpression(
                col("status"),
                Collections.emptyList()
            )).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Values list is unmodifiable")
        void testUnmodifiableValues() {
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1))
            );

            assertThatThrownBy(() -> in.values().add(lit(2)))
                .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Always returns BooleanType")
        void testBooleanReturnType() {
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(in.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Schema-aware resolution still returns Boolean")
        void testSchemaAwareResolution() {
            StructType schema = new StructType(Arrays.asList(
                new StructField("status", IntegerType.get(), true)
            ));

            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1))
            );

            DataType type = TypeInferenceEngine.resolveType(in, schema);
            assertThat(type).isInstanceOf(BooleanType.class);
        }
    }

    @Nested
    @DisplayName("Nullability")
    class Nullability {

        @Test
        @DisplayName("Not nullable when all expressions are non-nullable")
        void testNonNullable() {
            // Literals are not nullable
            InExpression in = new InExpression(
                lit(1),
                Arrays.asList(lit(2), lit(3))
            );

            assertThat(in.nullable()).isFalse();
        }

        @Test
        @DisplayName("Nullable when test expression is nullable")
        void testNullableTestExpr() {
            // Columns are nullable by default
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(in.nullable()).isTrue();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("Generates correct SQL for IN")
        void testInSql() {
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2), lit(3))
            );

            assertThat(in.toSQL()).isEqualTo("status IN (1, 2, 3)");
        }

        @Test
        @DisplayName("Generates correct SQL for NOT IN")
        void testNotInSql() {
            InExpression notIn = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2)),
                true
            );

            assertThat(notIn.toSQL()).isEqualTo("status NOT IN (1, 2)");
        }

        @Test
        @DisplayName("Generates correct SQL with string values")
        void testStringValuesSql() {
            InExpression in = new InExpression(
                col("name"),
                Arrays.asList(lit("Alice"), lit("Bob"))
            );

            assertThat(in.toSQL()).isEqualTo("name IN ('Alice', 'Bob')");
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal expressions are equal")
        void testEquality() {
            InExpression in1 = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2))
            );
            InExpression in2 = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2))
            );

            assertThat(in1).isEqualTo(in2);
            assertThat(in1.hashCode()).isEqualTo(in2.hashCode());
        }

        @Test
        @DisplayName("Different expressions are not equal")
        void testInequality() {
            InExpression in1 = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(2))
            );
            InExpression in2 = new InExpression(
                col("status"),
                Arrays.asList(lit(1), lit(3))
            );

            assertThat(in1).isNotEqualTo(in2);
        }

        @Test
        @DisplayName("IN and NOT IN are not equal")
        void testNegationInequality() {
            InExpression in = new InExpression(
                col("status"),
                Arrays.asList(lit(1)),
                false
            );
            InExpression notIn = new InExpression(
                col("status"),
                Arrays.asList(lit(1)),
                true
            );

            assertThat(in).isNotEqualTo(notIn);
        }
    }
}
