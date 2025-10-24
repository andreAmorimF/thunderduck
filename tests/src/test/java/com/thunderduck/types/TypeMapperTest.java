package com.thunderduck.types;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.test.TestDataBuilder;
import com.thunderduck.test.DifferentialAssertion;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for TypeMapper.
 *
 * <p>Tests 50+ scenarios including:
 * <ul>
 *   <li>Primitive type mappings (8 types)</li>
 *   <li>Temporal type mappings (2 types)</li>
 *   <li>Decimal precision/scale variations (10 tests)</li>
 *   <li>Array type mappings (10 tests)</li>
 *   <li>Map type mappings (10 tests)</li>
 *   <li>Nested complex types (10 tests)</li>
 *   <li>Edge cases and error handling (10 tests)</li>
 * </ul>
 */
@TestCategories.Tier1
@TestCategories.Unit
@TestCategories.TypeMapping
@DisplayName("TypeMapper Tests")
public class TypeMapperTest extends TestBase {

    // ==================== Primitive Type Mappings ====================

    @Nested
    @DisplayName("Primitive Type Mappings")
    class PrimitiveTypeMappings {

        @Test
        @DisplayName("ByteType maps to TINYINT")
        void testByteTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(ByteType.get()),
                result -> assertThat(result).isEqualTo("TINYINT")
            );
        }

        @Test
        @DisplayName("ShortType maps to SMALLINT")
        void testShortTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(ShortType.get()),
                result -> assertThat(result).isEqualTo("SMALLINT")
            );
        }

        @Test
        @DisplayName("IntegerType maps to INTEGER")
        void testIntegerTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(IntegerType.get()),
                result -> assertThat(result).isEqualTo("INTEGER")
            );
        }

        @Test
        @DisplayName("LongType maps to BIGINT")
        void testLongTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(LongType.get()),
                result -> assertThat(result).isEqualTo("BIGINT")
            );
        }

        @Test
        @DisplayName("FloatType maps to FLOAT")
        void testFloatTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(FloatType.get()),
                result -> assertThat(result).isEqualTo("FLOAT")
            );
        }

        @Test
        @DisplayName("DoubleType maps to DOUBLE")
        void testDoubleTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(DoubleType.get()),
                result -> assertThat(result).isEqualTo("DOUBLE")
            );
        }

        @Test
        @DisplayName("StringType maps to VARCHAR")
        void testStringTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(StringType.get()),
                result -> assertThat(result).isEqualTo("VARCHAR")
            );
        }

        @Test
        @DisplayName("BooleanType maps to BOOLEAN")
        void testBooleanTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(BooleanType.get()),
                result -> assertThat(result).isEqualTo("BOOLEAN")
            );
        }
    }

    // ==================== Temporal Type Mappings ====================

    @Nested
    @DisplayName("Temporal Type Mappings")
    class TemporalTypeMappings {

        @Test
        @DisplayName("DateType maps to DATE")
        void testDateTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(DateType.get()),
                result -> assertThat(result).isEqualTo("DATE")
            );
        }

        @Test
        @DisplayName("TimestampType maps to TIMESTAMP")
        void testTimestampTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(TimestampType.get()),
                result -> assertThat(result).isEqualTo("TIMESTAMP")
            );
        }
    }

    // ==================== Binary Type Mapping ====================

    @Nested
    @DisplayName("Binary Type Mapping")
    class BinaryTypeMapping {

        @Test
        @DisplayName("BinaryType maps to BLOB")
        void testBinaryTypeMapping() {
            executeScenario(
                () -> TypeMapper.toDuckDBType(BinaryType.get()),
                result -> assertThat(result).isEqualTo("BLOB")
            );
        }
    }

    // ==================== Decimal Type Mappings ====================

    @Nested
    @DisplayName("Decimal Type Mappings")
    class DecimalTypeMappings {

        @Test
        @DisplayName("DecimalType(10, 2) maps to DECIMAL(10,2)")
        void testDecimalType10_2() {
            DecimalType decimal = new DecimalType(10, 2);
            executeScenario(
                () -> TypeMapper.toDuckDBType(decimal),
                result -> assertThat(result).isEqualTo("DECIMAL(10,2)")
            );
        }

        @Test
        @DisplayName("DecimalType(18, 4) maps to DECIMAL(18,4)")
        void testDecimalType18_4() {
            DecimalType decimal = new DecimalType(18, 4);
            executeScenario(
                () -> TypeMapper.toDuckDBType(decimal),
                result -> assertThat(result).isEqualTo("DECIMAL(18,4)")
            );
        }

        @Test
        @DisplayName("DecimalType(38, 0) maps to DECIMAL(38,0)")
        void testDecimalType38_0() {
            DecimalType decimal = new DecimalType(38, 0);
            executeScenario(
                () -> TypeMapper.toDuckDBType(decimal),
                result -> assertThat(result).isEqualTo("DECIMAL(38,0)")
            );
        }

        @ParameterizedTest
        @CsvSource({
            "5, 2, 'DECIMAL(5,2)'",
            "10, 5, 'DECIMAL(10,5)'",
            "15, 3, 'DECIMAL(15,3)'",
            "20, 10, 'DECIMAL(20,10)'",
            "30, 15, 'DECIMAL(30,15)'",
            "38, 18, 'DECIMAL(38,18)'",
            "38, 38, 'DECIMAL(38,38)'"
        })
        @DisplayName("DecimalType with various precision/scale")
        void testDecimalTypeVariations(int precision, int scale, String expected) {
            DecimalType decimal = new DecimalType(precision, scale);
            String result = TypeMapper.toDuckDBType(decimal);
            assertThat(result).isEqualTo(expected);
        }
    }

    // ==================== Array Type Mappings ====================

    @Nested
    @DisplayName("Array Type Mappings")
    class ArrayTypeMappings {

        @Test
        @DisplayName("ArrayType(IntegerType) maps to INTEGER[]")
        void testIntegerArrayMapping() {
            ArrayType arrayType = new ArrayType(IntegerType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(arrayType),
                result -> assertThat(result).isEqualTo("INTEGER[]")
            );
        }

        @Test
        @DisplayName("ArrayType(StringType) maps to VARCHAR[]")
        void testStringArrayMapping() {
            ArrayType arrayType = new ArrayType(StringType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(arrayType),
                result -> assertThat(result).isEqualTo("VARCHAR[]")
            );
        }

        @Test
        @DisplayName("ArrayType(DoubleType) maps to DOUBLE[]")
        void testDoubleArrayMapping() {
            ArrayType arrayType = new ArrayType(DoubleType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(arrayType),
                result -> assertThat(result).isEqualTo("DOUBLE[]")
            );
        }

        @Test
        @DisplayName("ArrayType(DateType) maps to DATE[]")
        void testDateArrayMapping() {
            ArrayType arrayType = new ArrayType(DateType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(arrayType),
                result -> assertThat(result).isEqualTo("DATE[]")
            );
        }

        @Test
        @DisplayName("ArrayType(DecimalType(10,2)) maps to DECIMAL(10,2)[]")
        void testDecimalArrayMapping() {
            ArrayType arrayType = new ArrayType(new DecimalType(10, 2));
            executeScenario(
                () -> TypeMapper.toDuckDBType(arrayType),
                result -> assertThat(result).isEqualTo("DECIMAL(10,2)[]")
            );
        }

        @Test
        @DisplayName("Nested ArrayType(ArrayType(IntegerType)) maps to INTEGER[][]")
        void testNestedArrayMapping() {
            ArrayType innerArray = new ArrayType(IntegerType.get());
            ArrayType outerArray = new ArrayType(innerArray);
            executeScenario(
                () -> TypeMapper.toDuckDBType(outerArray),
                result -> assertThat(result).isEqualTo("INTEGER[][]")
            );
        }

        @Test
        @DisplayName("Triple nested array maps correctly")
        void testTripleNestedArrayMapping() {
            ArrayType level1 = new ArrayType(StringType.get());
            ArrayType level2 = new ArrayType(level1);
            ArrayType level3 = new ArrayType(level2);
            executeScenario(
                () -> TypeMapper.toDuckDBType(level3),
                result -> assertThat(result).isEqualTo("VARCHAR[][][]")
            );
        }

        @ParameterizedTest
        @CsvSource({
            "TINYINT, ByteType",
            "SMALLINT, ShortType",
            "BIGINT, LongType",
            "FLOAT, FloatType",
            "BOOLEAN, BooleanType"
        })
        @DisplayName("Various primitive arrays map correctly")
        void testVariousPrimitiveArrays(String expectedPrefix, String typeName) {
            DataType elementType = getTypeByName(typeName);
            ArrayType arrayType = new ArrayType(elementType);
            String result = TypeMapper.toDuckDBType(arrayType);
            assertThat(result).isEqualTo(expectedPrefix + "[]");
        }
    }

    // ==================== Map Type Mappings ====================

    @Nested
    @DisplayName("Map Type Mappings")
    class MapTypeMappings {

        @Test
        @DisplayName("MapType(StringType, IntegerType) maps to MAP(VARCHAR, INTEGER)")
        void testStringIntegerMapMapping() {
            MapType mapType = new MapType(StringType.get(), IntegerType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(mapType),
                result -> assertThat(result).isEqualTo("MAP(VARCHAR, INTEGER)")
            );
        }

        @Test
        @DisplayName("MapType(IntegerType, StringType) maps to MAP(INTEGER, VARCHAR)")
        void testIntegerStringMapMapping() {
            MapType mapType = new MapType(IntegerType.get(), StringType.get());
            executeScenario(
                () -> TypeMapper.toDuckDBType(mapType),
                result -> assertThat(result).isEqualTo("MAP(INTEGER, VARCHAR)")
            );
        }

        @Test
        @DisplayName("MapType(StringType, DecimalType(10,2)) maps correctly")
        void testStringDecimalMapMapping() {
            MapType mapType = new MapType(StringType.get(), new DecimalType(10, 2));
            executeScenario(
                () -> TypeMapper.toDuckDBType(mapType),
                result -> assertThat(result).isEqualTo("MAP(VARCHAR, DECIMAL(10,2))")
            );
        }

        @Test
        @DisplayName("MapType with ArrayType value maps correctly")
        void testMapWithArrayValueMapping() {
            MapType mapType = new MapType(StringType.get(), new ArrayType(IntegerType.get()));
            executeScenario(
                () -> TypeMapper.toDuckDBType(mapType),
                result -> assertThat(result).isEqualTo("MAP(VARCHAR, INTEGER[])")
            );
        }

        @Test
        @DisplayName("MapType with nested MapType value maps correctly")
        void testNestedMapMapping() {
            MapType innerMap = new MapType(StringType.get(), IntegerType.get());
            MapType outerMap = new MapType(StringType.get(), innerMap);
            executeScenario(
                () -> TypeMapper.toDuckDBType(outerMap),
                result -> assertThat(result).isEqualTo("MAP(VARCHAR, MAP(VARCHAR, INTEGER))")
            );
        }

        @ParameterizedTest
        @CsvSource({
            "INTEGER, BIGINT, 'MAP(INTEGER, BIGINT)'",
            "DOUBLE, FLOAT, 'MAP(DOUBLE, FLOAT)'",
            "DATE, TIMESTAMP, 'MAP(DATE, TIMESTAMP)'",
            "BOOLEAN, VARCHAR, 'MAP(BOOLEAN, VARCHAR)'"
        })
        @DisplayName("Various map type combinations")
        void testVariousMapCombinations(String keyDuckDB, String valueDuckDB, String expected) {
            DataType keyType = TypeMapper.toSparkType(keyDuckDB);
            DataType valueType = TypeMapper.toSparkType(valueDuckDB);
            MapType mapType = new MapType(keyType, valueType);
            String result = TypeMapper.toDuckDBType(mapType);
            assertThat(result).isEqualTo(expected);
        }
    }

    // ==================== Reverse Mapping Tests ====================

    @Nested
    @DisplayName("Reverse Mapping (DuckDB to Spark)")
    class ReverseMappingTests {

        @Test
        @DisplayName("TINYINT maps to ByteType")
        void testTinyIntReverseMapping() {
            executeScenario(
                () -> TypeMapper.toSparkType("TINYINT"),
                result -> assertThat(result).isInstanceOf(ByteType.class)
            );
        }

        @Test
        @DisplayName("INTEGER and INT both map to IntegerType")
        void testIntegerAliasReverseMapping() {
            DataType fromInteger = TypeMapper.toSparkType("INTEGER");
            DataType fromInt = TypeMapper.toSparkType("INT");
            assertThat(fromInteger).isInstanceOf(IntegerType.class);
            assertThat(fromInt).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("VARCHAR, TEXT, STRING all map to StringType")
        void testStringAliasReverseMapping() {
            DataType fromVarchar = TypeMapper.toSparkType("VARCHAR");
            DataType fromText = TypeMapper.toSparkType("TEXT");
            DataType fromString = TypeMapper.toSparkType("STRING");
            assertThat(fromVarchar).isInstanceOf(StringType.class);
            assertThat(fromText).isInstanceOf(StringType.class);
            assertThat(fromString).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("DECIMAL(10,2) maps to DecimalType(10,2)")
        void testDecimalReverseMapping() {
            DataType result = TypeMapper.toSparkType("DECIMAL(10,2)");
            assertThat(result).isInstanceOf(DecimalType.class);
            DecimalType decimal = (DecimalType) result;
            assertThat(decimal.precision()).isEqualTo(10);
            assertThat(decimal.scale()).isEqualTo(2);
        }

        @Test
        @DisplayName("INTEGER[] maps to ArrayType(IntegerType)")
        void testArrayReverseMapping() {
            DataType result = TypeMapper.toSparkType("INTEGER[]");
            assertThat(result).isInstanceOf(ArrayType.class);
            ArrayType arrayType = (ArrayType) result;
            assertThat(arrayType.elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("MAP(VARCHAR, INTEGER) maps to MapType correctly")
        void testMapReverseMapping() {
            DataType result = TypeMapper.toSparkType("MAP(VARCHAR, INTEGER)");
            assertThat(result).isInstanceOf(MapType.class);
            MapType mapType = (MapType) result;
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(IntegerType.class);
        }
    }

    // ==================== Type Compatibility Tests ====================

    @Nested
    @DisplayName("Type Compatibility Tests")
    class CompatibilityTests {

        @Test
        @DisplayName("IntegerType is compatible with INTEGER")
        void testIntegerCompatibility() {
            boolean result = TypeMapper.isCompatible(IntegerType.get(), "INTEGER");
            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("IntegerType is compatible with INT alias")
        void testIntegerAliasCompatibility() {
            boolean result = TypeMapper.isCompatible(IntegerType.get(), "INT");
            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("StringType is compatible with VARCHAR")
        void testStringCompatibility() {
            boolean result = TypeMapper.isCompatible(StringType.get(), "VARCHAR");
            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("StringType is compatible with TEXT alias")
        void testStringAliasCompatibility() {
            boolean result = TypeMapper.isCompatible(StringType.get(), "TEXT");
            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("FloatType is compatible with REAL alias")
        void testFloatAliasCompatibility() {
            boolean result = TypeMapper.isCompatible(FloatType.get(), "REAL");
            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("IntegerType is not compatible with VARCHAR")
        void testIncompatibleTypes() {
            boolean result = TypeMapper.isCompatible(IntegerType.get(), "VARCHAR");
            assertThat(result).isFalse();
        }
    }

    // ==================== Edge Cases and Error Handling ====================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Null DataType throws IllegalArgumentException")
        void testNullDataTypeThrows() {
            assertThatThrownBy(() -> TypeMapper.toDuckDBType(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null");
        }

        @Test
        @DisplayName("Null DuckDB type string throws IllegalArgumentException")
        void testNullDuckDBTypeThrows() {
            assertThatThrownBy(() -> TypeMapper.toSparkType(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null");
        }

        @Test
        @DisplayName("Empty DuckDB type string throws IllegalArgumentException")
        void testEmptyDuckDBTypeThrows() {
            assertThatThrownBy(() -> TypeMapper.toSparkType(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
        }

        @Test
        @DisplayName("Unsupported DuckDB type throws UnsupportedOperationException")
        void testUnsupportedDuckDBTypeThrows() {
            assertThatThrownBy(() -> TypeMapper.toSparkType("UNKNOWN_TYPE"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported DuckDB type");
        }

        @Test
        @DisplayName("Invalid DECIMAL format throws exception")
        void testInvalidDecimalFormatThrows() {
            assertThatThrownBy(() -> TypeMapper.toSparkType("DECIMAL(10)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid DECIMAL type");
        }

        @Test
        @DisplayName("Case-insensitive type parsing works")
        void testCaseInsensitiveTypeParsing() {
            DataType lower = TypeMapper.toSparkType("integer");
            DataType upper = TypeMapper.toSparkType("INTEGER");
            DataType mixed = TypeMapper.toSparkType("InTeGeR");
            
            assertThat(lower).isInstanceOf(IntegerType.class);
            assertThat(upper).isInstanceOf(IntegerType.class);
            assertThat(mixed).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Whitespace in type names is handled")
        void testWhitespaceHandling() {
            DataType result = TypeMapper.toSparkType("  INTEGER  ");
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Round-trip conversion preserves type")
        void testRoundTripConversion() {
            DataType original = new DecimalType(18, 4);
            String duckdbType = TypeMapper.toDuckDBType(original);
            DataType roundTrip = TypeMapper.toSparkType(duckdbType);
            
            assertThat(roundTrip).isInstanceOf(DecimalType.class);
            DecimalType decimal = (DecimalType) roundTrip;
            assertThat(decimal.precision()).isEqualTo(18);
            assertThat(decimal.scale()).isEqualTo(4);
        }

        @Test
        @DisplayName("Complex nested type round-trip")
        void testComplexNestedTypeRoundTrip() {
            // MAP(VARCHAR, INTEGER[])
            MapType original = new MapType(
                StringType.get(),
                new ArrayType(IntegerType.get())
            );
            
            String duckdbType = TypeMapper.toDuckDBType(original);
            DataType roundTrip = TypeMapper.toSparkType(duckdbType);
            
            assertThat(roundTrip).isInstanceOf(MapType.class);
            MapType mapType = (MapType) roundTrip;
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(ArrayType.class);
        }
    }

    // ==================== Helper Methods ====================

    private DataType getTypeByName(String typeName) {
        switch (typeName) {
            case "ByteType": return ByteType.get();
            case "ShortType": return ShortType.get();
            case "IntegerType": return IntegerType.get();
            case "LongType": return LongType.get();
            case "FloatType": return FloatType.get();
            case "DoubleType": return DoubleType.get();
            case "StringType": return StringType.get();
            case "BooleanType": return BooleanType.get();
            case "DateType": return DateType.get();
            case "TimestampType": return TimestampType.get();
            case "BinaryType": return BinaryType.get();
            default: throw new IllegalArgumentException("Unknown type: " + typeName);
        }
    }
}
