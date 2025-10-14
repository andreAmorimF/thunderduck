package com.catalyst2sql.test;

import com.catalyst2sql.types.*;

/**
 * Fluent API for building test data.
 *
 * <p>Provides a convenient builder pattern for creating test schemas,
 * expressions, and logical plans in a readable, maintainable way.
 *
 * <p>Example usage:
 * <pre>
 * StructType schema = TestDataBuilder.schema()
 *     .field("id", IntegerType.get())
 *     .field("name", StringType.get())
 *     .field("amount", new DecimalType(10, 2))
 *     .build();
 * </pre>
 */
public class TestDataBuilder {

    /**
     * Creates a new schema builder.
     *
     * @return a new schema builder
     */
    public static SchemaBuilder schema() {
        return new SchemaBuilder();
    }

    /**
     * Builder for creating StructType schemas.
     */
    public static class SchemaBuilder {
        private final java.util.List<StructField> fields = new java.util.ArrayList<>();

        /**
         * Adds a non-nullable field to the schema.
         *
         * @param name the field name
         * @param dataType the field data type
         * @return this builder
         */
        public SchemaBuilder field(String name, DataType dataType) {
            return field(name, dataType, false);
        }

        /**
         * Adds a field to the schema.
         *
         * @param name the field name
         * @param dataType the field data type
         * @param nullable whether the field can be null
         * @return this builder
         */
        public SchemaBuilder field(String name, DataType dataType, boolean nullable) {
            fields.add(new StructField(name, dataType, nullable));
            return this;
        }

        /**
         * Builds the StructType.
         *
         * @return the built StructType
         */
        public StructType build() {
            return new StructType(fields.toArray(new StructField[0]));
        }
    }

    /**
     * Creates a new type builder for parameterized types.
     *
     * @return a new type builder
     */
    public static TypeBuilder type() {
        return new TypeBuilder();
    }

    /**
     * Builder for creating complex data types.
     */
    public static class TypeBuilder {

        /**
         * Creates an ArrayType.
         *
         * @param elementType the array element type
         * @return the ArrayType
         */
        public ArrayType array(DataType elementType) {
            return new ArrayType(elementType);
        }

        /**
         * Creates a MapType.
         *
         * @param keyType the map key type
         * @param valueType the map value type
         * @return the MapType
         */
        public MapType map(DataType keyType, DataType valueType) {
            return new MapType(keyType, valueType);
        }

        /**
         * Creates a DecimalType.
         *
         * @param precision the decimal precision
         * @param scale the decimal scale
         * @return the DecimalType
         */
        public DecimalType decimal(int precision, int scale) {
            return new DecimalType(precision, scale);
        }
    }

    /**
     * Creates primitive types for testing.
     */
    public static class Types {
        public static ByteType byteType() {
            return ByteType.get();
        }

        public static ShortType shortType() {
            return ShortType.get();
        }

        public static IntegerType intType() {
            return IntegerType.get();
        }

        public static LongType longType() {
            return LongType.get();
        }

        public static FloatType floatType() {
            return FloatType.get();
        }

        public static DoubleType doubleType() {
            return DoubleType.get();
        }

        public static StringType stringType() {
            return StringType.get();
        }

        public static BooleanType booleanType() {
            return BooleanType.get();
        }

        public static DateType dateType() {
            return DateType.get();
        }

        public static TimestampType timestampType() {
            return TimestampType.get();
        }

        public static BinaryType binaryType() {
            return BinaryType.get();
        }
    }

    /**
     * Creates a test scenario builder for BDD-style testing.
     *
     * @param description the scenario description
     * @return a new scenario builder
     */
    public static ScenarioBuilder scenario(String description) {
        return new ScenarioBuilder(description);
    }

    /**
     * Builder for creating test scenarios in BDD style.
     */
    public static class ScenarioBuilder {
        private final String description;
        private Object given;
        private Object when;
        private Object then;

        public ScenarioBuilder(String description) {
            this.description = description;
        }

        public ScenarioBuilder given(Object input) {
            this.given = input;
            return this;
        }

        public ScenarioBuilder when(Object action) {
            this.when = action;
            return this;
        }

        public ScenarioBuilder then(Object expected) {
            this.then = expected;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public Object getGiven() {
            return given;
        }

        public Object getWhen() {
            return when;
        }

        public Object getThen() {
            return then;
        }
    }
}
