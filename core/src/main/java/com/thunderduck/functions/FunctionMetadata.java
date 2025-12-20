package com.thunderduck.functions;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;

import java.util.List;
import java.util.Objects;

/**
 * Metadata for a function including translation and type inference information.
 *
 * <p>This provides a single source of truth for:
 * <ul>
 *   <li>Spark function name to DuckDB function name mapping</li>
 *   <li>Custom translation logic (if direct mapping is not sufficient)</li>
 *   <li>Return type resolution based on argument types</li>
 *   <li>Nullable inference based on argument nullability</li>
 * </ul>
 *
 * <p>Use the Builder pattern to construct instances:
 * <pre>
 * FunctionMetadata.builder("greatest")
 *     .duckdbName("greatest")
 *     .returnType(args -> args.isEmpty() ? StringType.get() : args.get(0).dataType())
 *     .nullable(args -> args.stream().allMatch(Expression::nullable))
 *     .build();
 * </pre>
 */
public class FunctionMetadata {

    private final String sparkName;
    private final String duckdbName;
    private final FunctionTranslator translator;
    private final ReturnTypeResolver returnTypeResolver;
    private final NullableResolver nullableResolver;

    private FunctionMetadata(Builder builder) {
        this.sparkName = Objects.requireNonNull(builder.sparkName, "sparkName must not be null");
        this.duckdbName = builder.duckdbName;
        this.translator = builder.translator;
        this.returnTypeResolver = builder.returnTypeResolver;
        this.nullableResolver = builder.nullableResolver;
    }

    /**
     * Returns the Spark function name.
     */
    public String sparkName() {
        return sparkName;
    }

    /**
     * Returns the DuckDB function name (null if custom translator is used).
     */
    public String duckdbName() {
        return duckdbName;
    }

    /**
     * Returns true if this function has a custom translator.
     */
    public boolean hasCustomTranslator() {
        return translator != null;
    }

    /**
     * Returns the custom translator, or null if direct mapping.
     */
    public FunctionTranslator translator() {
        return translator;
    }

    /**
     * Resolves the return type based on arguments.
     *
     * @param args the function arguments
     * @return the resolved return type
     */
    public DataType resolveReturnType(List<Expression> args) {
        if (returnTypeResolver != null) {
            return returnTypeResolver.resolve(args);
        }
        return StringType.get(); // Default fallback
    }

    /**
     * Resolves whether the function result is nullable based on arguments.
     *
     * @param args the function arguments
     * @return true if the result is nullable
     */
    public boolean resolveNullable(List<Expression> args) {
        if (nullableResolver != null) {
            return nullableResolver.resolve(args);
        }
        // Default: nullable if any argument is nullable
        return args.isEmpty() || args.stream().anyMatch(Expression::nullable);
    }

    /**
     * Creates a builder for FunctionMetadata.
     *
     * @param sparkName the Spark function name
     * @return a new builder
     */
    public static Builder builder(String sparkName) {
        return new Builder(sparkName);
    }

    // ==================== Resolver Interfaces ====================

    /**
     * Resolves the return type of a function based on its arguments.
     */
    @FunctionalInterface
    public interface ReturnTypeResolver {
        /**
         * Resolves the return type.
         *
         * @param args the function arguments
         * @return the return type
         */
        DataType resolve(List<Expression> args);
    }

    /**
     * Resolves whether a function result is nullable based on its arguments.
     */
    @FunctionalInterface
    public interface NullableResolver {
        /**
         * Resolves nullability.
         *
         * @param args the function arguments
         * @return true if the result is nullable
         */
        boolean resolve(List<Expression> args);
    }

    // ==================== Builder ====================

    /**
     * Builder for FunctionMetadata.
     */
    public static class Builder {
        private final String sparkName;
        private String duckdbName;
        private FunctionTranslator translator;
        private ReturnTypeResolver returnTypeResolver;
        private NullableResolver nullableResolver;

        private Builder(String sparkName) {
            this.sparkName = sparkName;
        }

        /**
         * Sets the DuckDB function name for direct mapping.
         */
        public Builder duckdbName(String name) {
            this.duckdbName = name;
            return this;
        }

        /**
         * Sets a custom translator for functions that need argument transformation.
         */
        public Builder translator(FunctionTranslator translator) {
            this.translator = translator;
            return this;
        }

        /**
         * Sets the return type resolver.
         */
        public Builder returnType(ReturnTypeResolver resolver) {
            this.returnTypeResolver = resolver;
            return this;
        }

        /**
         * Sets the nullable resolver.
         */
        public Builder nullable(NullableResolver resolver) {
            this.nullableResolver = resolver;
            return this;
        }

        /**
         * Builds the FunctionMetadata.
         *
         * @return the built metadata
         * @throws IllegalStateException if neither duckdbName nor translator is set
         */
        public FunctionMetadata build() {
            if (duckdbName == null && translator == null) {
                throw new IllegalStateException(
                    "FunctionMetadata must have either duckdbName or translator");
            }
            return new FunctionMetadata(this);
        }
    }

    // ==================== Common Resolvers ====================

    /**
     * Returns a resolver that preserves the first argument's type.
     */
    public static ReturnTypeResolver firstArgTypePreserving() {
        return args -> args.isEmpty() ? StringType.get() : args.get(0).dataType();
    }

    /**
     * Returns a resolver that returns a constant type.
     */
    public static ReturnTypeResolver constantType(DataType type) {
        return args -> type;
    }

    /**
     * Returns a resolver for null-coalescing functions.
     * Result is non-null if ANY argument is non-null.
     */
    public static NullableResolver nullCoalescing() {
        return args -> args.isEmpty() || args.stream().allMatch(Expression::nullable);
    }

    /**
     * Returns a resolver where result is nullable if ANY argument is nullable.
     */
    public static NullableResolver anyArgNullable() {
        return args -> args.isEmpty() || args.stream().anyMatch(Expression::nullable);
    }

    /**
     * Returns a resolver where result is always non-null.
     */
    public static NullableResolver alwaysNonNull() {
        return args -> false;
    }
}
