package com.thunderduck.test;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Objects;

/**
 * AssertJ custom assertions for differential testing between Spark and DuckDB.
 *
 * <p>Provides specialized assertion methods for comparing:
 * <ul>
 *   <li>SQL query results</li>
 *   <li>Type mappings</li>
 *   <li>Expression translations</li>
 *   <li>Schema definitions</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * DifferentialAssertion.assertThat(duckdbResult)
 *     .isEquivalentTo(sparkResult)
 *     .withTolerance(0.0001);
 * </pre>
 */
public class DifferentialAssertion {

    /**
     * Creates an assertion for SQL strings.
     *
     * @param actual the actual SQL string
     * @return a SQLAssertion instance
     */
    public static SQLAssertion assertThatSQL(String actual) {
        return new SQLAssertion(actual);
    }

    /**
     * Creates an assertion for type mappings.
     *
     * @param actualType the actual type string
     * @return a TypeAssertion instance
     */
    public static TypeAssertion assertThatType(String actualType) {
        return new TypeAssertion(actualType);
    }

    /**
     * Creates an assertion for numeric results with tolerance.
     *
     * @param actual the actual numeric value
     * @return a NumericAssertion instance
     */
    public static NumericAssertion assertThatNumeric(Number actual) {
        return new NumericAssertion(actual);
    }

    /**
     * Asserts that two objects are deeply equal, handling nulls gracefully.
     *
     * @param actual the actual value
     * @param expected the expected value
     */
    public static void assertEquivalent(Object actual, Object expected) {
        assertEquivalent(actual, expected, "Values should be equivalent");
    }

    /**
     * Asserts that two objects are deeply equal with a custom message.
     *
     * @param actual the actual value
     * @param expected the expected value
     * @param message the assertion message
     */
    public static void assertEquivalent(Object actual, Object expected, String message) {
        if (actual == null && expected == null) {
            return;
        }
        if (actual == null || expected == null) {
            throw new AssertionError(message + ": expected " + expected + " but was " + actual);
        }
        if (!Objects.equals(actual, expected)) {
            throw new AssertionError(message + ": expected " + expected + " but was " + actual);
        }
    }

    /**
     * Custom assertion for SQL strings.
     */
    public static class SQLAssertion extends AbstractAssert<SQLAssertion, String> {

        public SQLAssertion(String actual) {
            super(actual, SQLAssertion.class);
        }

        /**
         * Asserts that the SQL is semantically equivalent to the expected SQL.
         * Ignores whitespace differences and case.
         *
         * @param expected the expected SQL
         * @return this assertion
         */
        public SQLAssertion isSemanticallyEquivalentTo(String expected) {
            isNotNull();
            String normalizedActual = normalizeSQL(actual);
            String normalizedExpected = normalizeSQL(expected);

            if (!normalizedActual.equals(normalizedExpected)) {
                failWithMessage("Expected SQL to be semantically equivalent to:\n%s\nbut was:\n%s",
                    expected, actual);
            }
            return this;
        }

        /**
         * Asserts that the SQL contains a specific clause or pattern.
         *
         * @param clause the clause to check for
         * @return this assertion
         */
        public SQLAssertion containsClause(String clause) {
            isNotNull();
            String normalizedActual = actual.toUpperCase().replaceAll("\\s+", " ");
            String normalizedClause = clause.toUpperCase().replaceAll("\\s+", " ");

            if (!normalizedActual.contains(normalizedClause)) {
                failWithMessage("Expected SQL to contain clause:\n%s\nbut was:\n%s",
                    clause, actual);
            }
            return this;
        }

        /**
         * Asserts that the SQL is valid (no syntax errors).
         *
         * @return this assertion
         */
        public SQLAssertion isValidSQL() {
            isNotNull();
            // Basic validation: check for balanced parentheses
            int depth = 0;
            for (char c : actual.toCharArray()) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                if (depth < 0) {
                    failWithMessage("SQL has unbalanced parentheses: %s", actual);
                }
            }
            if (depth != 0) {
                failWithMessage("SQL has unbalanced parentheses: %s", actual);
            }
            return this;
        }

        private String normalizeSQL(String sql) {
            return sql.toUpperCase()
                      .replaceAll("\\s+", " ")
                      .replaceAll("\\( ", "(")
                      .replaceAll(" \\)", ")")
                      .trim();
        }
    }

    /**
     * Custom assertion for type strings.
     */
    public static class TypeAssertion extends AbstractAssert<TypeAssertion, String> {

        public TypeAssertion(String actual) {
            super(actual, TypeAssertion.class);
        }

        /**
         * Asserts that the type is compatible with the expected type.
         * Handles type aliases (e.g., INTEGER vs INT).
         *
         * @param expected the expected type
         * @return this assertion
         */
        public TypeAssertion isCompatibleWith(String expected) {
            isNotNull();
            String normalizedActual = normalizeType(actual);
            String normalizedExpected = normalizeType(expected);

            if (!normalizedActual.equals(normalizedExpected)) {
                failWithMessage("Expected type to be compatible with:\n%s\nbut was:\n%s",
                    expected, actual);
            }
            return this;
        }

        /**
         * Asserts that the type is a primitive type.
         *
         * @return this assertion
         */
        public TypeAssertion isPrimitive() {
            isNotNull();
            String normalized = actual.toUpperCase();
            boolean isPrimitive = normalized.matches(
                "TINYINT|SMALLINT|INTEGER|INT|BIGINT|FLOAT|REAL|DOUBLE|DOUBLE PRECISION|" +
                "VARCHAR|TEXT|STRING|BOOLEAN|BOOL|DATE|TIMESTAMP|TIMESTAMP WITHOUT TIME ZONE|BLOB|BYTEA"
            );

            if (!isPrimitive) {
                failWithMessage("Expected type to be primitive but was: %s", actual);
            }
            return this;
        }

        /**
         * Asserts that the type is a complex type (array, map, struct).
         *
         * @return this assertion
         */
        public TypeAssertion isComplex() {
            isNotNull();
            String normalized = actual.toUpperCase();
            boolean isComplex = normalized.contains("[]") ||
                                normalized.startsWith("MAP(") ||
                                normalized.startsWith("STRUCT(");

            if (!isComplex) {
                failWithMessage("Expected type to be complex but was: %s", actual);
            }
            return this;
        }

        private String normalizeType(String type) {
            return type.toUpperCase()
                       .replace("INT", "INTEGER")
                       .replace("REAL", "FLOAT")
                       .replace("TEXT", "VARCHAR")
                       .replace("STRING", "VARCHAR")
                       .replace("BOOL", "BOOLEAN")
                       .replace("BYTEA", "BLOB")
                       .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")
                       .replace("DOUBLE PRECISION", "DOUBLE")
                       .replaceAll("\\s+", " ")
                       .trim();
        }
    }

    /**
     * Custom assertion for numeric values with tolerance.
     */
    public static class NumericAssertion extends AbstractAssert<NumericAssertion, Number> {

        public NumericAssertion(Number actual) {
            super(actual, NumericAssertion.class);
        }

        /**
         * Asserts that the numeric value is close to the expected value within a tolerance.
         *
         * @param expected the expected value
         * @param tolerance the tolerance
         * @return this assertion
         */
        public NumericAssertion isCloseTo(Number expected, double tolerance) {
            isNotNull();
            double actualDouble = actual.doubleValue();
            double expectedDouble = expected.doubleValue();
            double diff = Math.abs(actualDouble - expectedDouble);

            if (diff > tolerance) {
                failWithMessage("Expected value to be close to:\n%s\nwithin tolerance %s\nbut was:\n%s (diff: %s)",
                    expected, tolerance, actual, diff);
            }
            return this;
        }

        /**
         * Asserts that the numeric value is positive.
         *
         * @return this assertion
         */
        public NumericAssertion isPositive() {
            isNotNull();
            if (actual.doubleValue() <= 0) {
                failWithMessage("Expected value to be positive but was: %s", actual);
            }
            return this;
        }

        /**
         * Asserts that the numeric value is negative.
         *
         * @return this assertion
         */
        public NumericAssertion isNegative() {
            isNotNull();
            if (actual.doubleValue() >= 0) {
                failWithMessage("Expected value to be negative but was: %s", actual);
            }
            return this;
        }

        /**
         * Asserts that the numeric value is within a range.
         *
         * @param min the minimum value (inclusive)
         * @param max the maximum value (inclusive)
         * @return this assertion
         */
        public NumericAssertion isBetween(Number min, Number max) {
            isNotNull();
            double actualDouble = actual.doubleValue();
            double minDouble = min.doubleValue();
            double maxDouble = max.doubleValue();

            if (actualDouble < minDouble || actualDouble > maxDouble) {
                failWithMessage("Expected value to be between %s and %s but was: %s",
                    min, max, actual);
            }
            return this;
        }
    }
}
