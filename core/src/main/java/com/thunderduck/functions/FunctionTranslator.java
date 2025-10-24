package com.thunderduck.functions;

/**
 * Interface for custom function translators.
 *
 * <p>Some Spark functions require custom translation logic beyond simple name mapping.
 * Implementations of this interface handle those special cases.
 *
 * <p>Examples of functions needing custom translation:
 * <ul>
 *   <li>Functions with different argument orders</li>
 *   <li>Functions requiring syntax transformations</li>
 *   <li>Functions with conditional logic</li>
 * </ul>
 */
@FunctionalInterface
public interface FunctionTranslator {

    /**
     * Translates a Spark function call to DuckDB SQL.
     *
     * @param args the function arguments (as SQL strings)
     * @return the translated DuckDB SQL expression
     */
    String translate(String... args);
}
