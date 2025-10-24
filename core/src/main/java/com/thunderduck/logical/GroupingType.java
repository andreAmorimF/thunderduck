package com.thunderduck.logical;

/**
 * Enum representing different types of grouping operations in SQL.
 *
 * <p>This enum defines the various multi-dimensional aggregation strategies
 * supported by the system:
 * <ul>
 *   <li>{@link #SIMPLE} - Standard GROUP BY with explicit grouping columns</li>
 *   <li>{@link #ROLLUP} - Hierarchical grouping generating subtotals</li>
 *   <li>{@link #CUBE} - Multi-dimensional grouping generating all combinations</li>
 *   <li>{@link #GROUPING_SETS} - Explicit specification of grouping sets</li>
 * </ul>
 *
 * <p><b>SQL Examples:</b>
 * <pre>
 * -- SIMPLE (default)
 * SELECT year, month, SUM(amount)
 * FROM sales
 * GROUP BY year, month
 *
 * -- ROLLUP (hierarchical subtotals)
 * SELECT year, month, SUM(amount)
 * FROM sales
 * GROUP BY ROLLUP(year, month)
 * -- Generates: (year, month), (year), ()
 *
 * -- CUBE (all combinations)
 * SELECT region, product, SUM(amount)
 * FROM sales
 * GROUP BY CUBE(region, product)
 * -- Generates: (region, product), (region), (product), ()
 *
 * -- GROUPING SETS (explicit sets)
 * SELECT region, product, SUM(amount)
 * FROM sales
 * GROUP BY GROUPING SETS((region, product), (region), ())
 * </pre>
 *
 * @see GroupingSets
 * @see Aggregate
 * @since 1.0
 */
public enum GroupingType {

    /**
     * Standard GROUP BY with explicit grouping columns.
     *
     * <p>This is the default grouping type where rows are grouped exactly
     * by the specified columns without any additional grouping sets.
     *
     * <p><b>Example:</b>
     * <pre>
     * SELECT category, SUM(amount) FROM sales GROUP BY category
     * </pre>
     */
    SIMPLE,

    /**
     * Hierarchical grouping that generates subtotals at each level.
     *
     * <p>ROLLUP generates N+1 grouping sets for N grouping columns,
     * representing a hierarchy from the most detailed to the grand total.
     *
     * <p><b>Example:</b>
     * <pre>
     * GROUP BY ROLLUP(year, quarter, month)
     * -- Generates 4 grouping sets:
     * -- 1. (year, quarter, month) - most detailed
     * -- 2. (year, quarter)        - quarterly subtotal
     * -- 3. (year)                 - yearly subtotal
     * -- 4. ()                     - grand total
     * </pre>
     */
    ROLLUP,

    /**
     * Multi-dimensional grouping that generates all possible combinations.
     *
     * <p>CUBE generates 2^N grouping sets for N grouping columns,
     * representing all possible combinations of grouping dimensions.
     *
     * <p><b>Warning:</b> Due to exponential growth, it is recommended to
     * limit CUBE to 10 or fewer dimensions (2^10 = 1,024 grouping sets).
     *
     * <p><b>Example:</b>
     * <pre>
     * GROUP BY CUBE(region, product, quarter)
     * -- Generates 8 grouping sets (2^3):
     * -- 1. (region, product, quarter)
     * -- 2. (region, product)
     * -- 3. (region, quarter)
     * -- 4. (product, quarter)
     * -- 5. (region)
     * -- 6. (product)
     * -- 7. (quarter)
     * -- 8. ()
     * </pre>
     */
    CUBE,

    /**
     * Explicit specification of custom grouping sets.
     *
     * <p>GROUPING SETS allows precise control over which combinations
     * of columns to group by, without generating all possible combinations
     * like CUBE.
     *
     * <p><b>Example:</b>
     * <pre>
     * GROUP BY GROUPING SETS(
     *   (region, product),  -- regional product totals
     *   (region),           -- regional totals
     *   (product),          -- product totals
     *   ()                  -- grand total
     * )
     * </pre>
     */
    GROUPING_SETS
}
