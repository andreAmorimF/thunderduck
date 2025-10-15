package com.catalyst2sql.tpch;

/**
 * TPC-H Query Constants and Metadata.
 *
 * <p>This class provides constants, descriptions, and metadata for all 22 TPC-H queries.
 * Each query constant can be used to reference specific TPC-H queries in benchmarks,
 * tests, and documentation.
 *
 * <p>The TPC-H benchmark consists of 22 business-oriented queries that exercise
 * different aspects of database systems:
 * <ul>
 *   <li>Simple aggregations (Q1, Q6)</li>
 *   <li>Complex joins (Q2, Q3, Q5, Q7, Q8, Q9, Q10)</li>
 *   <li>Subqueries and correlated queries (Q2, Q4, Q17, Q20, Q21, Q22)</li>
 *   <li>Window functions and analytics (Q12, Q13, Q15, Q16, Q18, Q19)</li>
 *   <li>Set operations and unions (Q11, Q14)</li>
 * </ul>
 *
 * @see <a href="http://www.tpc.org/tpch/">TPC-H Benchmark Specification</a>
 */
public final class TPCHQueries {

    // Prevent instantiation
    private TPCHQueries() {
        throw new AssertionError("TPCHQueries is a utility class and should not be instantiated");
    }

    // ==================== Query Constants ====================

    /** Query 1: Pricing Summary Report */
    public static final String Q1_PRICING_SUMMARY = "q1_pricing_summary";

    /** Query 2: Minimum Cost Supplier Query */
    public static final String Q2_MINIMUM_COST_SUPPLIER = "q2_minimum_cost_supplier";

    /** Query 3: Shipping Priority Query */
    public static final String Q3_SHIPPING_PRIORITY = "q3_shipping_priority";

    /** Query 4: Order Priority Checking Query */
    public static final String Q4_ORDER_PRIORITY = "q4_order_priority";

    /** Query 5: Local Supplier Volume Query */
    public static final String Q5_LOCAL_SUPPLIER_VOLUME = "q5_local_supplier_volume";

    /** Query 6: Forecasting Revenue Change Query */
    public static final String Q6_FORECASTING_REVENUE = "q6_forecasting_revenue";

    /** Query 7: Volume Shipping Query */
    public static final String Q7_VOLUME_SHIPPING = "q7_volume_shipping";

    /** Query 8: National Market Share Query */
    public static final String Q8_NATIONAL_MARKET_SHARE = "q8_national_market_share";

    /** Query 9: Product Type Profit Measure Query */
    public static final String Q9_PRODUCT_TYPE_PROFIT = "q9_product_type_profit";

    /** Query 10: Returned Item Reporting Query */
    public static final String Q10_RETURNED_ITEM = "q10_returned_item";

    /** Query 11: Important Stock Identification Query */
    public static final String Q11_IMPORTANT_STOCK = "q11_important_stock";

    /** Query 12: Shipping Modes and Order Priority Query */
    public static final String Q12_SHIPPING_MODES = "q12_shipping_modes";

    /** Query 13: Customer Distribution Query */
    public static final String Q13_CUSTOMER_DISTRIBUTION = "q13_customer_distribution";

    /** Query 14: Promotion Effect Query */
    public static final String Q14_PROMOTION_EFFECT = "q14_promotion_effect";

    /** Query 15: Top Supplier Query */
    public static final String Q15_TOP_SUPPLIER = "q15_top_supplier";

    /** Query 16: Parts/Supplier Relationship Query */
    public static final String Q16_PARTS_SUPPLIER = "q16_parts_supplier";

    /** Query 17: Small-Quantity-Order Revenue Query */
    public static final String Q17_SMALL_QUANTITY_ORDER = "q17_small_quantity_order";

    /** Query 18: Large Volume Customer Query */
    public static final String Q18_LARGE_VOLUME_CUSTOMER = "q18_large_volume_customer";

    /** Query 19: Discounted Revenue Query */
    public static final String Q19_DISCOUNTED_REVENUE = "q19_discounted_revenue";

    /** Query 20: Potential Part Promotion Query */
    public static final String Q20_POTENTIAL_PART_PROMOTION = "q20_potential_part_promotion";

    /** Query 21: Suppliers Who Kept Orders Waiting Query */
    public static final String Q21_SUPPLIERS_WAITING = "q21_suppliers_waiting";

    /** Query 22: Global Sales Opportunity Query */
    public static final String Q22_GLOBAL_SALES = "q22_global_sales";

    // ==================== Query Descriptions ====================

    /**
     * Returns a human-readable description for a TPC-H query.
     *
     * @param queryName the query constant (e.g., Q1_PRICING_SUMMARY)
     * @return a description of what the query measures
     */
    public static String getDescription(String queryName) {
        switch (queryName) {
            case Q1_PRICING_SUMMARY:
                return "Reports amount of business billed, shipped, and returned. " +
                       "Tests: scan, filter, aggregate, sort";

            case Q2_MINIMUM_COST_SUPPLIER:
                return "Finds which supplier should be selected for a given part in a region. " +
                       "Tests: complex joins, subqueries";

            case Q3_SHIPPING_PRIORITY:
                return "Retrieves the 10 unshipped orders with highest value. " +
                       "Tests: joins, aggregation, top-N";

            case Q4_ORDER_PRIORITY:
                return "Examines how order priorities align with actual shipping performance. " +
                       "Tests: semi-joins, exists subqueries";

            case Q5_LOCAL_SUPPLIER_VOLUME:
                return "Lists revenue for each nation's suppliers within a region. " +
                       "Tests: multi-way joins, aggregation";

            case Q6_FORECASTING_REVENUE:
                return "Quantifies revenue from eliminating certain discounts. " +
                       "Tests: simple scan and filter aggregation";

            case Q7_VOLUME_SHIPPING:
                return "Determines shipping volume between two nations. " +
                       "Tests: bi-directional joins";

            case Q8_NATIONAL_MARKET_SHARE:
                return "Determines market share for a specific part type by nation. " +
                       "Tests: complex joins, aggregation";

            case Q9_PRODUCT_TYPE_PROFIT:
                return "Determines profit for products across nations and years. " +
                       "Tests: multi-level grouping";

            case Q10_RETURNED_ITEM:
                return "Identifies customers with returned items. " +
                       "Tests: joins, filtering, top-N";

            case Q11_IMPORTANT_STOCK:
                return "Finds most important subset of suppliers' stock. " +
                       "Tests: having clause, aggregation";

            case Q12_SHIPPING_MODES:
                return "Determines shipping mode priorities. " +
                       "Tests: case expressions, grouping";

            case Q13_CUSTOMER_DISTRIBUTION:
                return "Analyzes customer order distribution. " +
                       "Tests: outer joins, grouping";

            case Q14_PROMOTION_EFFECT:
                return "Monitors promotional campaign effectiveness. " +
                       "Tests: conditional aggregation";

            case Q15_TOP_SUPPLIER:
                return "Determines top supplier for a time period. " +
                       "Tests: views, max aggregation";

            case Q16_PARTS_SUPPLIER:
                return "Counts suppliers who can supply parts meeting criteria. " +
                       "Tests: distinct count, not in subquery";

            case Q17_SMALL_QUANTITY_ORDER:
                return "Determines average yearly revenue for small orders. " +
                       "Tests: correlated subquery";

            case Q18_LARGE_VOLUME_CUSTOMER:
                return "Ranks customers based on large order volume. " +
                       "Tests: in subquery, top-N";

            case Q19_DISCOUNTED_REVENUE:
                return "Reports revenue from discounted parts shipped by air. " +
                       "Tests: complex OR conditions";

            case Q20_POTENTIAL_PART_PROMOTION:
                return "Identifies suppliers with excess inventory. " +
                       "Tests: in subquery with aggregation";

            case Q21_SUPPLIERS_WAITING:
                return "Identifies suppliers who kept orders waiting. " +
                       "Tests: exists, not exists, multi-table";

            case Q22_GLOBAL_SALES:
                return "Identifies customers likely to place new orders. " +
                       "Tests: not in, substring functions";

            default:
                return "Unknown query: " + queryName;
        }
    }

    /**
     * Returns the complexity level of a query (1=simple, 5=very complex).
     *
     * @param queryName the query constant
     * @return complexity rating from 1 to 5
     */
    public static int getComplexity(String queryName) {
        switch (queryName) {
            case Q1_PRICING_SUMMARY:
            case Q6_FORECASTING_REVENUE:
                return 1; // Simple: single table, basic aggregation

            case Q3_SHIPPING_PRIORITY:
            case Q10_RETURNED_ITEM:
            case Q12_SHIPPING_MODES:
                return 2; // Moderate: 2-3 table joins

            case Q5_LOCAL_SUPPLIER_VOLUME:
            case Q7_VOLUME_SHIPPING:
            case Q8_NATIONAL_MARKET_SHARE:
            case Q13_CUSTOMER_DISTRIBUTION:
            case Q14_PROMOTION_EFFECT:
                return 3; // Complex: multiple joins, aggregations

            case Q2_MINIMUM_COST_SUPPLIER:
            case Q4_ORDER_PRIORITY:
            case Q9_PRODUCT_TYPE_PROFIT:
            case Q11_IMPORTANT_STOCK:
            case Q16_PARTS_SUPPLIER:
            case Q19_DISCOUNTED_REVENUE:
                return 4; // Very complex: subqueries, complex filters

            case Q15_TOP_SUPPLIER:
            case Q17_SMALL_QUANTITY_ORDER:
            case Q18_LARGE_VOLUME_CUSTOMER:
            case Q20_POTENTIAL_PART_PROMOTION:
            case Q21_SUPPLIERS_WAITING:
            case Q22_GLOBAL_SALES:
                return 5; // Extremely complex: correlated subqueries, multiple levels

            default:
                return 0;
        }
    }

    /**
     * Returns the primary table accessed by the query.
     *
     * @param queryName the query constant
     * @return the primary table name
     */
    public static String getPrimaryTable(String queryName) {
        switch (queryName) {
            case Q1_PRICING_SUMMARY:
            case Q6_FORECASTING_REVENUE:
                return "lineitem";

            case Q2_MINIMUM_COST_SUPPLIER:
                return "part";

            case Q3_SHIPPING_PRIORITY:
            case Q4_ORDER_PRIORITY:
                return "orders";

            case Q5_LOCAL_SUPPLIER_VOLUME:
            case Q7_VOLUME_SHIPPING:
            case Q8_NATIONAL_MARKET_SHARE:
            case Q9_PRODUCT_TYPE_PROFIT:
                return "lineitem";

            case Q10_RETURNED_ITEM:
            case Q13_CUSTOMER_DISTRIBUTION:
            case Q18_LARGE_VOLUME_CUSTOMER:
            case Q22_GLOBAL_SALES:
                return "customer";

            case Q11_IMPORTANT_STOCK:
                return "partsupp";

            case Q12_SHIPPING_MODES:
            case Q14_PROMOTION_EFFECT:
            case Q17_SMALL_QUANTITY_ORDER:
            case Q19_DISCOUNTED_REVENUE:
                return "lineitem";

            case Q15_TOP_SUPPLIER:
            case Q20_POTENTIAL_PART_PROMOTION:
            case Q21_SUPPLIERS_WAITING:
                return "supplier";

            case Q16_PARTS_SUPPLIER:
                return "partsupp";

            default:
                return "unknown";
        }
    }

    /**
     * Returns all TPC-H query names in order.
     *
     * @return array of all 22 query names
     */
    public static String[] getAllQueries() {
        return new String[] {
            Q1_PRICING_SUMMARY,
            Q2_MINIMUM_COST_SUPPLIER,
            Q3_SHIPPING_PRIORITY,
            Q4_ORDER_PRIORITY,
            Q5_LOCAL_SUPPLIER_VOLUME,
            Q6_FORECASTING_REVENUE,
            Q7_VOLUME_SHIPPING,
            Q8_NATIONAL_MARKET_SHARE,
            Q9_PRODUCT_TYPE_PROFIT,
            Q10_RETURNED_ITEM,
            Q11_IMPORTANT_STOCK,
            Q12_SHIPPING_MODES,
            Q13_CUSTOMER_DISTRIBUTION,
            Q14_PROMOTION_EFFECT,
            Q15_TOP_SUPPLIER,
            Q16_PARTS_SUPPLIER,
            Q17_SMALL_QUANTITY_ORDER,
            Q18_LARGE_VOLUME_CUSTOMER,
            Q19_DISCOUNTED_REVENUE,
            Q20_POTENTIAL_PART_PROMOTION,
            Q21_SUPPLIERS_WAITING,
            Q22_GLOBAL_SALES
        };
    }

    /**
     * Returns queries suitable for CI/quick testing (simple queries only).
     *
     * @return array of simple query names
     */
    public static String[] getSimpleQueries() {
        return new String[] {
            Q1_PRICING_SUMMARY,
            Q6_FORECASTING_REVENUE
        };
    }

    /**
     * Returns queries suitable for comprehensive testing.
     *
     * @return array of moderately complex query names
     */
    public static String[] getStandardQueries() {
        return new String[] {
            Q1_PRICING_SUMMARY,
            Q3_SHIPPING_PRIORITY,
            Q6_FORECASTING_REVENUE,
            Q10_RETURNED_ITEM,
            Q12_SHIPPING_MODES
        };
    }
}
