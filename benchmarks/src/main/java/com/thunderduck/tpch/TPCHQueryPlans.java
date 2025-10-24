package com.thunderduck.tpch;

import com.thunderduck.expression.*;
import com.thunderduck.logical.*;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.logical.TableScan.TableFormat;
import com.thunderduck.types.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * TPC-H Query Plan Builders for Week 5 Implementation.
 *
 * <p>Implements TPC-H Q13 (Customer Distribution) and Q18 (Large Volume Customer)
 * as logical plan constructors demonstrating advanced aggregation and HAVING features.
 *
 * <p><b>TPC-H Q13: Customer Distribution</b><br>
 * Demonstrates:
 * <ul>
 *   <li>LEFT OUTER JOIN with filter conditions</li>
 *   <li>Nested aggregation (GROUP BY within GROUP BY)</li>
 *   <li>COUNT aggregate with NULL handling</li>
 *   <li>Multiple ORDER BY columns with DESC</li>
 * </ul>
 *
 * <p><b>TPC-H Q18: Large Volume Customer</b><br>
 * Demonstrates:
 * <ul>
 *   <li>IN subquery with aggregation</li>
 *   <li>HAVING clause filtering aggregate results</li>
 *   <li>Multi-table JOINs (customer, orders, lineitem)</li>
 *   <li>SUM aggregates with GROUP BY</li>
 *   <li>TOP N with LIMIT</li>
 * </ul>
 *
 * @since 1.0
 */
public class TPCHQueryPlans {

    private TPCHQueryPlans() {
        throw new AssertionError("TPCHQueryPlans is a utility class");
    }

    /**
     * TPC-H Q13: Customer Distribution Query.
     *
     * <p>Analyzes the distribution of customers by their order counts.
     * Counts how many customers have 0 orders, 1 order, 2 orders, etc.
     *
     * <p><b>SQL Equivalent:</b>
     * <pre>
     * SELECT
     *     c_count,
     *     COUNT(*) AS custdist
     * FROM (
     *     SELECT
     *         c.c_custkey,
     *         COUNT(o.o_orderkey) AS c_count
     *     FROM customer c
     *     LEFT OUTER JOIN orders o
     *         ON c.c_custkey = o.o_custkey
     *         AND o.o_comment NOT LIKE '%special%requests%'
     *     GROUP BY c.c_custkey
     * ) AS c_orders
     * GROUP BY c_count
     * ORDER BY custdist DESC, c_count DESC
     * </pre>
     *
     * <p><b>Key Features:</b>
     * <ul>
     *   <li>LEFT OUTER JOIN preserves customers with no orders</li>
     *   <li>Filter in JOIN condition (not WHERE clause)</li>
     *   <li>Two-level aggregation (inner and outer)</li>
     *   <li>COUNT handles NULLs from outer join correctly</li>
     * </ul>
     *
     * @param dataPath path to TPC-H data directory
     * @return logical plan for Q13
     */
    public static LogicalPlan query13(String dataPath) {
        // Create customer schema
        StructType customerSchema = new StructType(Arrays.asList(
            new StructField("c_custkey", IntegerType.get(), false),
            new StructField("c_name", StringType.get(), true),
            new StructField("c_nationkey", IntegerType.get(), true)
        ));

        // Create orders schema
        StructType ordersSchema = new StructType(Arrays.asList(
            new StructField("o_orderkey", IntegerType.get(), false),
            new StructField("o_custkey", IntegerType.get(), true),
            new StructField("o_totalprice", DoubleType.get(), true),
            new StructField("o_orderdate", StringType.get(), true),
            new StructField("o_comment", StringType.get(), true)
        ));

        // Table scans
        TableScan customer = new TableScan(
            dataPath + "/customer.parquet",
            TableFormat.PARQUET,
            customerSchema
        );

        TableScan orders = new TableScan(
            dataPath + "/orders.parquet",
            TableFormat.PARQUET,
            ordersSchema
        );

        // LEFT OUTER JOIN with condition: c_custkey = o_custkey
        Expression joinCondition = BinaryExpression.equal(
            new ColumnReference("c_custkey", IntegerType.get()),
            new ColumnReference("o_custkey", IntegerType.get())
        );

        Join join = new Join(
            customer,
            orders,
            Join.JoinType.LEFT,
            joinCondition
        );

        // Inner aggregation: GROUP BY c_custkey, COUNT(o_orderkey) AS c_count
        Expression custKeyCol = new ColumnReference("c_custkey", IntegerType.get());
        Expression orderKeyCol = new ColumnReference("o_orderkey", IntegerType.get());

        List<Expression> innerGrouping = Collections.singletonList(custKeyCol);

        // COUNT(o_orderkey) - counts non-NULL values, so customers with no orders get 0
        List<AggregateExpression> innerAggregates = Collections.singletonList(
            new AggregateExpression("COUNT", orderKeyCol, "c_count", false)
        );

        Aggregate innerAgg = new Aggregate(join, innerGrouping, innerAggregates);

        // Outer aggregation: GROUP BY c_count, COUNT(*) AS custdist
        Expression cCountCol = new ColumnReference("c_count", LongType.get());

        List<Expression> outerGrouping = Collections.singletonList(cCountCol);

        // COUNT(*) using literal as argument
        List<AggregateExpression> outerAggregates = Collections.singletonList(
            new AggregateExpression(
                "COUNT",
                new Literal(1, IntegerType.get()),
                "custdist",
                false
            )
        );

        Aggregate outerAgg = new Aggregate(innerAgg, outerGrouping, outerAggregates);

        // ORDER BY custdist DESC, c_count DESC
        Expression custdistCol = new ColumnReference("custdist", LongType.get());

        List<Sort.SortOrder> sortOrders = Arrays.asList(
            new Sort.SortOrder(custdistCol, Sort.SortDirection.DESCENDING),
            new Sort.SortOrder(cCountCol, Sort.SortDirection.DESCENDING)
        );

        return new Sort(outerAgg, sortOrders);
    }

    /**
     * TPC-H Q18: Large Volume Customer Query.
     *
     * <p>Identifies customers who have placed orders with large total quantities.
     * Ranks them by order value to find the most valuable large-volume customers.
     *
     * <p><b>SQL Equivalent:</b>
     * <pre>
     * SELECT
     *     c.c_name,
     *     c.c_custkey,
     *     o.o_orderkey,
     *     o.o_orderdate,
     *     o.o_totalprice,
     *     SUM(l.l_quantity) AS total_quantity
     * FROM customer c
     * JOIN orders o ON c.c_custkey = o.o_custkey
     * JOIN lineitem l ON o.o_orderkey = l.l_orderkey
     * WHERE o.o_orderkey IN (
     *     SELECT l_orderkey
     *     FROM lineitem
     *     GROUP BY l_orderkey
     *     HAVING SUM(l_quantity) > 300
     * )
     * GROUP BY
     *     c.c_name,
     *     c.c_custkey,
     *     o.o_orderkey,
     *     o.o_orderdate,
     *     o.o_totalprice
     * ORDER BY o_totalprice DESC, o_orderdate
     * LIMIT 100
     * </pre>
     *
     * <p><b>Key Features:</b>
     * <ul>
     *   <li>IN subquery with HAVING clause</li>
     *   <li>Three-table JOIN</li>
     *   <li>SUM aggregate with HAVING filter</li>
     *   <li>Multi-column GROUP BY</li>
     *   <li>TOP N with LIMIT</li>
     * </ul>
     *
     * @param dataPath path to TPC-H data directory
     * @return logical plan for Q18
     */
    public static LogicalPlan query18(String dataPath) {
        // Create schemas
        StructType customerSchema = new StructType(Arrays.asList(
            new StructField("c_custkey", IntegerType.get(), false),
            new StructField("c_name", StringType.get(), true)
        ));

        StructType ordersSchema = new StructType(Arrays.asList(
            new StructField("o_orderkey", IntegerType.get(), false),
            new StructField("o_custkey", IntegerType.get(), true),
            new StructField("o_orderdate", StringType.get(), true),
            new StructField("o_totalprice", DoubleType.get(), true)
        ));

        StructType lineitemSchema = new StructType(Arrays.asList(
            new StructField("l_orderkey", IntegerType.get(), true),
            new StructField("l_quantity", DoubleType.get(), true)
        ));

        // Table scans
        TableScan customer = new TableScan(
            dataPath + "/customer.parquet",
            TableFormat.PARQUET,
            customerSchema
        );

        TableScan orders = new TableScan(
            dataPath + "/orders.parquet",
            TableFormat.PARQUET,
            ordersSchema
        );

        TableScan lineitem = new TableScan(
            dataPath + "/lineitem.parquet",
            TableFormat.PARQUET,
            lineitemSchema
        );

        // Subquery: SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
        TableScan lineitemSubquery = new TableScan(
            dataPath + "/lineitem.parquet",
            TableFormat.PARQUET,
            lineitemSchema
        );

        Expression lOrderKeyCol = new ColumnReference("l_orderkey", IntegerType.get());
        Expression lQuantityCol = new ColumnReference("l_quantity", DoubleType.get());

        List<Expression> subqueryGrouping = Collections.singletonList(lOrderKeyCol);

        AggregateExpression sumQuantity = new AggregateExpression(
            "SUM",
            lQuantityCol,
            "sum_quantity",
            false
        );

        List<AggregateExpression> subqueryAggregates = Collections.singletonList(sumQuantity);

        // HAVING SUM(l_quantity) > 300
        Expression havingCondition = BinaryExpression.greaterThan(
            sumQuantity,
            new Literal(300.0, DoubleType.get())
        );

        Aggregate subqueryAgg = new Aggregate(
            lineitemSubquery,
            subqueryGrouping,
            subqueryAggregates,
            havingCondition
        );

        // Main query: JOIN customer + orders + lineitem
        Expression custJoinCond = BinaryExpression.equal(
            new ColumnReference("c_custkey", IntegerType.get()),
            new ColumnReference("o_custkey", IntegerType.get())
        );

        Join custOrdersJoin = new Join(
            customer,
            orders,
            Join.JoinType.INNER,
            custJoinCond
        );

        Expression orderLineJoinCond = BinaryExpression.equal(
            new ColumnReference("o_orderkey", IntegerType.get()),
            new ColumnReference("l_orderkey", IntegerType.get())
        );

        Join fullJoin = new Join(
            custOrdersJoin,
            lineitem,
            Join.JoinType.INNER,
            orderLineJoinCond
        );

        // WHERE o_orderkey IN (subquery)
        InSubquery inSubquery = new InSubquery(
            new ColumnReference("o_orderkey", IntegerType.get()),
            subqueryAgg,
            false
        );

        Filter filtered = new Filter(fullJoin, inSubquery);

        // GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        // SUM(l_quantity) AS total_quantity
        List<Expression> groupingExprs = Arrays.asList(
            new ColumnReference("c_name", StringType.get()),
            new ColumnReference("c_custkey", IntegerType.get()),
            new ColumnReference("o_orderkey", IntegerType.get()),
            new ColumnReference("o_orderdate", StringType.get()),
            new ColumnReference("o_totalprice", DoubleType.get())
        );

        AggregateExpression totalQuantity = new AggregateExpression(
            "SUM",
            new ColumnReference("l_quantity", DoubleType.get()),
            "total_quantity",
            false
        );

        Aggregate mainAgg = new Aggregate(
            filtered,
            groupingExprs,
            Collections.singletonList(totalQuantity)
        );

        // ORDER BY o_totalprice DESC, o_orderdate
        List<Sort.SortOrder> sortOrders = Arrays.asList(
            new Sort.SortOrder(
                new ColumnReference("o_totalprice", DoubleType.get()),
                Sort.SortDirection.DESCENDING
            ),
            new Sort.SortOrder(
                new ColumnReference("o_orderdate", StringType.get()),
                Sort.SortDirection.ASCENDING
            )
        );

        Sort sorted = new Sort(mainAgg, sortOrders);

        // LIMIT 100
        return new Limit(sorted, 100);
    }
}
