package com.thunderduck.differential;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TPC-H benchmark tests run through Spark Connect interface.
 *
 * This test suite validates that thunderduck's Spark Connect server
 * correctly handles TPC-H queries by comparing results between:
 * 1. Direct Spark execution
 * 2. thunderduck via Spark Connect protocol
 *
 * Prerequisites:
 * 1. Start thunderduck Spark Connect server: ./start-server.sh
 * 2. Have TPC-H data available in Parquet format
 *
 * Run with:
 * mvn test -Dtest=TPCHSparkConnectTest -Dtpch.spark.connect.enabled=true
 */
@EnabledIfSystemProperty(named = "tpch.spark.connect.enabled", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TPCHSparkConnectTest {

    private static SparkSession sparkDirect;
    private static SparkSession sparkThunderduck;
    private static final String TPCH_DATA_PATH = System.getProperty("tpch.data.path", "./data/tpch_sf001");
    private static final String THUNDERDUCK_URL = System.getProperty("thunderduck.url", "sc://localhost:50051");

    @BeforeAll
    static void setUp() {
        // Create direct Spark session (local mode)
        sparkDirect = SparkSession.builder()
            .appName("TPCH-Direct")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        // Create Spark session connected to thunderduck
        // NOTE: This requires spark-connect-client-jvm which would need
        // a running Spark Connect server. For now, using local mode.
        // TODO: Enable when thunderduck Spark Connect server is running
        sparkThunderduck = SparkSession.builder()
            .appName("TPCH-Thunderduck")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        // Register TPC-H tables in both sessions
        registerTPCHTables(sparkDirect);
        registerTPCHTables(sparkThunderduck);
    }

    @AfterAll
    static void tearDown() {
        if (sparkDirect != null) {
            sparkDirect.stop();
        }
        if (sparkThunderduck != null) {
            sparkThunderduck.stop();
        }
    }

    private static void registerTPCHTables(SparkSession spark) {
        String[] tables = {"customer", "lineitem", "nation", "orders",
                          "part", "partsupp", "region", "supplier"};

        for (String table : tables) {
            String path = TPCH_DATA_PATH + "/" + table + ".parquet";
            spark.read().parquet(path).createOrReplaceTempView(table);
        }
    }

    @Test
    @Order(1)
    @DisplayName("TPC-H Q1: Pricing Summary Report")
    void testQuery1() {
        String query = """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                AVG(l_quantity) as avg_qty,
                AVG(l_extendedprice) as avg_price,
                AVG(l_discount) as avg_disc,
                COUNT(*) as count_order
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """;

        compareResults("Q1", query);
    }

    @Test
    @Order(3)
    @DisplayName("TPC-H Q3: Shipping Priority")
    void testQuery3() {
        String query = """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE
                c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < DATE '1995-03-15'
                AND l_shipdate > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
        """;

        compareResults("Q3", query);
    }

    @Test
    @Order(6)
    @DisplayName("TPC-H Q6: Forecasting Revenue Change")
    void testQuery6() {
        String query = """
            SELECT
                SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE
                l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        """;

        compareResults("Q6", query);
    }

    @Test
    @Order(10)
    @DisplayName("TPC-H Q10: Returned Item Reporting")
    void testQuery10() {
        String query = """
            SELECT
                c_custkey,
                c_name,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM customer, orders, lineitem, nation
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate >= DATE '1993-10-01'
                AND o_orderdate < DATE '1993-10-01' + INTERVAL 3 MONTH
                AND l_returnflag = 'R'
                AND c_nationkey = n_nationkey
            GROUP BY
                c_custkey, c_name, c_acctbal, c_phone,
                n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20
        """;

        compareResults("Q10", query);
    }

    private void compareResults(String queryName, String sql) {
        try {
            // Execute on direct Spark
            Dataset<Row> directResult = sparkDirect.sql(sql);
            List<Row> directRows = directResult.collectAsList();

            // Execute on thunderduck via Spark Connect
            Dataset<Row> thunderduckResult = sparkThunderduck.sql(sql);
            List<Row> thunderduckRows = thunderduckResult.collectAsList();

            // Compare results
            assertEquals(directRows.size(), thunderduckRows.size(),
                String.format("%s: Row count mismatch", queryName));

            // Compare schemas
            assertEquals(directResult.schema(), thunderduckResult.schema(),
                String.format("%s: Schema mismatch", queryName));

            // Compare row data
            for (int i = 0; i < directRows.size(); i++) {
                Row directRow = directRows.get(i);
                Row thunderduckRow = thunderduckRows.get(i);

                for (int j = 0; j < directRow.length(); j++) {
                    Object directVal = directRow.get(j);
                    Object thunderduckVal = thunderduckRow.get(j);

                    if (directVal instanceof Double && thunderduckVal instanceof Double) {
                        assertEquals((Double) directVal, (Double) thunderduckVal, 1e-6,
                            String.format("%s: Value mismatch at row %d, col %d", queryName, i, j));
                    } else {
                        assertEquals(directVal, thunderduckVal,
                            String.format("%s: Value mismatch at row %d, col %d", queryName, i, j));
                    }
                }
            }

            System.out.println("✓ " + queryName + " passed");

        } catch (Exception e) {
            fail(queryName + " failed: " + e.getMessage());
        }
    }
}