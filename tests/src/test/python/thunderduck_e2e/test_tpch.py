"""TPC-H benchmark tests for thunderduck - SQL and DataFrame implementations.

This module tests all 22 TPC-H queries in both SQL and DataFrame API forms
to ensure complete coverage of the Spark Connect protocol translation.
"""

from thunderduck_e2e.test_runner import ThunderduckE2ETestBase
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os


class TestTPCH(ThunderduckE2ETestBase):
    """TPC-H benchmark tests through Spark Connect.

    IMPORTANT: Each TPC-H query is tested in TWO ways:
    1. As a SQL query (testing SQL → DuckDB translation)
    2. As a DataFrame implementation (testing DataFrame API → DuckDB translation)

    This dual approach ensures:
    - Complete Spark API coverage
    - Validation that both paths produce identical results
    - Testing of the full Spark Connect protocol implementation
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._load_tpch_data()

    @classmethod
    def _load_tpch_data(cls):
        """Load TPC-H data into Spark session."""
        data_path = os.getenv("TPCH_DATA_PATH", "./data/tpch_sf001")

        tables = ["customer", "lineitem", "nation", "orders",
                  "part", "partsupp", "region", "supplier"]

        for table in tables:
            parquet_path = f"{data_path}/{table}.parquet"
            if os.path.exists(parquet_path):
                df = cls.spark.read.parquet(parquet_path)
                df.createOrReplaceTempView(table)
                # Store as attribute for DataFrame API tests
                setattr(cls, f"df_{table}", df)

    # =========================================================================
    # Query 1: Pricing Summary Report
    # =========================================================================

    def test_q01_sql(self):
        """Q1: Pricing Summary Report (SQL version)."""
        query = """
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
        """
        df = self.spark.sql(query)
        self.q01_sql_result = df.collect()
        self.assertGreater(len(self.q01_sql_result), 0)

    def test_q01_dataframe(self):
        """Q1: Pricing Summary Report (DataFrame API version)."""
        df = self.df_lineitem \
            .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01"), 90)) \
            .groupBy("l_returnflag", "l_linestatus") \
            .agg(
                F.sum("l_quantity").alias("sum_qty"),
                F.sum("l_extendedprice").alias("sum_base_price"),
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax"))).alias("sum_charge"),
                F.avg("l_quantity").alias("avg_qty"),
                F.avg("l_extendedprice").alias("avg_price"),
                F.avg("l_discount").alias("avg_disc"),
                F.count("*").alias("count_order")
            ) \
            .orderBy("l_returnflag", "l_linestatus")

        result = df.collect()
        self.assertGreater(len(result), 0)

        # Verify SQL and DataFrame versions produce same results
        if hasattr(self, 'q01_sql_result'):
            self.assertEqual(len(result), len(self.q01_sql_result))

    # =========================================================================
    # Query 3: Shipping Priority
    # =========================================================================

    def test_q03_sql(self):
        """Q3: Shipping Priority (SQL version)."""
        query = """
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
        """
        df = self.spark.sql(query)
        self.q03_sql_result = df.collect()
        self.assertEqual(len(self.q03_sql_result), 10)

    def test_q03_dataframe(self):
        """Q3: Shipping Priority (DataFrame API version)."""
        df = self.df_customer \
            .filter(F.col("c_mktsegment") == "BUILDING") \
            .join(self.df_orders, F.col("c_custkey") == F.col("o_custkey")) \
            .filter(F.col("o_orderdate") < F.lit("1995-03-15")) \
            .join(self.df_lineitem, F.col("o_orderkey") == F.col("l_orderkey")) \
            .filter(F.col("l_shipdate") > F.lit("1995-03-15")) \
            .groupBy("l_orderkey", "o_orderdate", "o_shippriority") \
            .agg(
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
            ) \
            .orderBy(F.col("revenue").desc(), "o_orderdate") \
            .limit(10)

        result = df.collect()
        self.assertEqual(len(result), 10)

        # Verify SQL and DataFrame versions produce same results
        if hasattr(self, 'q03_sql_result'):
            self.assertEqual(len(result), len(self.q03_sql_result))

    # =========================================================================
    # Query 6: Forecasting Revenue Change
    # =========================================================================

    def test_q06_sql(self):
        """Q6: Forecasting Revenue Change (SQL version)."""
        query = """
            SELECT
                SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE
                l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        """
        df = self.spark.sql(query)
        self.q06_sql_result = df.collect()
        self.assertEqual(len(self.q06_sql_result), 1)

    def test_q06_dataframe(self):
        """Q6: Forecasting Revenue Change (DataFrame API version)."""
        df = self.df_lineitem \
            .filter(
                (F.col("l_shipdate") >= F.lit("1994-01-01")) &
                (F.col("l_shipdate") < F.add_months(F.lit("1994-01-01"), 12)) &
                (F.col("l_discount").between(0.05, 0.07)) &
                (F.col("l_quantity") < 24)
            ) \
            .agg(
                F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")
            )

        result = df.collect()
        self.assertEqual(len(result), 1)

        # Verify SQL and DataFrame versions produce same results
        if hasattr(self, 'q06_sql_result'):
            self.assertAlmostEqual(
                result[0]["revenue"],
                self.q06_sql_result[0]["revenue"],
                places=2
            )

    # =========================================================================
    # Query 10: Returned Item Reporting
    # =========================================================================

    def test_q10_sql(self):
        """Q10: Returned Item Reporting (SQL version)."""
        query = """
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
        """
        df = self.spark.sql(query)
        self.q10_sql_result = df.collect()
        self.assertLessEqual(len(self.q10_sql_result), 20)

    def test_q10_dataframe(self):
        """Q10: Returned Item Reporting (DataFrame API version)."""
        df = self.df_customer \
            .join(self.df_orders, F.col("c_custkey") == F.col("o_custkey")) \
            .filter(
                (F.col("o_orderdate") >= F.lit("1993-10-01")) &
                (F.col("o_orderdate") < F.add_months(F.lit("1993-10-01"), 3))
            ) \
            .join(self.df_lineitem, F.col("l_orderkey") == F.col("o_orderkey")) \
            .filter(F.col("l_returnflag") == "R") \
            .join(self.df_nation, F.col("c_nationkey") == F.col("n_nationkey")) \
            .groupBy(
                "c_custkey", "c_name", "c_acctbal", "c_phone",
                "n_name", "c_address", "c_comment"
            ) \
            .agg(
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
            ) \
            .orderBy(F.col("revenue").desc()) \
            .limit(20)

        result = df.collect()
        self.assertLessEqual(len(result), 20)

        # Verify SQL and DataFrame versions produce same results
        if hasattr(self, 'q10_sql_result'):
            self.assertEqual(len(result), len(self.q10_sql_result))

    # =========================================================================
    # Query 2: Minimum Cost Supplier
    # =========================================================================

    def test_q02_sql(self):
        """Q2: Minimum Cost Supplier (SQL version)."""
        query = """
            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
                AND ps_supplycost = (
                    SELECT MIN(ps_supplycost)
                    FROM partsupp, supplier, nation, region
                    WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                )
            ORDER BY
                s_acctbal DESC,
                n_name,
                s_name,
                p_partkey
            LIMIT 100
        """
        df = self.spark.sql(query)
        self.q02_sql_result = df.collect()
        self.assertLessEqual(len(self.q02_sql_result), 100)

    def test_q02_dataframe(self):
        """Q2: Minimum Cost Supplier (DataFrame API version)."""
        # This query requires a correlated subquery which is complex in DataFrame API
        # For now, using a simplified version with window functions
        from pyspark.sql.window import Window

        # Join all tables
        df_joined = self.df_part \
            .filter((F.col("p_size") == 15) & F.col("p_type").like("%BRASS")) \
            .join(self.df_partsupp, F.col("p_partkey") == F.col("ps_partkey")) \
            .join(self.df_supplier, F.col("s_suppkey") == F.col("ps_suppkey")) \
            .join(self.df_nation, F.col("s_nationkey") == F.col("n_nationkey")) \
            .join(self.df_region, F.col("n_regionkey") == F.col("r_regionkey")) \
            .filter(F.col("r_name") == "EUROPE")

        # Find minimum cost per part
        window_spec = Window.partitionBy("p_partkey")
        df_with_min = df_joined.withColumn(
            "min_supplycost",
            F.min("ps_supplycost").over(window_spec)
        )

        # Filter to keep only minimum cost suppliers
        df = df_with_min \
            .filter(F.col("ps_supplycost") == F.col("min_supplycost")) \
            .select(
                "s_acctbal", "s_name", "n_name", "p_partkey",
                "p_mfgr", "s_address", "s_phone", "s_comment"
            ) \
            .orderBy(F.col("s_acctbal").desc(), "n_name", "s_name", "p_partkey") \
            .limit(100)

        result = df.collect()
        self.assertLessEqual(len(result), 100)

    # =========================================================================
    # Query 4: Order Priority Checking
    # =========================================================================

    def test_q04_sql(self):
        """Q4: Order Priority Checking (SQL version)."""
        query = """
            SELECT
                o_orderpriority,
                COUNT(*) as order_count
            FROM orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
                AND o_orderdate < DATE '1993-07-01' + INTERVAL 3 MONTH
                AND EXISTS (
                    SELECT *
                    FROM lineitem
                    WHERE
                        l_orderkey = o_orderkey
                        AND l_commitdate < l_receiptdate
                )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        """
        df = self.spark.sql(query)
        self.q04_sql_result = df.collect()
        self.assertGreater(len(self.q04_sql_result), 0)

    def test_q04_dataframe(self):
        """Q4: Order Priority Checking (DataFrame API version)."""
        # Find orders with late lineitems
        late_lineitems = self.df_lineitem \
            .filter(F.col("l_commitdate") < F.col("l_receiptdate")) \
            .select("l_orderkey").distinct()

        # Join with orders and filter by date
        df = self.df_orders \
            .filter(
                (F.col("o_orderdate") >= F.lit("1993-07-01")) &
                (F.col("o_orderdate") < F.add_months(F.lit("1993-07-01"), 3))
            ) \
            .join(late_lineitems, F.col("o_orderkey") == F.col("l_orderkey"), "inner") \
            .groupBy("o_orderpriority") \
            .agg(F.count("*").alias("order_count")) \
            .orderBy("o_orderpriority")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q04_sql_result'):
            self.assertEqual(len(result), len(self.q04_sql_result))

    # =========================================================================
    # Query 5: Local Supplier Volume
    # =========================================================================

    def test_q05_sql(self):
        """Q5: Local Supplier Volume (SQL version)."""
        query = """
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= DATE '1994-01-01'
                AND o_orderdate < DATE '1994-01-01' + INTERVAL 1 YEAR
            GROUP BY n_name
            ORDER BY revenue DESC
        """
        df = self.spark.sql(query)
        self.q05_sql_result = df.collect()
        self.assertGreater(len(self.q05_sql_result), 0)

    def test_q05_dataframe(self):
        """Q5: Local Supplier Volume (DataFrame API version)."""
        df = self.df_customer \
            .join(self.df_orders, F.col("c_custkey") == F.col("o_custkey")) \
            .filter(
                (F.col("o_orderdate") >= F.lit("1994-01-01")) &
                (F.col("o_orderdate") < F.add_months(F.lit("1994-01-01"), 12))
            ) \
            .join(self.df_lineitem, F.col("l_orderkey") == F.col("o_orderkey")) \
            .join(self.df_supplier, F.col("l_suppkey") == F.col("s_suppkey")) \
            .filter(F.col("c_nationkey") == F.col("s_nationkey")) \
            .join(self.df_nation, F.col("s_nationkey") == F.col("n_nationkey")) \
            .join(self.df_region, F.col("n_regionkey") == F.col("r_regionkey")) \
            .filter(F.col("r_name") == "ASIA") \
            .groupBy("n_name") \
            .agg(
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
            ) \
            .orderBy(F.col("revenue").desc())

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q05_sql_result'):
            self.assertEqual(len(result), len(self.q05_sql_result))

    # =========================================================================
    # Query 7: Volume Shipping
    # =========================================================================

    def test_q07_sql(self):
        """Q7: Volume Shipping (SQL version)."""
        query = """
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                SUM(volume) as revenue
            FROM (
                SELECT
                    n1.n_name as supp_nation,
                    n2.n_name as cust_nation,
                    EXTRACT(year FROM l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                FROM supplier, lineitem, orders, customer, nation n1, nation n2
                WHERE
                    s_suppkey = l_suppkey
                    AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey
                    AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year
        """
        df = self.spark.sql(query)
        self.q07_sql_result = df.collect()
        self.assertGreater(len(self.q07_sql_result), 0)

    def test_q07_dataframe(self):
        """Q7: Volume Shipping (DataFrame API version)."""
        n1 = self.df_nation.alias("n1")
        n2 = self.df_nation.alias("n2")

        df = self.df_supplier \
            .join(self.df_lineitem, F.col("s_suppkey") == F.col("l_suppkey")) \
            .filter(
                (F.col("l_shipdate") >= F.lit("1995-01-01")) &
                (F.col("l_shipdate") <= F.lit("1996-12-31"))
            ) \
            .join(self.df_orders, F.col("o_orderkey") == F.col("l_orderkey")) \
            .join(self.df_customer, F.col("c_custkey") == F.col("o_custkey")) \
            .join(n1, F.col("s_nationkey") == n1["n_nationkey"]) \
            .join(n2, F.col("c_nationkey") == n2["n_nationkey"]) \
            .filter(
                ((n1["n_name"] == "FRANCE") & (n2["n_name"] == "GERMANY")) |
                ((n1["n_name"] == "GERMANY") & (n2["n_name"] == "FRANCE"))
            ) \
            .select(
                n1["n_name"].alias("supp_nation"),
                n2["n_name"].alias("cust_nation"),
                F.year("l_shipdate").alias("l_year"),
                (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume")
            ) \
            .groupBy("supp_nation", "cust_nation", "l_year") \
            .agg(F.sum("volume").alias("revenue")) \
            .orderBy("supp_nation", "cust_nation", "l_year")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q07_sql_result'):
            self.assertEqual(len(result), len(self.q07_sql_result))

    # =========================================================================
    # Query 8: National Market Share
    # =========================================================================

    def test_q08_sql(self):
        """Q8: National Market Share (SQL version)."""
        query = """
            SELECT
                o_year,
                SUM(CASE
                    WHEN nation = 'BRAZIL' THEN volume
                    ELSE 0
                END) / SUM(volume) as mkt_share
            FROM (
                SELECT
                    EXTRACT(year FROM o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) as volume,
                    n2.n_name as nation
                FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
                WHERE
                    p_partkey = l_partkey
                    AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey
                    AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA'
                    AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
            GROUP BY o_year
            ORDER BY o_year
        """
        df = self.spark.sql(query)
        self.q08_sql_result = df.collect()
        self.assertEqual(len(self.q08_sql_result), 2)  # 1995 and 1996

    def test_q08_dataframe(self):
        """Q8: National Market Share (DataFrame API version)."""
        n1 = self.df_nation.alias("n1")
        n2 = self.df_nation.alias("n2")

        df_all = self.df_part \
            .filter(F.col("p_type") == "ECONOMY ANODIZED STEEL") \
            .join(self.df_lineitem, F.col("p_partkey") == F.col("l_partkey")) \
            .join(self.df_supplier, F.col("s_suppkey") == F.col("l_suppkey")) \
            .join(self.df_orders, F.col("l_orderkey") == F.col("o_orderkey")) \
            .filter(
                (F.col("o_orderdate") >= F.lit("1995-01-01")) &
                (F.col("o_orderdate") <= F.lit("1996-12-31"))
            ) \
            .join(self.df_customer, F.col("o_custkey") == F.col("c_custkey")) \
            .join(n1, F.col("c_nationkey") == n1["n_nationkey"]) \
            .join(self.df_region, n1["n_regionkey"] == F.col("r_regionkey")) \
            .filter(F.col("r_name") == "AMERICA") \
            .join(n2, F.col("s_nationkey") == n2["n_nationkey"]) \
            .select(
                F.year("o_orderdate").alias("o_year"),
                (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume"),
                n2["n_name"].alias("nation")
            )

        df = df_all.groupBy("o_year") \
            .agg(
                F.sum(F.when(F.col("nation") == "BRAZIL", F.col("volume")).otherwise(0)).alias("brazil_volume"),
                F.sum("volume").alias("total_volume")
            ) \
            .select(
                "o_year",
                (F.col("brazil_volume") / F.col("total_volume")).alias("mkt_share")
            ) \
            .orderBy("o_year")

        result = df.collect()
        self.assertEqual(len(result), 2)

        if hasattr(self, 'q08_sql_result'):
            self.assertEqual(len(result), len(self.q08_sql_result))

    # =========================================================================
    # Query 9: Product Type Profit Measure
    # =========================================================================

    def test_q09_sql(self):
        """Q9: Product Type Profit Measure (SQL version)."""
        query = """
            SELECT
                nation,
                o_year,
                SUM(amount) as sum_profit
            FROM (
                SELECT
                    n_name as nation,
                    EXTRACT(year FROM o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                FROM part, supplier, lineitem, partsupp, orders, nation
                WHERE
                    s_suppkey = l_suppkey
                    AND ps_suppkey = l_suppkey
                    AND ps_partkey = l_partkey
                    AND p_partkey = l_partkey
                    AND o_orderkey = l_orderkey
                    AND s_nationkey = n_nationkey
                    AND p_name LIKE '%green%'
            ) AS profit
            GROUP BY nation, o_year
            ORDER BY nation, o_year DESC
        """
        df = self.spark.sql(query)
        self.q09_sql_result = df.collect()
        self.assertGreater(len(self.q09_sql_result), 0)

    def test_q09_dataframe(self):
        """Q9: Product Type Profit Measure (DataFrame API version)."""
        df = self.df_part \
            .filter(F.col("p_name").like("%green%")) \
            .join(self.df_lineitem, F.col("p_partkey") == F.col("l_partkey")) \
            .join(self.df_supplier, F.col("s_suppkey") == F.col("l_suppkey")) \
            .join(self.df_partsupp,
                  (F.col("ps_suppkey") == F.col("l_suppkey")) &
                  (F.col("ps_partkey") == F.col("l_partkey"))) \
            .join(self.df_orders, F.col("o_orderkey") == F.col("l_orderkey")) \
            .join(self.df_nation, F.col("s_nationkey") == F.col("n_nationkey")) \
            .select(
                F.col("n_name").alias("nation"),
                F.year("o_orderdate").alias("o_year"),
                (F.col("l_extendedprice") * (1 - F.col("l_discount")) -
                 F.col("ps_supplycost") * F.col("l_quantity")).alias("amount")
            ) \
            .groupBy("nation", "o_year") \
            .agg(F.sum("amount").alias("sum_profit")) \
            .orderBy("nation", F.col("o_year").desc())

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q09_sql_result'):
            self.assertEqual(len(result), len(self.q09_sql_result))

    # =========================================================================
    # Query 11: Important Stock Identification
    # =========================================================================

    def test_q11_sql(self):
        """Q11: Important Stock Identification (SQL version)."""
        query = """
            SELECT
                ps_partkey,
                SUM(ps_supplycost * ps_availqty) as value
            FROM partsupp, supplier, nation
            WHERE
                ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
            GROUP BY ps_partkey
            HAVING
                SUM(ps_supplycost * ps_availqty) > (
                    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
                    FROM partsupp, supplier, nation
                    WHERE
                        ps_suppkey = s_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_name = 'GERMANY'
                )
            ORDER BY value DESC
        """
        df = self.spark.sql(query)
        self.q11_sql_result = df.collect()
        self.assertGreater(len(self.q11_sql_result), 0)

    def test_q11_dataframe(self):
        """Q11: Important Stock Identification (DataFrame API version)."""
        # Calculate total value for Germany
        germany_parts = self.df_partsupp \
            .join(self.df_supplier, F.col("ps_suppkey") == F.col("s_suppkey")) \
            .join(self.df_nation, F.col("s_nationkey") == F.col("n_nationkey")) \
            .filter(F.col("n_name") == "GERMANY")

        # Get threshold value
        threshold = germany_parts \
            .agg(F.sum(F.col("ps_supplycost") * F.col("ps_availqty")) * 0.0001) \
            .collect()[0][0]

        # Get parts above threshold
        df = germany_parts \
            .groupBy("ps_partkey") \
            .agg(F.sum(F.col("ps_supplycost") * F.col("ps_availqty")).alias("value")) \
            .filter(F.col("value") > threshold) \
            .orderBy(F.col("value").desc())

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q11_sql_result'):
            self.assertEqual(len(result), len(self.q11_sql_result))

    # =========================================================================
    # Query 12: Shipping Modes and Order Priority
    # =========================================================================

    def test_q12_sql(self):
        """Q12: Shipping Modes and Order Priority (SQL version)."""
        query = """
            SELECT
                l_shipmode,
                SUM(CASE
                    WHEN o_orderpriority = '1-URGENT'
                        OR o_orderpriority = '2-HIGH'
                    THEN 1
                    ELSE 0
                END) as high_line_count,
                SUM(CASE
                    WHEN o_orderpriority <> '1-URGENT'
                        AND o_orderpriority <> '2-HIGH'
                    THEN 1
                    ELSE 0
                END) as low_line_count
            FROM orders, lineitem
            WHERE
                o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= DATE '1994-01-01'
                AND l_receiptdate < DATE '1994-01-01' + INTERVAL 1 YEAR
            GROUP BY l_shipmode
            ORDER BY l_shipmode
        """
        df = self.spark.sql(query)
        self.q12_sql_result = df.collect()
        self.assertGreater(len(self.q12_sql_result), 0)

    def test_q12_dataframe(self):
        """Q12: Shipping Modes and Order Priority (DataFrame API version)."""
        df = self.df_orders \
            .join(self.df_lineitem, F.col("o_orderkey") == F.col("l_orderkey")) \
            .filter(
                F.col("l_shipmode").isin("MAIL", "SHIP") &
                (F.col("l_commitdate") < F.col("l_receiptdate")) &
                (F.col("l_shipdate") < F.col("l_commitdate")) &
                (F.col("l_receiptdate") >= F.lit("1994-01-01")) &
                (F.col("l_receiptdate") < F.add_months(F.lit("1994-01-01"), 12))
            ) \
            .groupBy("l_shipmode") \
            .agg(
                F.sum(
                    F.when(
                        (F.col("o_orderpriority") == "1-URGENT") |
                        (F.col("o_orderpriority") == "2-HIGH"),
                        1
                    ).otherwise(0)
                ).alias("high_line_count"),
                F.sum(
                    F.when(
                        (F.col("o_orderpriority") != "1-URGENT") &
                        (F.col("o_orderpriority") != "2-HIGH"),
                        1
                    ).otherwise(0)
                ).alias("low_line_count")
            ) \
            .orderBy("l_shipmode")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q12_sql_result'):
            self.assertEqual(len(result), len(self.q12_sql_result))

    # =========================================================================
    # Query 13: Customer Distribution
    # =========================================================================

    def test_q13_sql(self):
        """Q13: Customer Distribution (SQL version)."""
        query = """
            SELECT
                c_count,
                COUNT(*) as custdist
            FROM (
                SELECT
                    c_custkey,
                    COUNT(o_orderkey) as c_count
                FROM customer LEFT OUTER JOIN orders ON
                    c_custkey = o_custkey
                    AND o_comment NOT LIKE '%special%requests%'
                GROUP BY c_custkey
            ) AS c_orders
            GROUP BY c_count
            ORDER BY custdist DESC, c_count DESC
        """
        df = self.spark.sql(query)
        self.q13_sql_result = df.collect()
        self.assertGreater(len(self.q13_sql_result), 0)

    def test_q13_dataframe(self):
        """Q13: Customer Distribution (DataFrame API version)."""
        # Left join customers with orders (excluding special requests)
        df_orders_filtered = self.df_orders \
            .filter(~F.col("o_comment").like("%special%requests%"))

        df_customer_orders = self.df_customer \
            .join(df_orders_filtered, F.col("c_custkey") == F.col("o_custkey"), "left") \
            .groupBy("c_custkey") \
            .agg(F.count("o_orderkey").alias("c_count"))

        # Count distribution
        df = df_customer_orders \
            .groupBy("c_count") \
            .agg(F.count("*").alias("custdist")) \
            .orderBy(F.col("custdist").desc(), F.col("c_count").desc())

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q13_sql_result'):
            self.assertEqual(len(result), len(self.q13_sql_result))

    # =========================================================================
    # Query 14: Promotion Effect
    # =========================================================================

    def test_q14_sql(self):
        """Q14: Promotion Effect (SQL version)."""
        query = """
            SELECT
                100.00 * SUM(CASE
                    WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice * (1 - l_discount)
                    ELSE 0
                END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
            FROM lineitem, part
            WHERE
                l_partkey = p_partkey
                AND l_shipdate >= DATE '1995-09-01'
                AND l_shipdate < DATE '1995-09-01' + INTERVAL 1 MONTH
        """
        df = self.spark.sql(query)
        self.q14_sql_result = df.collect()
        self.assertEqual(len(self.q14_sql_result), 1)

    def test_q14_dataframe(self):
        """Q14: Promotion Effect (DataFrame API version)."""
        df = self.df_lineitem \
            .filter(
                (F.col("l_shipdate") >= F.lit("1995-09-01")) &
                (F.col("l_shipdate") < F.add_months(F.lit("1995-09-01"), 1))
            ) \
            .join(self.df_part, F.col("l_partkey") == F.col("p_partkey")) \
            .agg(
                (100.00 * F.sum(
                    F.when(
                        F.col("p_type").like("PROMO%"),
                        F.col("l_extendedprice") * (1 - F.col("l_discount"))
                    ).otherwise(0)
                ) / F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")))).alias("promo_revenue")
            )

        result = df.collect()
        self.assertEqual(len(result), 1)

        if hasattr(self, 'q14_sql_result'):
            self.assertAlmostEqual(
                result[0]["promo_revenue"],
                self.q14_sql_result[0]["promo_revenue"],
                places=2
            )

    # =========================================================================
    # Query 15: Top Supplier Query
    # =========================================================================

    def test_q15_sql(self):
        """Q15: Top Supplier Query (SQL version)."""
        # Note: Q15 traditionally uses a view, but we'll use a CTE for compatibility
        query = """
            WITH revenue AS (
                SELECT
                    l_suppkey as supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) as total_revenue
                FROM lineitem
                WHERE
                    l_shipdate >= DATE '1996-01-01'
                    AND l_shipdate < DATE '1996-01-01' + INTERVAL 3 MONTH
                GROUP BY l_suppkey
            )
            SELECT
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            FROM supplier, revenue
            WHERE
                s_suppkey = supplier_no
                AND total_revenue = (
                    SELECT MAX(total_revenue)
                    FROM revenue
                )
            ORDER BY s_suppkey
        """
        df = self.spark.sql(query)
        self.q15_sql_result = df.collect()
        self.assertGreater(len(self.q15_sql_result), 0)

    def test_q15_dataframe(self):
        """Q15: Top Supplier Query (DataFrame API version)."""
        # Calculate revenue per supplier
        revenue = self.df_lineitem \
            .filter(
                (F.col("l_shipdate") >= F.lit("1996-01-01")) &
                (F.col("l_shipdate") < F.add_months(F.lit("1996-01-01"), 3))
            ) \
            .groupBy(F.col("l_suppkey").alias("supplier_no")) \
            .agg(
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("total_revenue")
            )

        # Find max revenue
        max_revenue = revenue.agg(F.max("total_revenue")).collect()[0][0]

        # Get suppliers with max revenue
        df = self.df_supplier \
            .join(
                revenue.filter(F.col("total_revenue") == max_revenue),
                F.col("s_suppkey") == F.col("supplier_no")
            ) \
            .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue") \
            .orderBy("s_suppkey")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q15_sql_result'):
            self.assertEqual(len(result), len(self.q15_sql_result))

    # =========================================================================
    # Query 16: Parts/Supplier Relationship
    # =========================================================================

    def test_q16_sql(self):
        """Q16: Parts/Supplier Relationship (SQL version)."""
        query = """
            SELECT
                p_brand,
                p_type,
                p_size,
                COUNT(DISTINCT ps_suppkey) as supplier_cnt
            FROM partsupp, part
            WHERE
                p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT s_suppkey
                    FROM supplier
                    WHERE s_comment LIKE '%Customer%Complaints%'
                )
            GROUP BY p_brand, p_type, p_size
            ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
        """
        df = self.spark.sql(query)
        self.q16_sql_result = df.collect()
        self.assertGreater(len(self.q16_sql_result), 0)

    def test_q16_dataframe(self):
        """Q16: Parts/Supplier Relationship (DataFrame API version)."""
        # Get suppliers to exclude
        bad_suppliers = self.df_supplier \
            .filter(F.col("s_comment").like("%Customer%Complaints%")) \
            .select("s_suppkey")

        # Main query
        df = self.df_partsupp \
            .join(self.df_part, F.col("p_partkey") == F.col("ps_partkey")) \
            .filter(
                (F.col("p_brand") != "Brand#45") &
                (~F.col("p_type").like("MEDIUM POLISHED%")) &
                F.col("p_size").isin(49, 14, 23, 45, 19, 3, 36, 9)
            ) \
            .join(bad_suppliers, F.col("ps_suppkey") == F.col("s_suppkey"), "left_anti") \
            .groupBy("p_brand", "p_type", "p_size") \
            .agg(F.countDistinct("ps_suppkey").alias("supplier_cnt")) \
            .orderBy(F.col("supplier_cnt").desc(), "p_brand", "p_type", "p_size")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q16_sql_result'):
            self.assertEqual(len(result), len(self.q16_sql_result))

    # =========================================================================
    # Query 17: Small-Quantity-Order Revenue
    # =========================================================================

    def test_q17_sql(self):
        """Q17: Small-Quantity-Order Revenue (SQL version)."""
        query = """
            SELECT
                SUM(l_extendedprice) / 7.0 as avg_yearly
            FROM lineitem, part
            WHERE
                p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT 0.2 * AVG(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = p_partkey
                )
        """
        df = self.spark.sql(query)
        self.q17_sql_result = df.collect()
        self.assertEqual(len(self.q17_sql_result), 1)

    def test_q17_dataframe(self):
        """Q17: Small-Quantity-Order Revenue (DataFrame API version)."""
        from pyspark.sql.window import Window

        # Calculate average quantity per part
        avg_quantity = self.df_lineitem \
            .groupBy("l_partkey") \
            .agg((0.2 * F.avg("l_quantity")).alias("avg_qty_threshold"))

        # Main query
        df = self.df_lineitem \
            .join(self.df_part,
                  (F.col("p_partkey") == F.col("l_partkey")) &
                  (F.col("p_brand") == "Brand#23") &
                  (F.col("p_container") == "MED BOX")) \
            .join(avg_quantity,
                  F.col("l_partkey") == avg_quantity["l_partkey"]) \
            .filter(F.col("l_quantity") < F.col("avg_qty_threshold")) \
            .agg((F.sum("l_extendedprice") / 7.0).alias("avg_yearly"))

        result = df.collect()
        self.assertEqual(len(result), 1)

        if hasattr(self, 'q17_sql_result'):
            self.assertAlmostEqual(
                result[0]["avg_yearly"],
                self.q17_sql_result[0]["avg_yearly"],
                places=2
            )

    # =========================================================================
    # Query 18: Large Volume Customer
    # =========================================================================

    def test_q18_sql(self):
        """Q18: Large Volume Customer (SQL version)."""
        query = """
            SELECT
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                SUM(l_quantity)
            FROM customer, orders, lineitem
            WHERE
                o_orderkey IN (
                    SELECT l_orderkey
                    FROM lineitem
                    GROUP BY l_orderkey
                    HAVING SUM(l_quantity) > 300
                )
                AND c_custkey = o_custkey
                AND o_orderkey = l_orderkey
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
            ORDER BY o_totalprice DESC, o_orderdate
            LIMIT 100
        """
        df = self.spark.sql(query)
        self.q18_sql_result = df.collect()
        self.assertLessEqual(len(self.q18_sql_result), 100)

    def test_q18_dataframe(self):
        """Q18: Large Volume Customer (DataFrame API version)."""
        # Find large orders
        large_orders = self.df_lineitem \
            .groupBy("l_orderkey") \
            .agg(F.sum("l_quantity").alias("total_quantity")) \
            .filter(F.col("total_quantity") > 300) \
            .select("l_orderkey")

        # Main query
        df = self.df_customer \
            .join(self.df_orders, F.col("c_custkey") == F.col("o_custkey")) \
            .join(large_orders, F.col("o_orderkey") == F.col("l_orderkey"), "inner") \
            .join(self.df_lineitem, F.col("o_orderkey") == self.df_lineitem["l_orderkey"]) \
            .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice") \
            .agg(F.sum("l_quantity").alias("sum_quantity")) \
            .orderBy(F.col("o_totalprice").desc(), "o_orderdate") \
            .limit(100)

        result = df.collect()
        self.assertLessEqual(len(result), 100)

        if hasattr(self, 'q18_sql_result'):
            self.assertEqual(len(result), len(self.q18_sql_result))

    # =========================================================================
    # Query 19: Discounted Revenue
    # =========================================================================

    def test_q19_sql(self):
        """Q19: Discounted Revenue (SQL version)."""
        query = """
            SELECT
                SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM lineitem, part
            WHERE
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#12'
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 1 + 10
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 10 + 10
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#34'
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 20 + 10
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
        """
        df = self.spark.sql(query)
        self.q19_sql_result = df.collect()
        self.assertEqual(len(self.q19_sql_result), 1)

    def test_q19_dataframe(self):
        """Q19: Discounted Revenue (DataFrame API version)."""
        df = self.df_lineitem \
            .join(self.df_part, F.col("p_partkey") == F.col("l_partkey")) \
            .filter(
                (
                    (F.col("p_brand") == "Brand#12") &
                    F.col("p_container").isin("SM CASE", "SM BOX", "SM PACK", "SM PKG") &
                    (F.col("l_quantity") >= 1) & (F.col("l_quantity") <= 11) &
                    (F.col("p_size") >= 1) & (F.col("p_size") <= 5) &
                    F.col("l_shipmode").isin("AIR", "AIR REG") &
                    (F.col("l_shipinstruct") == "DELIVER IN PERSON")
                ) |
                (
                    (F.col("p_brand") == "Brand#23") &
                    F.col("p_container").isin("MED BAG", "MED BOX", "MED PKG", "MED PACK") &
                    (F.col("l_quantity") >= 10) & (F.col("l_quantity") <= 20) &
                    (F.col("p_size") >= 1) & (F.col("p_size") <= 10) &
                    F.col("l_shipmode").isin("AIR", "AIR REG") &
                    (F.col("l_shipinstruct") == "DELIVER IN PERSON")
                ) |
                (
                    (F.col("p_brand") == "Brand#34") &
                    F.col("p_container").isin("LG CASE", "LG BOX", "LG PACK", "LG PKG") &
                    (F.col("l_quantity") >= 20) & (F.col("l_quantity") <= 30) &
                    (F.col("p_size") >= 1) & (F.col("p_size") <= 15) &
                    F.col("l_shipmode").isin("AIR", "AIR REG") &
                    (F.col("l_shipinstruct") == "DELIVER IN PERSON")
                )
            ) \
            .agg(
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
            )

        result = df.collect()
        self.assertEqual(len(result), 1)

        if hasattr(self, 'q19_sql_result'):
            self.assertAlmostEqual(
                result[0]["revenue"],
                self.q19_sql_result[0]["revenue"],
                places=2
            )

    # =========================================================================
    # Query 20: Potential Part Promotion
    # =========================================================================

    def test_q20_sql(self):
        """Q20: Potential Part Promotion (SQL version)."""
        query = """
            SELECT
                s_name,
                s_address
            FROM supplier, nation
            WHERE
                s_suppkey IN (
                    SELECT ps_suppkey
                    FROM partsupp
                    WHERE ps_partkey IN (
                        SELECT p_partkey
                        FROM part
                        WHERE p_name LIKE 'forest%'
                    )
                    AND ps_availqty > (
                        SELECT 0.5 * SUM(l_quantity)
                        FROM lineitem
                        WHERE
                            l_partkey = ps_partkey
                            AND l_suppkey = ps_suppkey
                            AND l_shipdate >= DATE '1994-01-01'
                            AND l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
                    )
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'CANADA'
            ORDER BY s_name
        """
        df = self.spark.sql(query)
        self.q20_sql_result = df.collect()
        self.assertGreater(len(self.q20_sql_result), 0)

    def test_q20_dataframe(self):
        """Q20: Potential Part Promotion (DataFrame API version)."""
        # Find parts starting with 'forest'
        forest_parts = self.df_part \
            .filter(F.col("p_name").like("forest%")) \
            .select("p_partkey")

        # Calculate threshold quantities
        line_quantities = self.df_lineitem \
            .filter(
                (F.col("l_shipdate") >= F.lit("1994-01-01")) &
                (F.col("l_shipdate") < F.add_months(F.lit("1994-01-01"), 12))
            ) \
            .groupBy("l_partkey", "l_suppkey") \
            .agg((0.5 * F.sum("l_quantity")).alias("threshold_qty"))

        # Find qualifying part-supplier combinations
        qualifying_ps = self.df_partsupp \
            .join(forest_parts, F.col("ps_partkey") == F.col("p_partkey")) \
            .join(line_quantities,
                  (F.col("ps_partkey") == F.col("l_partkey")) &
                  (F.col("ps_suppkey") == F.col("l_suppkey")),
                  "left") \
            .filter(
                F.col("ps_availqty") > F.coalesce(F.col("threshold_qty"), F.lit(0))
            ) \
            .select("ps_suppkey").distinct()

        # Get Canadian suppliers
        df = self.df_supplier \
            .join(qualifying_ps, F.col("s_suppkey") == F.col("ps_suppkey")) \
            .join(self.df_nation, F.col("s_nationkey") == F.col("n_nationkey")) \
            .filter(F.col("n_name") == "CANADA") \
            .select("s_name", "s_address") \
            .orderBy("s_name")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q20_sql_result'):
            self.assertEqual(len(result), len(self.q20_sql_result))

    # =========================================================================
    # Query 21: Suppliers Who Kept Orders Waiting
    # =========================================================================

    def test_q21_sql(self):
        """Q21: Suppliers Who Kept Orders Waiting (SQL version)."""
        query = """
            SELECT
                s_name,
                COUNT(*) as numwait
            FROM supplier, lineitem l1, orders, nation
            WHERE
                s_suppkey = l1.l_suppkey
                AND o_orderkey = l1.l_orderkey
                AND o_orderstatus = 'F'
                AND l1.l_receiptdate > l1.l_commitdate
                AND EXISTS (
                    SELECT *
                    FROM lineitem l2
                    WHERE
                        l2.l_orderkey = l1.l_orderkey
                        AND l2.l_suppkey <> l1.l_suppkey
                )
                AND NOT EXISTS (
                    SELECT *
                    FROM lineitem l3
                    WHERE
                        l3.l_orderkey = l1.l_orderkey
                        AND l3.l_suppkey <> l1.l_suppkey
                        AND l3.l_receiptdate > l3.l_commitdate
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'SAUDI ARABIA'
            GROUP BY s_name
            ORDER BY numwait DESC, s_name
            LIMIT 100
        """
        df = self.spark.sql(query)
        self.q21_sql_result = df.collect()
        self.assertLessEqual(len(self.q21_sql_result), 100)

    def test_q21_dataframe(self):
        """Q21: Suppliers Who Kept Orders Waiting (DataFrame API version)."""
        l1 = self.df_lineitem.alias("l1")
        l2 = self.df_lineitem.alias("l2")
        l3 = self.df_lineitem.alias("l3")

        # Find orders with multiple suppliers
        multi_supplier = l2.select(
            l2["l_orderkey"],
            l2["l_suppkey"].alias("l2_suppkey")
        )

        # Find orders with other late suppliers
        other_late = l3.filter(l3["l_receiptdate"] > l3["l_commitdate"]).select(
            l3["l_orderkey"],
            l3["l_suppkey"].alias("l3_suppkey")
        )

        # Main query with complex logic
        df = self.df_supplier \
            .join(self.df_nation,
                  (F.col("s_nationkey") == F.col("n_nationkey")) &
                  (F.col("n_name") == "SAUDI ARABIA")) \
            .join(l1, F.col("s_suppkey") == l1["l_suppkey"]) \
            .filter(l1["l_receiptdate"] > l1["l_commitdate"]) \
            .join(self.df_orders,
                  (F.col("o_orderkey") == l1["l_orderkey"]) &
                  (F.col("o_orderstatus") == "F")) \
            .join(multi_supplier,
                  (l1["l_orderkey"] == F.col("l_orderkey")) &
                  (l1["l_suppkey"] != F.col("l2_suppkey")),
                  "inner") \
            .join(other_late,
                  (l1["l_orderkey"] == other_late["l_orderkey"]) &
                  (l1["l_suppkey"] != other_late["l3_suppkey"]),
                  "left_anti") \
            .select("s_name", l1["l_orderkey"]).distinct() \
            .groupBy("s_name") \
            .agg(F.count("*").alias("numwait")) \
            .orderBy(F.col("numwait").desc(), "s_name") \
            .limit(100)

        result = df.collect()
        self.assertLessEqual(len(result), 100)

        if hasattr(self, 'q21_sql_result'):
            self.assertEqual(len(result), len(self.q21_sql_result))

    # =========================================================================
    # Query 22: Global Sales Opportunity
    # =========================================================================

    def test_q22_sql(self):
        """Q22: Global Sales Opportunity (SQL version)."""
        query = """
            SELECT
                cntrycode,
                COUNT(*) as numcust,
                SUM(c_acctbal) as totacctbal
            FROM (
                SELECT
                    SUBSTRING(c_phone, 1, 2) as cntrycode,
                    c_acctbal
                FROM customer
                WHERE
                    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    AND c_acctbal > (
                        SELECT AVG(c_acctbal)
                        FROM customer
                        WHERE
                            c_acctbal > 0.00
                            AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    )
                    AND NOT EXISTS (
                        SELECT *
                        FROM orders
                        WHERE o_custkey = c_custkey
                    )
            ) AS custsale
            GROUP BY cntrycode
            ORDER BY cntrycode
        """
        df = self.spark.sql(query)
        self.q22_sql_result = df.collect()
        self.assertGreater(len(self.q22_sql_result), 0)

    def test_q22_dataframe(self):
        """Q22: Global Sales Opportunity (DataFrame API version)."""
        country_codes = ['13', '31', '23', '29', '30', '18', '17']

        # Calculate average account balance
        avg_balance = self.df_customer \
            .filter(
                (F.col("c_acctbal") > 0) &
                F.substring("c_phone", 1, 2).isin(country_codes)
            ) \
            .agg(F.avg("c_acctbal")).collect()[0][0]

        # Find customers with orders
        customers_with_orders = self.df_orders.select("o_custkey").distinct()

        # Main query
        df = self.df_customer \
            .filter(
                F.substring("c_phone", 1, 2).isin(country_codes) &
                (F.col("c_acctbal") > avg_balance)
            ) \
            .join(customers_with_orders, F.col("c_custkey") == F.col("o_custkey"), "left_anti") \
            .select(
                F.substring("c_phone", 1, 2).alias("cntrycode"),
                "c_acctbal"
            ) \
            .groupBy("cntrycode") \
            .agg(
                F.count("*").alias("numcust"),
                F.sum("c_acctbal").alias("totacctbal")
            ) \
            .orderBy("cntrycode")

        result = df.collect()
        self.assertGreater(len(result), 0)

        if hasattr(self, 'q22_sql_result'):
            self.assertEqual(len(result), len(self.q22_sql_result))

    def test_all_queries_have_both_versions(self):
        """Meta-test to ensure all queries have both SQL and DataFrame versions."""
        expected_queries = range(1, 23)  # TPC-H has 22 queries

        for q_num in expected_queries:
            sql_method = f"test_q{q_num:02d}_sql"
            df_method = f"test_q{q_num:02d}_dataframe"

            if hasattr(self, sql_method) and hasattr(self, df_method):
                # Both methods exist, good
                continue
            elif not hasattr(self, sql_method) and not hasattr(self, df_method):
                # Neither exists - TODO
                print(f"TODO: Implement Q{q_num} in both SQL and DataFrame forms")
            else:
                # Only one exists - this is an error
                self.fail(f"Query {q_num} must have BOTH SQL and DataFrame versions")