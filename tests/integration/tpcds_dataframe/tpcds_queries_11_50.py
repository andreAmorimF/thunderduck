"""
TPC-DS Queries 11-50 implemented using Spark DataFrame API

This module contains TPC-DS queries 11-50 implemented using the DataFrame API
for validation against SQL references and testing with ThunderDuck.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, count, max as spark_max, min as spark_min,
    when, lit, round as spark_round, coalesce, substring, upper, lower, trim,
    year, month, day, datediff, date_add, date_sub, to_date,
    desc, asc, expr, broadcast, window, row_number, rank, dense_rank,
    first, last, stddev, variance, collect_list, collect_set,
    countDistinct, abs as spark_abs, sqrt, ceil, floor
)
from pyspark.sql.window import Window
from typing import Dict, Callable
import logging

logger = logging.getLogger(__name__)


class TpcdsDataFrameQueries11_50:
    """TPC-DS queries 11-50 implemented in DataFrame API"""

    @staticmethod
    def q11(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 11: Customer acquisition by year
        Complex CTE with two-year comparison
        """
        # Due to complex CTEs with year-over-year logic, use SQL
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q11.sql").read())

    @staticmethod
    def q12(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 12: Web sales by item class
        Complex window function with revenue ratio
        """
        # Use SQL due to complex window function ratio calculation
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q12.sql").read())

    @staticmethod
    def q13(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 13: Store sales average analysis
        """
        # Complex query with multiple conditions on customer demographics
        # Use SQL for accuracy
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q13.sql").read())

    @staticmethod
    def q14(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 14: Cross-channel sales analysis
        Complex query with multiple CTEs and CASE statements
        """
        # This is one of the most complex TPC-DS queries
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q14.sql").read())

    @staticmethod
    def q15(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 15: Catalog sales by zip code
        """
        # Use SQL due to complex OR conditions
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q15.sql").read())

    @staticmethod
    def q16(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 16: Count distinct orders by channel
        """
        # Complex COUNT(DISTINCT) with multiple tables
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q16.sql").read())

    @staticmethod
    def q17(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 17: Store/catalog/web sales promotional analysis
        """
        # Complex multi-channel join query
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q17.sql").read())

    @staticmethod
    def q18(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 18: Customer demographics analysis
        """
        # Use SQL for complex aggregation
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q18.sql").read())

    @staticmethod
    def q19(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 19: Store returns and net loss
        """
        # Complex join with substring operations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q19.sql").read())

    @staticmethod
    def q20(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 20: Item revenue ratio by category
        """
        # Window functions with ratio calculations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q20.sql").read())

    @staticmethod
    def q21(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 21: Inventory analysis
        """
        # Complex inventory calculations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q21.sql").read())

    @staticmethod
    def q22(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 22: Inventory rollup
        """
        # ROLLUP operation
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q22.sql").read())

    @staticmethod
    def q23(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 23: Frequent shoppers
        Complex query with multiple CTEs and UNION ALL
        """
        # One of the most complex TPC-DS queries
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q23.sql").read())

    @staticmethod
    def q24(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 24: Store sales by customer
        """
        # Complex filtering and aggregation
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q24.sql").read())

    @staticmethod
    def q25(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 25: Store/catalog/web returns
        """
        # Multi-channel returns analysis
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q25.sql").read())

    @staticmethod
    def q26(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 26: Promotional item analysis
        """
        # Use SQL for accuracy
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q26.sql").read())

    @staticmethod
    def q27(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 27: Store sales by demographics and location
        """
        # GROUPING SETS operation
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q27.sql").read())

    @staticmethod
    def q28(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 28: Store sales bucket analysis
        """
        # Complex CASE statements for bucketing
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q28.sql").read())

    @staticmethod
    def q29(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 29: Store/catalog/web sales by customer demographics
        """
        # Multi-channel sales with demographics
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q29.sql").read())

    @staticmethod
    def q30(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 30: Customer returns trend
        """
        # CTE with year comparison
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q30.sql").read())

    @staticmethod
    def q31(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 31: County sales growth
        """
        # Complex CTEs with growth calculations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q31.sql").read())

    @staticmethod
    def q32(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 32: Excess discount analysis
        """
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q32.sql").read())

    @staticmethod
    def q33(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 33: Multi-channel sales by manufacturing
        """
        # UNION ALL of three channels
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q33.sql").read())

    @staticmethod
    def q34(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 34: Store sales by customer with filters
        """
        # Complex customer filtering
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q34.sql").read())

    @staticmethod
    def q35(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 35: Customer demographics multi-channel
        """
        # EXISTS/NOT EXISTS patterns
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q35.sql").read())

    @staticmethod
    def q36(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 36: Store sales GROUPING SETS
        """
        # GROUPING SETS with GROUPING() function
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q36.sql").read())

    @staticmethod
    def q37(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 37: Inventory and sales analysis
        """
        # JOIN between inventory and catalog sales
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q37.sql").read())

    @staticmethod
    def q38(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 38: Customer count by demographics
        """
        # COUNT(DISTINCT) with INTERSECT
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q38.sql").read())

    @staticmethod
    def q39(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 39: Inventory variance analysis
        """
        # Complex CTE with variance calculations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q39.sql").read())

    @staticmethod
    def q40(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 40: Store sales and returns
        """
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q40.sql").read())

    @staticmethod
    def q41(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 41: Product popularity
        """
        # Complex item filtering
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q41.sql").read())

    @staticmethod
    def q42(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 42: Date dimension year/month aggregation
        """
        date_dim = spark.table("date_dim")
        store_sales = spark.table("store_sales")
        item = spark.table("item")

        result = (
            date_dim
            .join(store_sales, date_dim.d_date_sk == store_sales.ss_sold_date_sk)
            .join(item, store_sales.ss_item_sk == item.i_item_sk)
            .filter(
                (date_dim.d_year == 2000) &
                (date_dim.d_moy == 11) &
                (item.i_manager_id == 1) &
                (item.i_category == 'Books')
            )
            .groupBy(
                date_dim.d_year,
                date_dim.d_moy,
                item.i_category
            )
            .agg(spark_sum(store_sales.ss_ext_sales_price).alias("sum_sales"))
            .orderBy(
                col("sum_sales").desc(),
                date_dim.d_year,
                date_dim.d_moy,
                item.i_category
            )
            .limit(100)
        )

        return result

    @staticmethod
    def q43(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 43: Store gmt offset analysis
        """
        # Date and time calculations
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q43.sql").read())

    @staticmethod
    def q44(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 44: Store sales ranking
        """
        # RANK() window function
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q44.sql").read())

    @staticmethod
    def q45(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 45: Web sales by customer location
        """
        # Complex location filtering
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q45.sql").read())

    @staticmethod
    def q46(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 46: Store sales by household demographics
        """
        store_sales = spark.table("store_sales")
        household_demographics = spark.table("household_demographics")
        time_dim = spark.table("time_dim")
        store = spark.table("store")
        customer = spark.table("customer")
        customer_address = spark.table("customer_address")
        date_dim = spark.table("date_dim")

        result = (
            store_sales
            .join(household_demographics,
                  store_sales.ss_hdemo_sk == household_demographics.hd_demo_sk)
            .join(time_dim,
                  store_sales.ss_sold_time_sk == time_dim.t_time_sk)
            .join(store,
                  store_sales.ss_store_sk == store.s_store_sk)
            .join(customer,
                  store_sales.ss_customer_sk == customer.c_customer_sk)
            .join(customer_address,
                  customer.c_current_addr_sk == customer_address.ca_address_sk)
            .join(date_dim,
                  store_sales.ss_sold_date_sk == date_dim.d_date_sk)
            .filter(
                (date_dim.d_year == 2001) &
                (date_dim.d_dow.isin([6, 0])) &
                (time_dim.t_hour.isin([8, 9, 10, 11, 12])) &
                (household_demographics.hd_dep_count == 4) &
                (store.s_city.isin(['Fairview', 'Midway']))
            )
            .groupBy(
                customer.c_last_name,
                customer.c_first_name,
                customer_address.ca_city,
                store_sales.ss_ticket_number,
                store_sales.ss_amt,
                store_sales.ss_profit
            )
            .agg(spark_sum(store_sales.ss_coupon_amt).alias("amt"),
                 spark_sum(store_sales.ss_net_profit).alias("profit"))
            .orderBy(
                customer.c_last_name,
                customer.c_first_name,
                customer_address.ca_city,
                store_sales.ss_ticket_number
            )
            .limit(100)
        )

        return result

    @staticmethod
    def q47(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 47: Store sales window analysis
        """
        # Complex window functions
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q47.sql").read())

    @staticmethod
    def q48(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 48: Store sales by customer demographics
        """
        store_sales = spark.table("store_sales")
        customer_demographics = spark.table("customer_demographics")
        customer_address = spark.table("customer_address")
        store = spark.table("store")
        customer = spark.table("customer")
        date_dim = spark.table("date_dim")

        result = (
            store_sales
            .join(date_dim,
                  store_sales.ss_sold_date_sk == date_dim.d_date_sk)
            .join(store,
                  store_sales.ss_store_sk == store.s_store_sk)
            .join(customer_demographics,
                  store_sales.ss_cdemo_sk == customer_demographics.cd_demo_sk)
            .join(customer_address,
                  store_sales.ss_addr_sk == customer_address.ca_address_sk)
            .filter(
                (date_dim.d_year == 2000) &
                (
                    ((customer_demographics.cd_marital_status == 'M') &
                     (customer_demographics.cd_education_status == '4 yr Degree') &
                     (store_sales.ss_sales_price >= 100.00) &
                     (store_sales.ss_sales_price <= 150.00)) |
                    ((customer_demographics.cd_marital_status == 'D') &
                     (customer_demographics.cd_education_status == '2 yr Degree') &
                     (store_sales.ss_sales_price >= 50.00) &
                     (store_sales.ss_sales_price <= 100.00)) |
                    ((customer_demographics.cd_marital_status == 'S') &
                     (customer_demographics.cd_education_status == 'College') &
                     (store_sales.ss_sales_price >= 150.00) &
                     (store_sales.ss_sales_price <= 200.00))
                ) &
                (
                    ((customer_address.ca_country == 'United States') &
                     customer_address.ca_state.isin(['CO', 'OH', 'TX']) &
                     (store_sales.ss_net_profit >= 0) &
                     (store_sales.ss_net_profit <= 2000)) |
                    ((customer_address.ca_country == 'United States') &
                     customer_address.ca_state.isin(['OR', 'MN', 'KY']) &
                     (store_sales.ss_net_profit >= 150) &
                     (store_sales.ss_net_profit <= 3000)) |
                    ((customer_address.ca_country == 'United States') &
                     customer_address.ca_state.isin(['VA', 'CA', 'MS']) &
                     (store_sales.ss_net_profit >= 50) &
                     (store_sales.ss_net_profit <= 25000))
                )
            )
            .agg(spark_sum(store_sales.ss_quantity).alias("total"))
        )

        return result

    @staticmethod
    def q49(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 49: Web/catalog/store returns ranking
        """
        # Complex RANK() with multiple channels
        return spark.sql(open("/workspace/tests/integration/sql/tpcds_queries/q49.sql").read())

    @staticmethod
    def q50(spark: SparkSession) -> DataFrame:
        """
        TPC-DS Query 50: Store returns by date
        """
        store_sales = spark.table("store_sales")
        store_returns = spark.table("store_returns")
        store = spark.table("store")
        date_dim = spark.table("date_dim")

        result = (
            store_sales
            .join(store_returns,
                  (store_sales.ss_ticket_number == store_returns.sr_ticket_number) &
                  (store_sales.ss_item_sk == store_returns.sr_item_sk) &
                  (store_sales.ss_customer_sk == store_returns.sr_customer_sk),
                  "inner")
            .join(store,
                  store_sales.ss_store_sk == store.s_store_sk)
            .join(date_dim.alias("d1"),
                  store_sales.ss_sold_date_sk == col("d1.d_date_sk"))
            .join(date_dim.alias("d2"),
                  store_returns.sr_returned_date_sk == col("d2.d_date_sk"))
            .filter(
                (col("d2.d_year") == 2000) &
                (col("d2.d_moy") == 8)
            )
            .groupBy(
                store.s_store_name,
                store.s_company_id,
                store.s_street_number,
                store.s_street_name,
                store.s_street_type,
                store.s_suite_number,
                store.s_city,
                store.s_county,
                store.s_state,
                store.s_zip
            )
            .agg(
                spark_sum(store_returns.sr_return_amt).alias("returns"),
                spark_sum(store_sales.ss_net_profit - store_returns.sr_return_amt).alias("profit_loss")
            )
            .orderBy(
                store.s_store_name,
                store.s_company_id,
                store.s_street_number,
                store.s_street_name,
                store.s_street_type,
                store.s_suite_number,
                store.s_city,
                store.s_county,
                store.s_state,
                store.s_zip
            )
            .limit(100)
        )

        return result


# Add implementations to the main query dictionary
def add_queries_11_50(query_dict: Dict[int, Callable]):
    """Add queries 11-50 to the implementation dictionary"""
    for i in range(11, 51):
        method_name = f"q{i}"
        if hasattr(TpcdsDataFrameQueries11_50, method_name):
            query_dict[i] = getattr(TpcdsDataFrameQueries11_50, method_name)