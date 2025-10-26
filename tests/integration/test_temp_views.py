"""
Temporary View Tests for Thunderduck Spark Connect

Tests createOrReplaceTempView functionality and SQL queries against temp views.
"""

import pytest


@pytest.mark.tempview
class TestTempViewBasics:
    """Basic temporary view operations"""

    def test_create_temp_view(self, spark, tpch_data_dir):
        """Test creating a simple temporary view"""
        print("\n" + "=" * 80)
        print("TEST: Create Temporary View")
        print("=" * 80)

        # Load data
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")
        df = spark.read.parquet(lineitem_path)

        # Create temp view
        print("\n1. Creating temp view 'lineitem'...")
        df.createOrReplaceTempView("lineitem")
        print("   ✓ Temp view created")

        # Query the view
        print("\n2. Querying temp view...")
        result = spark.sql("SELECT COUNT(*) as cnt FROM lineitem")
        count = result.collect()[0]['cnt']

        print(f"   ✓ Query executed: {count:,} rows")

        # Verify count matches original DataFrame
        original_count = df.count()
        assert count == original_count, f"Count mismatch: view={count}, df={original_count}"

        print(f"\n✓ TEST PASSED: Temp view works correctly")


    def test_create_temp_view_with_filter(self, spark, tpch_data_dir):
        """Test creating temp view from filtered DataFrame"""
        print("\n" + "=" * 80)
        print("TEST: Temp View with Filter")
        print("=" * 80)

        # Load and filter data
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")
        df = spark.read.parquet(lineitem_path)
        filtered = df.filter("l_quantity > 40")

        # Create temp view from filtered DataFrame
        print("\n1. Creating temp view from filtered DataFrame...")
        filtered.createOrReplaceTempView("high_quantity")
        print("   ✓ Temp view 'high_quantity' created")

        # Query the view
        print("\n2. Querying filtered temp view...")
        result = spark.sql("SELECT COUNT(*) as cnt FROM high_quantity")
        count = result.collect()[0]['cnt']

        print(f"   ✓ Query executed: {count:,} rows")

        # Verify count matches filtered DataFrame
        original_count = filtered.count()
        assert count == original_count, f"Count mismatch: view={count}, filtered_df={original_count}"

        print(f"\n✓ TEST PASSED: Filtered temp view works")


    def test_multiple_temp_views(self, spark, tpch_data_dir):
        """Test multiple temporary views in same session"""
        print("\n" + "=" * 80)
        print("TEST: Multiple Temp Views")
        print("=" * 80)

        # Load multiple tables
        orders_path = str(tpch_data_dir / "orders.parquet")
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        orders = spark.read.parquet(orders_path)
        lineitem = spark.read.parquet(lineitem_path)

        # Create multiple temp views
        print("\n1. Creating temp views...")
        orders.createOrReplaceTempView("orders")
        print("   ✓ 'orders' view created")

        lineitem.createOrReplaceTempView("lineitem")
        print("   ✓ 'lineitem' view created")

        # Query joining both views
        print("\n2. Querying join of both temp views...")
        result = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM orders o
            JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        """)
        count = result.collect()[0]['cnt']

        print(f"   ✓ Join executed: {count:,} rows")

        # Verify (all lineitems should match orders)
        lineitem_count = lineitem.count()
        assert count == lineitem_count, f"Expected {lineitem_count} rows, got {count}"

        print(f"\n✓ TEST PASSED: Multiple temp views and join work")


    def test_replace_temp_view(self, spark, tpch_data_dir):
        """Test replacing an existing temporary view"""
        print("\n" + "=" * 80)
        print("TEST: Replace Temp View")
        print("=" * 80)

        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Create initial view
        print("\n1. Creating initial temp view...")
        df1 = spark.read.parquet(lineitem_path)
        df1.createOrReplaceTempView("test_view")
        result1 = spark.sql("SELECT COUNT(*) as cnt FROM test_view")
        count1 = result1.collect()[0]['cnt']
        print(f"   ✓ Initial view: {count1:,} rows")

        # Replace with filtered version
        print("\n2. Replacing temp view with filtered version...")
        df2 = df1.filter("l_quantity > 45")
        df2.createOrReplaceTempView("test_view")  # Should replace
        result2 = spark.sql("SELECT COUNT(*) as cnt FROM test_view")
        count2 = result2.collect()[0]['cnt']
        print(f"   ✓ Replaced view: {count2:,} rows")

        # Verify different counts
        assert count2 < count1, f"Filtered count should be less: {count2} vs {count1}"

        print(f"\n✓ TEST PASSED: View replacement works")


@pytest.mark.tempview
@pytest.mark.tpch
class TestTPCHWithTempViews:
    """TPC-H queries using temporary views"""

    def test_tpch_q1_with_temp_view(self, spark, tpch_data_dir, load_tpch_query, validator):
        """Test TPC-H Q1 using temporary view"""
        print("\n" + "=" * 80)
        print("TEST: TPC-H Q1 with Temp View")
        print("=" * 80)

        # Load lineitem and create temp view
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")
        lineitem = spark.read.parquet(lineitem_path)

        print("\n1. Creating temp view 'lineitem'...")
        lineitem.createOrReplaceTempView("lineitem")
        print("   ✓ Temp view created")

        # Execute TPC-H Q1
        print("\n2. Executing TPC-H Q1...")
        query = load_tpch_query(1)
        result = spark.sql(query)

        # Collect and validate
        rows = result.collect()
        print(f"   ✓ Query executed: {len(rows)} rows returned")

        # Validate results
        validator.validate_row_count(result, 4)

        expected_columns = [
            'l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price',
            'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price',
            'avg_disc', 'count_order'
        ]
        validator.validate_schema(result, expected_columns)

        # Check aggregates are positive
        for row in rows:
            assert row['sum_qty'] > 0
            assert row['sum_base_price'] > 0
            assert row['count_order'] > 0

        print(f"\n✓ TEST PASSED: TPC-H Q1 with temp view executed successfully")


    def test_tpch_q3_with_temp_views(self, spark, tpch_data_dir, load_tpch_query, validator):
        """Test TPC-H Q3 using multiple temporary views"""
        print("\n" + "=" * 80)
        print("TEST: TPC-H Q3 with Multiple Temp Views")
        print("=" * 80)

        # Load all required tables
        customer_path = str(tpch_data_dir / "customer.parquet")
        orders_path = str(tpch_data_dir / "orders.parquet")
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        print("\n1. Creating temp views...")
        spark.read.parquet(customer_path).createOrReplaceTempView("customer")
        print("   ✓ 'customer' view created")

        spark.read.parquet(orders_path).createOrReplaceTempView("orders")
        print("   ✓ 'orders' view created")

        spark.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")
        print("   ✓ 'lineitem' view created")

        # Execute TPC-H Q3
        print("\n2. Executing TPC-H Q3...")
        query = load_tpch_query(3)
        result = spark.sql(query)

        # Collect and validate
        rows = result.collect()
        print(f"   ✓ Query executed: {len(rows)} rows returned")

        # Should return up to 10 rows (LIMIT 10)
        assert len(rows) <= 10, f"Expected at most 10 rows, got {len(rows)}"

        # Validate schema
        expected_columns = ['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']
        validator.validate_schema(result, expected_columns)

        # Validate ordering
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                assert rows[i]['revenue'] >= rows[i + 1]['revenue']

        print(f"\n✓ TEST PASSED: TPC-H Q3 with multiple temp views works")


    def test_tpch_q6_with_temp_view(self, spark, tpch_data_dir, load_tpch_query, validator):
        """Test TPC-H Q6 using temporary view"""
        print("\n" + "=" * 80)
        print("TEST: TPC-H Q6 with Temp View")
        print("=" * 80)

        # Load lineitem and create temp view
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")
        lineitem = spark.read.parquet(lineitem_path)

        print("\n1. Creating temp view 'lineitem'...")
        lineitem.createOrReplaceTempView("lineitem")
        print("   ✓ Temp view created")

        # Execute TPC-H Q6
        print("\n2. Executing TPC-H Q6...")
        query = load_tpch_query(6)
        result = spark.sql(query)

        # Collect and validate
        rows = result.collect()
        print(f"   ✓ Query executed: {len(rows)} rows returned")

        # Should return exactly 1 row
        validator.validate_row_count(result, 1)

        # Validate schema
        expected_columns = ['revenue']
        validator.validate_schema(result, expected_columns)

        # Validate revenue is positive
        revenue = rows[0]['revenue']
        assert revenue > 0, f"Expected positive revenue, got {revenue}"

        print(f"   ✓ Revenue: {revenue:.2f}")
        print(f"\n✓ TEST PASSED: TPC-H Q6 with temp view works")
