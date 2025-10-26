"""
TPC-H Tier 1 & Tier 2 Query Tests

Tests additional queries required for Week 13 Phase 3 completion.
"""

import pytest


@pytest.mark.tpch
@pytest.mark.timeout(60)
class TestTPCHQuery13:
    """TPC-H Q13: Customer Distribution (Tier 1)"""

    def test_q13_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q13 via SQL"""
        query = load_tpch_query(13)
        result = spark.sql(query)
        rows = result.collect()

        assert len(rows) > 0, "Q13 should return results"
        print(f"\n✓ TPC-H Q13 (SQL) passed: {len(rows)} rows returned")


@pytest.mark.tpch
@pytest.mark.timeout(60)
class TestTPCHQuery5:
    """TPC-H Q5: Local Supplier Volume (Tier 2)"""

    def test_q5_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q5 via SQL"""
        query = load_tpch_query(5)
        result = spark.sql(query)
        rows = result.collect()

        assert len(rows) > 0, "Q5 should return results"
        print(f"\n✓ TPC-H Q5 (SQL) passed: {len(rows)} rows returned")


@pytest.mark.tpch
@pytest.mark.timeout(60)
class TestTPCHQuery10:
    """TPC-H Q10: Returned Item Reporting (Tier 2)"""

    def test_q10_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q10 via SQL"""
        query = load_tpch_query(10)
        result = spark.sql(query)
        rows = result.collect()

        assert len(rows) > 0, "Q10 should return results"
        # Q10 has LIMIT 20
        assert len(rows) <= 20, f"Q10 should return at most 20 rows, got {len(rows)}"
        print(f"\n✓ TPC-H Q10 (SQL) passed: {len(rows)} rows returned")


@pytest.mark.tpch
@pytest.mark.timeout(60)
class TestTPCHQuery12:
    """TPC-H Q12: Shipping Modes and Order Priority (Tier 2)"""

    def test_q12_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q12 via SQL"""
        query = load_tpch_query(12)
        result = spark.sql(query)
        rows = result.collect()

        assert len(rows) > 0, "Q12 should return results"
        print(f"\n✓ TPC-H Q12 (SQL) passed: {len(rows)} rows returned")


@pytest.mark.tpch
@pytest.mark.timeout(60)
class TestTPCHQuery18:
    """TPC-H Q18: Large Volume Customer (Tier 2)"""

    def test_q18_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q18 via SQL"""
        query = load_tpch_query(18)
        result = spark.sql(query)
        rows = result.collect()

        assert len(rows) > 0, "Q18 should return results"
        # Q18 has LIMIT 100
        assert len(rows) <= 100, f"Q18 should return at most 100 rows, got {len(rows)}"
        print(f"\n✓ TPC-H Q18 (SQL) passed: {len(rows)} rows returned")
