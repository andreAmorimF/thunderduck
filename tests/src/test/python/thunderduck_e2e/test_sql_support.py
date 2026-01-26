"""SQL support tests for Thunderduck.

Tests SQL expression strings and spark.sql() queries.
"""

from thunderduck_e2e.test_runner import ThunderduckE2ETestBase
from pyspark.sql import functions as F


class TestSQLSupport(ThunderduckE2ETestBase):
    """Test SQL support: spark.sql() and SQL expression strings."""

    def test_simple_sql_query(self):
        """Test simple spark.sql() query."""
        result = self.spark.sql("SELECT 1 AS id, 'hello' AS message")
        rows = result.collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['id'], 1)
        self.assertEqual(rows[0]['message'], 'hello')

    def test_sql_with_temp_view(self):
        """Test spark.sql() with temp view."""
        result = self.spark.sql("SELECT COUNT(*) as count FROM employees")
        rows = result.collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['count'], 5)

    def test_sql_with_where(self):
        """Test spark.sql() with WHERE clause."""
        result = self.spark.sql(
            "SELECT name, salary FROM employees WHERE salary > 70000 ORDER BY salary DESC"
        )
        rows = result.collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['name'], 'Bob')    # 80000
        self.assertEqual(rows[1]['name'], 'John')   # 75000

    def test_sql_with_aggregation(self):
        """Test spark.sql() with aggregation."""
        result = self.spark.sql("""
            SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
            ORDER BY count DESC
        """)
        rows = result.collect()

        self.assertEqual(len(rows), 3)
        # Engineering has 3 employees
        eng_dept = [r for r in rows if r['department'] == 'Engineering'][0]
        self.assertEqual(eng_dept['count'], 3)
        self.assertEqual(eng_dept['avg_salary'], 75000.0)

    def test_sql_with_join(self):
        """Test spark.sql() with JOIN."""
        result = self.spark.sql("""
            SELECT e.name, e.salary, d.location
            FROM employees e
            JOIN departments d ON e.department = d.name
            WHERE e.salary > 60000
            ORDER BY e.salary DESC
        """)
        rows = result.collect()

        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]['name'], 'Bob')
        self.assertEqual(rows[0]['location'], 'Building A')

    def test_filter_with_sql_expression_string(self):
        """Test df.filter() with SQL expression string."""
        df = self.spark.table("employees")

        # Test simple comparison
        result = df.filter("salary > 70000")
        self.assertEqual(result.count(), 2)

        # Test complex expression
        result = df.filter("salary > 60000 AND department = 'Engineering'")
        self.assertEqual(result.count(), 3)

    def test_selectExpr_with_sql_expressions(self):
        """Test df.selectExpr() with SQL expressions."""
        df = self.spark.table("employees")

        # Test arithmetic expression
        result = df.selectExpr("name", "salary", "salary * 1.1 as salary_with_raise")
        rows = result.collect()

        self.assertEqual(len(rows), 5)
        john = [r for r in rows if r['name'] == 'John'][0]
        self.assertEqual(john['salary'], 75000)
        self.assertAlmostEqual(john['salary_with_raise'], 82500.0, places=2)

    def test_selectExpr_with_sql_functions(self):
        """Test df.selectExpr() with SQL functions."""
        df = self.spark.table("employees")

        # Test string functions
        result = df.selectExpr("id", "upper(name) as upper_name", "lower(department) as lower_dept")
        rows = result.collect()

        self.assertEqual(len(rows), 5)
        john = [r for r in rows if r['id'] == 1][0]
        self.assertEqual(john['upper_name'], 'JOHN')
        self.assertEqual(john['lower_dept'], 'engineering')

    def test_sql_case_expression(self):
        """Test spark.sql() with CASE expression."""
        result = self.spark.sql("""
            SELECT name, salary,
                   CASE
                       WHEN salary >= 80000 THEN 'high'
                       WHEN salary >= 70000 THEN 'medium'
                       ELSE 'low'
                   END as salary_band
            FROM employees
            ORDER BY salary DESC
        """)
        rows = result.collect()

        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0]['name'], 'Bob')
        self.assertEqual(rows[0]['salary_band'], 'high')
        self.assertEqual(rows[1]['name'], 'John')
        self.assertEqual(rows[1]['salary_band'], 'medium')

    def test_sql_with_subquery(self):
        """Test spark.sql() with subquery."""
        result = self.spark.sql("""
            SELECT name, salary
            FROM employees
            WHERE salary > (SELECT AVG(salary) FROM employees)
            ORDER BY salary DESC
        """)
        rows = result.collect()

        # Average is 69000, so Bob (80000), John (75000), and Charlie (70000) should be returned
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]['name'], 'Bob')
        self.assertEqual(rows[1]['name'], 'John')
        self.assertEqual(rows[2]['name'], 'Charlie')

    def test_combined_sql_and_dataframe_api(self):
        """Test combining spark.sql() with DataFrame operations."""
        # Start with SQL
        df = self.spark.sql("SELECT * FROM employees WHERE department = 'Engineering'")

        # Continue with DataFrame API using SQL expression
        result = df.filter("salary >= 75000").selectExpr("name", "salary * 12 as annual_salary")
        rows = result.collect()

        self.assertEqual(len(rows), 2)  # John and Bob
        bob = [r for r in rows if r['name'] == 'Bob'][0]
        self.assertEqual(bob['annual_salary'], 960000)
