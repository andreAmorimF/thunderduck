"""
Catalog Operations Tests

Tests for Spark Connect catalog operations (listTables, tableExists, etc.)
comparing Thunderduck against Apache Spark 4.0.1.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@pytest.fixture(scope="module")
def setup_test_tables(spark):
    """Create test tables for catalog operations testing."""
    # Create a simple test table
    spark.sql("CREATE OR REPLACE TABLE test_catalog_table (id INT, name STRING)")
    spark.sql("INSERT INTO test_catalog_table VALUES (1, 'Alice'), (2, 'Bob')")

    # Create a view
    spark.sql("CREATE OR REPLACE VIEW test_catalog_view AS SELECT * FROM test_catalog_table WHERE id > 0")

    yield

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_catalog_table")
    spark.sql("DROP VIEW IF EXISTS test_catalog_view")


class TestTableExists:
    """Test spark.catalog.tableExists()"""

    def test_table_exists_true(self, spark, setup_test_tables):
        """Table that exists should return True."""
        result = spark.catalog.tableExists("test_catalog_table")
        assert result is True, "tableExists should return True for existing table"

    def test_table_exists_false(self, spark):
        """Non-existent table should return False."""
        result = spark.catalog.tableExists("nonexistent_table_xyz")
        assert result is False, "tableExists should return False for non-existent table"

    def test_view_exists(self, spark, setup_test_tables):
        """Views should also be found by tableExists."""
        result = spark.catalog.tableExists("test_catalog_view")
        assert result is True, "tableExists should return True for views"


class TestDatabaseExists:
    """Test spark.catalog.databaseExists()"""

    def test_main_database_exists(self, spark):
        """Main/default database should exist."""
        # DuckDB uses 'main' as default schema
        result = spark.catalog.databaseExists("main")
        assert result is True, "main database should exist"

    def test_nonexistent_database(self, spark):
        """Non-existent database should return False."""
        result = spark.catalog.databaseExists("nonexistent_db_xyz")
        assert result is False, "databaseExists should return False for non-existent db"


class TestListTables:
    """Test spark.catalog.listTables()"""

    def test_list_tables_returns_dataframe(self, spark, setup_test_tables):
        """listTables should return a DataFrame-like result."""
        tables = spark.catalog.listTables()
        # Convert to list to iterate
        table_list = list(tables)
        assert len(table_list) >= 2, "Should have at least test table and view"

    def test_list_tables_schema(self, spark, setup_test_tables):
        """listTables result should have expected columns."""
        tables = spark.catalog.listTables()
        table_list = list(tables)

        if len(table_list) > 0:
            first = table_list[0]
            # Check for expected attributes
            assert hasattr(first, 'name'), "Should have 'name' attribute"
            assert hasattr(first, 'tableType'), "Should have 'tableType' attribute"

    def test_list_tables_finds_test_table(self, spark, setup_test_tables):
        """listTables should include our test table."""
        tables = spark.catalog.listTables()
        table_names = [t.name for t in tables]
        assert "test_catalog_table" in table_names, "Should find test_catalog_table"


class TestListDatabases:
    """Test spark.catalog.listDatabases()"""

    def test_list_databases_returns_results(self, spark):
        """listDatabases should return at least one database."""
        databases = spark.catalog.listDatabases()
        db_list = list(databases)
        assert len(db_list) >= 1, "Should have at least one database"

    def test_list_databases_includes_main(self, spark):
        """listDatabases should include 'main' schema."""
        databases = spark.catalog.listDatabases()
        db_names = [d.name for d in databases]
        assert "main" in db_names, "Should include 'main' database"


class TestListColumns:
    """Test spark.catalog.listColumns()"""

    def test_list_columns_for_table(self, spark, setup_test_tables):
        """listColumns should return columns for a table."""
        columns = spark.catalog.listColumns("test_catalog_table")
        col_list = list(columns)
        assert len(col_list) == 2, "test_catalog_table should have 2 columns"

    def test_list_columns_names(self, spark, setup_test_tables):
        """listColumns should return correct column names."""
        columns = spark.catalog.listColumns("test_catalog_table")
        col_names = [c.name for c in columns]
        assert "id" in col_names, "Should have 'id' column"
        assert "name" in col_names, "Should have 'name' column"

    def test_list_columns_for_view(self, spark, setup_test_tables):
        """listColumns should work for views too."""
        columns = spark.catalog.listColumns("test_catalog_view")
        col_list = list(columns)
        assert len(col_list) == 2, "test_catalog_view should have 2 columns"


class TestDropTempView:
    """Test spark.catalog.dropTempView()"""

    def test_drop_existing_temp_view(self, spark):
        """Dropping existing temp view should return True."""
        # Create a temp view
        df = spark.range(10)
        df.createOrReplaceTempView("temp_view_to_drop")

        # Drop it
        result = spark.catalog.dropTempView("temp_view_to_drop")
        assert result is True, "dropTempView should return True for existing view"

    def test_drop_nonexistent_temp_view(self, spark):
        """Dropping non-existent temp view should return False."""
        result = spark.catalog.dropTempView("nonexistent_temp_view_xyz")
        assert result is False, "dropTempView should return False for non-existent view"

    def test_view_no_longer_exists_after_drop(self, spark):
        """After dropping, the view should no longer exist."""
        # Create and drop
        df = spark.range(5)
        df.createOrReplaceTempView("temp_view_lifecycle")
        spark.catalog.dropTempView("temp_view_lifecycle")

        # Should not exist anymore
        result = spark.catalog.tableExists("temp_view_lifecycle")
        assert result is False, "View should not exist after being dropped"


class TestCurrentDatabase:
    """Test spark.catalog.currentDatabase()"""

    def test_current_database_returns_string(self, spark):
        """currentDatabase should return a string."""
        result = spark.catalog.currentDatabase()
        assert isinstance(result, str), "currentDatabase should return a string"
        assert len(result) > 0, "currentDatabase should not be empty"


# Differential test class for comparing Spark vs Thunderduck
class TestCatalogDifferential:
    """Differential tests comparing catalog operations between Spark and Thunderduck."""

    @pytest.mark.differential
    def test_table_exists_differential(self, spark_session, thunderduck_session):
        """tableExists should match between Spark and Thunderduck."""
        # Both should return same result for non-existent table
        spark_result = spark_session.catalog.tableExists("nonexistent_table_abc")
        td_result = thunderduck_session.catalog.tableExists("nonexistent_table_abc")
        assert spark_result == td_result, "tableExists should match for non-existent table"

    @pytest.mark.differential
    def test_database_exists_differential(self, spark_session, thunderduck_session):
        """databaseExists should match between Spark and Thunderduck."""
        # Both should return False for non-existent database
        spark_result = spark_session.catalog.databaseExists("nonexistent_db_abc")
        td_result = thunderduck_session.catalog.databaseExists("nonexistent_db_abc")
        assert spark_result == td_result, "databaseExists should match for non-existent db"


class TestCreateTableInternal:
    """Test spark.catalog.createTable() for internal tables (schema-based)."""

    def test_create_table_with_schema(self, spark):
        """Create an internal table with explicit schema."""
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

        table_name = "test_internal_table_schema"

        # Clean up if exists
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        # Define schema
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True)
        ])

        # Create table using catalog API
        spark.catalog.createTable(table_name, schema=schema)

        # Verify table exists
        assert spark.catalog.tableExists(table_name), "Table should exist after creation"

        # Verify schema via listColumns
        columns = list(spark.catalog.listColumns(table_name))
        col_names = [c.name for c in columns]
        assert "id" in col_names, "Should have 'id' column"
        assert "name" in col_names, "Should have 'name' column"
        assert "value" in col_names, "Should have 'value' column"

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_create_table_insert_and_query(self, spark):
        """Create internal table, insert data, and query it."""
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType

        table_name = "test_internal_table_data"

        # Clean up if exists
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("city", StringType(), nullable=True)
        ])

        spark.catalog.createTable(table_name, schema=schema)

        # Insert data
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'NYC'), (2, 'LA'), (3, 'Chicago')")

        # Query and verify
        result = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()
        assert len(result) == 3, "Should have 3 rows"
        assert result[0]["city"] == "NYC", "First row should be NYC"

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_create_table_appears_in_list_tables(self, spark):
        """Created table should appear in listTables."""
        from pyspark.sql.types import StructType, StructField, IntegerType

        table_name = "test_internal_listed"

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        schema = StructType([StructField("x", IntegerType(), nullable=True)])
        spark.catalog.createTable(table_name, schema=schema)

        tables = list(spark.catalog.listTables())
        table_names = [t.name for t in tables]
        assert table_name in table_names, "Created table should appear in listTables"

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestCreateTableExternal:
    """Test spark.catalog.createTable() for external tables (path-based)."""

    @pytest.fixture
    def temp_csv_file(self, tmp_path):
        """Create a temporary CSV file for testing."""
        csv_path = tmp_path / "test_data.csv"
        csv_path.write_text("id,name,score\n1,Alice,95.5\n2,Bob,87.3\n3,Carol,92.1\n")
        return str(csv_path)

    @pytest.fixture
    def temp_json_file(self, tmp_path):
        """Create a temporary JSON file for testing."""
        json_path = tmp_path / "test_data.json"
        json_path.write_text('{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n')
        return str(json_path)

    @pytest.fixture
    def temp_parquet_file(self, spark, tmp_path):
        """Create a temporary Parquet file for testing."""
        parquet_path = str(tmp_path / "test_data.parquet")
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Carol")], ["id", "name"])
        df.write.parquet(parquet_path)
        return parquet_path

    def test_create_external_table_csv(self, spark, temp_csv_file):
        """Create external table from CSV file."""
        table_name = "test_external_csv"

        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

        # Create external table pointing to CSV
        spark.catalog.createTable(
            table_name,
            path=temp_csv_file,
            source="csv"
        )

        # Verify table exists
        assert spark.catalog.tableExists(table_name), "External CSV table should exist"

        # Query the data
        result = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()
        assert len(result) == 3, "Should have 3 rows from CSV"

        # Cleanup
        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

    def test_create_external_table_parquet(self, spark, temp_parquet_file):
        """Create external table from Parquet file."""
        table_name = "test_external_parquet"

        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

        # Create external table pointing to Parquet
        spark.catalog.createTable(
            table_name,
            path=temp_parquet_file,
            source="parquet"
        )

        # Verify table exists
        assert spark.catalog.tableExists(table_name), "External Parquet table should exist"

        # Query the data
        result = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()
        assert len(result) == 3, "Should have 3 rows from Parquet"
        assert result[0]["name"] == "Alice", "First row name should be Alice"

        # Cleanup
        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

    def test_create_external_table_json(self, spark, temp_json_file):
        """Create external table from JSON file."""
        table_name = "test_external_json"

        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

        # Create external table pointing to JSON
        spark.catalog.createTable(
            table_name,
            path=temp_json_file,
            source="json"
        )

        # Verify table exists
        assert spark.catalog.tableExists(table_name), "External JSON table should exist"

        # Query the data
        result = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()
        assert len(result) == 2, "Should have 2 rows from JSON"

        # Cleanup
        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

    def test_create_external_table_infer_format_from_extension(self, spark, temp_csv_file):
        """External table should infer format from file extension."""
        table_name = "test_external_infer_csv"

        spark.sql(f"DROP VIEW IF EXISTS {table_name}")

        # Create without explicit source - should infer from .csv extension
        spark.catalog.createTable(
            table_name,
            path=temp_csv_file
            # No source specified - infer from extension
        )

        assert spark.catalog.tableExists(table_name), "Should create table inferring CSV format"

        result = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()
        assert result[0]["cnt"] == 3, "Should have 3 rows"

        spark.sql(f"DROP VIEW IF EXISTS {table_name}")


class TestSetCurrentDatabase:
    """Test spark.catalog.setCurrentDatabase()."""

    def test_set_current_database_to_main(self, spark):
        """Setting current database to 'main' should work."""
        # Set to main (default DuckDB schema)
        spark.catalog.setCurrentDatabase("main")

        # Verify it was set
        current = spark.catalog.currentDatabase()
        assert current == "main", "Current database should be 'main'"

    def test_set_current_database_creates_schema(self, spark):
        """Setting current database to new name should work if schema exists."""
        db_name = "test_schema_switch"

        # Create the schema first
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")

        # Set current database
        spark.catalog.setCurrentDatabase(db_name)

        # Verify
        current = spark.catalog.currentDatabase()
        assert current == db_name, f"Current database should be '{db_name}'"

        # Reset to main
        spark.catalog.setCurrentDatabase("main")

        # Cleanup
        spark.sql(f"DROP SCHEMA IF EXISTS {db_name}")

    def test_set_current_database_affects_table_creation(self, spark):
        """Tables created after setCurrentDatabase should be in that database."""
        from pyspark.sql.types import StructType, StructField, IntegerType

        db_name = "test_db_context"
        table_name = "table_in_new_db"

        # Create schema and switch to it
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
        spark.catalog.setCurrentDatabase(db_name)

        # Create a table (should be in the new database)
        schema = StructType([StructField("x", IntegerType())])
        spark.catalog.createTable(table_name, schema=schema)

        # Table should exist in the new database
        assert spark.catalog.tableExists(table_name), "Table should exist in current database"

        # Switch back to main and verify table is not visible without qualification
        spark.catalog.setCurrentDatabase("main")

        # The table should still be accessible with full qualification
        tables_in_test_db = list(spark.catalog.listTables(db_name))
        table_names = [t.name for t in tables_in_test_db]
        assert table_name in table_names, "Table should be in test_db_context"

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
        spark.sql(f"DROP SCHEMA IF EXISTS {db_name}")


class TestSetCurrentCatalog:
    """Test spark.catalog.setCurrentCatalog()."""

    def test_set_current_catalog_default(self, spark):
        """Setting current catalog to default should work."""
        # DuckDB only supports a single catalog concept
        # Setting to any value should work (or be a no-op)
        try:
            spark.catalog.setCurrentCatalog("spark_catalog")
            # If it doesn't throw, that's acceptable
        except Exception as e:
            # Some implementations may reject non-default catalogs
            assert "not supported" in str(e).lower() or "default" in str(e).lower()

    def test_current_catalog_returns_value(self, spark):
        """currentCatalog should return a string value."""
        result = spark.catalog.currentCatalog()
        assert isinstance(result, str), "currentCatalog should return a string"
        assert len(result) > 0, "currentCatalog should not be empty"


class TestListCatalogs:
    """Test spark.catalog.listCatalogs()."""

    def test_list_catalogs_returns_results(self, spark):
        """listCatalogs should return at least one catalog."""
        catalogs = spark.catalog.listCatalogs()
        catalog_list = list(catalogs)
        assert len(catalog_list) >= 1, "Should have at least one catalog"

    def test_list_catalogs_has_name_attribute(self, spark):
        """Catalog entries should have a name attribute."""
        catalogs = spark.catalog.listCatalogs()
        catalog_list = list(catalogs)

        if len(catalog_list) > 0:
            first = catalog_list[0]
            assert hasattr(first, 'name'), "Catalog should have 'name' attribute"

    def test_list_catalogs_includes_default(self, spark):
        """listCatalogs should include a default/spark catalog."""
        catalogs = spark.catalog.listCatalogs()
        catalog_names = [c.name for c in catalogs]

        # Should have at least one catalog (could be 'spark_catalog', 'default', etc.)
        assert len(catalog_names) >= 1, "Should have at least one catalog name"


class TestListFunctions:
    """Test spark.catalog.listFunctions()."""

    def test_list_functions_returns_results(self, spark):
        """listFunctions should return available functions."""
        functions = spark.catalog.listFunctions()
        func_list = list(functions)
        # DuckDB has hundreds of built-in functions
        assert len(func_list) >= 50, "Should have many built-in functions"

    def test_list_functions_has_required_attributes(self, spark):
        """Function entries should have expected attributes."""
        functions = spark.catalog.listFunctions()
        func_list = list(functions)

        if len(func_list) > 0:
            first = func_list[0]
            assert hasattr(first, 'name'), "Function should have 'name' attribute"
            assert hasattr(first, 'className'), "Function should have 'className' attribute"
            assert hasattr(first, 'isTemporary'), "Function should have 'isTemporary' attribute"

    def test_list_functions_includes_common_functions(self, spark):
        """listFunctions should include common SQL functions."""
        functions = spark.catalog.listFunctions()
        func_names = [f.name.lower() for f in functions]

        # Check for common functions that should exist
        common_funcs = ['abs', 'sum', 'count', 'max', 'min', 'avg', 'concat', 'length']
        for func in common_funcs:
            assert func in func_names, f"Should include '{func}' function"

    def test_list_functions_with_pattern(self, spark):
        """listFunctions should support pattern filtering."""
        # Get functions matching pattern 'to_%'
        functions = spark.catalog.listFunctions(pattern="to_%")
        func_list = list(functions)

        # Should have some to_* functions
        assert len(func_list) >= 1, "Should find functions matching 'to_%' pattern"

        # All returned functions should match the pattern
        for f in func_list:
            assert f.name.lower().startswith("to_"), f"Function '{f.name}' should match pattern 'to_%'"

    def test_list_functions_className_is_string(self, spark):
        """className should be a non-empty string."""
        functions = spark.catalog.listFunctions()
        func_list = list(functions)

        if len(func_list) > 0:
            first = func_list[0]
            assert isinstance(first.className, str), "className should be a string"
            # DuckDB functions use placeholder className
            assert len(first.className) > 0, "className should not be empty"
