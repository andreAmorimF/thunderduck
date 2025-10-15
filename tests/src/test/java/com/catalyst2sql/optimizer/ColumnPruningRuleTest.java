package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import com.catalyst2sql.types.IntegerType;
import com.catalyst2sql.types.StringType;
import com.catalyst2sql.types.StructField;
import com.catalyst2sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for Column Pruning Rule optimization (Week 4, Task W4-5).
 *
 * <p>Tests 8 scenarios covering:
 * - Column pruning from TableScan (remove unused columns)
 * - Column pruning through Project
 * - Column pruning through Filter (preserves condition columns)
 * - Column pruning through Join (prunes left and right separately)
 * - Column pruning through Aggregate (keeps grouping + aggregate columns)
 * - Column pruning through Union (prunes both branches)
 * - No pruning when all columns required
 * - End-to-end pruning through complex plan
 *
 * <p>This test suite validates that the column pruning rule:
 * <ul>
 *   <li>Removes unused columns from table scans</li>
 *   <li>Preserves required columns (used in filters, joins, aggregates)</li>
 *   <li>Propagates column requirements correctly through plan tree</li>
 *   <li>Reduces schema size while maintaining query correctness</li>
 * </ul>
 */
@DisplayName("Column Pruning Rule Tests")
@Tag("optimizer")
@Tag("tier1")
@TestCategories.Unit
public class ColumnPruningRuleTest extends TestBase {

    // Helper method to create a wide test schema with many columns
    private StructType createWideSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("age", IntegerType.get(), true),
            new StructField("category", StringType.get(), true),
            new StructField("price", IntegerType.get(), true),
            new StructField("quantity", IntegerType.get(), true),
            new StructField("description", StringType.get(), true),
            new StructField("status", StringType.get(), true)
        ));
    }

    // Helper method to create a simple schema for joins
    private StructType createLeftSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("dept_id", IntegerType.get(), true),
            new StructField("salary", IntegerType.get(), true),
            new StructField("bonus", IntegerType.get(), true)
        ));
    }

    private StructType createRightSchema() {
        return new StructType(Arrays.asList(
            new StructField("dept_id", IntegerType.get(), false),
            new StructField("dept_name", StringType.get(), true),
            new StructField("location", StringType.get(), true),
            new StructField("budget", IntegerType.get(), true)
        ));
    }

    @Nested
    @DisplayName("Basic Column Pruning")
    class BasicColumnPruning {

        @Test
        @DisplayName("Column pruning from TableScan removes unused columns")
        void testColumnPruningFromTableScan() {
            // Given: Project selecting only 'name' from a wide table with 8 columns
            StructType wideSchema = createWideSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, wideSchema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Project project = new Project(scan, Collections.singletonList(nameCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(project);

            // Then: Result should still be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: The child should still be a TableScan
            assertThat(prunedProject.child()).isInstanceOf(TableScan.class);
            TableScan prunedScan = (TableScan) prunedProject.child();

            // And: TableScan schema should be pruned to only 'name' (1 column instead of 8)
            assertThat(prunedScan.schema().fields()).hasSize(1);
            assertThat(prunedScan.schema().fields().get(0).name()).isEqualTo("name");
            assertThat(prunedScan.schema().fields().get(0).dataType()).isEqualTo(StringType.get());

            // And: Output schema should be preserved (still 'name')
            assertThat(result.schema().fields()).hasSize(1);
            assertThat(result.schema().fields().get(0).name()).isEqualTo("name");

            // And: Original wide schema had 8 columns, now reduced to 1
            assertThat(wideSchema.fields()).hasSize(8);
            assertThat(prunedScan.schema().fields()).hasSize(1);
        }

        @Test
        @DisplayName("Column pruning through Project maintains required columns")
        void testColumnPruningThroughProject() {
            // Given: Nested projects where inner project has extra unused columns
            // Project(name, age) -> Project(name, age, category, price) -> TableScan(8 columns)
            StructType wideSchema = createWideSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, wideSchema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            // Inner project selects 4 columns
            Project innerProject = new Project(scan, Arrays.asList(nameCol, ageCol, categoryCol, priceCol));

            // Outer project selects only 2 of those 4 columns
            Project outerProject = new Project(innerProject, Arrays.asList(nameCol, ageCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(outerProject);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedOuter = (Project) result;

            // And: Output should still be name, age
            assertThat(prunedOuter.projections()).hasSize(2);

            // And: Inner project should be pruned to only name, age (not category, price)
            assertThat(prunedOuter.child()).isInstanceOf(Project.class);
            Project prunedInner = (Project) prunedOuter.child();
            assertThat(prunedInner.projections()).hasSize(2);

            // And: TableScan should be pruned to only name, age
            assertThat(prunedInner.child()).isInstanceOf(TableScan.class);
            TableScan prunedScan = (TableScan) prunedInner.child();
            assertThat(prunedScan.schema().fields()).hasSize(2);
            assertThat(prunedScan.schema().fieldByName("name")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("age")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("category")).isNull();
            assertThat(prunedScan.schema().fieldByName("price")).isNull();
        }
    }

    @Nested
    @DisplayName("Column Pruning Through Filters")
    class ColumnPruningThroughFilters {

        @Test
        @DisplayName("Column pruning through Filter preserves condition columns")
        void testColumnPruningThroughFilter() {
            // Given: Project(name) -> Filter(age > 25) -> TableScan(8 columns)
            // Filter needs 'age' but output only needs 'name'
            StructType wideSchema = createWideSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, wideSchema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression condition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter filter = new Filter(scan, condition);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Project project = new Project(filter, Collections.singletonList(nameCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(project);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: Child should be a Filter
            assertThat(prunedProject.child()).isInstanceOf(Filter.class);
            Filter prunedFilter = (Filter) prunedProject.child();

            // And: Filter's child should be TableScan
            assertThat(prunedFilter.child()).isInstanceOf(TableScan.class);
            TableScan prunedScan = (TableScan) prunedFilter.child();

            // And: TableScan should have both 'name' (for output) and 'age' (for filter condition)
            assertThat(prunedScan.schema().fields()).hasSize(2);
            assertThat(prunedScan.schema().fieldByName("name")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("age")).isNotNull();

            // And: Other columns should be pruned
            assertThat(prunedScan.schema().fieldByName("category")).isNull();
            assertThat(prunedScan.schema().fieldByName("price")).isNull();
            assertThat(prunedScan.schema().fieldByName("quantity")).isNull();

            // And: Output schema should only have 'name'
            assertThat(result.schema().fields()).hasSize(1);
            assertThat(result.schema().fields().get(0).name()).isEqualTo("name");
        }
    }

    @Nested
    @DisplayName("Column Pruning Through Joins")
    class ColumnPruningThroughJoins {

        @Test
        @DisplayName("Column pruning through Join prunes left and right separately")
        void testColumnPruningThroughJoin() {
            // Given: Project(name, dept_name) -> Join(id=dept_id) -> left(5 cols), right(4 cols)
            // Join condition needs 'id' from left and 'dept_id' from right
            // Output needs 'name' from left and 'dept_name' from right
            StructType leftSchema = createLeftSchema();
            StructType rightSchema = createRightSchema();

            TableScan leftScan = new TableScan("employees.parquet", TableScan.TableFormat.PARQUET, leftSchema);
            TableScan rightScan = new TableScan("departments.parquet", TableScan.TableFormat.PARQUET, rightSchema);

            Expression leftId = new ColumnReference("id", IntegerType.get());
            Expression rightDeptId = new ColumnReference("dept_id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(leftId, rightDeptId);

            Join join = new Join(leftScan, rightScan, Join.JoinType.INNER, joinCondition);

            // Project only name (from left) and dept_name (from right)
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression deptNameCol = new ColumnReference("dept_name", StringType.get());
            Project project = new Project(join, Arrays.asList(nameCol, deptNameCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(project);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: Child should be a Join
            assertThat(prunedProject.child()).isInstanceOf(Join.class);
            Join prunedJoin = (Join) prunedProject.child();

            // And: Left side should be pruned to 'id' (join key) and 'name' (output)
            assertThat(prunedJoin.left()).isInstanceOf(TableScan.class);
            TableScan prunedLeft = (TableScan) prunedJoin.left();
            assertThat(prunedLeft.schema().fields()).hasSize(2);
            assertThat(prunedLeft.schema().fieldByName("id")).isNotNull();
            assertThat(prunedLeft.schema().fieldByName("name")).isNotNull();
            assertThat(prunedLeft.schema().fieldByName("salary")).isNull();
            assertThat(prunedLeft.schema().fieldByName("bonus")).isNull();

            // And: Right side should be pruned to 'dept_id' (join key) and 'dept_name' (output)
            assertThat(prunedJoin.right()).isInstanceOf(TableScan.class);
            TableScan prunedRight = (TableScan) prunedJoin.right();
            assertThat(prunedRight.schema().fields()).hasSize(2);
            assertThat(prunedRight.schema().fieldByName("dept_id")).isNotNull();
            assertThat(prunedRight.schema().fieldByName("dept_name")).isNotNull();
            assertThat(prunedRight.schema().fieldByName("location")).isNull();
            assertThat(prunedRight.schema().fieldByName("budget")).isNull();

            // And: Output schema should have name and dept_name
            assertThat(result.schema().fields()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Column Pruning Through Aggregates")
    class ColumnPruningThroughAggregates {

        @Test
        @DisplayName("Column pruning through Aggregate keeps grouping and aggregate columns")
        void testColumnPruningThroughAggregate() {
            // Given: Project(category, total) -> Aggregate(groupBy=category, agg=sum(price)) -> TableScan(8 columns)
            // Wrapping in Project ensures pruning propagates correctly
            StructType wideSchema = createWideSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, wideSchema);

            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            Aggregate.AggregateExpression sumPrice = new Aggregate.AggregateExpression(
                "SUM",
                priceCol,
                "total_price"
            );

            Aggregate aggregate = new Aggregate(
                scan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumPrice)
            );

            // Wrap in project to select specific output columns
            Expression groupCol = new ColumnReference("group_0", StringType.get());
            Expression aggCol = new ColumnReference("total_price", IntegerType.get());
            Project project = new Project(aggregate, Arrays.asList(groupCol, aggCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(project);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: Child should be an Aggregate
            assertThat(prunedProject.child()).isInstanceOf(Aggregate.class);
            Aggregate prunedAggregate = (Aggregate) prunedProject.child();

            // And: Aggregate's child should be a TableScan
            assertThat(prunedAggregate.child()).isInstanceOf(TableScan.class);
            TableScan prunedScan = (TableScan) prunedAggregate.child();

            // And: TableScan should only have 'category' and 'price' (2 columns instead of 8)
            assertThat(prunedScan.schema().fields()).hasSize(2);
            assertThat(prunedScan.schema().fieldByName("category")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("price")).isNotNull();

            // And: Other columns should be pruned
            assertThat(prunedScan.schema().fieldByName("id")).isNull();
            assertThat(prunedScan.schema().fieldByName("name")).isNull();
            assertThat(prunedScan.schema().fieldByName("age")).isNull();
            assertThat(prunedScan.schema().fieldByName("quantity")).isNull();
            assertThat(prunedScan.schema().fieldByName("description")).isNull();
            assertThat(prunedScan.schema().fieldByName("status")).isNull();

            // And: Output schema should have 2 columns
            assertThat(result.schema().fields()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Column Pruning Through Unions")
    class ColumnPruningThroughUnions {

        @Test
        @DisplayName("Column pruning through Union prunes both branches")
        void testColumnPruningThroughUnion() {
            // Given: Project(name, age) -> Union -> left(Project(name,age,category) -> Scan), right(Project(name,age,price) -> Scan)
            // To make union schemas compatible, we need projects with same columns on both sides
            StructType wideSchema = createWideSchema();
            TableScan leftScan = new TableScan("data1.parquet", TableScan.TableFormat.PARQUET, wideSchema);
            TableScan rightScan = new TableScan("data2.parquet", TableScan.TableFormat.PARQUET, wideSchema);

            // Project same columns from both sides to make schemas compatible
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            Project leftProject = new Project(leftScan, Arrays.asList(nameCol, ageCol, categoryCol));
            Project rightProject = new Project(rightScan, Arrays.asList(nameCol, ageCol, categoryCol));

            Union union = new Union(leftProject, rightProject, true); // UNION ALL

            // Final project selecting only name and age
            Expression finalName = new ColumnReference("name", StringType.get());
            Expression finalAge = new ColumnReference("age", IntegerType.get());
            Project finalProject = new Project(union, Arrays.asList(finalName, finalAge));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(finalProject);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: Child should be a Union
            assertThat(prunedProject.child()).isInstanceOf(Union.class);
            Union prunedUnion = (Union) prunedProject.child();

            // And: Left side should be a Project with pruned projections
            assertThat(prunedUnion.left()).isInstanceOf(Project.class);
            Project prunedLeftProject = (Project) prunedUnion.left();
            assertThat(prunedLeftProject.projections()).hasSize(2); // Only name, age (not category)

            // And: Left scan should be pruned to name and age
            assertThat(prunedLeftProject.child()).isInstanceOf(TableScan.class);
            TableScan prunedLeftScan = (TableScan) prunedLeftProject.child();
            assertThat(prunedLeftScan.schema().fields()).hasSize(2);
            assertThat(prunedLeftScan.schema().fieldByName("name")).isNotNull();
            assertThat(prunedLeftScan.schema().fieldByName("age")).isNotNull();
            assertThat(prunedLeftScan.schema().fieldByName("category")).isNull();

            // And: Right side should be a Project with pruned projections
            assertThat(prunedUnion.right()).isInstanceOf(Project.class);
            Project prunedRightProject = (Project) prunedUnion.right();
            assertThat(prunedRightProject.projections()).hasSize(2); // Only name, age (not category)

            // And: Right scan should be pruned to name and age
            assertThat(prunedRightProject.child()).isInstanceOf(TableScan.class);
            TableScan prunedRightScan = (TableScan) prunedRightProject.child();
            assertThat(prunedRightScan.schema().fields()).hasSize(2);
            assertThat(prunedRightScan.schema().fieldByName("name")).isNotNull();
            assertThat(prunedRightScan.schema().fieldByName("age")).isNotNull();
            assertThat(prunedRightScan.schema().fieldByName("category")).isNull();

            // And: Output schema should have name and age
            assertThat(result.schema().fields()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("No pruning when all columns are required")
        void testNoPruningWhenAllColumnsRequired() {
            // Given: Project selecting all columns from a table
            StructType schema = new StructType(Arrays.asList(
                new StructField("id", IntegerType.get(), false),
                new StructField("name", StringType.get(), true),
                new StructField("age", IntegerType.get(), true)
            ));
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression idCol = new ColumnReference("id", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());

            Project project = new Project(scan, Arrays.asList(idCol, nameCol, ageCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(project);

            // Then: Result should be the same as original (no pruning)
            assertThat(result).isInstanceOf(Project.class);
            Project prunedProject = (Project) result;

            // And: TableScan should still have all 3 columns
            assertThat(prunedProject.child()).isInstanceOf(TableScan.class);
            TableScan prunedScan = (TableScan) prunedProject.child();
            assertThat(prunedScan.schema().fields()).hasSize(3);
            assertThat(prunedScan.schema().fieldByName("id")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("name")).isNotNull();
            assertThat(prunedScan.schema().fieldByName("age")).isNotNull();

            // And: Output schema should have all 3 columns
            assertThat(result.schema().fields()).hasSize(3);
        }
    }

    @Nested
    @DisplayName("Complex End-to-End Scenarios")
    class ComplexEndToEndScenarios {

        @Test
        @DisplayName("End-to-end pruning through complex plan with multiple operators")
        void testEndToEndPruningThroughComplexPlan() {
            // Given: Simpler but still complex plan to test end-to-end pruning:
            // Project(name, price)
            //   -> Filter(age > 25 AND category = 'electronics')
            //     -> Join(left.id = right.product_id)
            //       -> left: TableScan(id, name, age, category, price, quantity, description, status)
            //       -> right: TableScan(product_id, supplier, warehouse, stock_level)
            //
            // Expected pruning:
            // - Left scan should have: id (join key), name (output), age (filter), category (filter), price (output)
            // - Right scan should only have: product_id (join key)

            // Create wide left schema (8 columns)
            StructType leftSchema = createWideSchema();

            // Create right schema (4 columns)
            StructType rightSchema = new StructType(Arrays.asList(
                new StructField("product_id", IntegerType.get(), false),
                new StructField("supplier", StringType.get(), true),
                new StructField("warehouse", StringType.get(), true),
                new StructField("stock_level", IntegerType.get(), true)
            ));

            TableScan leftScan = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, leftSchema);
            TableScan rightScan = new TableScan("inventory.parquet", TableScan.TableFormat.PARQUET, rightSchema);

            // Build the complex plan
            Expression leftId = new ColumnReference("id", IntegerType.get());
            Expression rightProductId = new ColumnReference("product_id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(leftId, rightProductId);

            Join join = new Join(leftScan, rightScan, Join.JoinType.INNER, joinCondition);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression ageCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Expression categoryCondition = BinaryExpression.equal(
                categoryCol,
                new Literal("electronics", StringType.get())
            );
            Expression filterCondition = BinaryExpression.and(ageCondition, categoryCondition);
            Filter filter = new Filter(join, filterCondition);

            // Final project selecting only name and price
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Project finalProject = new Project(filter, Arrays.asList(nameCol, priceCol));

            // And: Column pruning rule
            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply column pruning
            LogicalPlan result = rule.apply(finalProject);

            // Then: Result should be a Project
            assertThat(result).isInstanceOf(Project.class);
            Project prunedFinalProject = (Project) result;

            // And: Navigate down to the Join to check pruning
            assertThat(prunedFinalProject.child()).isInstanceOf(Filter.class);
            Filter prunedFilter = (Filter) prunedFinalProject.child();

            assertThat(prunedFilter.child()).isInstanceOf(Join.class);
            Join prunedJoin = (Join) prunedFilter.child();

            // And: Left scan should be pruned to id, name, age, category, price (5 columns instead of 8)
            assertThat(prunedJoin.left()).isInstanceOf(TableScan.class);
            TableScan prunedLeft = (TableScan) prunedJoin.left();
            assertThat(prunedLeft.schema().fields()).hasSize(5);
            assertThat(prunedLeft.schema().fieldByName("id")).isNotNull();       // join key
            assertThat(prunedLeft.schema().fieldByName("name")).isNotNull();     // output
            assertThat(prunedLeft.schema().fieldByName("age")).isNotNull();      // filter condition
            assertThat(prunedLeft.schema().fieldByName("category")).isNotNull(); // filter condition
            assertThat(prunedLeft.schema().fieldByName("price")).isNotNull();    // output

            // And: Unused columns should be pruned from left
            assertThat(prunedLeft.schema().fieldByName("quantity")).isNull();
            assertThat(prunedLeft.schema().fieldByName("description")).isNull();
            assertThat(prunedLeft.schema().fieldByName("status")).isNull();

            // And: Right scan should be pruned to only product_id (1 column instead of 4)
            assertThat(prunedJoin.right()).isInstanceOf(TableScan.class);
            TableScan prunedRight = (TableScan) prunedJoin.right();
            assertThat(prunedRight.schema().fields()).hasSize(1);
            assertThat(prunedRight.schema().fieldByName("product_id")).isNotNull(); // join key only

            // And: Unused columns should be pruned from right
            assertThat(prunedRight.schema().fieldByName("supplier")).isNull();
            assertThat(prunedRight.schema().fieldByName("warehouse")).isNull();
            assertThat(prunedRight.schema().fieldByName("stock_level")).isNull();

            // And: Final output schema should have name and price
            assertThat(result.schema().fields()).hasSize(2);
            assertThat(result.schema().fieldByName("name")).isNotNull();
            assertThat(result.schema().fieldByName("price")).isNotNull();

            // And: Verify significant reduction in columns: 8 + 4 = 12 original columns -> 5 + 1 = 6 pruned columns
            int originalColumns = leftSchema.fields().size() + rightSchema.fields().size(); // 8 + 4 = 12
            int prunedColumns = prunedLeft.schema().fields().size() + prunedRight.schema().fields().size(); // 5 + 1 = 6
            assertThat(originalColumns).isEqualTo(12);
            assertThat(prunedColumns).isEqualTo(6);

            // Pruned 6 out of 12 columns (50% reduction)
            int columnsPruned = originalColumns - prunedColumns;
            assertThat(columnsPruned).isEqualTo(6);
        }
    }
}
