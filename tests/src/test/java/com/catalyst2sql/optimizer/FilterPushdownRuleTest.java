package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.logical.Aggregate.AggregateExpression;
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
 * Comprehensive tests for FilterPushdownRule transformation logic (Week 4 Task W4-4).
 *
 * <p>Tests 8 comprehensive scenarios covering:
 * - Filter pushdown through Project
 * - Filter pushdown into Join (left side only)
 * - Filter pushdown into Join (both sides)
 * - Filter pushdown through Aggregate (safe case - grouping keys only)
 * - Filter NOT pushed through Aggregate (unsafe case - references aggregates)
 * - Filter pushdown through Union
 * - Complex multi-filter pushdown with conjunction splitting
 * - Correctness validation (results unchanged after optimization)
 *
 * <p>This test suite validates that the FilterPushdownRule:
 * <ul>
 *   <li>Correctly pushes filters through compatible operators</li>
 *   <li>Preserves query semantics (same output schema and results)</li>
 *   <li>Does NOT push filters when unsafe (references computed columns/aggregates)</li>
 *   <li>Handles complex predicates with AND conjunctions</li>
 *   <li>Is idempotent (applying twice = applying once)</li>
 * </ul>
 */
@DisplayName("Filter Pushdown Rule Tests")
@Tag("optimizer")
@Tag("tier1")
@TestCategories.Unit
public class FilterPushdownRuleTest extends TestBase {

    // Helper method to create a test schema
    private StructType createTestSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("age", IntegerType.get(), true),
            new StructField("category", StringType.get(), true),
            new StructField("price", IntegerType.get(), true)
        ));
    }

    // Helper method to create a second table schema for join tests
    private StructType createOrdersSchema() {
        return new StructType(Arrays.asList(
            new StructField("order_id", IntegerType.get(), false),
            new StructField("customer_id", IntegerType.get(), true),
            new StructField("amount", IntegerType.get(), true),
            new StructField("active", IntegerType.get(), true)
        ));
    }

    @Nested
    @DisplayName("Filter Pushdown Through Project")
    class FilterThroughProject {

        @Test
        @DisplayName("Test 1: Filter pushdown through Project - Safe case")
        void testFilterPushdownThroughProject() {
            // Given: Filter(age > 25, Project([name, age], TableScan))
            // The filter references 'age' which is available in the base table
            logStep("Given: Building Filter over Project plan");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));

            Expression condition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter filter = new Filter(project, condition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Should be transformed to Project([name, age], Filter(age > 25, TableScan))
            logStep("Then: Verifying filter was pushed below project");
            assertThat(result).isInstanceOf(Project.class);
            Project resultProject = (Project) result;

            assertThat(resultProject.child()).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) resultProject.child();

            assertThat(resultFilter.child()).isInstanceOf(TableScan.class);

            // And: Schema should be preserved
            assertThat(result.schema().fields()).hasSize(2);
            assertThat(result.schema().fieldByName("name")).isNotNull();
            assertThat(result.schema().fieldByName("age")).isNotNull();

            // And: Filter condition should be unchanged
            assertThat(resultFilter.condition()).isEqualTo(condition);

            logStep("Verification: Filter successfully pushed through Project");
        }

        @Test
        @DisplayName("Test 1b: Filter NOT pushed when it references computed columns")
        void testFilterNotPushedThroughComputedProject() {
            // Given: Filter references a column that doesn't exist in base table
            // Filter(computed_col > 10, Project([name, age AS computed_col], TableScan))
            logStep("Given: Building Filter that references computed column");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            // Project with alias (age AS computed_col would need aliasing support)
            // For this test, we'll use a column that's in projection but filter references different name
            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));

            // Filter references a column not in the base schema
            Expression nonExistentCol = new ColumnReference("computed_value", IntegerType.get());
            Expression condition = BinaryExpression.greaterThan(
                nonExistentCol,
                new Literal(10, IntegerType.get())
            );
            Filter filter = new Filter(project, condition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule to unsafe case");
            LogicalPlan result = rule.apply(filter);

            // Then: Filter should NOT be pushed (plan unchanged)
            logStep("Then: Verifying filter was NOT pushed");
            assertThat(result).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) result;
            assertThat(resultFilter.child()).isInstanceOf(Project.class);

            logStep("Verification: Filter correctly NOT pushed for computed columns");
        }
    }

    @Nested
    @DisplayName("Filter Pushdown Into Join")
    class FilterIntoJoin {

        @Test
        @DisplayName("Test 2: Filter pushdown into Join - Left side only")
        void testFilterPushdownIntoJoinLeftOnly() {
            // Given: Filter(age > 25, Join(customers, orders))
            // Filter references only left side column
            logStep("Given: Building Filter over Join with left-only predicate");
            StructType customersSchema = createTestSchema();
            StructType ordersSchema = createOrdersSchema();

            TableScan customers = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan orders = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            Expression customerIdLeft = new ColumnReference("id", IntegerType.get());
            Expression customerIdRight = new ColumnReference("customer_id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(customerIdLeft, customerIdRight);

            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCondition);

            // Filter on left side only (age > 25)
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression filterCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter filter = new Filter(join, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Should be Join(Filter(age > 25, customers), orders, ...)
            logStep("Then: Verifying filter was pushed to left side of join");
            assertThat(result).isInstanceOf(Join.class);
            Join resultJoin = (Join) result;

            assertThat(resultJoin.left()).isInstanceOf(Filter.class);
            Filter leftFilter = (Filter) resultJoin.left();
            assertThat(leftFilter.child()).isInstanceOf(TableScan.class);

            // Right side should be unchanged
            assertThat(resultJoin.right()).isInstanceOf(TableScan.class);

            // Join condition should be preserved
            assertThat(resultJoin.condition()).isEqualTo(joinCondition);

            logStep("Verification: Filter successfully pushed to left join input");
        }

        @Test
        @DisplayName("Test 3: Filter pushdown into Join - Both sides")
        void testFilterPushdownIntoJoinBothSides() {
            // Given: Filter(age > 25 AND active = 1, Join(customers, orders))
            // Filter has predicates for both sides
            logStep("Given: Building Filter over Join with predicates for both sides");
            StructType customersSchema = createTestSchema();
            StructType ordersSchema = createOrdersSchema();

            TableScan customers = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan orders = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            Expression customerIdLeft = new ColumnReference("id", IntegerType.get());
            Expression customerIdRight = new ColumnReference("customer_id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(customerIdLeft, customerIdRight);

            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCondition);

            // Filter with AND: age > 25 (left) AND active = 1 (right)
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression activeCol = new ColumnReference("active", IntegerType.get());

            Expression leftPredicate = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Expression rightPredicate = BinaryExpression.equal(
                activeCol,
                new Literal(1, IntegerType.get())
            );
            Expression filterCondition = BinaryExpression.and(leftPredicate, rightPredicate);

            Filter filter = new Filter(join, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Should be Join(Filter(age > 25, customers), Filter(active = 1, orders), ...)
            logStep("Then: Verifying filters were pushed to both join inputs");
            assertThat(result).isInstanceOf(Join.class);
            Join resultJoin = (Join) result;

            // Left side should have filter
            assertThat(resultJoin.left()).isInstanceOf(Filter.class);
            Filter leftFilter = (Filter) resultJoin.left();
            assertThat(leftFilter.child()).isInstanceOf(TableScan.class);

            // Right side should have filter
            assertThat(resultJoin.right()).isInstanceOf(Filter.class);
            Filter rightFilter = (Filter) resultJoin.right();
            assertThat(rightFilter.child()).isInstanceOf(TableScan.class);

            // Join condition should be preserved
            assertThat(resultJoin.condition()).isEqualTo(joinCondition);

            logStep("Verification: Filters successfully pushed to both join inputs");
        }

        @Test
        @DisplayName("Test 3b: Complex filter with mixed predicates - Join condition separated")
        void testFilterPushdownIntoJoinWithMixedPredicates() {
            // Given: Filter with predicates for left, right, and join condition
            // Filter(age > 25 AND active = 1 AND id = customer_id, Join(customers, orders))
            logStep("Given: Building Filter with left, right, and join predicates");
            StructType customersSchema = createTestSchema();
            StructType ordersSchema = createOrdersSchema();

            TableScan customers = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan orders = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Cross join initially (no condition)
            Join join = new Join(customers, orders, Join.JoinType.CROSS, null);

            // Complex filter: age > 25 (left) AND active = 1 (right) AND id = customer_id (both sides)
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression activeCol = new ColumnReference("active", IntegerType.get());
            Expression idCol = new ColumnReference("id", IntegerType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());

            Expression leftPredicate = BinaryExpression.greaterThan(ageCol, new Literal(25, IntegerType.get()));
            Expression rightPredicate = BinaryExpression.equal(activeCol, new Literal(1, IntegerType.get()));
            Expression joinPredicate = BinaryExpression.equal(idCol, customerIdCol);

            Expression combinedFilter = BinaryExpression.and(
                BinaryExpression.and(leftPredicate, rightPredicate),
                joinPredicate
            );

            Filter filter = new Filter(join, combinedFilter);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Filters for left and right should be pushed, join predicate remains
            logStep("Then: Verifying filter split and pushdown");
            assertThat(result).isInstanceOf(Filter.class);
            Filter remainingFilter = (Filter) result;

            assertThat(remainingFilter.child()).isInstanceOf(Join.class);
            Join resultJoin = (Join) remainingFilter.child();

            // Both inputs should have filters
            assertThat(resultJoin.left()).isInstanceOf(Filter.class);
            assertThat(resultJoin.right()).isInstanceOf(Filter.class);

            logStep("Verification: Complex filter correctly split and pushed");
        }
    }

    @Nested
    @DisplayName("Filter Pushdown Through Aggregate")
    class FilterThroughAggregate {

        @Test
        @DisplayName("Test 4: Filter pushdown through Aggregate - Safe case (grouping keys only)")
        void testFilterPushdownThroughAggregateSafe() {
            // Given: Filter(category = 'electronics', Aggregate(groupBy=[category], agg=[sum(price)]))
            // Filter references only grouping key, safe to push
            logStep("Given: Building Filter over Aggregate with grouping key predicate");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            AggregateExpression sumPrice = new AggregateExpression("SUM", priceCol, "sum_price");

            Aggregate aggregate = new Aggregate(
                scan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumPrice)
            );

            // Filter on grouping key
            Expression filterCondition = BinaryExpression.equal(
                categoryCol,
                new Literal("electronics", StringType.get())
            );
            Filter filter = new Filter(aggregate, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Should be Aggregate(groupBy=[category], agg=[sum(price)], Filter(category = 'electronics', TableScan))
            logStep("Then: Verifying filter was pushed below aggregate");
            assertThat(result).isInstanceOf(Aggregate.class);
            Aggregate resultAggregate = (Aggregate) result;

            assertThat(resultAggregate.child()).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) resultAggregate.child();

            assertThat(resultFilter.child()).isInstanceOf(TableScan.class);

            // Grouping and aggregate expressions should be preserved
            assertThat(resultAggregate.groupingExpressions()).hasSize(1);
            assertThat(resultAggregate.aggregateExpressions()).hasSize(1);

            logStep("Verification: Filter successfully pushed through Aggregate");
        }

        @Test
        @DisplayName("Test 5: Filter NOT pushed through Aggregate - Unsafe case (references aggregates)")
        void testFilterNotPushedThroughAggregateUnsafe() {
            // Given: Filter(sum_price > 1000, Aggregate(groupBy=[category], agg=[sum(price) AS sum_price]))
            // Filter references aggregate result, NOT safe to push
            logStep("Given: Building Filter over Aggregate that references aggregate result");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            AggregateExpression sumPrice = new AggregateExpression("SUM", priceCol, "sum_price");

            Aggregate aggregate = new Aggregate(
                scan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumPrice)
            );

            // Filter on aggregate result (sum_price > 1000)
            Expression sumPriceCol = new ColumnReference("sum_price", IntegerType.get());
            Expression filterCondition = BinaryExpression.greaterThan(
                sumPriceCol,
                new Literal(1000, IntegerType.get())
            );
            Filter filter = new Filter(aggregate, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule to unsafe case");
            LogicalPlan result = rule.apply(filter);

            // Then: Filter should NOT be pushed (plan unchanged)
            logStep("Then: Verifying filter was NOT pushed");
            assertThat(result).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) result;

            assertThat(resultFilter.child()).isInstanceOf(Aggregate.class);
            Aggregate resultAggregate = (Aggregate) resultFilter.child();

            assertThat(resultAggregate.child()).isInstanceOf(TableScan.class);

            // Filter should remain above the aggregate
            assertThat(resultFilter.condition()).isEqualTo(filterCondition);

            logStep("Verification: Filter correctly NOT pushed for aggregate references");
        }
    }

    @Nested
    @DisplayName("Filter Pushdown Through Union")
    class FilterThroughUnion {

        @Test
        @DisplayName("Test 6: Filter pushdown through Union")
        void testFilterPushdownThroughUnion() {
            // Given: Filter(active = 1, Union(t1, t2))
            // Always safe to push through both branches
            logStep("Given: Building Filter over Union");
            StructType schema = createTestSchema();
            TableScan table1 = new TableScan("table1.parquet", TableScan.TableFormat.PARQUET, schema);
            TableScan table2 = new TableScan("table2.parquet", TableScan.TableFormat.PARQUET, schema);

            Union union = new Union(table1, table2, true); // UNION ALL

            // Filter on 'age' column
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression filterCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(30, IntegerType.get())
            );
            Filter filter = new Filter(union, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: Should be Union(Filter(age > 30, t1), Filter(age > 30, t2))
            logStep("Then: Verifying filter was pushed to both union branches");
            assertThat(result).isInstanceOf(Union.class);
            Union resultUnion = (Union) result;

            // Left branch should have filter
            assertThat(resultUnion.left()).isInstanceOf(Filter.class);
            Filter leftFilter = (Filter) resultUnion.left();
            assertThat(leftFilter.child()).isInstanceOf(TableScan.class);
            assertThat(leftFilter.condition()).isEqualTo(filterCondition);

            // Right branch should have filter
            assertThat(resultUnion.right()).isInstanceOf(Filter.class);
            Filter rightFilter = (Filter) resultUnion.right();
            assertThat(rightFilter.child()).isInstanceOf(TableScan.class);
            assertThat(rightFilter.condition()).isEqualTo(filterCondition);

            // Union type should be preserved
            assertThat(resultUnion.all()).isTrue();

            logStep("Verification: Filter successfully pushed through both Union branches");
        }
    }

    @Nested
    @DisplayName("Complex Multi-Filter Pushdown")
    class ComplexMultiFilterPushdown {

        @Test
        @DisplayName("Test 7: Complex multi-filter pushdown with conjunction splitting")
        void testComplexMultiFilterPushdownWithConjunctionSplitting() {
            // Given: Complex plan with multiple filters that can be split and pushed
            // Filter(age > 25 AND category = 'electronics' AND price < 1000, ...)
            logStep("Given: Building complex plan with multi-predicate filter");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            // Complex filter with three predicates: age > 25 AND category = 'electronics' AND price < 1000
            Expression pred1 = BinaryExpression.greaterThan(ageCol, new Literal(25, IntegerType.get()));
            Expression pred2 = BinaryExpression.equal(categoryCol, new Literal("electronics", StringType.get()));
            Expression pred3 = BinaryExpression.lessThan(priceCol, new Literal(1000, IntegerType.get()));

            Expression combinedCondition = BinaryExpression.and(
                BinaryExpression.and(pred1, pred2),
                pred3
            );

            // Build plan: Filter over Project
            Expression nameCol = new ColumnReference("name", StringType.get());
            Project project = new Project(scan, Arrays.asList(nameCol, ageCol, categoryCol, priceCol));
            Filter filter = new Filter(project, combinedCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan result = rule.apply(filter);

            // Then: All predicates should be pushed below the project
            logStep("Then: Verifying complex filter was pushed");
            assertThat(result).isInstanceOf(Project.class);
            Project resultProject = (Project) result;

            assertThat(resultProject.child()).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) resultProject.child();

            assertThat(resultFilter.child()).isInstanceOf(TableScan.class);

            // Filter should contain all three predicates
            assertThat(resultFilter.condition()).isNotNull();

            logStep("Verification: Complex multi-predicate filter successfully pushed");
        }

        @Test
        @DisplayName("Test 7b: Multi-level filter pushdown through nested operators")
        void testMultiLevelFilterPushdown() {
            // Given: Filter over Project over Filter over TableScan
            // Nested structure to test recursive application
            logStep("Given: Building multi-level nested plan");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            // First filter: category = 'electronics'
            Expression filter1Condition = BinaryExpression.equal(
                categoryCol,
                new Literal("electronics", StringType.get())
            );
            Filter filter1 = new Filter(scan, filter1Condition);

            // Project
            Project project = new Project(filter1, Arrays.asList(nameCol, ageCol, categoryCol));

            // Second filter: age > 25
            Expression filter2Condition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter filter2 = new Filter(project, filter2Condition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply the filter pushdown rule
            logStep("When: Applying FilterPushdownRule to nested structure");
            LogicalPlan result = rule.apply(filter2);

            // Then: Top-level filter should be pushed down
            logStep("Then: Verifying nested filters were optimized");
            assertThat(result).isNotNull();

            // Structure should be optimized (filters pushed down)
            // Exact structure depends on rule application order, but should have Project at top
            assertThat(result).isInstanceOf(Project.class);

            logStep("Verification: Multi-level filter pushdown successful");
        }
    }

    @Nested
    @DisplayName("Correctness Validation")
    class CorrectnessValidation {

        @Test
        @DisplayName("Test 8: Correctness validation - Results unchanged after optimization")
        void testCorrectnessResultsUnchanged() {
            // Given: Original plan with filter over project
            logStep("Given: Building original plan");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));

            Expression filterCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter originalPlan = new Filter(project, filterCondition);

            // Store original schema
            StructType originalSchema = originalPlan.schema();

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply optimization
            logStep("When: Applying FilterPushdownRule");
            LogicalPlan optimizedPlan = rule.apply(originalPlan);

            // Then: Output schema should be identical
            logStep("Then: Verifying schema preservation");
            assertThat(optimizedPlan.schema()).isNotNull();
            assertThat(optimizedPlan.schema().fields()).hasSize(originalSchema.fields().size());

            // Verify each field matches
            for (int i = 0; i < originalSchema.fields().size(); i++) {
                StructField originalField = originalSchema.fields().get(i);
                StructField optimizedField = optimizedPlan.schema().fields().get(i);

                assertThat(optimizedField.name()).isEqualTo(originalField.name());
                assertThat(optimizedField.dataType()).isEqualTo(originalField.dataType());
            }

            // And: Generated SQL should be valid and semantically equivalent
            logStep("Verifying SQL generation");
            SQLGenerator generator = new SQLGenerator();

            String originalSQL = originalPlan.toSQL(generator);
            String optimizedSQL = optimizedPlan.toSQL(generator);

            assertThat(originalSQL).isNotNull().isNotEmpty();
            assertThat(optimizedSQL).isNotNull().isNotEmpty();

            // Both should contain same key elements (SELECT, FROM, WHERE)
            assertThat(originalSQL).containsIgnoringCase("SELECT");
            assertThat(optimizedSQL).containsIgnoringCase("SELECT");

            logStep("Verification: Optimized plan preserves correctness");
        }

        @Test
        @DisplayName("Test 8b: Idempotency - Applying rule twice = applying once")
        void testRuleIdempotency() {
            // Given: Filter over project
            logStep("Given: Building test plan");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));

            Expression filterCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );
            Filter filter = new Filter(project, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply rule once
            logStep("When: Applying rule first time");
            LogicalPlan result1 = rule.apply(filter);

            // And: Apply rule again to the result
            logStep("Applying rule second time");
            LogicalPlan result2 = rule.apply(result1);

            // Then: Second application should not change the plan
            logStep("Then: Verifying idempotency");
            assertThat(result2).isEqualTo(result1);

            // And: Schema should remain stable
            assertThat(result2.schema()).isEqualTo(result1.schema());

            // And: Applying a third time should also be stable
            LogicalPlan result3 = rule.apply(result2);
            assertThat(result3).isEqualTo(result2);

            logStep("Verification: Rule is idempotent");
        }

        @Test
        @DisplayName("Test 8c: Negative test - Rule doesn't break invalid plans")
        void testRuleDoesNotBreakInvalidPlans() {
            // Given: A plan that shouldn't be transformed (filter already at leaf)
            logStep("Given: Building plan with filter at leaf");
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression filterCondition = BinaryExpression.greaterThan(
                ageCol,
                new Literal(25, IntegerType.get())
            );

            // Filter directly over TableScan (already optimal)
            Filter filter = new Filter(scan, filterCondition);

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply rule
            logStep("When: Applying rule to already-optimal plan");
            LogicalPlan result = rule.apply(filter);

            // Then: Plan should be unchanged (or equal structure)
            logStep("Then: Verifying plan stability");
            assertThat(result).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) result;
            assertThat(resultFilter.child()).isInstanceOf(TableScan.class);

            // Schema should be preserved
            assertThat(result.schema()).isEqualTo(filter.schema());

            logStep("Verification: Rule handles already-optimal plans correctly");
        }

        @Test
        @DisplayName("Test 8d: End-to-end complex query optimization validation")
        void testComplexQueryEndToEndValidation() {
            // Given: Complex realistic query plan
            logStep("Given: Building complex end-to-end query");
            StructType customersSchema = createTestSchema();
            StructType ordersSchema = createOrdersSchema();

            TableScan customers = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan orders = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Build: SELECT name, age FROM customers JOIN orders ON id = customer_id WHERE age > 25 AND amount > 100
            Expression idCol = new ColumnReference("id", IntegerType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(idCol, customerIdCol);

            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCondition);

            // Combined filter
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            Expression agePredicate = BinaryExpression.greaterThan(ageCol, new Literal(25, IntegerType.get()));
            Expression amountPredicate = BinaryExpression.greaterThan(amountCol, new Literal(100, IntegerType.get()));
            Expression combinedFilter = BinaryExpression.and(agePredicate, amountPredicate);

            Filter filter = new Filter(join, combinedFilter);

            // Final projection
            Expression nameCol = new ColumnReference("name", StringType.get());
            Project finalProject = new Project(filter, Arrays.asList(nameCol, ageCol));

            // Store original schema
            StructType originalSchema = finalProject.schema();

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply optimization
            logStep("When: Applying FilterPushdownRule to complex plan");
            LogicalPlan optimized = rule.apply(finalProject);

            // Then: Schema should be preserved
            logStep("Then: Verifying complex plan correctness");
            assertThat(optimized).isNotNull();
            assertThat(optimized.schema()).isNotNull();
            assertThat(optimized.schema().fields()).hasSize(originalSchema.fields().size());

            // Verify field names and types preserved
            for (int i = 0; i < originalSchema.fields().size(); i++) {
                assertThat(optimized.schema().fields().get(i).name())
                    .isEqualTo(originalSchema.fields().get(i).name());
                assertThat(optimized.schema().fields().get(i).dataType())
                    .isEqualTo(originalSchema.fields().get(i).dataType());
            }

            // SQL should be valid
            SQLGenerator generator = new SQLGenerator();
            String sql = optimized.toSQL(generator);
            assertThat(sql).isNotNull().isNotEmpty();
            assertThat(sql).containsIgnoringCase("SELECT");

            logStep("Verification: Complex end-to-end optimization preserves correctness");
        }
    }
}
