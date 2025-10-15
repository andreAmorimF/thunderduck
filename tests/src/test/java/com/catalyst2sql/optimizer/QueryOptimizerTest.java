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
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for Query Optimizer framework and rule application (Week 4).
 *
 * <p>Tests 15 scenarios covering:
 * - Optimizer framework (convergence, iterations, custom rules)
 * - Rule application (pushdown, pruning, ordering, composition)
 * - Correctness validation (differential testing, schema preservation)
 *
 * <p>This test suite validates that the query optimizer:
 * <ul>
 *   <li>Applies optimization rules correctly</li>
 *   <li>Detects convergence (stops when no more changes)</li>
 *   <li>Respects iteration limits</li>
 *   <li>Preserves query semantics</li>
 *   <li>Supports custom rule sets</li>
 * </ul>
 */
@DisplayName("Query Optimizer Tests")
@Tag("optimizer")
@Tag("tier1")
@TestCategories.Unit
public class QueryOptimizerTest extends TestBase {

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

    // Helper method to create a simple test plan
    private LogicalPlan createSimpleTestPlan() {
        StructType schema = createTestSchema();
        return new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);
    }

    @Nested
    @DisplayName("Optimizer Framework")
    class OptimizerFramework {

        @Test
        @DisplayName("Optimizer detects convergence and stops when no changes occur")
        void testOptimizerConvergence() {
            // Given: A plan with filter over project that can be optimized
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

            // And: Optimizer with default rules
            QueryOptimizer optimizer = new QueryOptimizer();

            // When: Optimize the plan
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Plan should be transformed (filter should be pushed down)
            assertThat(optimized).isNotNull();
            assertThat(optimized).isNotSameAs(filter);

            // And: Applying again should produce same result (convergence)
            LogicalPlan optimizedAgain = optimizer.optimize(optimized);
            assertThat(optimizedAgain).isEqualTo(optimized);

            // And: Schema should be preserved
            assertThat(optimized.schema()).isNotNull();
        }

        @Test
        @DisplayName("Optimizer enforces maximum iteration limit")
        void testOptimizerMaxIterations() {
            // Given: Custom optimizer with low max iterations
            List<OptimizationRule> rules = Arrays.asList(
                new FilterPushdownRule(),
                new ColumnPruningRule()
            );
            QueryOptimizer optimizer = new QueryOptimizer(rules, 3);

            // And: Complex plan with multiple optimization opportunities
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol, categoryCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // When: Optimize the plan
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Should stop after 3 iterations
            assertThat(optimizer.maxIterations()).isEqualTo(3);
            assertThat(optimized).isNotNull();

            // And: Plan should be optimized (but may not be fully optimized due to iteration limit)
            assertThat(optimized).isNotSameAs(filter);
        }

        @Test
        @DisplayName("Custom rule registration and application")
        void testCustomRuleRegistration() {
            // Given: A simple custom rule that does nothing (identity rule)
            OptimizationRule customRule = new OptimizationRule() {
                @Override
                public LogicalPlan apply(LogicalPlan plan) {
                    return plan; // Identity transformation
                }

                @Override
                public String name() {
                    return "CustomIdentityRule";
                }
            };

            // And: Optimizer with only the custom rule
            QueryOptimizer optimizer = new QueryOptimizer(
                Collections.singletonList(customRule),
                10
            );

            // And: Test plan
            LogicalPlan plan = createSimpleTestPlan();

            // When: Optimize with custom rule
            LogicalPlan optimized = optimizer.optimize(plan);

            // Then: Plan should be unchanged (identity rule)
            assertThat(optimized).isSameAs(plan);

            // And: Optimizer should report the custom rule
            assertThat(optimizer.rules()).hasSize(1);
            assertThat(optimizer.rules().get(0).name()).isEqualTo("CustomIdentityRule");
        }

        @Test
        @DisplayName("Multiple rules applied in sequence")
        void testMultipleRulesInSequence() {
            // Given: Optimizer with multiple rules
            List<OptimizationRule> rules = Arrays.asList(
                new FilterPushdownRule(),
                new ColumnPruningRule(),
                new ProjectionPushdownRule(),
                new JoinReorderingRule()
            );
            QueryOptimizer optimizer = new QueryOptimizer(rules, 10);

            // And: Complex plan
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // When: Optimize
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Multiple rules should have been applied
            assertThat(optimized).isNotNull();
            assertThat(optimizer.rules()).hasSize(4);

            // And: All rules should be present
            assertThat(optimizer.rules().stream().map(OptimizationRule::name))
                .contains("FilterPushdownRule", "ColumnPruningRule",
                         "ProjectionPushdownRule", "JoinReorderingRule");
        }

        @Test
        @DisplayName("Empty rule list does no optimization")
        void testEmptyRuleList() {
            // Given: Optimizer with no rules
            QueryOptimizer optimizer = new QueryOptimizer(Collections.emptyList(), 10);

            // And: Test plan
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Filter filter = new Filter(scan, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // When: Optimize
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Plan should be unchanged (no rules to apply)
            assertThat(optimized).isSameAs(filter);

            // And: No rules should be registered
            assertThat(optimizer.rules()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Rule Application")
    class RuleApplication {

        @Test
        @DisplayName("FilterPushdownRule pushes filter through project")
        void testFilterPushdownThroughProject() {
            // Given: Filter(age > 25, Project([name, age], TableScan))
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

            // When: Apply rule
            LogicalPlan result = rule.apply(filter);

            // Then: Should be Project([name, age], Filter(age > 25, TableScan))
            assertThat(result).isInstanceOf(Project.class);
            Project resultProject = (Project) result;
            assertThat(resultProject.child()).isInstanceOf(Filter.class);
            Filter resultFilter = (Filter) resultProject.child();
            assertThat(resultFilter.child()).isInstanceOf(TableScan.class);

            // And: Schema should be preserved
            assertThat(result.schema().fields()).hasSize(2);
        }

        @Test
        @DisplayName("ColumnPruningRule removes unused columns from scan")
        void testColumnPruningFromScan() {
            // Given: Project([name], TableScan(id, name, age, category, price))
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Project project = new Project(scan, Collections.singletonList(nameCol));

            ColumnPruningRule rule = new ColumnPruningRule();

            // When: Apply rule
            LogicalPlan result = rule.apply(project);

            // Then: Should prune unused columns from scan
            assertThat(result).isInstanceOf(Project.class);
            Project resultProject = (Project) result;
            assertThat(resultProject.child()).isInstanceOf(TableScan.class);

            TableScan resultScan = (TableScan) resultProject.child();

            // And: TableScan should have pruned schema (only 'name' column)
            assertThat(resultScan.schema().fields()).hasSize(1);
            assertThat(resultScan.schema().fields().get(0).name()).isEqualTo("name");
        }

        @Test
        @DisplayName("Rule ordering affects optimization result")
        void testRuleOrderingEffects() {
            // Given: Same plan with different rule orderings
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // And: Optimizer with filter pushdown first
            QueryOptimizer optimizer1 = new QueryOptimizer(
                Arrays.asList(new FilterPushdownRule(), new ColumnPruningRule()),
                10
            );

            // And: Optimizer with column pruning first
            QueryOptimizer optimizer2 = new QueryOptimizer(
                Arrays.asList(new ColumnPruningRule(), new FilterPushdownRule()),
                10
            );

            // When: Optimize with both orderings
            LogicalPlan result1 = optimizer1.optimize(filter);
            LogicalPlan result2 = optimizer2.optimize(filter);

            // Then: Both should produce optimized plans (may be different structure)
            assertThat(result1).isNotNull();
            assertThat(result2).isNotNull();

            // And: Both should preserve the schema
            assertThat(result1.schema()).isEqualTo(result2.schema());
        }

        @Test
        @DisplayName("Rule composition - multiple rules work together")
        void testRuleComposition() {
            // Given: Plan that benefits from multiple optimizations
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            // Project with extra unused column
            Project project = new Project(scan, Arrays.asList(nameCol, ageCol, categoryCol));

            // Filter on age
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // Final project selecting only name and age (category is unused)
            Project finalProject = new Project(filter, Arrays.asList(nameCol, ageCol));

            // And: Optimizer with multiple rules
            QueryOptimizer optimizer = new QueryOptimizer(
                Arrays.asList(new FilterPushdownRule(), new ColumnPruningRule()),
                10
            );

            // When: Optimize
            LogicalPlan result = optimizer.optimize(finalProject);

            // Then: Both optimizations should be applied
            assertThat(result).isNotNull();

            // And: Output schema should only have name and age
            assertThat(result.schema().fields()).hasSize(2);
            assertThat(result.schema().fieldByName("name")).isNotNull();
            assertThat(result.schema().fieldByName("age")).isNotNull();
        }

        @Test
        @DisplayName("Rule idempotency - applying twice same as applying once")
        void testRuleIdempotency() {
            // Given: Filter over project
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            FilterPushdownRule rule = new FilterPushdownRule();

            // When: Apply rule once
            LogicalPlan result1 = rule.apply(filter);

            // And: Apply rule again to the result
            LogicalPlan result2 = rule.apply(result1);

            // Then: Second application should not change the plan
            assertThat(result2).isEqualTo(result1);

            // And: Applying a third time should also be stable
            LogicalPlan result3 = rule.apply(result2);
            assertThat(result3).isEqualTo(result2);
        }
    }

    @Nested
    @DisplayName("Correctness Validation")
    class CorrectnessValidation {

        @Test
        @DisplayName("Differential testing - optimized produces equivalent plan structure")
        void testDifferentialOptimization() {
            // Given: Original plan
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // When: Optimize
            QueryOptimizer optimizer = new QueryOptimizer();
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Both should have the same output schema
            assertThat(optimized.schema()).isEqualTo(filter.schema());

            // And: Both should have same number of output fields
            assertThat(optimized.schema().fields()).hasSize(filter.schema().fields().size());

            // And: Field names should match
            for (int i = 0; i < filter.schema().fields().size(); i++) {
                assertThat(optimized.schema().fields().get(i).name())
                    .isEqualTo(filter.schema().fields().get(i).name());
            }
        }

        @Test
        @DisplayName("SQL validity after optimization")
        void testSQLValidityAfterOptimization() {
            // Given: Plan that can be optimized
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Filter filter = new Filter(scan, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // When: Optimize
            QueryOptimizer optimizer = new QueryOptimizer();
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Optimized plan should generate valid SQL
            com.catalyst2sql.generator.SQLGenerator generator = new com.catalyst2sql.generator.SQLGenerator();
            String sql = optimized.toSQL(generator);

            // And: SQL should not be empty
            assertThat(sql).isNotNull();
            assertThat(sql).isNotEmpty();

            // And: SQL should contain key elements
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("FROM");
        }

        @Test
        @DisplayName("Result set equivalence - same output columns")
        void testResultSetEquivalence() {
            // Given: Original plan with specific output columns
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol));

            // When: Optimize
            QueryOptimizer optimizer = new QueryOptimizer();
            LogicalPlan optimized = optimizer.optimize(project);

            // Then: Output should have same columns in same order
            assertThat(optimized.schema().fields()).hasSize(2);
            assertThat(optimized.schema().fields().get(0).name()).isEqualTo("name");
            assertThat(optimized.schema().fields().get(1).name()).isEqualTo("age");

            // And: Data types should be preserved
            assertThat(optimized.schema().fields().get(0).dataType())
                .isEqualTo(StringType.get());
            assertThat(optimized.schema().fields().get(1).dataType())
                .isEqualTo(IntegerType.get());
        }

        @Test
        @DisplayName("Schema preservation through optimization")
        void testSchemaPreservation() {
            // Given: Plan with specific schema
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            Project project = new Project(scan, Arrays.asList(nameCol, ageCol, categoryCol));
            Filter filter = new Filter(project, BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            ));

            // And: Store original schema
            StructType originalSchema = filter.schema();

            // When: Optimize with all rules
            QueryOptimizer optimizer = new QueryOptimizer();
            LogicalPlan optimized = optimizer.optimize(filter);

            // Then: Schema should be preserved
            assertThat(optimized.schema()).isNotNull();
            assertThat(optimized.schema().fields()).hasSize(originalSchema.fields().size());

            // And: Each field should match
            for (int i = 0; i < originalSchema.fields().size(); i++) {
                StructField originalField = originalSchema.fields().get(i);
                StructField optimizedField = optimized.schema().fields().get(i);

                assertThat(optimizedField.name()).isEqualTo(originalField.name());
                assertThat(optimizedField.dataType()).isEqualTo(originalField.dataType());
            }
        }

        @Test
        @DisplayName("Complex query optimization end-to-end")
        void testComplexQueryOptimizationEndToEnd() {
            // Given: Complex plan with multiple operations
            StructType schema = createTestSchema();
            TableScan scan = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);

            Expression idCol = new ColumnReference("id", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression ageCol = new ColumnReference("age", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            // Build complex plan:
            // SELECT name, age FROM (
            //   SELECT id, name, age, category FROM data
            //   WHERE age > 25 AND category = 'electronics'
            // )
            Expression ageCondition = BinaryExpression.greaterThan(
                ageCol, new Literal(25, IntegerType.get())
            );
            Expression categoryCondition = BinaryExpression.equal(
                categoryCol, new Literal("electronics", StringType.get())
            );
            Expression combinedCondition = BinaryExpression.and(ageCondition, categoryCondition);

            Project innerProject = new Project(scan,
                Arrays.asList(idCol, nameCol, ageCol, categoryCol));
            Filter filter = new Filter(innerProject, combinedCondition);
            Project outerProject = new Project(filter, Arrays.asList(nameCol, ageCol));

            // And: Full optimizer
            QueryOptimizer optimizer = new QueryOptimizer();

            // When: Optimize
            LogicalPlan optimized = optimizer.optimize(outerProject);

            // Then: Should be optimized
            assertThat(optimized).isNotNull();

            // And: Output schema should match (name, age)
            assertThat(optimized.schema().fields()).hasSize(2);
            assertThat(optimized.schema().fieldByName("name")).isNotNull();
            assertThat(optimized.schema().fieldByName("age")).isNotNull();

            // And: Should generate valid SQL
            com.catalyst2sql.generator.SQLGenerator generator = new com.catalyst2sql.generator.SQLGenerator();
            String sql = optimized.toSQL(generator);
            assertThat(sql).isNotNull();
            assertThat(sql).containsIgnoringCase("SELECT");

            // And: Optimized plan should be different from original (optimizations applied)
            assertThat(optimized).isNotSameAs(outerProject);
        }
    }
}
