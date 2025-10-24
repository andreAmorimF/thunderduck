package com.thunderduck.test;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * JUnit 5 test category annotations for organizing and filtering tests.
 *
 * <p>Test categories allow selective test execution based on:
 * <ul>
 *   <li>Test tier (Tier1 = fast unit tests, Tier2 = integration tests, Tier3 = slow tests)</li>
 *   <li>Test type (Unit, Integration, E2E, Performance)</li>
 *   <li>Feature area (TypeMapping, Expression, Function, etc.)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * {@literal @}Tier1
 * {@literal @}Unit
 * {@literal @}TypeMapping
 * public class TypeMapperTest {
 *     // Test methods
 * }
 * </pre>
 *
 * <p>Running tests by category:
 * <pre>
 * mvn test -Dgroups="tier1"              # Run only tier 1 tests
 * mvn test -Dgroups="unit"               # Run only unit tests
 * mvn test -Dgroups="tier1 & unit"       # Run tier 1 unit tests
 * mvn test -Dgroups="!slow"              # Exclude slow tests
 * </pre>
 */
public class TestCategories {

    // ==================== Test Tiers ====================

    /**
     * Tier 1: Fast unit tests (< 100ms each).
     * Run on every commit.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("tier1")
    public @interface Tier1 {
    }

    /**
     * Tier 2: Integration tests (< 1s each).
     * Run before merge to main.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("tier2")
    public @interface Tier2 {
    }

    /**
     * Tier 3: Slow tests (> 1s each).
     * Run nightly or on-demand.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("tier3")
    public @interface Tier3 {
    }

    // ==================== Test Types ====================

    /**
     * Unit test: Tests a single class or method in isolation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("unit")
    public @interface Unit {
    }

    /**
     * Integration test: Tests multiple components together.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("integration")
    public @interface Integration {
    }

    /**
     * End-to-end test: Tests the entire system from input to output.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("e2e")
    public @interface E2E {
    }

    /**
     * Performance test: Measures performance metrics (throughput, latency, memory).
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("performance")
    public @interface Performance {
    }

    /**
     * Regression test: Tests for previously fixed bugs.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("regression")
    public @interface Regression {
    }

    // ==================== Feature Areas ====================

    /**
     * Type mapping tests: Spark DataType to DuckDB SQL type conversion.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("type-mapping")
    public @interface TypeMapping {
    }

    /**
     * Expression tests: Expression tree translation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("expression")
    public @interface Expression {
    }

    /**
     * Function tests: Function registry and translation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("function")
    public @interface Function {
    }

    /**
     * Logical plan tests: LogicalPlan node translation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("logical-plan")
    public @interface LogicalPlan {
    }

    /**
     * SQL generation tests: Final SQL string generation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("sql-generation")
    public @interface SQLGeneration {
    }

    // ==================== Special Categories ====================

    /**
     * Slow test: Takes more than 1 second to execute.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("slow")
    public @interface Slow {
    }

    /**
     * Flaky test: Occasionally fails due to timing or external factors.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("flaky")
    public @interface Flaky {
    }

    /**
     * Disabled test: Temporarily disabled, needs investigation.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("disabled")
    public @interface Disabled {
    }

    /**
     * Critical test: Must always pass, blocks deployment if failing.
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Tag("critical")
    public @interface Critical {
    }
}
