package com.thunderduck.expression;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for IntervalExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("IntervalExpression Tests")
public class IntervalExpressionTest extends TestBase {

    @Nested
    @DisplayName("Year-Month Interval")
    class YearMonthInterval {

        @Test
        @DisplayName("Creates year-month interval")
        void testConstruction() {
            IntervalExpression interval = IntervalExpression.yearMonth(12);

            assertThat(interval.intervalType()).isEqualTo(IntervalExpression.IntervalType.YEAR_MONTH);
            assertThat(interval.months()).isEqualTo(12);
        }

        @Test
        @DisplayName("Generates correct SQL for positive months")
        void testPositiveMonthsSql() {
            IntervalExpression interval = IntervalExpression.yearMonth(18);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '18' MONTH");
        }

        @Test
        @DisplayName("Generates correct SQL for negative months")
        void testNegativeMonthsSql() {
            IntervalExpression interval = IntervalExpression.yearMonth(-6);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '-6' MONTH");
        }
    }

    @Nested
    @DisplayName("Day-Time Interval")
    class DayTimeInterval {

        private static final long MICROS_PER_SECOND = 1_000_000L;
        private static final long MICROS_PER_MINUTE = 60L * MICROS_PER_SECOND;
        private static final long MICROS_PER_HOUR = 60L * MICROS_PER_MINUTE;
        private static final long MICROS_PER_DAY = 24L * MICROS_PER_HOUR;

        @Test
        @DisplayName("Creates day-time interval")
        void testConstruction() {
            long micros = 2 * MICROS_PER_DAY + 3 * MICROS_PER_HOUR;
            IntervalExpression interval = IntervalExpression.dayTime(micros);

            assertThat(interval.intervalType()).isEqualTo(IntervalExpression.IntervalType.DAY_TIME);
            assertThat(interval.microseconds()).isEqualTo(micros);
        }

        @Test
        @DisplayName("Generates SQL for days only")
        void testDaysOnlySql() {
            IntervalExpression interval = IntervalExpression.dayTime(3 * MICROS_PER_DAY);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '3' DAY");
        }

        @Test
        @DisplayName("Generates SQL for hours only")
        void testHoursOnlySql() {
            IntervalExpression interval = IntervalExpression.dayTime(5 * MICROS_PER_HOUR);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '5' HOUR");
        }

        @Test
        @DisplayName("Generates SQL for complex time")
        void testComplexTimeSql() {
            long micros = 1 * MICROS_PER_DAY + 2 * MICROS_PER_HOUR + 30 * MICROS_PER_MINUTE;
            IntervalExpression interval = IntervalExpression.dayTime(micros);

            assertThat(interval.toSQL()).contains("INTERVAL '1' DAY");
            assertThat(interval.toSQL()).contains("INTERVAL '2' HOUR");
            assertThat(interval.toSQL()).contains("INTERVAL '30' MINUTE");
        }

        @Test
        @DisplayName("Generates SQL for zero interval")
        void testZeroIntervalSql() {
            IntervalExpression interval = IntervalExpression.dayTime(0);

            assertThat(interval.toSQL()).contains("INTERVAL '0.000000' SECOND");
        }

        @Test
        @DisplayName("Generates SQL for seconds with microseconds")
        void testSecondsSql() {
            IntervalExpression interval = IntervalExpression.dayTime(1_500_000); // 1.5 seconds

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '1.500000' SECOND");
        }
    }

    @Nested
    @DisplayName("Calendar Interval")
    class CalendarInterval {

        private static final long MICROS_PER_SECOND = 1_000_000L;

        @Test
        @DisplayName("Creates calendar interval")
        void testConstruction() {
            IntervalExpression interval = IntervalExpression.calendar(12, 30, 5 * MICROS_PER_SECOND);

            assertThat(interval.intervalType()).isEqualTo(IntervalExpression.IntervalType.CALENDAR);
            assertThat(interval.months()).isEqualTo(12);
            assertThat(interval.days()).isEqualTo(30);
            assertThat(interval.microseconds()).isEqualTo(5 * MICROS_PER_SECOND);
        }

        @Test
        @DisplayName("Generates SQL for full calendar interval")
        void testFullCalendarSql() {
            IntervalExpression interval = IntervalExpression.calendar(12, 5, 30 * MICROS_PER_SECOND);

            String sql = interval.toSQL();
            assertThat(sql).contains("INTERVAL '12' MONTH");
            assertThat(sql).contains("INTERVAL '5' DAY");
            assertThat(sql).contains("INTERVAL '30.000000' SECOND");
        }

        @Test
        @DisplayName("Generates SQL for months only")
        void testMonthsOnlySql() {
            IntervalExpression interval = IntervalExpression.calendar(6, 0, 0);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '6' MONTH");
        }

        @Test
        @DisplayName("Generates SQL for zero calendar interval")
        void testZeroCalendarSql() {
            IntervalExpression interval = IntervalExpression.calendar(0, 0, 0);

            assertThat(interval.toSQL()).isEqualTo("INTERVAL '0' SECOND");
        }
    }

    @Nested
    @DisplayName("Type and Nullability")
    class TypeAndNullability {

        @Test
        @DisplayName("Returns StringType (placeholder)")
        void testDataType() {
            IntervalExpression interval = IntervalExpression.yearMonth(12);

            assertThat(interval.dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Interval literals are not nullable")
        void testNotNullable() {
            IntervalExpression interval = IntervalExpression.dayTime(1000000);

            assertThat(interval.nullable()).isFalse();
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal intervals are equal")
        void testEquality() {
            IntervalExpression int1 = IntervalExpression.yearMonth(12);
            IntervalExpression int2 = IntervalExpression.yearMonth(12);

            assertThat(int1).isEqualTo(int2);
            assertThat(int1.hashCode()).isEqualTo(int2.hashCode());
        }

        @Test
        @DisplayName("Different intervals are not equal")
        void testInequality() {
            IntervalExpression int1 = IntervalExpression.yearMonth(12);
            IntervalExpression int2 = IntervalExpression.yearMonth(6);

            assertThat(int1).isNotEqualTo(int2);
        }

        @Test
        @DisplayName("Different interval types are not equal")
        void testDifferentTypesInequality() {
            IntervalExpression yearMonth = IntervalExpression.yearMonth(12);
            IntervalExpression calendar = IntervalExpression.calendar(12, 0, 0);

            assertThat(yearMonth).isNotEqualTo(calendar);
        }
    }

    @Nested
    @DisplayName("ToString")
    class ToStringTest {

        @Test
        @DisplayName("YearMonth toString")
        void testYearMonthToString() {
            IntervalExpression interval = IntervalExpression.yearMonth(18);

            assertThat(interval.toString()).contains("YEAR_MONTH");
            assertThat(interval.toString()).contains("18 months");
        }

        @Test
        @DisplayName("DayTime toString")
        void testDayTimeToString() {
            IntervalExpression interval = IntervalExpression.dayTime(1000000);

            assertThat(interval.toString()).contains("DAY_TIME");
            assertThat(interval.toString()).contains("1000000 microseconds");
        }

        @Test
        @DisplayName("Calendar toString")
        void testCalendarToString() {
            IntervalExpression interval = IntervalExpression.calendar(12, 5, 1000000);

            assertThat(interval.toString()).contains("CALENDAR");
            assertThat(interval.toString()).contains("12 months");
            assertThat(interval.toString()).contains("5 days");
        }
    }
}
