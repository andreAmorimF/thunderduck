package org.apache.spark.sql;

import com.spark2sql.plan.Expression;
import com.spark2sql.plan.expressions.*;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Column expression for DataFrame operations.
 * Provides fluent API for building expressions.
 */
public class Column {
    private final Expression expression;

    public Column(String name) {
        this.expression = new ColumnReference(name);
    }

    public Column(Expression expr) {
        this.expression = expr;
    }

    // Package private for internal use
    Expression expr() {
        return expression;
    }

    // Comparison operations
    public Column equalTo(Object other) {
        return new Column(new EqualTo(expression, lit(other).expression));
    }

    public Column notEqual(Object other) {
        return new Column(new NotEqualTo(expression, lit(other).expression));
    }

    public Column gt(Object other) {
        return new Column(new GreaterThan(expression, lit(other).expression));
    }

    public Column lt(Object other) {
        return new Column(new LessThan(expression, lit(other).expression));
    }

    public Column geq(Object other) {
        return new Column(new GreaterThanOrEqual(expression, lit(other).expression));
    }

    public Column leq(Object other) {
        return new Column(new LessThanOrEqual(expression, lit(other).expression));
    }

    // Arithmetic operations
    public Column plus(Object other) {
        return new Column(new Add(expression, lit(other).expression));
    }

    public Column minus(Object other) {
        return new Column(new Subtract(expression, lit(other).expression));
    }

    public Column multiply(Object other) {
        return new Column(new Multiply(expression, lit(other).expression));
    }

    public Column divide(Object other) {
        return new Column(new Divide(expression, lit(other).expression));
    }

    public Column mod(Object other) {
        return new Column(new Remainder(expression, lit(other).expression));
    }

    // String operations
    public Column contains(Object other) {
        return new Column(new Contains(expression, lit(other).expression));
    }

    public Column startsWith(Object other) {
        return new Column(new StartsWith(expression, lit(other).expression));
    }

    public Column endsWith(Object other) {
        return new Column(new EndsWith(expression, lit(other).expression));
    }

    public Column like(String pattern) {
        return new Column(new Like(expression, lit(pattern).expression));
    }

    public Column rlike(String pattern) {
        return new Column(new RLike(expression, lit(pattern).expression));
    }

    public Column isin(Object... values) {
        List<Expression> exprs = new ArrayList<>();
        for (Object value : values) {
            exprs.add(lit(value).expression);
        }
        return new Column(new In(expression, exprs));
    }

    // Null handling
    public Column isNull() {
        return new Column(new IsNull(expression));
    }

    public Column isNotNull() {
        return new Column(new IsNotNull(expression));
    }

    public Column isNaN() {
        return new Column(new IsNaN(expression));
    }

    // Boolean operations
    public Column and(Column other) {
        return new Column(new And(expression, other.expression));
    }

    public Column or(Column other) {
        return new Column(new Or(expression, other.expression));
    }

    public Column not() {
        return new Column(new Not(expression));
    }

    // Aliasing
    public Column as(String alias) {
        return new Column(new Alias(expression, alias));
    }

    public Column alias(String alias) {
        return as(alias);
    }

    public Column name(String alias) {
        return as(alias);
    }

    public Column as(String[] aliases) {
        if (aliases.length == 1) {
            return as(aliases[0]);
        }
        // For struct expansion
        throw new UnsupportedOperationException("Multi-alias not yet supported");
    }

    // Casting
    public Column cast(DataType to) {
        return new Column(new Cast(expression, to));
    }

    public Column cast(String to) {
        return cast(DataType.fromDDL(to));
    }

    // Sorting
    public Column asc() {
        return new Column(new SortOrder(expression, true, true));
    }

    public Column asc_nulls_first() {
        return new Column(new SortOrder(expression, true, true));
    }

    public Column asc_nulls_last() {
        return new Column(new SortOrder(expression, true, false));
    }

    public Column desc() {
        return new Column(new SortOrder(expression, false, false));
    }

    public Column desc_nulls_first() {
        return new Column(new SortOrder(expression, false, true));
    }

    public Column desc_nulls_last() {
        return new Column(new SortOrder(expression, false, false));
    }

    // When/Otherwise
    public Column when(Column condition, Object value) {
        return new Column(new CaseWhen(
            Arrays.asList(condition.expression),
            Arrays.asList(lit(value).expression),
            null
        ));
    }

    public Column otherwise(Object value) {
        if (expression instanceof CaseWhen) {
            CaseWhen cw = (CaseWhen) expression;
            return new Column(new CaseWhen(
                cw.getConditions(),
                cw.getValues(),
                lit(value).expression
            ));
        }
        throw new IllegalStateException("otherwise() can only be applied after when()");
    }

    // Aggregate functions (used with groupBy)
    public Column sum() {
        return new Column(new Sum(expression));
    }

    public Column avg() {
        return new Column(new Average(expression));
    }

    public Column mean() {
        return avg();
    }

    public Column min() {
        return new Column(new Min(expression));
    }

    public Column max() {
        return new Column(new Max(expression));
    }

    public Column count() {
        return new Column(new Count(expression));
    }

    // Substring
    public Column substr(int startPos, int length) {
        return new Column(new Substring(expression, lit(startPos).expression, lit(length).expression));
    }

    public Column substr(Column startPos, Column length) {
        return new Column(new Substring(expression, startPos.expression, length.expression));
    }

    public Column substring(int startPos, int length) {
        return substr(startPos, length);
    }

    // Between
    public Column between(Object lowerBound, Object upperBound) {
        return this.geq(lowerBound).and(this.leq(upperBound));
    }

    // BitwiseOperations
    public Column bitwiseAND(Object other) {
        return new Column(new BitwiseAnd(expression, lit(other).expression));
    }

    public Column bitwiseOR(Object other) {
        return new Column(new BitwiseOr(expression, lit(other).expression));
    }

    public Column bitwiseXOR(Object other) {
        return new Column(new BitwiseXor(expression, lit(other).expression));
    }

    // Helper to create literal expressions
    private static Column lit(Object value) {
        if (value instanceof Column) {
            return (Column) value;
        }
        return new Column(Literal.create(value));
    }

    @Override
    public String toString() {
        return expression.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column)) return false;
        Column column = (Column) o;
        return expression.equals(column.expression);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }
}