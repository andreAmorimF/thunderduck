package com.thunderduck.logical;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.expression.WindowFunction;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical plan node representing adding or replacing columns.
 *
 * <p>This is used when Spark's withColumn() or select() with new columns
 * is called. It preserves all existing columns (except those being replaced)
 * and adds the new/replacement columns.
 *
 * <p>Examples:
 * <pre>
 *   df.withColumn("total", col("price") * col("quantity"))
 *   df.withColumn("row_num", row_number().over(window))
 * </pre>
 */
public class WithColumns extends LogicalPlan {

    private final List<String> columnNames;
    private final List<Expression> columnExpressions;

    /**
     * Creates a WithColumns node.
     *
     * @param child the child node
     * @param columnNames the names of columns to add/replace
     * @param columnExpressions the expressions for each column
     */
    public WithColumns(LogicalPlan child, List<String> columnNames, List<Expression> columnExpressions) {
        super(child);
        this.columnNames = new ArrayList<>(Objects.requireNonNull(columnNames, "columnNames must not be null"));
        this.columnExpressions = new ArrayList<>(Objects.requireNonNull(columnExpressions, "columnExpressions must not be null"));

        if (columnNames.size() != columnExpressions.size()) {
            throw new IllegalArgumentException("columnNames and columnExpressions must have the same size");
        }
    }

    /**
     * Returns the column names being added/replaced.
     *
     * @return an unmodifiable list of column names
     */
    public List<String> columnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    /**
     * Returns the column expressions.
     *
     * @return an unmodifiable list of expressions
     */
    public List<Expression> columnExpressions() {
        return Collections.unmodifiableList(columnExpressions);
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        String inputSql = generator.generate(child());

        // Build column exclusion list
        StringBuilder excludeFilter = new StringBuilder();
        excludeFilter.append("COLUMNS(c -> c NOT IN (");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                excludeFilter.append(", ");
            }
            excludeFilter.append("'").append(columnNames.get(i).replace("'", "''")).append("'");
        }
        excludeFilter.append("))");

        // Build new column expressions
        StringBuilder newColExprs = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                newColExprs.append(", ");
            }
            newColExprs.append(columnExpressions.get(i).toSQL());
            newColExprs.append(" AS ");
            newColExprs.append(com.thunderduck.generator.SQLQuoting.quoteIdentifier(columnNames.get(i)));
        }

        return String.format("SELECT %s, %s FROM (%s) AS _withcol_subquery",
            excludeFilter.toString(), newColExprs.toString(), inputSql);
    }

    @Override
    public StructType inferSchema() {
        // Get child schema
        StructType childSchema = child().schema();
        if (childSchema == null) {
            return null;
        }

        // Build set of column names being replaced
        Set<String> replacedColumns = new HashSet<>(columnNames);

        List<StructField> fields = new ArrayList<>();

        // Add all child columns except those being replaced
        for (StructField field : childSchema.fields()) {
            if (!replacedColumns.contains(field.name())) {
                fields.add(field);
            }
        }

        // Add new/replaced columns
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            Expression expr = columnExpressions.get(i);

            // Get type and nullable from expression, resolving against child schema
            DataType type = resolveExpressionType(expr, childSchema);
            boolean nullable = resolveNullable(expr, childSchema);

            fields.add(new StructField(name, type, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Resolves the type of an expression, looking up column types from the child schema.
     *
     * <p>This is needed because expressions like WindowFunction(LAG, UnresolvedColumn)
     * need to resolve the column type to determine the function return type.
     *
     * @param expr the expression to resolve
     * @param childSchema the schema to look up column types from
     * @return the resolved data type
     */
    private DataType resolveExpressionType(Expression expr, StructType childSchema) {
        // Handle WindowFunction - resolve argument types from child schema
        if (expr instanceof WindowFunction) {
            WindowFunction wf = (WindowFunction) expr;
            String func = wf.function().toUpperCase();

            // Ranking functions return IntegerType
            if (func.equals("ROW_NUMBER") || func.equals("RANK") ||
                func.equals("DENSE_RANK") || func.equals("NTILE")) {
                return com.thunderduck.types.IntegerType.get();
            }

            // PERCENT_RANK and CUME_DIST return DoubleType
            if (func.equals("PERCENT_RANK") || func.equals("CUME_DIST")) {
                return com.thunderduck.types.DoubleType.get();
            }

            // COUNT always returns LongType
            if (func.equals("COUNT")) {
                return com.thunderduck.types.LongType.get();
            }

            // Functions that return the type of their first argument
            // Analytic: LAG, LEAD, FIRST/FIRST_VALUE, LAST/LAST_VALUE, NTH_VALUE
            // Aggregate: MIN, MAX (preserve input type)
            if (func.equals("LAG") || func.equals("LEAD") ||
                func.equals("FIRST") || func.equals("FIRST_VALUE") ||
                func.equals("LAST") || func.equals("LAST_VALUE") ||
                func.equals("NTH_VALUE") ||
                func.equals("MIN") || func.equals("MAX")) {

                if (!wf.arguments().isEmpty()) {
                    Expression arg = wf.arguments().get(0);
                    DataType argType = resolveExpressionType(arg, childSchema);
                    if (argType != null) {
                        return argType;
                    }
                }
            }

            // SUM promotes type: Integer/Long -> Long, Float/Double -> Double, Decimal -> Decimal(p+10,s)
            if (func.equals("SUM")) {
                if (!wf.arguments().isEmpty()) {
                    Expression arg = wf.arguments().get(0);
                    DataType argType = resolveExpressionType(arg, childSchema);
                    if (argType != null) {
                        if (argType instanceof com.thunderduck.types.IntegerType ||
                            argType instanceof com.thunderduck.types.LongType ||
                            argType instanceof com.thunderduck.types.ShortType ||
                            argType instanceof com.thunderduck.types.ByteType) {
                            return com.thunderduck.types.LongType.get();
                        }
                        if (argType instanceof com.thunderduck.types.FloatType ||
                            argType instanceof com.thunderduck.types.DoubleType) {
                            return com.thunderduck.types.DoubleType.get();
                        }
                        if (argType instanceof com.thunderduck.types.DecimalType) {
                            com.thunderduck.types.DecimalType decType = (com.thunderduck.types.DecimalType) argType;
                            int newPrecision = Math.min(decType.precision() + 10, 38);
                            return new com.thunderduck.types.DecimalType(newPrecision, decType.scale());
                        }
                        return argType;  // Preserve other types
                    }
                }
                return com.thunderduck.types.LongType.get();  // Default fallback
            }

            // AVG returns Double for numeric types, Decimal for Decimal input
            if (func.equals("AVG")) {
                if (!wf.arguments().isEmpty()) {
                    Expression arg = wf.arguments().get(0);
                    DataType argType = resolveExpressionType(arg, childSchema);
                    if (argType != null) {
                        if (argType instanceof com.thunderduck.types.DecimalType) {
                            return argType;  // AVG(Decimal) returns same precision Decimal
                        }
                        return com.thunderduck.types.DoubleType.get();
                    }
                }
                return com.thunderduck.types.DoubleType.get();
            }

            // STDDEV, VARIANCE, etc. return Double
            if (func.equals("STDDEV") || func.equals("STDDEV_POP") || func.equals("STDDEV_SAMP") ||
                func.equals("VARIANCE") || func.equals("VAR_POP") || func.equals("VAR_SAMP")) {
                return com.thunderduck.types.DoubleType.get();
            }
        }

        // Handle BinaryExpression - recursively resolve operand types
        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            BinaryExpression.Operator op = binExpr.operator();

            // Comparison and logical operators always return boolean
            if (op.isComparison() || op.isLogical()) {
                return com.thunderduck.types.BooleanType.get();
            }

            // Arithmetic operators - resolve operand types and determine result type
            DataType leftType = resolveExpressionType(binExpr.left(), childSchema);
            DataType rightType = resolveExpressionType(binExpr.right(), childSchema);

            // Division always returns Double
            if (op == BinaryExpression.Operator.DIVIDE) {
                return com.thunderduck.types.DoubleType.get();
            }

            // For +, -, *, %, use numeric type promotion
            if (op.isArithmetic()) {
                return promoteNumericTypes(leftType, rightType);
            }

            // String concatenation returns String
            if (op == BinaryExpression.Operator.CONCAT) {
                return com.thunderduck.types.StringType.get();
            }

            // Default to left operand type
            return leftType;
        }

        // Handle UnresolvedColumn - look up type from child schema
        if (expr instanceof UnresolvedColumn) {
            UnresolvedColumn col = (UnresolvedColumn) expr;
            String colName = col.columnName();

            for (StructField field : childSchema.fields()) {
                if (field.name().equalsIgnoreCase(colName)) {
                    return field.dataType();
                }
            }
        }

        // Default: use the expression's own dataType()
        return expr.dataType();
    }

    /**
     * Promotes numeric types according to Spark type coercion rules.
     * Follows Spark's "widening" rules: Double > Float > Decimal > Long > Integer > Short > Byte.
     */
    private DataType promoteNumericTypes(DataType left, DataType right) {
        // If either is Double, result is Double
        if (left instanceof com.thunderduck.types.DoubleType ||
            right instanceof com.thunderduck.types.DoubleType) {
            return com.thunderduck.types.DoubleType.get();
        }

        // If either is Float, result is Float
        if (left instanceof com.thunderduck.types.FloatType ||
            right instanceof com.thunderduck.types.FloatType) {
            return com.thunderduck.types.FloatType.get();
        }

        // If either is Decimal, result is Decimal with appropriate precision
        if (left instanceof com.thunderduck.types.DecimalType ||
            right instanceof com.thunderduck.types.DecimalType) {
            if (left instanceof com.thunderduck.types.DecimalType &&
                right instanceof com.thunderduck.types.DecimalType) {
                com.thunderduck.types.DecimalType leftDec = (com.thunderduck.types.DecimalType) left;
                com.thunderduck.types.DecimalType rightDec = (com.thunderduck.types.DecimalType) right;
                int maxPrecision = Math.max(leftDec.precision(), rightDec.precision());
                int maxScale = Math.max(leftDec.scale(), rightDec.scale());
                return new com.thunderduck.types.DecimalType(maxPrecision, maxScale);
            }
            return left instanceof com.thunderduck.types.DecimalType ? left : right;
        }

        // If either is Long, result is Long
        if (left instanceof com.thunderduck.types.LongType ||
            right instanceof com.thunderduck.types.LongType) {
            return com.thunderduck.types.LongType.get();
        }

        // If either is Integer, result is Integer
        if (left instanceof com.thunderduck.types.IntegerType ||
            right instanceof com.thunderduck.types.IntegerType) {
            return com.thunderduck.types.IntegerType.get();
        }

        // Default: return Double for safety
        return com.thunderduck.types.DoubleType.get();
    }

    /**
     * Resolves the nullability of an expression, considering child schema and expression semantics.
     *
     * <p>For BinaryExpression, the result is non-nullable only if both operands are non-nullable.
     * For WindowFunction, we delegate to the expression's nullable() method which already
     * handles special cases like LAG/LEAD with default values.
     */
    private boolean resolveNullable(Expression expr, StructType childSchema) {
        // Handle WindowFunction - delegate to its proper nullable logic
        if (expr instanceof WindowFunction) {
            return expr.nullable();
        }

        // Handle BinaryExpression - non-nullable only if both operands are non-nullable
        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            boolean leftNullable = resolveNullable(binExpr.left(), childSchema);
            boolean rightNullable = resolveNullable(binExpr.right(), childSchema);
            return leftNullable || rightNullable;
        }

        // Handle UnresolvedColumn - look up nullable from child schema
        if (expr instanceof UnresolvedColumn) {
            UnresolvedColumn col = (UnresolvedColumn) expr;
            String colName = col.columnName();
            for (StructField field : childSchema.fields()) {
                if (field.name().equalsIgnoreCase(colName)) {
                    return field.nullable();
                }
            }
        }

        // Default: use the expression's own nullable()
        return expr.nullable();
    }

    @Override
    public String toString() {
        return String.format("WithColumns(%s)", columnNames);
    }
}
