package com.thunderduck.logical;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.expression.WindowFunction;
import com.thunderduck.functions.FunctionCategories;
import com.thunderduck.types.ArrayType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.UnresolvedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a projection (SELECT clause).
 *
 * <p>This node selects and potentially transforms columns from its child node.
 *
 * <p>Examples:
 * <pre>
 *   df.select("name", "age")
 *   df.select(col("price") * 1.1)
 *   df.withColumn("total", col("price") * col("quantity"))
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT expr1, expr2, ... FROM (child)</pre>
 */
public class Project extends LogicalPlan {

    private final List<Expression> projections;
    private final List<String> aliases;

    /**
     * Creates a projection node.
     *
     * @param child the child node
     * @param projections the projection expressions
     * @param aliases optional aliases for each projection (null for no alias)
     */
    public Project(LogicalPlan child, List<Expression> projections, List<String> aliases) {
        super(child);
        this.projections = new ArrayList<>(Objects.requireNonNull(projections, "projections must not be null"));
        this.aliases = aliases != null ? new ArrayList<>(aliases) : Collections.nCopies(projections.size(), null);

        if (this.projections.isEmpty()) {
            throw new IllegalArgumentException("projections must not be empty");
        }
        if (this.projections.size() != this.aliases.size()) {
            throw new IllegalArgumentException("projections and aliases must have the same size");
        }
    }

    /**
     * Creates a projection node without aliases.
     *
     * @param child the child node
     * @param projections the projection expressions
     */
    public Project(LogicalPlan child, List<Expression> projections) {
        this(child, projections, null);
    }

    /**
     * Returns the projection expressions.
     *
     * @return an unmodifiable list of projections
     */
    public List<Expression> projections() {
        return Collections.unmodifiableList(projections);
    }

    /**
     * Returns the aliases for each projection.
     *
     * @return an unmodifiable list of aliases (null elements for no alias)
     */
    public List<String> aliases() {
        return Collections.unmodifiableList(aliases);
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
        Objects.requireNonNull(generator, "generator must not be null");

        StringBuilder sql = new StringBuilder("SELECT ");

        // Generate projection list
        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            sql.append(expr.toSQL());

            // Add alias if provided
            String alias = aliases.get(i);
            if (alias != null && !alias.isEmpty()) {
                sql.append(" AS ");
                sql.append(com.thunderduck.generator.SQLQuoting.quoteIdentifier(alias));
            }
        }

        // Add FROM clause from child
        sql.append(" FROM (");
        sql.append(generator.generate(child()));
        sql.append(") AS subquery");

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        // Get child schema for resolving column types
        StructType childSchema = null;
        try {
            childSchema = child().schema();
        } catch (Exception e) {
            // Child schema resolution failed - return null to trigger DuckDB inference
            return null;
        }

        // If child schema is null, return null to trigger DuckDB-based inference
        if (childSchema == null) {
            return null;
        }

        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < projections.size(); i++) {
            Expression expr = projections.get(i);
            String alias = aliases.get(i);
            String fieldName = (alias != null) ? alias : ("col_" + i);

            // Resolve data type and nullable from child schema
            DataType resolvedType = resolveDataType(expr, childSchema);
            boolean resolvedNullable = resolveNullable(expr, childSchema);

            fields.add(new StructField(fieldName, resolvedType, resolvedNullable));
        }
        return new StructType(fields);
    }

    /**
     * Resolves the data type of an expression, looking up unresolved columns
     * in the child schema.
     */
    private DataType resolveDataType(Expression expr, StructType childSchema) {
        if (expr instanceof UnresolvedColumn) {
            String colName = ((UnresolvedColumn) expr).columnName();
            StructField field = childSchema.fieldByName(colName);
            if (field != null) {
                return field.dataType();
            }
        } else if (expr instanceof WindowFunction) {
            // Handle WindowFunction - resolve argument types from child schema
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
                    DataType argType = resolveDataType(arg, childSchema);
                    if (argType != null) {
                        return argType;
                    }
                }
            }

            // SUM promotes type: Integer/Long -> Long, Float/Double -> Double, Decimal -> Decimal(p+10,s)
            if (func.equals("SUM")) {
                if (!wf.arguments().isEmpty()) {
                    Expression arg = wf.arguments().get(0);
                    DataType argType = resolveDataType(arg, childSchema);
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
                    DataType argType = resolveDataType(arg, childSchema);
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

            // Fall back to WindowFunction's own type inference
            return wf.dataType();
        } else if (expr instanceof BinaryExpression) {
            // Handle binary expressions (arithmetic, comparison, etc.)
            BinaryExpression binExpr = (BinaryExpression) expr;
            BinaryExpression.Operator op = binExpr.operator();

            // Comparison and logical operators always return boolean
            if (op.isComparison() || op.isLogical()) {
                return com.thunderduck.types.BooleanType.get();
            }

            // Arithmetic operators - resolve operand types and determine result type
            DataType leftType = resolveDataType(binExpr.left(), childSchema);
            DataType rightType = resolveDataType(binExpr.right(), childSchema);

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
        } else if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            DataType declaredType = func.dataType();

            // If function already returns ArrayType or MapType, but with unresolved elements,
            // we may need to resolve the element type from child schema
            if (declaredType instanceof ArrayType) {
                ArrayType arrType = (ArrayType) declaredType;
                // Check for unresolved element types (UnresolvedType or StringType placeholder)
                boolean hasUnresolved = UnresolvedType.containsUnresolved(arrType) ||
                                       arrType.elementType() instanceof com.thunderduck.types.StringType;
                if (hasUnresolved) {
                    // Element type is unresolved - try to resolve from function semantics
                    String funcName = func.functionName().toLowerCase();
                    if (FunctionCategories.isArrayTypePreserving(funcName) ||
                        FunctionCategories.isArraySetOperation(funcName)) {
                        if (!func.arguments().isEmpty()) {
                            DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                            if (argType instanceof ArrayType) {
                                return argType;  // Return resolved array type
                            }
                        }
                    }
                }
                // Recursively resolve nested types in the array
                return resolveNestedType(declaredType, childSchema);
            }
            if (declaredType instanceof MapType) {
                // Recursively resolve nested types in the map
                return resolveNestedType(declaredType, childSchema);
            }

            // If the function's declared type is unresolved (either UnresolvedType or StringType
            // which is used as default for UnresolvedColumn), try to resolve from first argument
            if ((UnresolvedType.isUnresolved(declaredType) ||
                 declaredType instanceof com.thunderduck.types.StringType) &&
                !func.arguments().isEmpty()) {
                String funcName = func.functionName().toLowerCase();

                // Array functions that preserve input array type
                if (FunctionCategories.isArrayTypePreserving(funcName) ||
                    FunctionCategories.isArraySetOperation(funcName)) {
                    DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                    if (argType instanceof ArrayType) {
                        return argType;
                    }
                }

                // Map key/value extraction functions
                if (FunctionCategories.isMapExtraction(funcName)) {
                    DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                    if (argType instanceof MapType) {
                        MapType mapType = (MapType) argType;
                        // map_keys: keys can never be null
                        // map_values: inherits valueContainsNull from the map
                        return funcName.equals("map_keys")
                            ? new ArrayType(mapType.keyType(), false)
                            : new ArrayType(mapType.valueType(), mapType.valueContainsNull());
                    }
                }

                // Element extraction
                if (FunctionCategories.isElementExtraction(funcName)) {
                    DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                    if (argType instanceof ArrayType) {
                        return ((ArrayType) argType).elementType();
                    }
                    if (argType instanceof MapType) {
                        return ((MapType) argType).valueType();
                    }
                }

                // Explode functions
                if (FunctionCategories.isExplodeFunction(funcName)) {
                    DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                    if (argType instanceof ArrayType) {
                        return ((ArrayType) argType).elementType();
                    }
                }

                // Flatten - reduces nesting by 1 level
                if (funcName.equals("flatten")) {
                    DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                    if (argType instanceof ArrayType) {
                        DataType elemType = ((ArrayType) argType).elementType();
                        return (elemType instanceof ArrayType) ? elemType : argType;
                    }
                }

                // These functions preserve their first argument's type
                if (FunctionCategories.isTypePreserving(funcName)) {
                    return resolveDataType(func.arguments().get(0), childSchema);
                }
            }
            // Use the function's inferred return type
            return declaredType;
        }
        // Fall back to expression's declared type
        return expr.dataType();
    }

    /**
     * Resolves the nullability of an expression, looking up unresolved columns
     * in the child schema.
     */
    private boolean resolveNullable(Expression expr, StructType childSchema) {
        if (expr instanceof UnresolvedColumn) {
            String colName = ((UnresolvedColumn) expr).columnName();
            StructField field = childSchema.fieldByName(colName);
            if (field != null) {
                return field.nullable();
            }
        } else if (expr instanceof WindowFunction) {
            // WindowFunction.nullable() already has proper logic for all window function types
            return expr.nullable();
        } else if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            String funcName = func.functionName().toLowerCase();

            // For null-coalescing functions: non-null if ANY argument is non-null
            // This means only nullable if ALL arguments are nullable
            if (FunctionCategories.isNullCoalescing(funcName)) {
                // Check if ALL resolved arguments are nullable
                for (Expression arg : func.arguments()) {
                    boolean argNullable = resolveNullable(arg, childSchema);
                    if (!argNullable) {
                        // At least one arg is non-null, so result is non-null
                        return false;
                    }
                }
                // All args are nullable, so result is nullable
                return true;
            }

            // For other functions: use the function's inferred nullable flag
            return expr.nullable();
        }
        return expr.nullable();
    }

    /**
     * Recursively resolves nested types within complex types (ArrayType, MapType).
     *
     * <p>This method handles arbitrary nesting, e.g.:
     * <ul>
     *   <li>ArrayType(UnresolvedType) → ArrayType(resolvedType)</li>
     *   <li>MapType(UnresolvedType, ArrayType(UnresolvedType)) → fully resolved</li>
     *   <li>ArrayType(MapType(K, ArrayType(V))) → each level resolved</li>
     * </ul>
     *
     * @param type the type to resolve
     * @param childSchema the child schema for resolving column types
     * @return the resolved type, or the original if no resolution needed
     */
    private DataType resolveNestedType(DataType type, StructType childSchema) {
        if (type instanceof ArrayType) {
            ArrayType arrType = (ArrayType) type;
            DataType elementType = arrType.elementType();
            DataType resolvedElement = resolveNestedType(elementType, childSchema);
            // Only create new ArrayType if element type changed
            if (resolvedElement != elementType) {
                // Preserve the containsNull flag from the original type
                return new ArrayType(resolvedElement, arrType.containsNull());
            }
            return arrType;
        } else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            DataType keyType = mapType.keyType();
            DataType valueType = mapType.valueType();
            DataType resolvedKey = resolveNestedType(keyType, childSchema);
            DataType resolvedValue = resolveNestedType(valueType, childSchema);
            // Only create new MapType if key or value type changed
            if (resolvedKey != keyType || resolvedValue != valueType) {
                // Preserve the valueContainsNull flag from the original type
                return new MapType(resolvedKey, resolvedValue, mapType.valueContainsNull());
            }
            return mapType;
        } else if (type instanceof UnresolvedType) {
            // UnresolvedType at leaf level - cannot resolve without more context
            // Return StringType as fallback (the previous default behavior)
            return com.thunderduck.types.StringType.get();
        }
        // Primitive types are already resolved
        return type;
    }

    /**
     * Promotes numeric types according to Spark's type promotion rules.
     *
     * <p>Order: Byte < Short < Integer < Long < Float < Double
     * Decimal types are handled separately (widest precision wins).
     *
     * @param left the left operand type
     * @param right the right operand type
     * @return the promoted type
     */
    private DataType promoteNumericTypes(DataType left, DataType right) {
        // If either is Double, result is Double
        if (left instanceof com.thunderduck.types.DoubleType ||
            right instanceof com.thunderduck.types.DoubleType) {
            return com.thunderduck.types.DoubleType.get();
        }

        // If either is Float, result is Float (unless other is Double)
        if (left instanceof com.thunderduck.types.FloatType ||
            right instanceof com.thunderduck.types.FloatType) {
            return com.thunderduck.types.FloatType.get();
        }

        // If either is Decimal, result is Decimal with appropriate precision
        if (left instanceof com.thunderduck.types.DecimalType ||
            right instanceof com.thunderduck.types.DecimalType) {
            // Use the Decimal type with higher precision
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

        // If either is Short, result is Short
        if (left instanceof com.thunderduck.types.ShortType ||
            right instanceof com.thunderduck.types.ShortType) {
            return com.thunderduck.types.ShortType.get();
        }

        // Default to left type
        return left;
    }

    @Override
    public String toString() {
        return String.format("Project(%s)", projections);
    }
}
