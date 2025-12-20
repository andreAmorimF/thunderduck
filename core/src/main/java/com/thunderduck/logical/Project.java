package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.UnresolvedColumn;
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
                        return funcName.equals("map_keys")
                            ? new ArrayType(mapType.keyType())
                            : new ArrayType(mapType.valueType());
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
                return new ArrayType(resolvedElement);
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
                return new MapType(resolvedKey, resolvedValue);
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

    @Override
    public String toString() {
        return String.format("Project(%s)", projections);
    }
}
