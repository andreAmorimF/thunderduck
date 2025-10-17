package com.catalyst2sql.logical;

import com.catalyst2sql.types.StructType;
import java.util.Arrays;

/**
 * Logical plan node for INTERSECT operations.
 *
 * <p>Returns rows that appear in both left and right relations.
 */
public class Intersect extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean distinct;

    public Intersect(LogicalPlan left, LogicalPlan right, boolean distinct) {
        super(Arrays.asList(left, right));
        this.left = left;
        this.right = right;
        this.distinct = distinct;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        String leftSql = left.toSQL(generator);
        String rightSql = right.toSQL(generator);

        if (distinct) {
            return leftSql + " INTERSECT " + rightSql;
        } else {
            return leftSql + " INTERSECT ALL " + rightSql;
        }
    }

    @Override
    public StructType inferSchema() {
        // INTERSECT uses the schema from the left side
        return left.inferSchema();
    }

    @Override
    public String toString() {
        return "Intersect[distinct=" + distinct + "]";
    }
}