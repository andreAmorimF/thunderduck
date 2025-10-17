package com.catalyst2sql.logical;

import com.catalyst2sql.types.StructType;
import java.util.Arrays;

/**
 * Logical plan node for EXCEPT operations.
 *
 * <p>Returns rows from the left relation that don't appear in the right relation.
 */
public class Except extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean distinct;

    public Except(LogicalPlan left, LogicalPlan right, boolean distinct) {
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
            return leftSql + " EXCEPT " + rightSql;
        } else {
            return leftSql + " EXCEPT ALL " + rightSql;
        }
    }

    @Override
    public StructType inferSchema() {
        // EXCEPT uses the schema from the left side
        return left.inferSchema();
    }

    @Override
    public String toString() {
        return "Except[distinct=" + distinct + "]";
    }
}