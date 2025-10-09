package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;

public class Join extends LogicalPlan {
    private final Expression condition;
    private final String joinType;

    public Join(LogicalPlan left, LogicalPlan right, Expression condition, String joinType) {
        super(Arrays.asList(left, right));
        this.condition = condition;
        this.joinType = joinType;
    }

    @Override
    public String nodeName() { return "Join[" + joinType + "]"; }

    @Override
    protected StructType computeSchema() { return children.get(0).schema(); }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitJoin(this); }
}
