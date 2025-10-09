package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;

public class Filter extends LogicalPlan {
    private final Expression condition;

    public Filter(LogicalPlan child, Expression condition) {
        super(child);
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public String nodeName() {
        return "Filter[" + condition + "]";
    }

    @Override
    protected StructType computeSchema() {
        return children.get(0).schema();
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }
}