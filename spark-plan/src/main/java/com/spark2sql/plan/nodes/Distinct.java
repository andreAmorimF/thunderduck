package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;

public class Distinct extends LogicalPlan {
    public Distinct(LogicalPlan child) {
        super(child);
    }

    @Override
    public String nodeName() { return "Distinct"; }

    @Override
    protected StructType computeSchema() { return children.get(0).schema(); }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitDistinct(this); }
}
