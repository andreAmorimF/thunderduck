package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;

public class Aggregate extends LogicalPlan {
    @Override
    public String nodeName() { return "Aggregate"; }

    @Override
    protected StructType computeSchema() { return new StructType(); }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitAggregate(this); }
}
