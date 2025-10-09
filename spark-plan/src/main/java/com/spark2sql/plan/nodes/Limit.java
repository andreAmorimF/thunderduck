package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;

public class Limit extends LogicalPlan {
    private final int n;

    public Limit(LogicalPlan child, int n) {
        super(child);
        this.n = n;
    }

    public int getN() { return n; }

    @Override
    public String nodeName() { return "Limit[" + n + "]"; }

    @Override
    protected StructType computeSchema() { return children.get(0).schema(); }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitLimit(this); }
}
