package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;
import java.util.List;

public class Sort extends LogicalPlan {
    private final List<?> sortExpressions;  // Will be Column but avoid circular dependency
    private final boolean global;

    public Sort(LogicalPlan child, List<?> sortExpressions, boolean global) {
        super(child);
        this.sortExpressions = sortExpressions;
        this.global = global;
    }

    public List<?> getSortExpressions() { return sortExpressions; }

    @Override
    public String nodeName() { return "Sort"; }

    @Override
    protected StructType computeSchema() { return children.get(0).schema(); }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitSort(this); }
}
