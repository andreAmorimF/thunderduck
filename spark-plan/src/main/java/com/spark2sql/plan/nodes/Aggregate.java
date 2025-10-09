package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class Aggregate extends LogicalPlan {
    private final List<?> groupingExpressions;
    private final List<?> aggregateExpressions;

    public Aggregate(LogicalPlan child, List<?> groupingExpressions, List<?> aggregateExpressions) {
        super(child);
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
    }

    public List<?> getGroupingExpressions() {
        return groupingExpressions;
    }

    public List<?> getAggregateExpressions() {
        return aggregateExpressions;
    }

    @Override
    public String nodeName() {
        return "Aggregate";
    }

    @Override
    protected StructType computeSchema() {
        // In a full implementation, would compute schema from aggregate expressions
        return children.isEmpty() ? new StructType() : children.get(0).schema();
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitAggregate(this);
    }
}
