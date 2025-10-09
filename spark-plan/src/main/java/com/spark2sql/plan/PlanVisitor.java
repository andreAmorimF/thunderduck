package com.spark2sql.plan;

import com.spark2sql.plan.nodes.*;

/**
 * Visitor pattern for traversing logical plan trees.
 */
public interface PlanVisitor<T> {
    T visitProject(Project project);
    T visitFilter(Filter filter);
    T visitLimit(Limit limit);
    T visitJoin(Join join);
    T visitAggregate(Aggregate aggregate);
    T visitSort(Sort sort);
    T visitDistinct(Distinct distinct);
    T visitTableScan(TableScan scan);
    T visitLocalRelation(LocalRelation relation);
}