package com.spark2sql.plan;

import org.apache.spark.sql.types.StructType;
import java.util.Collections;
import java.util.List;

/**
 * Base class for all logical plan nodes.
 * Represents operations in the query plan tree.
 */
public abstract class LogicalPlan {
    protected List<LogicalPlan> children;
    protected StructType schema;

    public LogicalPlan() {
        this.children = Collections.emptyList();
    }

    public LogicalPlan(LogicalPlan child) {
        this.children = Collections.singletonList(child);
    }

    public LogicalPlan(List<LogicalPlan> children) {
        this.children = children;
    }

    public abstract String nodeName();

    public List<LogicalPlan> children() {
        return children;
    }

    public StructType schema() {
        if (schema == null) {
            schema = computeSchema();
        }
        return schema;
    }

    protected abstract StructType computeSchema();

    // Visitor pattern for tree traversal
    public abstract <T> T accept(PlanVisitor<T> visitor);

    // For debugging and logging
    public String treeString() {
        StringBuilder sb = new StringBuilder();
        treeString(sb, "", "");
        return sb.toString();
    }

    private void treeString(StringBuilder sb, String prefix, String childPrefix) {
        sb.append(prefix).append(nodeName()).append("\n");
        List<LogicalPlan> children = children();
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            if (i < children.size() - 1) {
                child.treeString(sb, childPrefix + "├── ", childPrefix + "│   ");
            } else {
                child.treeString(sb, childPrefix + "└── ", childPrefix + "    ");
            }
        }
    }

    @Override
    public String toString() {
        return nodeName();
    }
}