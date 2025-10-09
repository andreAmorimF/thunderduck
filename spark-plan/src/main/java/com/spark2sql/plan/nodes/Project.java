package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.StructType;
import java.util.List;

public class Project extends LogicalPlan {
    private final List<?> projectList;  // Will be Column but avoid circular dependency

    public Project(LogicalPlan child, List<?> columns) {
        super(child);
        this.projectList = columns;
    }

    public List<?> getProjectList() {
        return projectList;
    }

    @Override
    public String nodeName() {
        return "Project";
    }

    @Override
    protected StructType computeSchema() {
        // Simplified - in real implementation would derive from columns
        return children.get(0).schema();
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitProject(this);
    }
}