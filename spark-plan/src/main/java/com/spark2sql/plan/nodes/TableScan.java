package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.types.*;

public class TableScan extends LogicalPlan {
    private final String tableName;

    public TableScan(String tableName) {
        super();
        this.tableName = tableName;
    }

    public String getTableName() { return tableName; }

    @Override
    public String nodeName() { return "TableScan[" + tableName + "]"; }

    @Override
    protected StructType computeSchema() {
        // In real implementation would look up table schema
        return new StructType();
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitTableScan(this); }
}
