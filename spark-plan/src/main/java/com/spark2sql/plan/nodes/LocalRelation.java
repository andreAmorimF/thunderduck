package com.spark2sql.plan.nodes;

import com.spark2sql.plan.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import java.util.List;

public class LocalRelation extends LogicalPlan {
    private final List<Row> rows;
    private final StructType schema;

    public LocalRelation(List<Row> rows, StructType schema) {
        super();
        this.rows = rows;
        this.schema = schema;
    }

    public List<Row> getRows() { return rows; }

    @Override
    public String nodeName() { return "LocalRelation[" + rows.size() + " rows]"; }

    @Override
    protected StructType computeSchema() { return schema; }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) { return visitor.visitLocalRelation(this); }
}
