package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import java.util.List;

public class In extends Expression {
    private final Expression expr;
    private final List<Expression> list;

    public In(Expression expr, List<Expression> list) {
        this.expr = expr;
        this.list = list;
    }

    public Expression getExpr() {
        return expr;
    }

    public List<Expression> getList() {
        return list;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }

    @Override
    public boolean nullable() {
        return expr.nullable();
    }

    @Override
    public String toString() {
        return String.format("%s IN (%s)", expr, String.join(", ", list.stream().map(Object::toString).toArray(String[]::new)));
    }
}
