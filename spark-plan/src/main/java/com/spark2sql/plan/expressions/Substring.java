package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

public class Substring extends Expression {
    private final Expression str;
    private final Expression pos;
    private final Expression len;

    public Substring(Expression str, Expression pos, Expression len) {
        this.str = str;
        this.pos = pos;
        this.len = len;
    }

    public Expression getStr() {
        return str;
    }

    public Expression getPos() {
        return pos;
    }

    public Expression getLen() {
        return len;
    }

    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    @Override
    public boolean nullable() {
        return str.nullable();
    }

    @Override
    public String toString() {
        return String.format("SUBSTRING(%s, %s, %s)", str, pos, len);
    }
}
