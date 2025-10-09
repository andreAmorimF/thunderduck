package com.spark2sql.plan.expressions;

import com.spark2sql.plan.Expression;
import org.apache.spark.sql.types.DataType;
import java.util.List;

public class CaseWhen extends Expression {
    private final List<Expression> conditions;
    private final List<Expression> values;
    private final Expression elseValue;

    public CaseWhen(List<Expression> conditions, List<Expression> values, Expression elseValue) {
        this.conditions = conditions;
        this.values = values;
        this.elseValue = elseValue;
    }

    public List<Expression> getConditions() {
        return conditions;
    }

    public List<Expression> getValues() {
        return values;
    }

    public Expression getElseValue() {
        return elseValue;
    }

    @Override
    public DataType dataType() {
        // Return the data type of the first value expression
        return values.isEmpty() ? elseValue.dataType() : values.get(0).dataType();
    }

    @Override
    public boolean nullable() {
        // Nullable if any value or the else value is nullable
        for (Expression value : values) {
            if (value.nullable()) {
                return true;
            }
        }
        return elseValue != null && elseValue.nullable();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CASE");
        for (int i = 0; i < conditions.size(); i++) {
            sb.append(String.format(" WHEN %s THEN %s", conditions.get(i), values.get(i)));
        }
        if (elseValue != null) {
            sb.append(String.format(" ELSE %s", elseValue));
        }
        sb.append(" END");
        return sb.toString();
    }
}
