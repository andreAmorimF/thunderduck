package com.catalyst2sql.connect.converter;

import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.logical.TableScan;
import com.catalyst2sql.logical.Project;
import com.catalyst2sql.logical.Filter;
import com.catalyst2sql.logical.Aggregate;
import com.catalyst2sql.logical.Sort;
import com.catalyst2sql.logical.Limit;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.expression.FunctionCall;
import com.catalyst2sql.expression.BinaryExpression;
import com.catalyst2sql.types.DataType;
import com.catalyst2sql.types.StructType;
import com.catalyst2sql.types.StructField;

import org.apache.spark.connect.proto.Plan;
import org.apache.spark.connect.proto.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Spark Connect Protocol Buffers plans to catalyst2sql LogicalPlans.
 *
 * <p>This is the main entry point for plan deserialization, converting from
 * Protobuf representations to internal logical plan representations that can
 * be translated to SQL.
 */
public class PlanConverter {
    private static final Logger logger = LoggerFactory.getLogger(PlanConverter.class);

    private final RelationConverter relationConverter;
    private final ExpressionConverter expressionConverter;

    public PlanConverter() {
        this.expressionConverter = new ExpressionConverter();
        this.relationConverter = new RelationConverter(expressionConverter);
    }

    /**
     * Converts a Spark Connect Plan to a LogicalPlan.
     *
     * @param plan the Protobuf plan
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convert(Plan plan) {
        logger.debug("Converting plan: {}", plan.getOpTypeCase());

        if (plan.hasRoot()) {
            return convertRelation(plan.getRoot());
        }

        throw new PlanConversionException("Unsupported plan type: " + plan.getOpTypeCase());
    }

    /**
     * Converts a Spark Connect Relation to a LogicalPlan.
     *
     * @param relation the Protobuf relation
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convertRelation(Relation relation) {
        return relationConverter.convert(relation);
    }

    /**
     * Converts a Spark Connect Expression to a catalyst2sql Expression.
     *
     * @param expr the Protobuf expression
     * @return the converted Expression
     * @throws PlanConversionException if conversion fails
     */
    public Expression convertExpression(org.apache.spark.connect.proto.Expression expr) {
        return expressionConverter.convert(expr);
    }

    /**
     * Converts a list of Spark Connect Expressions.
     *
     * @param exprs the list of Protobuf expressions
     * @return the list of converted Expressions
     */
    public List<Expression> convertExpressions(List<org.apache.spark.connect.proto.Expression> exprs) {
        return exprs.stream()
                .map(this::convertExpression)
                .collect(Collectors.toList());
    }
}