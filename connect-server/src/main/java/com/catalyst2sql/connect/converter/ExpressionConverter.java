package com.catalyst2sql.connect.converter;

import com.catalyst2sql.expression.*;
import com.catalyst2sql.types.DataType;
import com.catalyst2sql.types.*;

import org.apache.spark.connect.proto.Expression.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Spark Connect Expression types to catalyst2sql Expression objects.
 *
 * <p>This class handles all expression types including literals, column references,
 * functions, arithmetic operations, comparisons, casts, etc.
 */
public class ExpressionConverter {
    private static final Logger logger = LoggerFactory.getLogger(ExpressionConverter.class);

    public ExpressionConverter() {
    }

    /**
     * Converts a Spark Connect Expression to a catalyst2sql Expression.
     *
     * @param expr the Protobuf expression
     * @return the converted Expression
     * @throws PlanConversionException if conversion fails
     */
    public com.catalyst2sql.expression.Expression convert(org.apache.spark.connect.proto.Expression expr) {
        logger.trace("Converting expression type: {}", expr.getExprTypeCase());

        switch (expr.getExprTypeCase()) {
            case LITERAL:
                return convertLiteral(expr.getLiteral());
            case UNRESOLVED_ATTRIBUTE:
                return convertUnresolvedAttribute(expr.getUnresolvedAttribute());
            case UNRESOLVED_FUNCTION:
                return convertUnresolvedFunction(expr.getUnresolvedFunction());
            case ALIAS:
                return convertAlias(expr.getAlias());
            case CAST:
                return convertCast(expr.getCast());
            case UNRESOLVED_STAR:
                return convertUnresolvedStar(expr.getUnresolvedStar());
            case EXPRESSION_STRING:
                return convertExpressionString(expr.getExpressionString());
            case SORT_ORDER:
                // SortOrder is handled specially in RelationConverter
                throw new PlanConversionException("SortOrder should be handled by RelationConverter");
            default:
                throw new PlanConversionException("Unsupported expression type: " + expr.getExprTypeCase());
        }
    }

    /**
     * Converts a Literal expression.
     */
    private com.catalyst2sql.expression.Expression convertLiteral(org.apache.spark.connect.proto.Expression.Literal literal) {
        switch (literal.getLiteralTypeCase()) {
            case NULL:
                return new com.catalyst2sql.expression.Literal(null, convertDataType(literal.getNull()));
            case BINARY:
                return new com.catalyst2sql.expression.Literal(
                    literal.getBinary().toByteArray(), BinaryType.get());
            case BOOLEAN:
                return new com.catalyst2sql.expression.Literal(literal.getBoolean(), BooleanType.get());
            case BYTE:
                return new com.catalyst2sql.expression.Literal((byte) literal.getByte(), ByteType.get());
            case SHORT:
                return new com.catalyst2sql.expression.Literal((short) literal.getShort(), ShortType.get());
            case INTEGER:
                return new com.catalyst2sql.expression.Literal(literal.getInteger(), IntegerType.get());
            case LONG:
                return new com.catalyst2sql.expression.Literal(literal.getLong(), LongType.get());
            case FLOAT:
                return new com.catalyst2sql.expression.Literal(literal.getFloat(), FloatType.get());
            case DOUBLE:
                return new com.catalyst2sql.expression.Literal(literal.getDouble(), DoubleType.get());
            case STRING:
                return new com.catalyst2sql.expression.Literal(literal.getString(), StringType.get());
            case DATE:
                // Date is stored as days since epoch
                return new com.catalyst2sql.expression.Literal(literal.getDate(), DateType.get());
            case TIMESTAMP:
                // Timestamp is stored as microseconds since epoch
                return new com.catalyst2sql.expression.Literal(literal.getTimestamp(), TimestampType.get());
            case DECIMAL:
                org.apache.spark.connect.proto.Expression.Literal.Decimal decimal = literal.getDecimal();
                return new com.catalyst2sql.expression.Literal(
                    decimal.getValue(),
                    new DecimalType(decimal.getPrecision(), decimal.getScale()));
            default:
                throw new PlanConversionException("Unsupported literal type: " + literal.getLiteralTypeCase());
        }
    }

    /**
     * Converts an UnresolvedAttribute (column reference).
     */
    private com.catalyst2sql.expression.Expression convertUnresolvedAttribute(UnresolvedAttribute attr) {
        String identifier = attr.getUnparsedIdentifier();

        // Handle qualified names (table.column)
        String[] parts = identifier.split("\\.");
        if (parts.length == 2) {
            return new UnresolvedColumn(parts[1], parts[0]);
        } else if (parts.length == 1) {
            return new UnresolvedColumn(parts[0]);
        } else {
            // For now, just use the full identifier as column name
            return new UnresolvedColumn(identifier);
        }
    }

    /**
     * Converts an UnresolvedFunction (function call).
     */
    private com.catalyst2sql.expression.Expression convertUnresolvedFunction(UnresolvedFunction func) {
        String functionName = func.getFunctionName().toUpperCase();
        List<com.catalyst2sql.expression.Expression> arguments = func.getArgumentsList().stream()
                .map(this::convert)
                .collect(Collectors.toList());

        // Handle special cases
        if (isBinaryOperator(functionName)) {
            if (arguments.size() != 2) {
                throw new PlanConversionException("Binary operator " + functionName + " requires exactly 2 arguments");
            }
            return mapBinaryOperator(functionName, arguments.get(0), arguments.get(1));
        }

        if (isUnaryOperator(functionName)) {
            if (arguments.size() != 1) {
                throw new PlanConversionException("Unary operator " + functionName + " requires exactly 1 argument");
            }
            return mapUnaryOperator(functionName, arguments.get(0));
        }

        // Handle aggregate functions with DISTINCT
        if (func.getIsDistinct()) {
            functionName = functionName + "_DISTINCT";
        }

        logger.trace("Creating function call: {} with {} arguments", functionName, arguments.size());
        return new FunctionCall(functionName, arguments);
    }

    /**
     * Converts an Alias expression.
     */
    private com.catalyst2sql.expression.Expression convertAlias(Alias alias) {
        com.catalyst2sql.expression.Expression expr = convert(alias.getExpr());

        // Get the alias name
        List<String> names = alias.getNameList();
        if (names.isEmpty()) {
            throw new PlanConversionException("Alias must have at least one name");
        }

        // For scalar columns, use the first name
        String aliasName = names.get(0);
        return new AliasExpression(expr, aliasName);
    }

    /**
     * Converts a Cast expression.
     */
    private com.catalyst2sql.expression.Expression convertCast(Cast cast) {
        com.catalyst2sql.expression.Expression expr = convert(cast.getExpr());

        DataType targetType = null;
        if (cast.hasType()) {
            targetType = convertDataType(cast.getType());
        } else if (cast.hasTypeStr()) {
            targetType = parseTypeString(cast.getTypeStr());
        } else {
            throw new PlanConversionException("Cast must specify target type");
        }

        return new CastExpression(expr, targetType);
    }

    /**
     * Converts an UnresolvedStar (SELECT *).
     */
    private com.catalyst2sql.expression.Expression convertUnresolvedStar(UnresolvedStar star) {
        // Handle qualified star (table.*)
        if (star.hasTarget()) {
            String target = star.getTarget();
            return new StarExpression(target);
        }
        // Unqualified star
        return new StarExpression();
    }

    /**
     * Converts an ExpressionString (SQL expression as string).
     */
    private com.catalyst2sql.expression.Expression convertExpressionString(ExpressionString exprString) {
        // For now, wrap it as a raw SQL expression
        return new RawSQLExpression(exprString.getExpression());
    }

    /**
     * Checks if the function name is a binary operator.
     */
    private boolean isBinaryOperator(String functionName) {
        switch (functionName) {
            case "+":
            case "-":
            case "*":
            case "/":
            case "%":
            case "=":
            case "==":
            case "!=":
            case "<>":
            case "<":
            case "<=":
            case ">":
            case ">=":
            case "AND":
            case "OR":
            case "&&":
            case "||":
                return true;
            default:
                return false;
        }
    }

    /**
     * Checks if the function name is a unary operator.
     */
    private boolean isUnaryOperator(String functionName) {
        switch (functionName) {
            case "-":
            case "NOT":
            case "!":
            case "~":
            case "ISNULL":
            case "ISNOTNULL":
                return true;
            default:
                return false;
        }
    }

    /**
     * Maps a function name to a UnaryExpression.
     */
    private com.catalyst2sql.expression.Expression mapUnaryOperator(String functionName,
            com.catalyst2sql.expression.Expression operand) {
        switch (functionName) {
            case "-":
                return UnaryExpression.negate(operand);
            case "NOT":
            case "!":
                return UnaryExpression.not(operand);
            case "ISNULL":
                return UnaryExpression.isNull(operand);
            case "ISNOTNULL":
                return UnaryExpression.isNotNull(operand);
            default:
                throw new PlanConversionException("Unsupported unary operator: " + functionName);
        }
    }

    /**
     * Maps a function name to a BinaryExpression.
     */
    private com.catalyst2sql.expression.Expression mapBinaryOperator(String functionName,
            com.catalyst2sql.expression.Expression left, com.catalyst2sql.expression.Expression right) {
        switch (functionName) {
            case "+":
                return BinaryExpression.add(left, right);
            case "-":
                return BinaryExpression.subtract(left, right);
            case "*":
                return BinaryExpression.multiply(left, right);
            case "/":
                return BinaryExpression.divide(left, right);
            case "%":
                return BinaryExpression.modulo(left, right);
            case "=":
            case "==":
                return BinaryExpression.equal(left, right);
            case "!=":
            case "<>":
                return BinaryExpression.notEqual(left, right);
            case "<":
                return BinaryExpression.lessThan(left, right);
            case "<=":
                return new BinaryExpression(left, BinaryExpression.Operator.LESS_THAN_OR_EQUAL, right);
            case ">":
                return BinaryExpression.greaterThan(left, right);
            case ">=":
                return new BinaryExpression(left, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, right);
            case "AND":
            case "&&":
                return BinaryExpression.and(left, right);
            case "OR":
            case "||":
                return BinaryExpression.or(left, right);
            default:
                throw new PlanConversionException("Unsupported binary operator: " + functionName);
        }
    }

    /**
     * Converts a Spark Connect DataType to a catalyst2sql DataType.
     */
    private DataType convertDataType(org.apache.spark.connect.proto.DataType protoType) {
        switch (protoType.getKindCase()) {
            case BOOLEAN:
                return BooleanType.get();
            case BYTE:
                return ByteType.get();
            case SHORT:
                return ShortType.get();
            case INTEGER:
                return IntegerType.get();
            case LONG:
                return LongType.get();
            case FLOAT:
                return FloatType.get();
            case DOUBLE:
                return DoubleType.get();
            case STRING:
                return StringType.get();
            case BINARY:
                return BinaryType.get();
            case DATE:
                return DateType.get();
            case TIMESTAMP:
                return TimestampType.get();
            case DECIMAL:
                org.apache.spark.connect.proto.DataType.Decimal decimal = protoType.getDecimal();
                return new DecimalType(decimal.getPrecision(), decimal.getScale());
            case ARRAY:
                DataType elementType = convertDataType(protoType.getArray().getElementType());
                return new ArrayType(elementType);
            case MAP:
                DataType keyType = convertDataType(protoType.getMap().getKeyType());
                DataType valueType = convertDataType(protoType.getMap().getValueType());
                return new MapType(keyType, valueType);
            default:
                throw new PlanConversionException("Unsupported DataType: " + protoType.getKindCase());
        }
    }

    /**
     * Parses a type string (e.g., "INT", "DECIMAL(10,2)") to a catalyst2sql DataType.
     */
    private DataType parseTypeString(String typeStr) {
        return TypeMapper.toSparkType(typeStr);
    }
}