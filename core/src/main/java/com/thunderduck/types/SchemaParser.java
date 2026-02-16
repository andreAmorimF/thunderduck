package com.thunderduck.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses schema strings into StructType objects.
 *
 * <p>Supports two formats:
 * <ul>
 *   <li>Spark DDL format: {@code struct<name:type,name2:type2>}</li>
 *   <li>JSON format: {@code {"type":"struct","fields":[{"name":"id","type":"integer","nullable":false},...]}}</li>
 * </ul>
 *
 * <p>Examples (DDL format):
 * <ul>
 *   <li>{@code struct<id:int,name:string>}</li>
 *   <li>{@code struct<tags:array<string>>}</li>
 *   <li>{@code struct<data:map<string,int>>}</li>
 * </ul>
 */
public class SchemaParser {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a Spark schema string into a StructType.
     *
     * @param schemaStr the schema string in DDL or JSON format
     * @return the parsed StructType
     * @throws IllegalArgumentException if the schema string is invalid
     */
    public static StructType parse(String schemaStr) {
        if (schemaStr == null || schemaStr.isEmpty()) {
            throw new IllegalArgumentException("Schema string cannot be null or empty");
        }

        String trimmed = schemaStr.trim();

        // Handle JSON format: {"type":"struct","fields":[...]}
        if (trimmed.startsWith("{")) {
            return parseJsonSchema(trimmed);
        }

        // Handle struct<...> format (DDL)
        if (trimmed.toLowerCase().startsWith("struct<") && trimmed.endsWith(">")) {
            String inner = trimmed.substring(7, trimmed.length() - 1);
            return parseStructFields(inner);
        }

        // Try parsing as simple field list (fallback)
        return parseStructFields(trimmed);
    }

    /**
     * Converts a Spark schema string to DuckDB's JSON schema format for use with json_transform().
     *
     * <p>Accepts both DDL and JSON schema formats (same as {@link #parse(String)}).
     * Produces a JSON object where keys are field names and values are DuckDB type strings.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code "a INT, b STRING"} → {@code {"a":"INTEGER","b":"VARCHAR"}}</li>
     *   <li>{@code "STRUCT<a: INT, b: STRING>"} → {@code {"a":"INTEGER","b":"VARCHAR"}}</li>
     *   <li>Nested struct: {@code "a INT, b STRUCT<x: INT>"} → {@code {"a":"INTEGER","b":{"x":"INTEGER"}}}</li>
     *   <li>Array field: {@code "a ARRAY<INT>"} → {@code {"a":["INTEGER"]}}</li>
     * </ul>
     *
     * @param sparkSchema the Spark schema string (DDL or JSON format)
     * @return the DuckDB JSON schema string
     * @throws IllegalArgumentException if the schema string is invalid
     */
    public static String toDuckDBJsonSchema(String sparkSchema) {
        StructType structType = parse(sparkSchema);
        return dataTypeToDuckDBJson(structType);
    }

    /**
     * Recursively converts a DataType to its DuckDB JSON schema representation.
     */
    private static String dataTypeToDuckDBJson(DataType dataType) {
        return switch (dataType) {
            case StructType st -> {
                StringBuilder sb = new StringBuilder("{");
                List<StructField> fields = st.fields();
                for (int i = 0; i < fields.size(); i++) {
                    if (i > 0) sb.append(",");
                    StructField f = fields.get(i);
                    sb.append("\"").append(f.name()).append("\":");
                    sb.append(dataTypeToDuckDBJson(f.dataType()));
                }
                sb.append("}");
                yield sb.toString();
            }
            case ArrayType a -> "[" + dataTypeToDuckDBJson(a.elementType()) + "]";
            case MapType m -> "\"MAP(" + escapeDuckDBType(TypeMapper.toDuckDBType(m.keyType()))
                    + ", " + escapeDuckDBType(TypeMapper.toDuckDBType(m.valueType())) + ")\"";
            default -> "\"" + TypeMapper.toDuckDBType(dataType) + "\"";
        };
    }

    /**
     * Escapes quotes in a DuckDB type string for embedding in JSON.
     */
    private static String escapeDuckDBType(String type) {
        return type.replace("\"", "\\\"");
    }

    /**
     * Parses a JSON schema string into a StructType.
     *
     * <p>Expected format:
     * <pre>
     * {
     *   "type": "struct",
     *   "fields": [
     *     {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
     *     {"name": "name", "type": "string", "nullable": true, "metadata": {}}
     *   ]
     * }
     * </pre>
     *
     * @param jsonStr the JSON schema string
     * @return the parsed StructType
     */
    private static StructType parseJsonSchema(String jsonStr) {
        try {
            JsonNode root = objectMapper.readTree(jsonStr);
            return parseJsonStructType(root);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON schema: " + e.getMessage(), e);
        }
    }

    /**
     * Parses a JSON node representing a struct type.
     */
    private static StructType parseJsonStructType(JsonNode node) {
        JsonNode fieldsNode = node.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray()) {
            return new StructType(new ArrayList<>());
        }

        List<StructField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldsNode) {
            String name = fieldNode.get("name").asText();
            boolean nullable = fieldNode.has("nullable") ? fieldNode.get("nullable").asBoolean() : true;
            DataType dataType = parseJsonDataType(fieldNode.get("type"));
            fields.add(new StructField(name, dataType, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Parses a JSON node representing a data type.
     * Handles both simple types (string like "integer") and complex types (object like {"type":"array",...}).
     */
    private static DataType parseJsonDataType(JsonNode typeNode) {
        if (typeNode == null) {
            throw new IllegalArgumentException("Type node cannot be null");
        }

        // Simple type as string: "integer", "string", etc.
        if (typeNode.isTextual()) {
            return parsePrimitiveType(typeNode.asText().toLowerCase());
        }

        // Complex type as object: {"type":"array", "elementType":...}
        if (typeNode.isObject()) {
            String typeName = typeNode.get("type").asText().toLowerCase();

            return switch (typeName) {
                case "array" -> new ArrayType(parseJsonDataType(typeNode.get("elementType")));
                case "map" -> new MapType(
                    parseJsonDataType(typeNode.get("keyType")),
                    parseJsonDataType(typeNode.get("valueType")));
                case "struct" -> parseJsonStructType(typeNode);
                default -> parsePrimitiveType(typeName);
            };
        }

        throw new IllegalArgumentException("Unsupported type node: " + typeNode);
    }

    /**
     * Parses the inner content of a struct (field definitions).
     *
     * @param fieldsStr the comma-separated field definitions
     * @return the parsed StructType
     */
    private static StructType parseStructFields(String fieldsStr) {
        if (fieldsStr.isEmpty()) {
            return new StructType(new ArrayList<>());
        }

        List<StructField> fields = new ArrayList<>();
        List<String> fieldDefs = splitTopLevel(fieldsStr, ',');

        for (String fieldDef : fieldDefs) {
            fields.add(parseField(fieldDef.trim()));
        }

        return new StructType(fields);
    }

    /**
     * Parses a single field definition.
     *
     * <p>Supports both colon-separated ({@code name:string}) and
     * space-separated ({@code name STRING}) formats.
     *
     * @param fieldDef the field definition
     * @return the parsed StructField
     */
    private static StructField parseField(String fieldDef) {
        int colonIndex = fieldDef.indexOf(':');
        String name;
        String typeStr;
        if (colonIndex != -1) {
            name = fieldDef.substring(0, colonIndex).trim();
            typeStr = fieldDef.substring(colonIndex + 1).trim();
        } else {
            // Space-separated DDL format: "name STRING", "age INT"
            int spaceIndex = fieldDef.indexOf(' ');
            if (spaceIndex == -1) {
                throw new IllegalArgumentException("Invalid field definition: " + fieldDef);
            }
            name = fieldDef.substring(0, spaceIndex).trim();
            typeStr = fieldDef.substring(spaceIndex + 1).trim();
        }

        // All fields are nullable by default in this format
        boolean nullable = true;

        DataType dataType = parseType(typeStr);
        return new StructField(name, dataType, nullable);
    }

    /**
     * Parses a type string into a DataType.
     *
     * @param typeStr the type string
     * @return the parsed DataType
     */
    private static DataType parseType(String typeStr) {
        String normalized = typeStr.toLowerCase().trim();

        // Handle array types: array<elementType>
        if (normalized.startsWith("array<") && normalized.endsWith(">")) {
            String elementTypeStr = typeStr.substring(6, typeStr.length() - 1);
            DataType elementType = parseType(elementTypeStr);
            return new ArrayType(elementType);
        }

        // Handle map types: map<keyType,valueType>
        if (normalized.startsWith("map<") && normalized.endsWith(">")) {
            String inner = typeStr.substring(4, typeStr.length() - 1);
            int commaIndex = findTopLevelComma(inner);
            if (commaIndex == -1) {
                throw new IllegalArgumentException("Invalid map type: " + typeStr);
            }
            String keyTypeStr = inner.substring(0, commaIndex).trim();
            String valueTypeStr = inner.substring(commaIndex + 1).trim();
            DataType keyType = parseType(keyTypeStr);
            DataType valueType = parseType(valueTypeStr);
            return new MapType(keyType, valueType);
        }

        // Handle nested struct types
        if (normalized.startsWith("struct<") && normalized.endsWith(">")) {
            String inner = typeStr.substring(7, typeStr.length() - 1);
            return parseStructFields(inner);
        }

        // Handle primitive types
        return parsePrimitiveType(normalized);
    }

    /**
     * Parses a primitive type string.
     *
     * @param typeStr the normalized type string (lowercase)
     * @return the DataType
     */
    private static DataType parsePrimitiveType(String typeStr) {
        return switch (typeStr) {
            case "byte", "tinyint"        -> ByteType.get();
            case "short", "smallint"      -> ShortType.get();
            case "int", "integer"         -> IntegerType.get();
            case "long", "bigint"         -> LongType.get();
            case "float", "real"          -> FloatType.get();
            case "double"                 -> DoubleType.get();
            case "string", "varchar", "text" -> StringType.get();
            case "boolean", "bool"        -> BooleanType.get();
            case "date"                   -> DateType.get();
            case "timestamp"              -> TimestampType.get();
            case "binary", "blob"         -> BinaryType.get();
            default -> {
                if (typeStr.startsWith("decimal(") || typeStr.startsWith("numeric(")) {
                    yield parseDecimalType(typeStr);
                }
                throw new UnsupportedOperationException("Unsupported type: " + typeStr);
            }
        };
    }

    /**
     * Parses a decimal type string.
     *
     * @param typeStr the decimal type string (e.g., "decimal(10,2)")
     * @return the DecimalType
     */
    private static DecimalType parseDecimalType(String typeStr) {
        int start = typeStr.indexOf('(');
        int end = typeStr.indexOf(')');
        if (start == -1 || end == -1) {
            // Default decimal
            return new DecimalType(10, 0);
        }

        String params = typeStr.substring(start + 1, end);
        String[] parts = params.split(",");
        int precision = Integer.parseInt(parts[0].trim());
        int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        return new DecimalType(precision, scale);
    }

    /**
     * Splits a string by a delimiter, respecting nested angle brackets.
     *
     * @param str the string to split
     * @param delimiter the delimiter character
     * @return the list of parts
     */
    private static List<String> splitTopLevel(String str, char delimiter) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == delimiter && depth == 0) {
                String part = str.substring(start, i).trim();
                if (!part.isEmpty()) {
                    parts.add(part);
                }
                start = i + 1;
            }
        }

        // Add the last part
        if (start < str.length()) {
            String part = str.substring(start).trim();
            if (!part.isEmpty()) {
                parts.add(part);
            }
        }

        return parts;
    }

    /**
     * Finds the top-level comma in a string (ignoring nested angle brackets).
     *
     * @param str the string to search
     * @return the index of the comma, or -1 if not found
     */
    private static int findTopLevelComma(String str) {
        int depth = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }
}
