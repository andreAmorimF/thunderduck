package org.apache.spark.sql;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import java.util.List;

/**
 * Utility for printing tables to console.
 * Simplified version for Phase 1.
 */
public class TablePrinter {

    /**
     * Print rows in a table format.
     */
    public static <T> void print(List<T> rows, StructType schema, int truncateLength, boolean truncate) {
        if (rows.isEmpty()) {
            System.out.println("Empty DataFrame");
            return;
        }

        // Print header
        printSeparator(schema, truncateLength);
        printHeader(schema, truncateLength);
        printSeparator(schema, truncateLength);

        // Print rows
        for (T row : rows) {
            if (row instanceof Row) {
                printRow((Row) row, schema, truncateLength, truncate);
            }
        }

        printSeparator(schema, truncateLength);
        System.out.println("Showing " + rows.size() + " rows");
    }

    private static void printHeader(StructType schema, int truncateLength) {
        System.out.print("|");
        for (StructField field : schema.fields()) {
            String name = field.name();
            if (name.length() > truncateLength) {
                name = name.substring(0, truncateLength - 3) + "...";
            }
            System.out.printf(" %-" + truncateLength + "s |", name);
        }
        System.out.println();
    }

    private static void printRow(Row row, StructType schema, int truncateLength, boolean truncate) {
        System.out.print("|");
        for (int i = 0; i < schema.fields().length; i++) {
            Object value = row.get(i);
            String str = value == null ? "null" : value.toString();
            if (truncate && str.length() > truncateLength) {
                str = str.substring(0, truncateLength - 3) + "...";
            }
            System.out.printf(" %-" + truncateLength + "s |", str);
        }
        System.out.println();
    }

    private static void printSeparator(StructType schema, int truncateLength) {
        System.out.print("+");
        for (int i = 0; i < schema.fields().length; i++) {
            for (int j = 0; j < truncateLength + 2; j++) {
                System.out.print("-");
            }
            System.out.print("+");
        }
        System.out.println();
    }
}