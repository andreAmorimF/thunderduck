package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for all logical plan nodes in the thunderduck translation layer.
 *
 * <p>This represents a node in the logical query plan tree. Each node can have zero or more
 * children and defines a schema (output columns and types).
 *
 * <p>The logical plan is translated to DuckDB SQL by calling {@link #toSQL(SQLGenerator)}.
 *
 * @see SQLGenerator
 */
public abstract class LogicalPlan {

    /** Child nodes in the plan tree */
    protected final List<LogicalPlan> children;

    /** Output schema of this node */
    protected StructType schema;

    /**
     * Creates a logical plan node with no children.
     */
    protected LogicalPlan() {
        this.children = Collections.emptyList();
    }

    /**
     * Creates a logical plan node with a single child.
     *
     * @param child the child node
     */
    protected LogicalPlan(LogicalPlan child) {
        this.children = Collections.singletonList(child);
    }

    /**
     * Creates a logical plan node with multiple children.
     *
     * @param children the child nodes
     */
    protected LogicalPlan(List<LogicalPlan> children) {
        this.children = new ArrayList<>(children);
    }

    /**
     * Translates this logical plan node to DuckDB SQL.
     *
     * @param generator the SQL generator to use
     * @return the generated SQL string
     */
    public abstract String toSQL(SQLGenerator generator);

    /**
     * Infers the output schema for this logical plan node.
     *
     * <p>This method should compute and cache the schema based on the node's
     * operation and its children's schemas.
     *
     * @return the output schema
     */
    public abstract StructType inferSchema();

    /**
     * Returns the child nodes of this plan.
     *
     * @return an unmodifiable list of children
     */
    public List<LogicalPlan> children() {
        return Collections.unmodifiableList(children);
    }

    /**
     * Returns the output schema of this plan node.
     *
     * <p>If the schema hasn't been computed yet, this calls {@link #inferSchema()}
     * to compute it.
     *
     * @return the output schema
     */
    public StructType schema() {
        if (schema == null) {
            schema = inferSchema();
        }
        return schema;
    }

    /**
     * Returns a human-readable string representation of this plan node.
     *
     * @return a string representation
     */
    @Override
    public abstract String toString();
}
