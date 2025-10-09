package org.apache.spark.sql;

import org.apache.spark.sql.types.StructType;
import java.io.Serializable;

/**
 * Base class for encoders that convert between JVM objects and Spark's internal row format.
 * Simplified version for Phase 1.
 *
 * @param <T> The type of objects encoded by this encoder.
 */
public abstract class Encoder<T> implements Serializable {

    /**
     * Returns the schema of the encoded form of objects.
     */
    public abstract StructType schema();

    /**
     * Returns the Java class of objects encoded by this encoder.
     */
    public abstract Class<T> clsTag();
}