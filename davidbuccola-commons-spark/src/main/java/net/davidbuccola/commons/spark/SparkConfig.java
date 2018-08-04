package net.davidbuccola.commons.spark;

import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A frontend to {@link SparkConf} that that is usable with {@link org.gwizard.config}.
 *
 * @see SparkModule
 */
public class SparkConfig implements Serializable {
    /**
     * Standard Spark properties (as defined in the Spark documentation.
     */
    private Map<String, String> properties = new HashMap<>();

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public SparkConfig withProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }
}
