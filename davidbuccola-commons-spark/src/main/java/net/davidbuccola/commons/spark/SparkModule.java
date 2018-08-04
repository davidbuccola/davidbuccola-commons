package net.davidbuccola.commons.spark;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.spark.SparkConf;

import java.util.Map;
import java.util.UUID;

/**
 * A Guice {@link Module} which provides a {@link SparkConf} based on input from {@link SparkConfig}.
 * <p>
 * The reason for this indirection is to support YAML configuration with the facilities of {@link org.gwizard.config}.
 * The {@link SparkConfig} class exposes Spark property configuration in a way that is usable with {@link
 * org.gwizard.config}. This {@link Module} transfers the configuration from {@link SparkConfig} to the {@link
 * SparkConf} that Spark expects.
 */
public class SparkModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    public SparkConf sparkConf(SparkConfig config) {

        // Transfer properties from configuration. Except for a couple special properties, let the values from the
        // YAML Config file (SparkConfig) take precedence.
        SparkConf sparkConf = new SparkConf();
        for (Map.Entry<String, String> propertyEntry : config.getProperties().entrySet()) {
            if (propertyEntry.getKey().equals("spark.master") && sparkConf.contains("spark.master")) {
                continue; // Don't want to overwrite value from spark-submit
            }
            if (propertyEntry.getKey().equals("spark.app.name") && sparkConf.contains("spark.app.name")) {
                continue; // Don't want to overwrite value from spark-submit
            }
            sparkConf.set(propertyEntry.getKey(), propertyEntry.getValue());
        }

        // Add defaults for certain properties if they haven't been set already through other sources.
        sparkConf.setIfMissing("spark.master", "local[*]");
        sparkConf.setIfMissing("spark.app.name", "Unnamed Application - " + UUID.randomUUID());

        return sparkConf;
    }
}
