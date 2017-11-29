package net.davidbuccola.commons.spark;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.spark.SparkConf;
import scala.util.Random;

import java.util.Map;

/**
 * A Guice {@link Module} which configures Spark access.
 */
public class SparkModule extends AbstractModule {

    @Override
    protected void configure() {
    }

    @Provides
    public SparkConf sparkConf(SparkConfig config) {

        // Transfer properties from configuration.
        SparkConf sparkConf = new SparkConf();
        for (Map.Entry<String, String> propertyEntry : config.getProperties().entrySet()) {
            if (sparkConf.contains("spark.master") && propertyEntry.getKey().equals("spark.master")) {
                continue; // Don't want to overwrite value from spark-submit
            }
            if (sparkConf.contains("spark.app.name") && propertyEntry.getKey().equals("spark.app.name")) {
                continue; // Don't want to overwrite value from spark-submit
            }
            sparkConf.set(propertyEntry.getKey(), propertyEntry.getValue());
        }

        // Finally, fill in a few defaults. This is done last to let spark-submit have precedence over the YAML
        // configuration.
        if (!sparkConf.contains("spark.master")) {
            sparkConf.set("spark.master", "local[*]");
        }
        if (!sparkConf.contains("spark.app.name")) {
            sparkConf.set("spark.app.name", "Unnamed Application - "
                + Integer.toHexString(new Random(System.currentTimeMillis()).nextInt(65536)));
        }
        return sparkConf;
    }
}
