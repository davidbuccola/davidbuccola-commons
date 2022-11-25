package net.davidbuccola.commons.cassandra;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.Map;

import static net.davidbuccola.commons.Slf4jUtils.info;

public class CassandraConnectorProvider implements Provider<CassandraConnector> {

    public static final String PROPERTIES = "cassandraProperties";

    // A dummy Spark config when we want to use the connector outside of Spark.
    private static final SparkConf DUMMY_SPARK_CONF = new SparkConf()
        .setMaster("local[*]")
        .setAppName("StatementExecutor")
        .set("spark.driver.userClassPathFirst", "true") // Reduces classpath problems
        .set("spark.executor.userClassPathFirst", "true"); // Reduces classpath problems

    private final Map<String, String> properties;

    @Inject
    public CassandraConnectorProvider(@Named(PROPERTIES) Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public CassandraConnector get() {
        // Retrieve the spark config needed by the connector. If running inside of Spark the SparkConf of the caller
        // is used. Otherwise, a dummy SparkConf is used.
        SparkConf sparkConf = SparkContext.getOrCreate(DUMMY_SPARK_CONF).getConf();

        transferConnectorPropertiesToSparkConf(properties, sparkConf);
        transferDriverPropertiesToSystem(properties);

        info(LoggerFactory.getLogger(getClass()), "Initializing Cassandra Connector", () -> ImmutableMap.of("sparkConf", Arrays.asList(sparkConf.getAll())));
        return CassandraConnector.apply(sparkConf);
    }

    private static void transferConnectorPropertiesToSparkConf(Map<String, String> properties, SparkConf sparkConf) {
        properties.forEach((key, value) -> {
            String effectiveKey = key.startsWith("cassandra.") ? "spark." + key : key;
            if (effectiveKey.startsWith("spark.cassandra.")) {
                sparkConf.setIfMissing(effectiveKey, value);
            }
        });
    }

    private static void transferDriverPropertiesToSystem(Map<String, String> properties) {
        properties.forEach((key, value) -> {
            if (key.startsWith("datastax-java-driver.")) {
                System.getProperties().computeIfAbsent(key, k -> value);
            }
        });
    }
}
