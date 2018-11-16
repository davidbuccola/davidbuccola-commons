package net.davidbuccola.commons.spark;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;
import net.davidbuccola.commons.guice.YamlConfigModule;
import net.davidbuccola.commons.guice.YamlConfigUtils;
import org.apache.spark.SparkFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities to help find and read a YAML configuration (for use with {@link YamlConfigModule}. This variation looks in
 * Spark locations in addition to the places tried by {@link YamlConfigUtils}.
 */
@SuppressWarnings("WeakerAccess")
public final class SparkYamlConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(YamlConfigUtils.class);

    private SparkYamlConfigUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Gets the configuration name from program arguments then reads the configuration and returns a {@link String}
     * representation of its contents. he following places are tried in order:
     * <nl>
     * <li>{@link SparkFiles} (The SparkContext must be initialized)</li>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static String getConfigAsString(String[] args) throws IOException {
        return getConfigAsString(YamlConfigUtils.getConfigName(args));
    }

    /**
     * Reads a configuration file and returns a {@link String} representation of its contents. The following places are
     * tried in order:
     * <nl>
     * <li>{@link SparkFiles} (The SparkContext must be initialized)</li>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static String getConfigAsString(String name) throws IOException {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return getConfigAsStream(name);
            }
        }.asCharSource(Charsets.UTF_8).read();
    }

    /**
     * Reads a configuration file and returns an {@link InputStream} representation of its contents. The following
     * places are tried in order:
     * <nl>
     * <li>{@link SparkFiles} (The SparkContext must be initialized)</li>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static InputStream getConfigAsStream(String name) throws IOException {
        Path path = Paths.get(SparkFiles.get(name));
        if (Files.isRegularFile(path)) {
            log.info("Reading configuration from the Spark Context: " + path.toAbsolutePath());

            return Files.newInputStream(path);
        }

        return YamlConfigUtils.getConfigAsStream(name);
    }
}
