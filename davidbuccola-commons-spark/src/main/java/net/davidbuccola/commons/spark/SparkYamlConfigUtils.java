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
public class SparkYamlConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(YamlConfigUtils.class);

    /**
     * Finds a configuration file and returns a {@link String} of its contents. The following places are tried in
     * order:
     * <nl>
     * <li>SparkContext using {@link SparkFiles} (The SparkContext must be initialized)</li>
     * <li>Current working directory of the local file system</li>
     * <li>Resource in the Classpath</li>
     * </nl>
     */
    public static String getConfigString(String name) throws IOException {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return getConfigStream(name);
            }
        }.asCharSource(Charsets.UTF_8).read();
    }

    /**
     * Finds a configuration file and returns an {@link InputStream} of its contents. The following places are tried in
     * order:
     * <nl>
     * <li>SparkContext using {@link SparkFiles} (The SparkContext must be initialized)</li>
     * <li>Current working directory of the local file system</li>
     * <li>Resource in the Classpath</li>
     * </nl>
     */
    public static InputStream getConfigStream(String name) throws IOException {
        Path path = Paths.get(SparkFiles.get(name));
        if (Files.isRegularFile(path)) {
            log.info("Reading configuration from the Spark Context: " + path.toAbsolutePath());

            return Files.newInputStream(path);
        }

        return YamlConfigUtils.getConfigStream(name);
    }
}
