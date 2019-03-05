package net.davidbuccola.commons;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static net.davidbuccola.commons.Slf4jUtils.debug;

/**
 * Utilities for loading properties from a configuration file or from command args
 */
public class PropertiesUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtils.class);

    private PropertiesUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Get properties from a file or resource.
     */
    public static Optional<Map<String, Object>> getProperties(String path) {
        Optional<Map<String, Object>> properties = loadProperties(path).map(PropertiesUtils::toPropertyMap);
        if (properties.isPresent()) {
            debug(log, "Read properties from " + path, properties::get);
        }
        return properties;
    }

    /**
     * Get properties from the command line. Arguments of the form 'key=value' are considered properties.
     */
    public static Optional<Map<String, Object>> getProperties(String[] args) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        for (String arg : args) {
            if (arg.length() > 0 && !arg.startsWith("-") && arg.contains("=")) {
                int equalOffset = arg.indexOf('=');
                mapBuilder.put(arg.substring(0, equalOffset), arg.substring(equalOffset + 1));
            }
        }
        Map<String, Object> properties = mapBuilder.build();

        if (properties.size() > 0) {
            debug(log, "Extracted properties from command args", () -> properties);

            return Optional.of(properties);

        } else {
            return Optional.empty();
        }
    }

    /**
     * Gets a merged set of properties from a property file and the command line. Properties specified on the command
     * line override values specified in the file.
     */
    public static Optional<Map<String, Object>> getProperties(String path, String[] args) {
        return getProperties(path)
            .map(propertiesFromFile -> ImmutableMap.<String, Object>builder()
                .putAll(propertiesFromFile)
                .putAll(getProperties(args).orElseGet(Collections::emptyMap))
                .build());
    }

    private static Optional<Properties> loadProperties(String path) {
        try {
            Properties properties = new Properties();
            Path filePath = Paths.get(path);
            if (Files.isRegularFile(filePath)) {
                properties.load(Files.newInputStream(filePath));
                return Optional.of(properties);
            }

            URL url = Thread.currentThread().getContextClassLoader().getResource(path);
            if (url != null) {
                properties.load(url.openStream());
                return Optional.of(properties);
            }

            return Optional.empty();

        } catch (IOException e) {
            throw new RuntimeException(e); // Lambdas don't like checked exceptions
        }
    }

    private static Map<String, Object> toPropertyMap(Properties properties) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        properties.forEach((key, value) -> mapBuilder.put(key.toString(), value));
        return mapBuilder.build();
    }
}

