package net.davidbuccola.commons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import io.dropwizard.configuration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.Validator;
import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

/**
 * Utilities for loading configuration files and binding them to configuration objects.
 */
public class ConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    private ConfigUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static <T> T buildConfig(String[] args, Class<T> configClass) {
        return buildConfig(args, configClass, true);
    }

    public static <T> T buildConfig(String[] args, Class<T> configClass, boolean failOnUnknownProperties) {
        return getConfigPath(args)
            .map(path -> buildConfig(path, configClass, failOnUnknownProperties))
            .orElseGet(() -> getInlineProperties(args)
                .map(properties -> buildConfig(properties, configClass, false))
                .orElseThrow(() -> new RuntimeException("Missing configuration, add \"--config=<path>\"")));
    }

    public static <T> T buildConfig(String path, Class<T> configClass) {
        return buildConfig(path, configClass, true);
    }

    public static <T> T buildConfig(String path, Class<T> configClass, boolean failOnUnknownProperties) {
        return buildConfig(path, configClass, failOnUnknownProperties, ConfigUtils::getConfigInputStream);
    }

    public static <T> T buildConfig(Properties properties, Class<T> configClass) {
        return buildConfig(properties, configClass, true);
    }

    public static <T> T buildConfig(Properties properties, Class<T> configClass, boolean failOnUnknownProperties) {
        return buildConfig("properties", configClass, failOnUnknownProperties, path -> {
            StringWriter temp = new StringWriter();
            properties.store(temp, path);
            return new ByteArrayInputStream(temp.toString().getBytes(Charset.forName("UTF-8")));
        });
    }

    private static <T> T buildConfig(String path, Class<T> configClass, boolean failOnUnknownProperties, ConfigurationSourceProvider source) {
        try {
            String format = path.substring(path.lastIndexOf('.') + 1);
            return getConfigurationFactory(format, configClass, failOnUnknownProperties).build(source, path);
        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException(e); // Convert to runtime to pass through lambdas
        }
    }

    private static <T> ConfigurationFactory<T> getConfigurationFactory(String format, Class<T> configClass, boolean failOnUnknownProperties) {
        ConfigurationFactoryFactory<T> factoryFactory = getConfigurationFactoryFactory(format);
        ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknownProperties);
        return factoryFactory.create(configClass, null, objectMapper, "override");
    }

    private static <T> ConfigurationFactoryFactory<T> getConfigurationFactoryFactory(String format) {
        switch (format) {
            case "json":
                return JsonConfigurationFactory::new;

            case "properties":
                return PropertiesConfigurationFactory::new;

            case "yaml":
            case "yml":
                return YamlConfigurationFactory::new;

            default:
                throw new IllegalArgumentException("Unsupported configuration format - " + format);
        }
    }

    private static Optional<String> getConfigPath(String[] args) {
        Iterator<String> argCursor = Arrays.asList(args).iterator();
        while (argCursor.hasNext()) {
            String arg = argCursor.next();
            if (arg.startsWith("--config")) {
                String[] pieces = arg.split("=");
                if (pieces.length > 1) {
                    return Optional.of(pieces[1]);
                } else {
                    if (argCursor.hasNext()) {
                        String value = argCursor.next();
                        if (!value.startsWith("-")) {
                            return Optional.of(value);
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Properties> getInlineProperties(String[] args) {
        Properties properties = new Properties();
        for (String arg: args) {
            if (arg.length() > 0 && !arg.startsWith("-") && arg.contains("=")) {
                String[] pieces = arg.split("=");
                properties.put(pieces[0], pieces[1]);
            }
        }
        return properties.size() > 0 ? Optional.of(properties) : Optional.empty();
    }

    private static InputStream getConfigInputStream(String path) throws IOException {
        Path filePath = Paths.get(path);
        if (Files.isRegularFile(filePath)) {
            log.info("Reading configuration from local file: " + filePath.toAbsolutePath());

            return Files.newInputStream(filePath);
        }

        URL url = Thread.currentThread().getContextClassLoader().getResource(path);
        if (url != null) {
            log.info("Reading configuration from resource: " + url);

            return url.openStream();
        }

        throw new FileNotFoundException("Failed to find configuration file: " + path);
    }

    /**
     * A factory class for loading properties configuration files, binding them to configuration objects, and validating
     * their constraints. Allows for overriding configuration parameters from system properties.
     *
     * @param <T> the type of the configuration objects to produce
     */
    private static class PropertiesConfigurationFactory<T> extends BaseConfigurationFactory<T> {

        public PropertiesConfigurationFactory(Class<T> klass,
            @Nullable Validator validator,
            ObjectMapper objectMapper,
            String propertyPrefix) {
            super(new JavaPropsFactory(), JavaPropsFactory.FORMAT_NAME_JAVA_PROPERTIES, klass, validator, objectMapper, propertyPrefix);
        }
    }
}
