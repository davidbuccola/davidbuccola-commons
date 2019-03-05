package net.davidbuccola.commons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import io.dropwizard.configuration.*;

import javax.annotation.Nullable;
import javax.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static net.davidbuccola.commons.PropertiesUtils.getProperties;

/**
 * Utilities for loading configuration files and binding them to configuration objects.
 */
public final class ConfigUtils {

    private ConfigUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static <T> Optional<T> getConfig(String path, Class<T> configClass) {
        return getProperties(path)
            .map(properties -> buildConfig(properties, configClass, path));
    }

    public static <T> Optional<T> getConfig(String[] args, Class<T> configClass) {
        return getProperties(args)
            .map(properties -> buildConfig(properties, configClass, "args"));
    }

    public static <T> Optional<T> getConfig(String path, String[] args, Class<T> configClass) {
        return getProperties(path, args)
            .map(properties -> buildConfig(properties, configClass, path));
    }

    private static <T> T buildConfig(Map<String, Object> properties, Class<T> configClass, String path) {
        try {
            return getConfigurationFactory(configClass).build(toConfigurationSourceProvider(properties), path);

        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException(e); // Lambdas don't like checked exceptions
        }
    }

    private static ConfigurationSourceProvider toConfigurationSourceProvider(Map<String, Object> properties) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Properties javaUtilProperties = new Properties();
            javaUtilProperties.putAll(properties);
            javaUtilProperties.store(outputStream, null);
            outputStream.close();

            return path -> new ByteArrayInputStream(outputStream.toByteArray());

        } catch (IOException e) {
            throw new RuntimeException(e); // Lambdas don't like checked exceptions
        }
    }

    private static <T> ConfigurationFactory<T> getConfigurationFactory(Class<T> configClass) {
        ConfigurationFactoryFactory<T> factoryFactory = PropertiesConfigurationFactory::new;
        ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return factoryFactory.create(configClass, null, objectMapper, "override");
    }

    /**
     * A factory class for loading properties configuration files, binding them to configuration objects, and validating
     * their constraints. Allows for overriding configuration parameters from system properties.
     *
     * @param <T> the type of the configuration objects to produce
     */
    private static class PropertiesConfigurationFactory<T> extends BaseConfigurationFactory<T> {

        PropertiesConfigurationFactory(Class<T> klass,
            @Nullable Validator validator,
            ObjectMapper objectMapper,
            String propertyPrefix) {
            super(new JavaPropsFactory(), JavaPropsFactory.FORMAT_NAME_JAVA_PROPERTIES, klass, validator, objectMapper, propertyPrefix);
        }
    }
}
