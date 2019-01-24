package net.davidbuccola.commons;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import io.dropwizard.configuration.*;

import javax.annotation.Nullable;
import javax.validation.Validator;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Utilities for loading configuration files and binding them to configuration objects.
 */
public class ConfigUtils {

    private ConfigUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static <T> T buildConfig(Properties properties, Class<T> configClass) throws IOException, ConfigurationException {
        return buildConfig(properties, configClass, true);
    }

    public static <T> T buildConfig(Properties properties, Class<T> configClass, boolean failOnUnknownProperties) throws IOException, ConfigurationException {
        return buildConfig(new PropertiesConfigurationSourceProvider(properties), "properties", configClass, failOnUnknownProperties);
    }

    public static <T> T buildConfig(String resource, Class<T> configClass) throws IOException, ConfigurationException {
        return buildConfig(resource, configClass, true);
    }

    public static <T> T buildConfig(String resource, Class<T> configClass, boolean failOnUnknownProperties) throws IOException, ConfigurationException {
        return buildConfig(new ResourceConfigurationSourceProvider(), resource, configClass, failOnUnknownProperties);
    }

    public static <T> T buildConfig(File file, Class<T> configClass) throws IOException, ConfigurationException {
        return buildConfig(file, configClass, true);
    }

    public static <T> T buildConfig(File file, Class<T> configClass, boolean failOnUnknownProperties) throws IOException, ConfigurationException {
        return buildConfig(new FileConfigurationSourceProvider(), file.getAbsolutePath(), configClass, failOnUnknownProperties);
    }

    private static <T> T buildConfig(ConfigurationSourceProvider source, String path, Class<T> configClass, boolean failOnUnknownProperties) throws IOException, ConfigurationException {
        String format = path.substring(path.lastIndexOf('.') + 1);
        return getConfigurationFactory(format, configClass, failOnUnknownProperties).build(source, path);
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

    /**
     * An implementation of {@link ConfigurationSourceProvider} that reads the configuration from {@link Properties}
     */
    private static class PropertiesConfigurationSourceProvider implements ConfigurationSourceProvider {

        private final Properties properties;

        PropertiesConfigurationSourceProvider(Properties properties) {
            this.properties = properties;
        }

        @Override
        public InputStream open(String path) throws IOException {
            StringWriter temp = new StringWriter();
            properties.store(temp, path);
            return new ByteArrayInputStream(temp.toString().getBytes(Charset.forName("UTF-8")));
        }
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
