package net.davidbuccola.commons.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.YamlConfigurationFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * A {@link Provider} of a configuration objects initialized by {@link YamlConfigModule}.
 */
class YamlConfigProvider<T> implements Provider<T> {

    private final String configString;
    private final ConfigurationFactory<?> configurationFactory;

    @Inject
    public YamlConfigProvider(
        ObjectMapper objectMapper,
        @YamlConfigModule.ConfigClass Class<?> configClass,
        @YamlConfigModule.ConfigString @Nullable String configString) {

        this.configString = configString;
        this.configurationFactory = new YamlConfigurationFactory<>(
            configClass, null, objectMapper, "config");
    }

    @SuppressWarnings("unchecked")
    public T get() {
        try {
            if (configString != null && configString.trim().length() != 0) {
                return (T) configurationFactory.build((ConfigurationSourceProvider) path -> inputStreamFrom(configString), "inline string");
            } else {
                return (T) configurationFactory.build();
            }
        } catch (IOException e) {
            throw new StringConfigException("Failed to process configuration", e);

        } catch (ConfigurationException e) {
            throw new StringConfigException("Invalid configuration", e);
        }
    }

    private static InputStream inputStreamFrom(String value) {
        return new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));
    }

    public static final class StringConfigException extends RuntimeException {
        StringConfigException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}