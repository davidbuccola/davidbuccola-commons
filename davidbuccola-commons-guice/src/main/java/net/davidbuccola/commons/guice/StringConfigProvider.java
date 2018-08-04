package net.davidbuccola.commons.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import org.gwizard.config.ConfigClass;
import org.gwizard.config.PropertyPrefix;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * A {@link Provider} of a configuration objects initialized by {@link StringConfigModule}.
 */
class StringConfigProvider<T> implements Provider<T> {

    private final String configString;
    private final ConfigurationFactory<?> configurationFactory;

    @Inject
    public StringConfigProvider(
        Validator validator,
        ObjectMapper objectMapper,
        @ConfigClass Class<?> configClass,
        @PropertyPrefix String propertyPrefix,
        @StringConfigModule.ConfigString @Nullable String configString) {

        this.configString = configString;
        this.configurationFactory = new ConfigurationFactory<>(configClass, validator, objectMapper, propertyPrefix);
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