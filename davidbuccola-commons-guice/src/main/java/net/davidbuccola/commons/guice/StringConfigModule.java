package net.davidbuccola.commons.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.gwizard.config.ConfigClass;
import org.gwizard.config.PropertyPrefix;

import javax.inject.Qualifier;
import javax.inject.Singleton;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A {@link Module} which initializes configuration using YAML supplied as a {@link String}.
 * <p>
 * This is an adaptation of {@link org.gwizard.config.ConfigModule}. The standard gwizard class takes input from a
 * {@link File}. This implementation accepts input from a {@link String} in order to support serialization in
 * distributed processing frameworks.
 */
public abstract class StringConfigModule extends AbstractModule {
    private final String configString;
    private final Class<?> configClass;
    private final String propertyPrefix;

    protected StringConfigModule(String configString, Class<?> configClass) {
        this(configString, configClass, "config");
    }

    protected StringConfigModule(String configString, Class<?> configClass, String propertyPrefix) {
        this.configString = configString;
        this.configClass = configClass;
        this.propertyPrefix = propertyPrefix;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void configure() {
        // Binding a dynamic class is surprisingly complicated
        bind((Class<Object>) configClass).toProvider(new TypeLiteral<StringConfigProvider<Object>>() {
        }).in(Singleton.class);
    }

    @Provides
    @PropertyPrefix
    public String propertyPrefix() {
        return propertyPrefix;
    }

    @Provides
    @ConfigClass
    public Class<?> configClass() {
        return configClass;
    }

    @Provides
    @ConfigString
    public String configString() {
        return configString;
    }

    @Provides
    @Singleton
    public Validator validator() {
        return Validation.buildDefaultValidatorFactory().getValidator();
    }

    /**
     * Qualifier for the config input string.
     */
    @Qualifier
    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @interface ConfigString {
    }
}