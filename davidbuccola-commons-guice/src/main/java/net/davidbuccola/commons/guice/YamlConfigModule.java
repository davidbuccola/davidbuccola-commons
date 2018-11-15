package net.davidbuccola.commons.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import javax.inject.Qualifier;
import javax.inject.Singleton;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A {@link Module} which initializes configuration using YAML supplied as a {@link String}.
 * <p>
 * Most of the complexity here is just get get dynamically generated providers for config classes with Java generics.
 */
public class YamlConfigModule extends AbstractModule {
    private final String configString;
    private final Class<?> configClass;

    public YamlConfigModule(String configString, Class<?> configClass) {
        this.configString = configString;
        this.configClass = configClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void configure() {
        // Binding a dynamic class is surprisingly complicated
        bind((Class<Object>) configClass).toProvider(new TypeLiteral<YamlConfigProvider<Object>>() {
        }).in(Singleton.class);
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

    /**
     * Qualifier for the config input string.
     */
    @Qualifier
    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @interface ConfigString {
    }

    /**
     * Qualifier for the config class.
     */
    @Qualifier
    @Target({FIELD, PARAMETER, METHOD})
    @Retention(RUNTIME)
    @interface ConfigClass {
    }
}