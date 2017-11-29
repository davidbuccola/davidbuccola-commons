package net.davidbuccola.commons.guice;

import com.google.common.collect.MapMaker;
import com.google.inject.*;
import com.google.inject.spi.TypeConverterBinding;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.*;

/**
 * An abstract base class for {@link Injector} implementations that lazily configured themselves in distributed
 * processing frameworks (like Spark, Storm and Flink) where the {@link Injector} needs to be {@link Serializable}.
 * <p>
 * This is a wrapper for a real {@link Injector} that is not initialized until the wrapper is serialized and
 * deserialized in it's target execution environment.
 */
public abstract class LazyInjector implements Injector, Serializable {
    private static final long serialVersionUID = -7398125979855798901L;

    /**
     * A cache of Injectors that have already been created so they can be reused and shared.
     */
    private static final transient Map<UUID, Injector> injectors = Collections.synchronizedMap(new MapMaker().weakValues().makeMap());

    /**
     * A unique identifier for this instance. In the face of serialization schemes that may not preserve the one-to-one
     * correspondence between a source instance and serialized/deserialized instance, this represents a logical, unique
     * identifier which ties together multiple deserialized instances that correspond to the same original instance.
     */
    private final UUID uuid = UUID.randomUUID();

    private volatile transient Injector injector;

    /**
     * Returns a list of {@link Module} to be used for initializing the {@link Injector}. It is called later in the
     * cycle once the {@link LazyInjector} is in its target execution environment.
     */
    protected abstract Collection<Module> getModules();

    @Override
    public final void injectMembers(Object instance) {
        getInjector().injectMembers(instance);
    }

    @Override
    public final <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
        return getInjector().getMembersInjector(typeLiteral);
    }

    @Override
    public final <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return getInjector().getMembersInjector(type);
    }

    @Override
    public final Map<Key<?>, Binding<?>> getBindings() {
        return getInjector().getBindings();
    }

    @Override
    public final Map<Key<?>, Binding<?>> getAllBindings() {
        return getInjector().getAllBindings();
    }

    @Override
    public final <T> Binding<T> getBinding(Key<T> key) {
        return getInjector().getBinding(key);
    }

    @Override
    public final <T> Binding<T> getBinding(Class<T> type) {
        return getInjector().getBinding(type);
    }

    @Override
    public final <T> Binding<T> getExistingBinding(Key<T> key) {
        return getInjector().getExistingBinding(key);
    }

    @Override
    public final <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type) {
        return getInjector().findBindingsByType(type);
    }

    @Override
    public final <T> Provider<T> getProvider(Key<T> key) {
        return getInjector().getProvider(key);
    }

    @Override
    public final <T> Provider<T> getProvider(Class<T> type) {
        return getInjector().getProvider(type);
    }

    @Override
    public final <T> T getInstance(Key<T> key) {
        return getInjector().getInstance(key);
    }

    @Override
    public final <T> T getInstance(Class<T> type) {
        return getInjector().getInstance(type);
    }

    @Override
    public final Injector getParent() {
        return getInjector().getParent();
    }

    @Override
    public final Injector createChildInjector(Iterable<? extends Module> modules) {
        return getInjector().createChildInjector(modules);
    }

    @Override
    public final Injector createChildInjector(Module... modules) {
        return getInjector().createChildInjector(modules);
    }

    @Override
    public final Map<Class<? extends Annotation>, Scope> getScopeBindings() {
        return getInjector().getScopeBindings();
    }

    @Override
    public final Set<TypeConverterBinding> getTypeConverterBindings() {
        return getInjector().getTypeConverterBindings();
    }

    /**
     * Gets a singleton "real" {@link Injector} built from this {@link LazyInjector} wrapper.
     * <p>
     * Extra work is done to make things work in the face of class serialization that moves the provider instance from
     * one JVM to another. Under some serialization mechanisms a single instance of the provider on the source JVM may
     * show up as two instances on the target JVM. Though the two providers have the same values, having two of them
     * destroys the singleton nature of the returned {@link Injector}. Logic in this class compensates for that
     * problem.
     */
    private Injector getInjector() {
        if (injector == null) {
            synchronized (this) {
                if (injector == null) {
                    injector = injectors.computeIfAbsent(uuid, key -> {
                        List<Module> modules = new ArrayList<>(getModules());
                        modules.add(new LazyInjectorModule());
                        return Guice.createInjector(modules);
                    });
                }
            }
        }
        return injector;
    }

    private class LazyInjectorModule implements Module {
        @Override
        public void configure(Binder binder) {
        }

        @Provides
        public LazyInjector lazyInjector() {
            return LazyInjector.this;
        }
    }
}
