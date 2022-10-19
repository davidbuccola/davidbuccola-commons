package net.davidbuccola.commons.guice;

import com.google.common.collect.MapMaker;
import com.google.inject.Module;
import com.google.inject.*;
import com.google.inject.internal.MoreTypes;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.*;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

/**
 * An {@link Injector} that lazily configures itself in distributed processing environments (like Spark, Storm and
 * Flink). The key requirement is that the injector be {@link Serializable}.
 * <p>
 * This is a wrapper for a real injector that isn't initialized until the wrapper is deserialized in it's target
 * execution environment.
 */
public final class LazyInjector implements Injector, Serializable {

    private static final long serialVersionUID = -492287354346653996L;

    /**
     * A cache of injections that have already been resolved so the injection is shared by multiple serialized copies
     * originating from the same {@link LazilyInjected}.
     */
    private static final Map<UUID, Injector> injectors = Collections.synchronizedMap(new MapMaker().weakValues().makeMap());

    /**
     * A unique identifier for this instance. In the face of serialization schemes that may not preserve the one-to-one
     * correspondence between a source instance and serialized/deserialized instance, this represents a logical, unique
     * identifier which ties together multiple deserialized instances that correspond to the same original instance.
     */
    private final UUID uuid = UUID.randomUUID();

    private final SerializableSupplier<Collection<Module>> moduleSupplier;

    private volatile transient Injector injector;

    LazyInjector(SerializableSupplier<Collection<Module>> moduleSupplier) {
        this.moduleSupplier = moduleSupplier;
    }

    @Override
    public void injectMembers(Object instance) {
        getInjector().injectMembers(instance);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
        return getInjector().getMembersInjector(typeLiteral);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return getInjector().getMembersInjector(type);
    }

    @Override
    public Map<Key<?>, Binding<?>> getBindings() {
        return getInjector().getBindings();
    }

    @Override
    public Map<Key<?>, Binding<?>> getAllBindings() {
        return getInjector().getAllBindings();
    }

    @Override
    public <T> Binding<T> getBinding(Key<T> key) {
        return getInjector().getBinding(key);
    }

    @Override
    public <T> Binding<T> getBinding(Class<T> type) {
        return getInjector().getBinding(type);
    }

    @Override
    public <T> Binding<T> getExistingBinding(Key<T> key) {
        return getInjector().getExistingBinding(key);
    }

    @Override
    public <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type) {
        return getInjector().findBindingsByType(type);
    }

    @Override
    public <T> Provider<T> getProvider(Key<T> key) {
        return getInjector().getProvider(key);
    }

    @Override
    public <T> Provider<T> getProvider(Class<T> type) {
        return getInjector().getProvider(type);
    }

    @Override
    public <T> T getInstance(Key<T> key) {
        return getInjector().getInstance(key);
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return getInjector().getInstance(type);
    }

    @Override
    public Injector getParent() {
        return getInjector().getParent();
    }

    @Override
    public Injector createChildInjector(Iterable<? extends Module> modules) {
        return getInjector().createChildInjector(modules);
    }

    @Override
    public Injector createChildInjector(Module... modules) {
        return getInjector().createChildInjector(modules);
    }

    @Override
    public Map<Class<? extends Annotation>, Scope> getScopeBindings() {
        return getInjector().getScopeBindings();
    }

    @Override
    public Set<TypeConverterBinding> getTypeConverterBindings() {
        return getInjector().getTypeConverterBindings();
    }

    @Override
    public List<Element> getElements() {
        return getInjector().getElements();
    }

    @Override
    public Map<TypeLiteral<?>, List<InjectionPoint>> getAllMembersInjectorInjectionPoints() {
        return getInjector().getAllMembersInjectorInjectionPoints();
    }

    /**
     * Gets a singleton "real" {@link Injector} built from this {@link LazyInjector} wrapper.
     * <p>
     * Extra work is done to make things work in the face of class serialization that moves the provider instance from
     * one JVM to another. Under some serialization mechanisms a single instance of the provider on the source JVM may
     * show up as two instances on the target JVM. Though the two providers have the same values, having two of them
     * destroys the singleton nature of the returned injector. Logic in this class compensates for that problem.
     */
    private Injector getInjector() {
        if (injector == null) {
            synchronized (this) {
                if (injector == null) {
                    injector = injectors.computeIfAbsent(uuid, key -> {
                        List<Module> modules = newArrayList(moduleSupplier.get());
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
            binder.bindListener(Matchers.any(), new TypeListener() {
                @Override
                public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
                    encounter.register((InjectionListener<I>) injectee -> {
                        if (injectee instanceof LazilyInjected) {
                            ((LazilyInjected) injectee).setType(
                                ((MoreTypes.ParameterizedTypeImpl) type.getType()).getActualTypeArguments()[0]);
                        }
                    });
                }
            });
        }

        @Provides
        public LazyInjector lazyInjector() {
            return LazyInjector.this;
        }
    }
}
