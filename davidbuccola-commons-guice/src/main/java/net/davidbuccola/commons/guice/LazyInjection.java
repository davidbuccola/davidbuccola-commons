package net.davidbuccola.commons.guice;

import com.google.common.collect.MapMaker;
import com.google.inject.Injector;
import com.google.inject.Key;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * A holder for a lazily injected value. Lazily injected means it is injected on demand (first use). This becomes
 * important in distributed processing frameworks where it's common for code to be serialized and deserialized. This
 * holder can be used when the injected value is not serializable. The injected value is marked as transient so it is
 * not serialized.  A new instance is obtained from a {@link LazyInjector} when this holder is deserialized at the
 * destination.
 */
public final class LazyInjection<T> implements Serializable, Provider<T> {
    private static final long serialVersionUID = -738998312511020129L;

    /**
     * A cache of injections that have already been resolved so the injection is shared by multiple serialized copies
     * originating from the same {@link LazyInjection}.
     */
    private static final transient Map<UUID, Object> values = Collections.synchronizedMap(new MapMaker().weakValues().makeMap());

    /**
     * A unique identifier for this instance. In the face of serialization schemes that may not preserve the one-to-one
     * correspondence between a source instance and serialized/deserialized instance, this represents a logical, unique
     * identifier which ties together multiple deserialized instances that correspond to the same original instance.
     */
    private final UUID uuid = UUID.randomUUID();

    private final Injector injector;
    private Type type;
    private volatile transient T value;

    @Inject
    public LazyInjection(LazyInjector injector) {
        this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
        if (value == null) {
            if (type == null) {
                throw new IllegalStateException(
                        "Value type hasn't been set. This LazyInjection probably wasn't instantiated using a LazyInjector");
            }

            synchronized (this) {
                if (value == null) {
                    value = (T) values.computeIfAbsent(uuid, key -> injector.getInstance(Key.get(type)));
                }
            }
        }
        return value;
    }

    /**
     * Indicates whether a value has been injected yet. (Used for unit tests).
     */
    boolean isInjected() {
        return value != null;
    }

    /**
     * Sets the value type. This is used in conjunction with {@link LazyInjector} to make lazy injection work with
     * generics.
     */
    void setType(Type type) {
        this.type = type;
    }
}
