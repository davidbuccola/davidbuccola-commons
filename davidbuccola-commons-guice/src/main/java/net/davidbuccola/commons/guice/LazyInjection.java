package net.davidbuccola.commons.guice;

import com.google.common.collect.MapMaker;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * A holder for lazily injected value. Lazily injected means it is injected on demand (first use). This becomes
 * important in distributed processing frameworks where it is common for code to be serialized and deserialized. This
 * holder can be used when the injected value is not serializable. The injected value is marked as transient so it is
 * not serialized. When this holder is deserialized at the destination a new instance is obtained from a {@link
 * LazyInjector}.
 */
public class LazyInjection<T> implements Serializable {
    private static final long serialVersionUID = 4066410296176198636L;

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

    private final LazyInjector injector;
    private final Class<T> type;
    private volatile transient T value;

    public LazyInjection(LazyInjector injector, Class<T> type) {
        this.injector = injector;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public T get() {
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    value = (T) values.computeIfAbsent(uuid, key -> injector.getInstance(type));
                }
            }
        }
        return value;
    }
}
