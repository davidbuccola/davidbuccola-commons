package net.davidbuccola.commons.guice;

import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.Collection;

/**
 * Factory for {@link LazyInjector}.
 * <p>
 * A {@link LazyInjector} is an injector that lazily configures itself in distributed processing environments (like
 * Spark, Storm and Flink).
 */
public final class LazyGuice {
    private LazyGuice() {
    }

    /**
     * Creates a {@link LazyInjector} which asks for modules at the time of lazy initialization.
     */
    public static Injector createInjector(SerializableSupplier<Collection<Module>> moduleSupplier) {
        return new LazyInjector(moduleSupplier);
    }
}
