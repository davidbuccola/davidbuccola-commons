package net.davidbuccola.commons.guice;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * An extension of {@link Supplier} which adds {@link Serializable}.
 */
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
