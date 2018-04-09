package net.davidbuccola.commons.guice;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An extension of {@link Consumer} which adds {@link Serializable}.
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

    default SerializableConsumer<T> andThen(SerializableConsumer<? super T> after) {
        Objects.requireNonNull(after);
        return (T t) -> {
            accept(t);
            after.accept(t);
        };
    }
}
