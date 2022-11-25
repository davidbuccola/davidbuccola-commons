package net.davidbuccola.commons;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Thrown when an exception occurs on one or more of a list of {@link CompletableFuture}.
 *
 * @see FutureUtils#forAllOf(Collection, Function)
 * @see FutureUtils#forAllOf(Collection, boolean, Function)
 */
public class AggregateFutureException extends RuntimeException {

    private final List<CompletableFuture<?>> elementFutures;

    public <T> AggregateFutureException(List<CompletableFuture<T>> elementFutures, Throwable cause) {
        this("One or more operations failed", elementFutures, cause);
    }

    public <T> AggregateFutureException(String message, List<CompletableFuture<T>> elementFutures, Throwable cause) {
        super(message, cause);
        this.elementFutures = new ArrayList<>(elementFutures);
    }

    /**
     * Gets the collection of element futures. Some futures may have succeeded and some may have failed. The fact that
     * this exception being thrown means that at least one failed.
     */
    public List<CompletableFuture<?>> getElementFutures() {
        return elementFutures;
    }
}
