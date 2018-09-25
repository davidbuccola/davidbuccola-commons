package net.davidbuccola.commons;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * Utilities to help with {@link Future} and {@link CompletableFuture}.
 */
public final class FutureUtils {

    private FutureUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Executes an asynchronous function for each the element of a collection and waits for them all to complete. The
     * results are returned as a list containing one result entry for each element from the input collection.
     */
    @SuppressWarnings("unchecked")
    public static <T, U> CompletableFuture<List<U>> forAllOf(Collection<T> elements, Function<T, CompletableFuture<U>> function) {
        CompletableFuture<U>[] elementFutures = elements.stream().map(function).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(elementFutures)
                .thenApply(v -> Arrays.stream(elementFutures).map(FutureUtils::getFutureResult).collect(toList()));
    }

    /**
     * Retrieves the result of a {@link Future}. The primary purpose of this helper is to unwrap the {@link
     * ExecutionException} and convert checked exceptions to unchecked exceptions for more convenient use in Lambdas.
     */
    public static <T> T getFutureResult(Future<T> future) {
        try {
            return future.get();

        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        } catch (RuntimeException e) {
            throw e;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
