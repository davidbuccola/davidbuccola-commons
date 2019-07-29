package net.davidbuccola.commons;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;

/**
 * Utilities to help with {@link Future} and {@link CompletableFuture}.
 */
public final class FutureUtils {

    private FutureUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Executes an asynchronous function for each the element of a collection and waits for them all to complete.
     * <p>
     * When an exception occurs for any element the remaining unfinished asynchronous operations are cancelled.
     *
     * @param elements the elements upon which to operate
     * @param function asynchronous operation applied to all elements
     */
    public static <A, R> CompletableFuture<List<R>> forAllOf(Collection<A> elements, Function<A, CompletableFuture<R>> function) throws AggregateFutureException {
        return forAllOf(elements, true, function);
    }

    /**
     * Executes an asynchronous function for each the element of a collection and waits for them all to complete. The
     * results are returned as a list containing one result entry for each element from the input collection.
     *
     * @param elements        the elements upon which to operate
     * @param stopOnException whether or not to cancel pending asynchronous operations when an exception occurs on any
     *                        operation.
     * @param function        asynchronous operation applied to all elements
     */
    public static <A, R> CompletableFuture<List<R>> forAllOf(Collection<A> elements, boolean stopOnException, Function<A, CompletableFuture<R>> function) throws AggregateFutureException {
        AtomicBoolean exceptionOccurred = new AtomicBoolean(false);
        List<CompletableFuture<R>> elementFutures = synchronizedList(new ArrayList<>());
        for (A element : elements) {
            if (!exceptionOccurred.get()) {
                CompletableFuture<R> elementFuture = function.apply(element);
                elementFutures.add(elementFuture);
                elementFuture.whenComplete((result, exception) -> {
                    if (stopOnException && exception != null) {
                        if (exceptionOccurred.compareAndSet(false, true)) {
                            elementFutures.forEach(future -> future.cancel(true));
                        }
                    }
                });
            } else {
                elementFutures.add(cancelledFuture());
            }
        }

        return CompletableFuture.allOf(toArray(elementFutures))
            .handle((nothing, exception) -> {
                if (exception == null) {
                    return elementFutures.stream().map(FutureUtils::getResult).collect(toList()); // Aggregate element results

                } else {
                    Throwable effectiveException = exception instanceof CompletionException ? exception.getCause() : exception;
                    throw new AggregateFutureException(elementFutures, effectiveException); // Rethrow with element information
                }
            });
    }

    /**
     * Retrieves the result of a {@link Future}. The primary purpose of this helper is to unwrap the {@link
     * ExecutionException} and convert checked exceptions to unchecked exceptions for more convenient use in Lambdas.
     */
    public static <R> R getResult(Future<R> future) {
        try {
            return future.get();

        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch (RuntimeException e) {
            throw e;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the exception from a {@link CompletableFuture} if one exists.
     */
    public static Optional<Throwable> getOptionalExceptionFrom(CompletableFuture<?> future) {
        if (future.isCompletedExceptionally()) {
            try {
                future.get(); // Should fail with appropriate exception
                return Optional.empty();

            } catch (Exception e) {
                return Optional.of(unwrapConcurrentThrowable(e));
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Removes a {@link CompletionException} or {@link ExecutionException} wrapper from an exception thrown by {@link
     * java.util.concurrent}.
     */
    public static Throwable unwrapConcurrentThrowable(Throwable throwable) {
        if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            return throwable.getCause();
        } else {
            return throwable;
        }
    }

    /**
     * Retrieves the exception from a failed {@link CompletableFuture}.
     */
    public static Throwable getExceptionFrom(CompletableFuture<?> future) {
        return getOptionalExceptionFrom(future)
            .orElseThrow(() -> new AssertionError("Future did't fail"));
    }

    private static <U> CompletableFuture<U> cancelledFuture() {
        CompletableFuture<U> future = new CompletableFuture<>();
        future.cancel(true);
        return future;
    }

    @SuppressWarnings({"unchecked", "SuspiciousToArrayCall"})
    private static <T> CompletableFuture<T>[] toArray(Collection<CompletableFuture<T>> elements) {
        return (CompletableFuture<T>[]) elements.toArray(new CompletableFuture[0]);
    }
}
