package net.davidbuccola.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;
import static net.davidbuccola.commons.Slf4jUtils.trace;

/**
 * Utilities to help with {@link Future} and {@link CompletableFuture}.
 */
public final class FutureUtils {

    private static final Logger log = LoggerFactory.getLogger(FutureUtils.class);

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
     * @param stopOnException whether to cancel pending asynchronous operations when an exception occurs on any
     *                        operation.
     * @param function        asynchronous operation applied to all elements
     */
    public static <A, R> CompletableFuture<List<R>> forAllOf(Collection<A> elements, boolean stopOnException, Function<A, CompletableFuture<R>> function) throws AggregateFutureException {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        List<CompletableFuture<R>> elementFutures = synchronizedList(new ArrayList<>());
        for (A element : elements) {
            if (firstException.get() == null) {

                trace(log, () -> "forAllOf: starting element processing: " + element);

                CompletableFuture<R> elementFuture = function.apply(element);
                elementFutures.add(elementFuture);
                elementFuture.whenComplete((result, exception) -> {

                    trace(log, () -> "forAllOf: element processing finished: " + element + ": " + elementFuture);

                    if (exception != null && !(exception instanceof CancellationException)) {
                        if (firstException.compareAndSet(null, exception)) {
                            if (stopOnException) {
                                // Cancel remaining activity elements
                                elementFutures.forEach(future -> {
                                    if (!future.isDone()) {
                                        future.cancel(true);
                                    }
                                });
                            }
                        }
                    }
                });
            } else {
                trace(log, () -> "forAllOf: element cancelled before processing: " + element);

                elementFutures.add(cancelledFuture());
            }
        }

        return CompletableFuture.allOf(toArray(elementFutures))
            .handle((nothing, exception) -> {
                log.trace("forAllOf: all elements have completed (either successfully or exceptionally", exception);

                if (firstException.get() != null) {
                    throw new AggregateFutureException(elementFutures, firstException.get()); // Throw with element information attached

                } else if (exception != null) {
                    throw new AggregateFutureException(elementFutures, exception); // Throw with element information attached

                } else {
                    return elementFutures.stream().map(FutureUtils::awaitFuture).collect(toList()); // Aggregate element results
                }
            });
    }

    /**
     * Retrieves the result of a {@link Future}. The primary purpose of this helper is to convert checked exceptions to
     * unchecked exceptions for more convenient use in Lambdas.
     */
    public static <R> R awaitCompletionStage(CompletionStage<R> completionStage) {
        return awaitFuture(completionStage.toCompletableFuture());
    }

    /**
     * Retrieves the result of a {@link Future}. The primary purpose of this helper is to convert checked exceptions to
     * unchecked exceptions for more convenient use in Lambdas.
     */
    public static <R> R awaitFuture(Future<R> future) {
        try {
            return future.get();

        } catch (ExecutionException e) {
            throw new RuntimeExecutionException(e);

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
            .orElseThrow(() -> new AssertionError("Future didn't fail"));
    }

    private static <U> CompletableFuture<U> cancelledFuture() {
        CompletableFuture<U> future = new CompletableFuture<>();
        future.cancel(true);
        return future;
    }

    @SuppressWarnings({"unchecked"})
    private static <T> CompletableFuture<T>[] toArray(Collection<CompletableFuture<T>> elements) {
        return (CompletableFuture<T>[]) elements.toArray(new CompletableFuture[0]);
    }
}
