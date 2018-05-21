package net.davidbuccola.commons;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Utilities to help with {@link ListenableFuture}.
 */
public final class FuturesUtils {

    private FuturesUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    /**
     * Performs an asynchronous operation and waits for completion, return the result.
     * <p>
     * The primary purpose of this helper is to unwrap the {@link ExecutionException}.
     */
    public static <T> T getAsyncResult(Supplier<ListenableFuture<T>> operation) throws Exception {
        try {
            return operation.get().get();

        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Performs an asynchronous operation and waits for completion.
     * <p>
     * The primary purpose of this helper is to unwrap the {@link ExecutionException}.
     */
    public static <T> void waitForAsyncCompletion(Supplier<ListenableFuture<T>> operation) throws Exception {
        getAsyncResult(operation);
    }
}
