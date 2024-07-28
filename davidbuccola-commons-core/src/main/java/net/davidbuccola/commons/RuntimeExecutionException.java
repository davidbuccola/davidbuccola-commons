package net.davidbuccola.commons;

import java.util.concurrent.ExecutionException;

/**
 * An unchecked substitute for {@link ExecutionException} since Lambdas and Streams don't like checked exceptions.
 */
public class RuntimeExecutionException extends RuntimeException {

    public RuntimeExecutionException(ExecutionException executionException) {
        super(executionException.getCause().getMessage(), executionException.getCause());
        setStackTrace(executionException.getStackTrace());
    }
}
