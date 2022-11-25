package net.davidbuccola.commons.cassandra;

@SuppressWarnings({"WeakerAccess", "unused"})
public class StatementExecutorException extends RuntimeException {
    public StatementExecutorException(String message) {
        super(message);
    }

    public StatementExecutorException(String message, Throwable cause) {
        super(message, cause);
    }
}
