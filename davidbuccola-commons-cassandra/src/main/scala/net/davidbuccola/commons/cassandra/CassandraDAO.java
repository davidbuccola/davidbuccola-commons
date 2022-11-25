package net.davidbuccola.commons.cassandra;

import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A base for DAOs that access Cassandra-based data. A DSL is implemented here for conveniently expressing asynchronous
 * CQL sequences.
 */
public abstract class CassandraDAO {

    private transient Logger logger;

    protected final ExecutorService executor;
    protected final CassandraConnector connector;
    private final BucketCalculator bucketCalculator;
    private final Map<String, SimpleStatement> namedStatements;

    protected CassandraDAO(CassandraConnector connector, ExecutorService executor, BucketCalculator bucketCalculator) {
        this.executor = executor;
        this.connector = connector;
        this.namedStatements = new ConcurrentHashMap<>();
        this.bucketCalculator = bucketCalculator;
    }

    protected final CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
        return connector.jWithSessionDo(session -> {
            try {
                return session.executeAsync(getLogger().isTraceEnabled() ? statement.setTracing(true) : statement)

                    .whenComplete((resultSet, exception) -> {
                        if (getLogger().isTraceEnabled()) {
                            logExecutionInfo(resultSet.getExecutionInfo());

                            if (resultSet.getExecutionInfo().getTracingId() != null) {
                                resultSet.getExecutionInfo().getQueryTraceAsync().whenComplete((queryTrace, e) -> {
                                    if (queryTrace != null) {
                                        logQueryTrace(queryTrace);
                                    }
                                });
                            }

                            if (exception == null && !resultSet.wasApplied()) {
                                throw new StatementExecutorException("Statement wasn't applied");
                            }
                        }
                    });

            } catch (Exception e) {
                CompletableFuture<AsyncResultSet> failedFuture = new CompletableFuture<>();
                failedFuture.completeExceptionally(e);
                return failedFuture;
            }
        });
    }

    protected final CompletionStage<PreparedStatement> prepareAsync(SimpleStatement statement) {
        return connector.jWithSessionDo(session -> {
            try {
                return session.prepareAsync(statement);

            } catch (Exception e) {
                CompletableFuture<PreparedStatement> failedFuture = new CompletableFuture<>();
                failedFuture.completeExceptionally(e);
                return failedFuture;
            }
        });
    }

    /**
     * Builds and caches a {@link SimpleStatement} from a CQL string with the help of the given builder.
     */
    protected final CompletableFuture<SimpleStatement> buildStatementOnce(String name, Supplier<SimpleStatement> statementBuilder) {
        return CompletableFuture.completedFuture(namedStatements.computeIfAbsent(name, key -> statementBuilder.get()));
    }

    protected <R> CompletableFuture<List<R>> process(AsyncResultSet resultSet, Function<List<Row>, CompletableFuture<List<R>>> processor) {
        List<R> accumulatedResults = new ArrayList<>();
        return processRemainingPages(resultSet, accumulatedResults, processor)
            .thenApply(v -> accumulatedResults);
    }

    private <R> CompletableFuture<Void> processRemainingPages(AsyncResultSet resultSet, List<R> accumulatedResults, Function<List<Row>, CompletableFuture<List<R>>> processor) {
        Optional<CompletionStage<AsyncResultSet>> futureNextPage = fetchNextPage(resultSet); // Start read-ahead
        CompletableFuture<Void> futurePageCompletion = processor.apply(getAvailableRows(resultSet)).thenAccept(accumulatedResults::addAll);

        if (futureNextPage.isPresent()) {
            return futurePageCompletion
                .thenCompose(unused -> futureNextPage.get())
                .thenComposeAsync(nextPage -> processRemainingPages(nextPage, accumulatedResults, processor), executor);

        } else {
            return futurePageCompletion;
        }
    }

    private static Optional<CompletionStage<AsyncResultSet>> fetchNextPage(AsyncResultSet currentPage) {
        if (currentPage.getExecutionInfo().getPagingState() != null) {
            return Optional.of(currentPage.fetchNextPage());
        } else {
            return Optional.empty();
        }
    }

    private static List<Row> getAvailableRows(AsyncResultSet resultSet) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0, limit = resultSet.remaining(); i < limit; i++) {
            rows.add(resultSet.one());
        }
        return rows;
    }

    protected final String computeBucket(Object value) {
        return bucketCalculator.computeBucket(value);
    }

    protected final <V> Map<String, List<V>> groupByBucket(Collection<V> values) {
        return bucketCalculator.groupByBucket(values);
    }

    /**
     * Lazily populate {@link #logger} so it can be transient for distributed serialization.
     */
    protected final Logger getLogger() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        return logger;
    }

    private void logExecutionInfo(ExecutionInfo executionInfo) {
        StringBuilder builder = new StringBuilder("C* Execution Info");
        builder.append(", responseSize=").append(executionInfo.getCompressedResponseSizeInBytes());
        if (!executionInfo.getWarnings().isEmpty()) {
            builder.append(", warnings=").append("[").append(String.join(",", executionInfo.getWarnings())).append("]");
        }
        builder.append(", tracingId=").append(executionInfo.getTracingId());

        getLogger().debug(builder.toString());
    }

    private void logQueryTrace(QueryTrace queryTrace) {
        StringBuilder builder = new StringBuilder("C* Query Trace");
        builder.append(", tracingId=").append(queryTrace.getTracingId());
        builder.append(", requestType=").append(queryTrace.getRequestType());
        builder.append(", duration=").append(String.format("%7.3fms", (double) queryTrace.getDurationMicros() / 1000.));
        builder.append(", trace=");

        queryTrace.getEvents().forEach(event -> builder.append("\n    ").append(formatTraceEvent(event)));
        builder.append("\n");

        getLogger().debug(builder.toString());
    }

    private static String formatTraceEvent(TraceEvent event) {
        double elapsedMillis = (double) event.getSourceElapsedMicros() / 1000.;
        return String.format("%.3fms %s[%s] %s", elapsedMillis, event.getSourceAddress(), event.getThreadName(), event.getActivity());
    }

}
