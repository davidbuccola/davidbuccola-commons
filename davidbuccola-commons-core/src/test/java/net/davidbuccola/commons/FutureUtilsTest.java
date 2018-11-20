package net.davidbuccola.commons;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static net.davidbuccola.commons.FutureUtils.getExceptionFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FutureUtilsTest {

    @Test
    public void testAllSuccess() throws Exception {
        Collection<Integer> inputValues = Arrays.asList(0, 1, 2);
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        CompletableFuture<List<Integer>> aggregateFuture = FutureUtils.forAllOf(inputValues, input -> {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        });

        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(0).complete(0);
        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(1).complete(1);
        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(2).complete(2);

        assertThat(aggregateFuture.isDone(), equalTo(true));
        List<Integer> result = aggregateFuture.get();
        assertThat(result, equalTo(inputValues));
    }

    @Test
    public void testFailureOnFirstWithStopping() {
        Collection<Integer> inputValues = Arrays.asList(0, 1, 2);
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        CompletableFuture<List<Integer>> aggregateFuture = FutureUtils.forAllOf(inputValues, input -> {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        });

        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(0).completeExceptionally(new RuntimeException("0"));

        assertThat(aggregateFuture.isDone(), equalTo(true));
        assertThat(aggregateFuture.isCompletedExceptionally(), equalTo(true));
        Throwable exception = getExceptionFrom(aggregateFuture);
        assertThat(exception, instanceOf(AggregateFutureException.class));
        assertThat(exception.getCause().getMessage(), equalTo("0"));

        assertThat(futures.get(0).isCompletedExceptionally(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(0)).getMessage(), equalTo("0"));
        assertThat(futures.get(1).isCancelled(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(1)), instanceOf(CancellationException.class));
        assertThat(futures.get(2).isCancelled(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(2)), instanceOf(CancellationException.class));
    }

    @Test
    public void testFailureOnSecondWithStopping() throws Exception {
        Collection<Integer> inputValues = Arrays.asList(0, 1, 2);
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        CompletableFuture<List<Integer>> aggregateFuture = FutureUtils.forAllOf(inputValues, input -> {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        });

        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(0).complete(0);

        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(1).completeExceptionally(new RuntimeException("1"));

        assertThat(aggregateFuture.isDone(), equalTo(true));
        assertThat(aggregateFuture.isCompletedExceptionally(), equalTo(true));
        Throwable exception = getExceptionFrom(aggregateFuture);
        assertThat(exception, instanceOf(AggregateFutureException.class));
        assertThat(exception.getCause().getMessage(), equalTo("1"));

        assertThat(futures.get(0).isDone(), equalTo(true));
        assertThat(futures.get(0).isCompletedExceptionally(), equalTo(false));
        assertThat(futures.get(0).get(), equalTo(0));
        assertThat(futures.get(1).isCompletedExceptionally(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(1)).getMessage(), equalTo("1"));
        assertThat(futures.get(2).isCancelled(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(2)), instanceOf(CancellationException.class));
    }

    @Test
    public void testFailureWithoutStopping() throws Exception {
        Collection<Integer> inputValues = Arrays.asList(0, 1, 2);
        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        CompletableFuture<List<Integer>> aggregateFuture = FutureUtils.forAllOf(inputValues, false, input -> {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            futures.add(future);
            return future;
        });

        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(0).completeExceptionally(new RuntimeException("0"));
        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(1).complete(1);
        assertThat(aggregateFuture.isDone(), equalTo(false));
        futures.get(2).complete(2);

        assertThat(aggregateFuture.isDone(), equalTo(true));
        assertThat(aggregateFuture.isCompletedExceptionally(), equalTo(true));
        Throwable exception = getExceptionFrom(aggregateFuture);
        assertThat(exception, instanceOf(AggregateFutureException.class));
        assertThat(exception.getCause().getMessage(), equalTo("0"));

        assertThat(futures.get(0).isCompletedExceptionally(), equalTo(true));
        assertThat(getExceptionFrom(futures.get(0)).getMessage(), equalTo("0"));
        assertThat(futures.get(1).isCancelled(), equalTo(false));
        assertThat(futures.get(1).isCompletedExceptionally(), equalTo(false));
        assertThat(futures.get(1).get(), equalTo(1));
        assertThat(futures.get(2).isCancelled(), equalTo(false));
        assertThat(futures.get(2).isCompletedExceptionally(), equalTo(false));
        assertThat(futures.get(2).get(), equalTo(2));
    }
}
