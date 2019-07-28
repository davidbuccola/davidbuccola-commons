package net.davidbuccola.commons;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;

import static java.lang.Math.min;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Utilities for making random choices from a set of candidates.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class ChoiceUtils {

    private static Random random = new SecureRandom(Long.toHexString(System.currentTimeMillis()).getBytes());
    private static Map<Object, SequentialContext> sequentialContexts = new WeakHashMap<>();
    private static Map<Object, GaussianContext> gaussianContexts = new WeakHashMap<>();

    private ChoiceUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static Random getRandom() {
        return random;
    }

    public static <T> T nextRandomChoiceOf(List<T> candidates) {
        if (candidates.size() == 0) {
            throw new IllegalStateException("No choices are available");
        }
        return nextRandomChoicesOf(candidates, 1).get(0);
    }

    public static <T> List<T> nextRandomChoicesOf(List<T> candidates, int targetCount) {
        List<T> selections = new ArrayList<>();
        List<T> remainingChoices = new ArrayList<>(candidates);
        for (int i = 0, limit = min(targetCount, remainingChoices.size()); i < limit; i++) {
            selections.add(remainingChoices.remove(random.nextInt(remainingChoices.size())));
        }
        return selections;
    }

    public static <T> T nextSequentialChoiceOf(List<T> candidates) {
        if (candidates.size() == 0) {
            throw new IllegalStateException("No choices are available");
        }
        return nextSequentialChoicesOf(candidates, 1).get(0);
    }

    public static <T> List<T> nextSequentialChoicesOf(List<T> candidates, int targetCount) {
        SequentialContext context = sequentialContexts.computeIfAbsent(candidates, it -> new SequentialContext());

        List<T> selections = new ArrayList<>();
        for (int i = 0, limit = min(targetCount, candidates.size()); i < limit; i++) {
            selections.add(candidates.get(context.nextIndex++ % candidates.size()));
        }
        return selections;
    }

    public static <T> T nextGaussianChoiceOf(List<T> candidates) {
        if (candidates.size() == 0) {
            throw new IllegalStateException("No choices are available");
        }
        int mean = candidates.size() / 2;
        GaussianContext context = gaussianContexts.computeIfAbsent(candidates, it -> new GaussianContext());
        return candidates.get(nextGaussianInt(context.random, mean, Math.max(1, mean / 3), 0, candidates.size() - 1));
    }

    public static <T> int nextGaussianCount(List<T> candidates, int maximumCount) {
        GaussianContext context = gaussianContexts.computeIfAbsent(candidates, it -> new GaussianContext());

        int mean = maximumCount / 2;
        return nextGaussianInt(context.random, mean, Math.max(1, mean / 3), 1, maximumCount);
    }

    public static int nextGaussianInt(Random random, int mean, double standardDeviation, int minimum, int maximum) {
        if (minimum > maximum) {
            throw new IllegalArgumentException("Invalid bounds");
        }
        if (minimum == maximum) {
            return minimum;
        }

        int count = -1;
        while (count < minimum || count > maximum) {
            count = Double.valueOf((random.nextGaussian() * standardDeviation) + mean).intValue();
        }
        return count;
    }

    public static boolean nextWeightedBoolean(double likelihoodOfTrue) {
        return random.nextDouble() <= likelihoodOfTrue;
    }

    public static Instant nextRandomTime(Instant earliestTime, Instant latestTime) {
        int secondRange = (int) earliestTime.until(latestTime, SECONDS);
        int secondsFromEarliestTime = secondRange > 0 ? random.nextInt(secondRange) : 0;
        return earliestTime.plus(secondsFromEarliestTime, SECONDS);
    }

    private static class SequentialContext {
        int nextIndex = 0;
    }

    private static class GaussianContext {
        Random random = new SecureRandom(Long.toHexString(System.currentTimeMillis()).getBytes());
    }
}
