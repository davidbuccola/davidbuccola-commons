package net.davidbuccola.commons.cassandra;

/**
 * A simple implementation of {@link BucketCalculator} that uses a fixed number of buckets.
 */
public final class FixedCountBucketCalculator implements BucketCalculator {

    private final long numberOfBuckets;

    public FixedCountBucketCalculator(long numberOfBuckets) {
        this.numberOfBuckets = numberOfBuckets;
    }

    @Override
    public String computeBucket(Object value) {
        long hashCode = value.hashCode();
        long usableHashCode = (value instanceof String || hashCode != System.identityHashCode(value)) ? hashCode : String.valueOf(value).hashCode();
        return Long.toString(Math.abs(usableHashCode) % numberOfBuckets);
    }

}
