package net.davidbuccola.commons.cassandra;

/**
 * An implementation of {@link BucketCalculator} that leverages the sequential nature of core IDs to group things
 * created around the same time together. This usually results in any given request being satisfied from a smaller
 * number of buckets since queries will often ask for things that are somewhere near in time. For example, much of
 * the relevant activity for an opportunity is likely to be clustered around the time of the opportunity rather than
 * scattered across 2 years.
 * 
 * If also means that the smaller the org the fewer number of buckets that will be needed to group the data.
 */
public final class ClusteringIdBucketCalculator implements BucketCalculator {

    private static final int DEFAULT_NUMBER_DIGITS_PER_BUCKET = 3; // (62 * 62 * 62) = 238,328 IDs per bucket.
    
    private final int relevantLengthOfId;

    public ClusteringIdBucketCalculator() {
        this(DEFAULT_NUMBER_DIGITS_PER_BUCKET);
    }
    
    public ClusteringIdBucketCalculator(int numberOfBase62DigitsPerBucket) {
        this.relevantLengthOfId = 15 - numberOfBase62DigitsPerBucket;
    }

    @Override
    public String computeBucket(Object value) {
        return value.toString().substring(0, relevantLengthOfId);
    }

}
