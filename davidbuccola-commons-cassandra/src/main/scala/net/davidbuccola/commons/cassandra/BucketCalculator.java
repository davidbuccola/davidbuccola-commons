package net.davidbuccola.commons.cassandra;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Calculates a bucket number that can be used to help with Cassandra partitioning.
 */
public interface BucketCalculator {

    /**
     * Calculates a bucket number that can be used to help with Cassandra partitioning.
     */
    String computeBucket(Object value);

    /**
     * Groups values into buckets. It's important that the order of iteration for both keys and values is predictable
     * because certain paging algorithms depend upon it.
     */
    default <T> Map<String, List<T>> groupByBucket(Collection<T> values) {
        ListMultimap<String, T> valuesByBucket = LinkedListMultimap.create(); // Important
        for (T value : values) {
            valuesByBucket.put(computeBucket(value), value);
        }
        return Maps.transformEntries(valuesByBucket.asMap(), (key, value) -> (List<T>) value);
    }
}
