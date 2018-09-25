package net.davidbuccola.commons;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utilities for working with collections.
 */
public final class CollectionUtils {

    private CollectionUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static <T> List<T> mergeLists(List<List<T>> listOfLists) {
        if (listOfLists.size() == 1) {
            return listOfLists.get(0);

        } else {
            List<T> mergedList = new ArrayList<>();
            for (List<T> list : listOfLists) {
                if (list != null) {
                    mergedList.addAll(list);
                }
            }
            return mergedList;
        }
    }

    public static <T> List<T> asList(Collection<T> values) {
        return values instanceof List ? (List<T>) values : new ArrayList<>(values);
    }
}
