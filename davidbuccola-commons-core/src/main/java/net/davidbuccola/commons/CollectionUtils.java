package net.davidbuccola.commons;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for working with collections.
 */
public final class CollectionUtils {

    private CollectionUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static <T> List<T> merge(List<List<T>> listOfLists) {
        if (listOfLists.size() == 1) {
            return listOfLists.get(0);

        } else {
            List<T> mergedList = new ArrayList<>();
            for (List<T> list : listOfLists) {
                mergedList.addAll(list);
            }
            return mergedList;
        }
    }
}
