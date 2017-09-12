package com.flipkart.learn.cascading.commons.cascading;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
* Created by thejus on 7/11/15.
*/
public class MapKeyComparator implements Comparator<Map<Comparable,?>>, Serializable {

    private final Comparable compareKey;
    private final int mul;

    public MapKeyComparator(Comparable compareKey) {
        this(compareKey, false);
    }

    public MapKeyComparator(Comparable compareKey, boolean reverse) {
        this.compareKey = compareKey;
        mul = reverse ? -1 : 1;
    }

    public int compare(Map<Comparable, ?> o1, Map<Comparable, ?> o2) {
        Object o1KeyVal = o1.get(compareKey);
        Object o2KeyVal = o2.get(compareKey);
        return mul * ((Comparable) o1KeyVal).compareTo(o2KeyVal);
    }

}
