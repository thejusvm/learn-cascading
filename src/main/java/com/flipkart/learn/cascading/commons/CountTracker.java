package com.flipkart.learn.cascading.commons;

import org.apache.commons.math3.util.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CountTracker {

    private Map<String, Integer> countMap;

    public CountTracker() {
        countMap = new ConcurrentHashMap<>();
    }

    public void add(String val, int count) {
        countMap.put(val, countMap.getOrDefault(val, 0) + count);
    }

    public List<String> getValsSortedByCount() {
        return getValsSortedByCount(-1);
    }

    public List<String> getValsSortedByCount(int minCount) {
        return countMap.entrySet().stream()
                .sorted((c1, c2) -> -1 * c1.getValue().compareTo(c2.getValue()))
                .filter(e -> e.getValue() > minCount)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public List<Pair<String, Integer>> getSortedByCount() {
        return getSortedByCount(-1);
    }

    public List<Pair<String, Integer>> getSortedByCount(int minCount) {
        return countMap.entrySet().stream()
                .sorted((c1, c2) -> -1 * c1.getValue().compareTo(c2.getValue()))
                .filter(e -> e.getValue() > minCount)
                .map(e -> new Pair<String, Integer>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }


}
