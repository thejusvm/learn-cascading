package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PivotedStatsTracker {

    ImmutableSet<String> defaultKey = ImmutableSet.of("default");

    private String fieldName;

    private Map<Set<String>, StatsTracker> pivotedStatsTracker;

    public PivotedStatsTracker(String fieldName) {
        this.fieldName = fieldName;
        pivotedStatsTracker = new HashMap<>();
    }

    public void track(Number val) {
        this.track(defaultKey, val);
    }

    public void track(Set<String> pivot, Number val) {

        if(!pivotedStatsTracker.containsKey(pivot)) {
            pivotedStatsTracker.put(pivot, new StatsTracker());
        }
        pivotedStatsTracker.get(pivot).track(val);

    }

    public Stats getDefaultPivotedStats() {
        return getPivotedStats().get(defaultKey);
    }

    public Map<Set<String>, Stats> getPivotedStats() {
        Map<Set<String>, Stats> pivotedStats = new HashMap<>();
        for (Map.Entry<Set<String>, StatsTracker> statsTrackers : pivotedStatsTracker.entrySet()) {
            pivotedStats.put(statsTrackers.getKey(), statsTrackers.getValue().getStats());
        }
        return pivotedStats;
    }

    private class StatsTracker {

        private Stats stats;

        public StatsTracker() {
            stats = new Stats();
        }

        public void track(Number val) {
            track(val.doubleValue());
        }

        public void track(double val) {
            stats.sum += val;
            stats.count++;
            stats.sumOfSquares += val * val;
        }

        public Stats getStats() {
            return stats;
        }

    }


    public static class Stats {

        double sum;
        int count;
        double sumOfSquares;

        public float getMean() {
            return (float) (sum / count);
        }

        public float getVariance() {
            return (float) (((count * sumOfSquares) - (sum * sum)) / (count * (count - 1.0D)));
        }

        public float getStdDev() {
            return (float) Math.sqrt(getVariance());
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "sum=" + sum +
                    ", count=" + count +
                    ", sumOfSquares=" + sumOfSquares +
                    ", mean=" + getMean() +
                    ", variance=" + getVariance() +
                    ", stdDev=" + getStdDev() +
                    '}';
        }
    }

}
