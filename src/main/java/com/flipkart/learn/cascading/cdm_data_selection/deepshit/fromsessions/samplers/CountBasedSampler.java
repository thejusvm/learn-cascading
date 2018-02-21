package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

public class CountBasedSampler extends AbstractSampler {

    private final double scaling;
    private EnumeratedIntegerDistribution sampler;

    public CountBasedSampler(String attributesPath) {
        this(attributesPath, 1);
    }

    public CountBasedSampler(String attributesPath, double scaling) {
        super(attributesPath);
        this.scaling = scaling;
    }

    @Override
    protected void subInit() {
        List<Integer> counts = wrapper.getCounts();
        double[] probabilities = getProbabilities(counts);
        if(scaling != 1) {
            probabilities = scaleProbabilities(probabilities, scaling);
        }
        int[] singletons = IntStream.rangeClosed(0, wrapper.getNumProducts() - 1).toArray();
        sampler = new EnumeratedIntegerDistribution(singletons, probabilities);
    }

    private double[] scaleProbabilities(double[] probabilities, double scaling) {
        double totalUnnormalizedProbability = 0;
        double[] scaledProbabilities = new double[probabilities.length];
        for (int i = 0; i < probabilities.length; i++) {
            double probability = probabilities[i];
            double scaledprobability = Math.pow(probability, scaling);
            scaledProbabilities[i] = scaledprobability;
            totalUnnormalizedProbability += scaledprobability;
        }

        for (int i = 0; i < scaledProbabilities.length; i++) {
            double scaledprobability = scaledProbabilities[i];
            scaledProbabilities[i] = scaledprobability / totalUnnormalizedProbability;
        }

        return scaledProbabilities;
    }

    private double[] getProbabilities(List<Integer> counts) {

        double totalCount = 0;
        for (Integer count : counts) {
            totalCount += count;
        }

        double[] probabilities = new double[counts.size()];
        for (int i = 0; i < counts.size(); i++) {
            Integer count = counts.get(i);
            probabilities[i] = count / totalCount;
        }

        return probabilities;

    }

    private int getSample() {
        return sampler.sample();
    }

    @Override
    public Map<String, Integer> getNextSample() {
        return wrapper.getAttributes(getSample());
    }


    public static void main(String[] args) throws IOException {

        double scaling = .75;
        CountBasedSampler sampler = new CountBasedSampler("/Users/thejus/workspace/learn-cascading/data/sessions-2017100.products-int.1/integerized_attributes/part-0", scaling);
        sampler.init();

        System.out.println("sampling now");
        int[] counter = new int[sampler.wrapper.getNumProducts()];
        int limit = 600000 * 33 / 25;
        for (int i = 0; i < limit; i++) {
            int r = sampler.getSample();
            counter[r]++;
        }

        for (int i = 0; i < 100; i++) {
            int count = counter[i];
            System.out.println(i + " " + count);
        }

    }

}
