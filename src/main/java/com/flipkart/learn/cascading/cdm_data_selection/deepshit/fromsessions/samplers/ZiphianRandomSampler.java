package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import java.util.Map;
import java.util.Random;

public class ZiphianRandomSampler extends AbstractSampler {

    public ZiphianRandomSampler(String attributesPath) {
         super(attributesPath);
    }

    @Override
    protected void subInit() {}

    @Override
    public Map<String, Integer> getNextSample() {
        int index = nextZipf(random, size);
        return wrapper.getAttributes(index);
    }

    private static int nextZipf(Random random, int size) {
        double r = random.nextDouble();
        return ((int)Math.ceil(Math.pow(size + 1, r))) - 2;
    }

    public static void main(String[] args) {
        Random random = new Random();
        int s = 789437;

        int[] counter = new int[s];
        for (int i = 0; i < 60000000; i++) {
            int r = nextZipf(random, s);
            counter[r]++;
        }

        for (int i = 0; i < 10000; i++) {
            int count = counter[i];
            System.out.println(i + " " + count);
        }

    }
}
