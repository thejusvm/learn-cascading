package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizedProductAttributesWrapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class ZiphianRandomSampler implements Sampler {

    private final IntegerizedProductAttributesWrapper wrapper;
    private final Random random;
    private final int size;

    public ZiphianRandomSampler(String attributesPath) throws IOException {
         wrapper = new IntegerizedProductAttributesWrapper(attributesPath);
         random = new Random();
         size = wrapper.getNumProducts();
    }

    @Override
    public Map<String, Integer> getNextSample() {
        double r = random.nextDouble();
        int index = ((int)Math.ceil(Math.pow(size + 1, r))) - 2;
        return wrapper.getAttributes(index);
    }
}
