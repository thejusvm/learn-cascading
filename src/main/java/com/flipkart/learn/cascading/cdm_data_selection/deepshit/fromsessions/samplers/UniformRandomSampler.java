package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizedProductAttributesWrapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class UniformRandomSampler implements Sampler {

    private final IntegerizedProductAttributesWrapper wrapper;
    private final Random random;
    private final int size;

    public UniformRandomSampler(String attributesPath) throws IOException {
         wrapper = new IntegerizedProductAttributesWrapper(attributesPath);
         random = new Random();
         size = wrapper.getNumProducts();
    }

    @Override
    public Map<String, Integer> getNextSample() {
        return wrapper.getAttributes(random.nextInt(size));
    }
}
