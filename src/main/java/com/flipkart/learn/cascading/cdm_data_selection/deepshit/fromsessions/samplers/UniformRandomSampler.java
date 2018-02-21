package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import java.util.Map;

public class UniformRandomSampler extends AbstractSampler {

    public UniformRandomSampler(String attributesPath) {
        super(attributesPath);
    }

    @Override
    protected void subInit() {}

    @Override
    public Map<String, Integer> getNextSample() {
        return wrapper.getAttributes(random.nextInt(size));
    }
}
