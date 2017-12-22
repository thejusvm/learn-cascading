package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import java.util.Map;

public interface Sampler {

    public Map<String, Integer> getNextSample();

}
