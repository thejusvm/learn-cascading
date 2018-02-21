package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import java.io.IOException;
import java.util.Map;

public interface Sampler {

    public void init() throws IOException;

    public Map<String, Integer> getNextSample();

}
