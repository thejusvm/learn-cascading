package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

public class NumericFeature extends Feature<Float> {

    public NumericFeature(String featureName, Source source) {
        super(featureName, source, FeatureType.numeric);
    }

    @Override
    public Float clean(Object data) {
        return ((Number)data).floatValue();
    }

}
