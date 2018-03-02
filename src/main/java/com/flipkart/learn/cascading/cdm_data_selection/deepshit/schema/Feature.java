package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import java.io.Serializable;

public abstract class Feature<T> implements Serializable {

    public enum Source {
        CMS, CDM
    }

    public enum FeatureType {
        enumeration, numeric
    }

    private final String featureName;

    private final Source source;

    private final FeatureType featureType;

    public Feature(String featureName, Source source, FeatureType featureType) {
        this.featureName = featureName;
        this.source = source;
        this.featureType = featureType;
    }

    public String getFeatureName() {
        return featureName;
    }

    public String getSourceKey() {
        return featureName;
    }

    public Source getSource() {
        return source;
    }

    public FeatureType getFeatureType() {
        return featureType;
    }

    public abstract T clean(Object data);

}
