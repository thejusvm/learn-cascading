package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FeatureSchema implements Serializable {

    private List<Feature> features;

    public FeatureSchema() {
        features = new ArrayList<>();
    }

    public void registerFeature(Feature feature) {
        features.add(feature);
    }

    public List<Feature> getFeaturesForSource(Feature.Source source) {
        return features.stream()
                .filter(x -> source.equals(x.getSource()))
                .collect(Collectors.toList());
    }

    public List<Feature> getFeaturesForType(Feature.FeatureType featureType) {
        return features.stream()
                .filter(x -> featureType.equals(x.getFeatureType()))
                .collect(Collectors.toList());
    }

    public List<String> getFeaturesNamesForType(Feature.FeatureType featureType) {
        return features.stream()
                .filter(x -> featureType.equals(x.getFeatureType()))
                .map(Feature::getFeatureName)
                .collect(Collectors.toList());
    }

    public List<Feature> getFeatures() {
        return features;
    }

    @JsonIgnore
    public List<String> getFeatureNames() {
        return this.getFeatures().stream().map(Feature::getFeatureName).collect(Collectors.toList());
    }
}