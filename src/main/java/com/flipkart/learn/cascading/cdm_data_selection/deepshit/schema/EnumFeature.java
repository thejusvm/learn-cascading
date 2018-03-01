package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import java.util.Map;
import java.util.Set;

public class EnumFeature extends Feature<String> {

    private final Set<String> allowedValues;
    private final Map<String, String> renameConfig;

    public EnumFeature(String featureName, Source source, Set<String> allowedValues, Map<String, String> renameConfig) {
        super(featureName, source, FeatureType.enumeration);
        this.allowedValues = allowedValues;
        this.renameConfig = renameConfig;
    }

    public EnumFeature(String featureName, Source source) {
        this(featureName, source, null, null);
    }

    private boolean isClean(String attributeValue) {
        return attributeValue.split(" ").length < 5;
    }

    @Override
    public String clean(Object attributeValueObj) {
        String attributeValue = (String) attributeValueObj;
        if(allowedValues != null && !allowedValues.contains(attributeValue)) {
            attributeValue = null;
        }
        if(attributeValue != null && !isClean(attributeValue)) {
            attributeValue = null;
        }
        if(attributeValue != null) {
            attributeValue = attributeValue.replaceAll(",", "").replaceAll("\t","");

        }

        if(attributeValue != null && renameConfig.containsKey(attributeValue)) {
            attributeValue = renameConfig.get(attributeValue);
        }

        if (attributeValue != null) {
            return attributeValue;
        } else {
            return "<missing-val>";
        }
    }
}
