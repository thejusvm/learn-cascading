package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import java.util.Map;
import java.util.Set;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils.MISSING_DATA;

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

        if(attributeValue == null || !isClean(attributeValue)) {
            return MISSING_DATA;
        }

        if(allowedValues != null && !allowedValues.contains(attributeValue)) {
            return MISSING_DATA;
        }

        attributeValue = attributeValue.replaceAll(",", "").replaceAll("\t","");
        return renameConfig == null ? attributeValue : renameConfig.getOrDefault(attributeValue, attributeValue);

    }
}
