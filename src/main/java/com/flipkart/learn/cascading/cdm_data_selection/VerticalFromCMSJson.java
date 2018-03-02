package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.EnumFeature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by shubhranshu.shekhar on 28/06/17.
 */
public class VerticalFromCMSJson extends BaseOperation implements Function {


    private List<Feature> features;

    public VerticalFromCMSJson(String[] attributes) {
        this(toConfig(attributes));
    }

    private static List<Feature> toConfig(String[] attributes) {
        List<Feature> config = new ArrayList<>();
        for (String attribute : attributes) {
            config.add(new EnumFeature(attribute, Feature.Source.CMS));
        }
        return config;
    }

    private static ObjectMapper mapper = new ObjectMapper();

    public VerticalFromCMSJson(List<Feature> cmsFeatures) {
        super(new Fields(getSourceKeys(cmsFeatures)));
        features = cmsFeatures;
    }

    private static String[] getSourceKeys(List<Feature> cmsFeatures) {
        return cmsFeatures.stream().map(Feature::getSourceKey).collect(Collectors.toList()).toArray(new String[0]);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String cmsJson = arguments.getString(DataFields._CMS);
        try {
            Map dataMap = mapper.readValue(cmsJson, Map.class);
            Tuple result = new Tuple();
            for (Feature feature : features) {
                List<String> attributeValues = (List<String>) dataMap.get(feature.getSourceKey());

                String attributeValue = null;
                if(attributeValues != null) {
                    attributeValue = attributeValues.get(0);
                }

                Object cleanedAttributeValue = feature.clean(attributeValue);
                result.add(cleanedAttributeValue);
            }
            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
