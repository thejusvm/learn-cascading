package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import cascading.tuple.TupleEntry;
import com.esotericsoftware.kryo.util.ObjectMap;
import org.apache.htrace.fasterxml.jackson.core.JsonParseException;
import org.apache.htrace.fasterxml.jackson.databind.JsonMappingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 * Created by shubhranshu.shekhar on 28/06/17.
 */
public class VerticalFromCMSJson extends BaseOperation implements Function {

    private Map<String, Set<String>> fetchConfig;
    private Map<String, String> renameConfig;

    public VerticalFromCMSJson(String[] attributes) {
        this(toConfig(attributes), Collections.emptyMap());
    }

    private static Map<String, Set<String>> toConfig(String[] attributes) {
        Map<String, Set<String>> config = new HashMap<>();
        for (String attribute : attributes) {
            config.put(attribute, null);
        }
        return config;
    }

    private static ObjectMapper mapper = new ObjectMapper();

    public VerticalFromCMSJson(Map<String, Set<String>> fetchConfig, Map<String, String> renameConfig) {
        super(new Fields(fetchConfig.keySet().toArray(new String[0])));
        this.fetchConfig = fetchConfig;
        this.renameConfig = renameConfig;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String cmsJson = arguments.getString(DataFields._CMS);

        try {
            Map dataMap = mapper.readValue(cmsJson, Map.class);
            Tuple result = new Tuple();
            for (String attribute : fetchConfig.keySet()) {
                Set<String> allowedValues = fetchConfig.get(attribute);
                List<String> attributeValues = (List<String>) dataMap.get(attribute);
                String attributeValue = null;
                if(attributeValues != null) {
                    attributeValue = attributeValues.get(0);
                }
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
                    result.add(attributeValue);
                } else {
                    result.add("<missing-val>");
                }
            }
            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean isClean(String attributeValue) {
        return attributeValue.split(" ").length < 5;
    }
}
