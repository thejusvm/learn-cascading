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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by shubhranshu.shekhar on 28/06/17.
 */
public class VerticalFromCMSJson extends BaseOperation implements Function {

    private String[] attributes;

    public VerticalFromCMSJson(String[] attributes) {
        super(new Fields(attributes));
        this.attributes = attributes;
    }

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String cmsJson = arguments.getString(DataFields._CMS);

        try {
            Map dataMap = mapper.readValue(cmsJson, Map.class);
            Tuple result = new Tuple();
            for (String attribute : attributes) {
                List<String> attributeValue = (List<String>) dataMap.get(attribute);
                if(attributeValue != null) {
                    result.add(attributeValue.get(0));
                } else {
                    result.add("<missing-val>");
                }
            }
            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
