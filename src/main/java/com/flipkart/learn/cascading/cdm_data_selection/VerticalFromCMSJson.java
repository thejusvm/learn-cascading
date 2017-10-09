package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import cascading.tuple.TupleEntry;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by shubhranshu.shekhar on 28/06/17.
 */
public class VerticalFromCMSJson extends BaseOperation implements Function {

    private String[] attributes;

    public VerticalFromCMSJson(String[] attributes) {
        super(new Fields(attributes));
        this.attributes = attributes;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String cmsJson = arguments.getString(DataFields._CMS);

        try {
            JSONObject json = new JSONObject(cmsJson);
            Tuple result = new Tuple();
            for (String attribute : attributes) {
                String attributeValue = json.getJSONArray(attribute).getString(0);
                result.add(attributeValue);
            }
            functionCall.getOutputCollector().add(result);
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
