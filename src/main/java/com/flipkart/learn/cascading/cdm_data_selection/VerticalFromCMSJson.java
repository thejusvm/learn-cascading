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

/**
 * Created by shubhranshu.shekhar on 28/06/17.
 */
public class VerticalFromCMSJson extends BaseOperation implements Function {

    public VerticalFromCMSJson(Fields outputFields) {
        super(outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String fsn = arguments.getString(DataFields._FSN);
        String cmsJson = arguments.getString(DataFields._CMS);

        try {
            JSONObject json = new JSONObject(cmsJson);
            String verticalName = json.getJSONArray("vertical").getString(0);
            String brand = "";
            if (json.has("brand")) {
                brand = json.getJSONArray("brand").getString(0);
            }
            Tuple result = new Tuple();
            result.add(fsn);
            result.add(brand);
            result.add(verticalName);
            if(verticalName.equalsIgnoreCase("t_shirt")){
                functionCall.getOutputCollector().add(result);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
