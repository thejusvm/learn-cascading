package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizer;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.Helpers;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class IntegerizeSessionExploder extends SubAssembly {

    private final Map<String, DictIntegerizer> attribueDict;
    private boolean jsonize;

    public IntegerizeSessionExploder(String attributeDictPath) throws IOException {
        this(attributeDictPath, true);
    }

    public IntegerizeSessionExploder(String attributeDictPath, boolean jsonize) throws IOException {
        attribueDict = Helpers.readAttributeDicts(attributeDictPath);
        this.jsonize = jsonize;
        setTails(getPipe());
    }

    private Pipe getPipe() {
        Pipe pipe = new Pipe("session-integerizer");

        if(jsonize) {
            pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS), Map.class);
            pipe = new JsonDecodeEach(pipe, new Fields(NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);
        }

        pipe = new Each(pipe, new Fields(POSITIVE_PRODUCTS), new ToList(new Fields(POSITIVE_PRODUCTS)), Fields.SWAP);
        Fields integerizingFields = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS);
        pipe = new Each(pipe, integerizingFields, new Integerize(integerizingFields, attribueDict), Fields.SWAP);

        if(jsonize) {
            pipe = new JsonEncodeEach(pipe, integerizingFields);
        }

        return pipe;
    }

    private static class ToList extends BaseOperation implements Function {
        public ToList(Fields field) {
            super(field);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            Object val = functionCall.getArguments().getObject(0);
            List<Object> valList = new ArrayList<>();
            valList.add(val);
            functionCall.getOutputCollector().add(new Tuple(valList));
        }
    }

    private class Integerize extends BaseOperation implements Function {

        private final Map<String, DictIntegerizer> attribueDict;

        public Integerize(Fields fields, Map<String, DictIntegerizer> attribueDict) {
            super(fields);
            this.attribueDict = attribueDict;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            int numArguments = arguments.size();
            Tuple result = new Tuple();
            for (int i = 0; i < numArguments; i++) {
                List<Map<String, String>> arg = (List<Map<String, String>>) arguments.getObject(i);
                result.add(integerizeData(arg));
            }
            functionCall.getOutputCollector().add(result);

        }

        private List<Map<String, Integer>> integerizeData(List<Map<String, String>> productAttributes) {
            List<Map<String, Integer>> integerAttributes = new ArrayList<>();
            for (Map<String, String> productAttribute : productAttributes) {
                Map<String, Integer> integerAttribute = new HashMap<String, Integer>();
                for (Map.Entry<String, String> attributeToValue : productAttribute.entrySet()) {
                    String attribute = attributeToValue.getKey();
                    DictIntegerizer dict = attribueDict.get(attribute);
                    int valInt = dict.only_get(attributeToValue.getValue(), dict.get(Helpers.MISSING_DATA));
                    integerAttribute.put(attribute, valInt);
                }
                integerAttributes.add(integerAttribute);
            }
            return integerAttributes;
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000",
                    "data/product-attributes.MOB.int/attribute_dicts.json",
                    "data/sessionexplode-2017-0801.1000.int"
            };
        }

        IntegerizeSessionExploder integerizer = null;
        try {
            integerizer = new IntegerizeSessionExploder(args[1]);
        } catch (IOException e) {
            throw  new RuntimeException(e);
        }

        PipeRunner runner = new PipeRunner("explode-integerize");
        runner.setNumReducers(600);
        runner.executeHfs(integerizer, args[0], args[2], true);


    }
}
