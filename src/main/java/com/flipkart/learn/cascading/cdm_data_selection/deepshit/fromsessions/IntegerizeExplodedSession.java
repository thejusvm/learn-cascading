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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class IntegerizeExplodedSession extends SubAssembly {

    private String attributeDictPath;
    private final FeatureSchema schema;
    private boolean jsonize;

    public IntegerizeExplodedSession(String attributeDictPath, FeatureSchema schema) {
        this(new Pipe("session-integerizer"), attributeDictPath, schema);
    }

    public IntegerizeExplodedSession(Pipe pipe, String attributeDictPath, FeatureSchema schema) {
        this(pipe, attributeDictPath, schema,true);
    }

    public IntegerizeExplodedSession(Pipe pipe, String attributeDictPath, FeatureSchema schema, boolean jsonize) {
        this.attributeDictPath = attributeDictPath;
        this.schema = schema;
        this.jsonize = jsonize;
        setTails(modifyPipe(pipe));
    }

    private Pipe modifyPipe(Pipe pipe) {

        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS), Map.class);
        pipe = new JsonDecodeEach(pipe, new Fields(NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);

        pipe = new Each(pipe, new Fields(POSITIVE_PRODUCTS), new ToList(new Fields(POSITIVE_PRODUCTS)), Fields.SWAP);
        Fields integerizingFields = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS);
        pipe = new Each(pipe, integerizingFields, new Integerize(integerizingFields, attributeDictPath, schema), Fields.SWAP);

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

    private static class Integerize extends BaseOperation implements Function {

        private final String attributeDictPath;
        private final FeatureSchema schema;
        private static Map<String, DictIntegerizer> attribueDict = null;
        private final ImmutableSet<String> enumFeatures;

        public Integerize(Fields fields, String attributeDictPath, FeatureSchema schema) {
            super(fields);
            this.attributeDictPath = attributeDictPath;
            this.schema = schema;
            enumFeatures = ImmutableSet.copyOf(schema.getFeaturesNamesForType(Feature.FeatureType.ENUMERATION));
        }

        private static synchronized void init(String attributeDictPath) throws IOException {
            if(attribueDict == null){
                List<DictIntegerizer> attribueDictList = DictIntegerizerUtils.readAttributeDicts(attributeDictPath);
                attribueDict = DictIntegerizerUtils.indexByName(attribueDictList);
                System.out.println("done reading attributes dict from path : " + attributeDictPath + ", " + attribueDict);
            }
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            try {
                HdfsUtils.setConfiguration((Configuration)flowProcess.getConfigCopy());
                init(attributeDictPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            TupleEntry arguments = functionCall.getArguments();
            int numArguments = arguments.size();
            Tuple result = new Tuple();
            for (int i = 0; i < numArguments; i++) {
                List<Map<String, String>> arg = (List<Map<String, String>>) arguments.getObject(i);
                result.add(integerizeData(arg));
            }
            functionCall.getOutputCollector().add(result);

        }

        private List<Map<String, Object>> integerizeData(List<Map<String, String>> productAttributes) {
            List<Map<String, Object>> integerAttributes = new ArrayList<>();
            for (Map<String, String> productAttribute : productAttributes) {
                Map<String, Object> integerAttribute = new HashMap<>();
                for (Map.Entry<String, String> attributeToValue : productAttribute.entrySet()) {
                    String attribute = attributeToValue.getKey();
                    if(enumFeatures.contains(attribute)) {
                        DictIntegerizer dict = attribueDict.get(attribute);
                        if(dict == null) {
                            throw new NullPointerException("dict for attribute " + attribute + " is missing");
                        }
                        int valInt = dict.only_get(attributeToValue.getValue(), dict.get(DictIntegerizerUtils.MISSING_DATA));
                        integerAttribute.put(attribute, valInt);
                    } else {
                        integerAttribute.put(attribute, attributeToValue.getValue());
                    }
                }
                integerAttributes.add(integerAttribute);
            }
            return integerAttributes;
        }
    }

    public static void flow(String input, String attributeDictPath, String output) {
        IntegerizeExplodedSession integerizer = null;
        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        integerizer = new IntegerizeExplodedSession(attributeDictPath, schema);

        PipeRunner runner = new PipeRunner("explode-integerize");
        runner.setNumReducers(600);
        runner.executeHfs(integerizer, input, output, true);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000",
                    "data/product-attributes.MOB.int/attribute_dicts",
                    "data/sessionexplode-2017-0801.1000.int"
            };
        }

        String input = args[0];
        String attributeDictPath = args[1];
        String output = args[2];

        flow(input, attributeDictPath, output);


    }
}
