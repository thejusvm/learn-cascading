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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.*;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class IntegerizeExplodedSession extends SubAssembly {

    private String attributeDictPath;
    private final FeatureSchema schema;
    private boolean jsonize;
    private String integerizedAttributesPath;
    private final int start;
    private final int end;

    public IntegerizeExplodedSession(String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema) {
        this(new Pipe("session-integerizer"), integerizedAttributesPath, attributeDictPath, schema);
    }

    public IntegerizeExplodedSession(Pipe pipe, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema) {
        this(pipe, integerizedAttributesPath, attributeDictPath, schema,true, 0, Integer.MAX_VALUE);
    }

    public IntegerizeExplodedSession(String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema,
                                     int start, int end) {
        this(new Pipe("session-integerizer"), integerizedAttributesPath, attributeDictPath, schema,true, start, end);
    }

    public IntegerizeExplodedSession(Pipe pipe, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema, boolean jsonize,
                                     int start, int end) {
        this.attributeDictPath = attributeDictPath;
        this.integerizedAttributesPath = integerizedAttributesPath;
        this.schema = schema;
        this.jsonize = jsonize;
        this.start = start;
        this.end = end;
        setTails(modifyPipe(pipe));
    }

    private Pipe modifyPipe(Pipe pipe) {

        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS), Map.class);
        pipe = new JsonDecodeEach(pipe, new Fields(NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);

        pipe = new Each(pipe, new Fields(POSITIVE_PRODUCTS), new ToList(new Fields(POSITIVE_PRODUCTS)), Fields.SWAP);
        Fields integerizingFields = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS);
        pipe = new Each(pipe, integerizingFields, new Integerize(integerizingFields, integerizedAttributesPath, attributeDictPath, schema, start, end), Fields.SWAP);
        pipe = new TransformEach(pipe, new Fields(POSITIVE_PRODUCTS), x -> ((List) x).get(0), Fields.SWAP);

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

        private static DictIntegerizer idDict;
        private final String integerizedAttributesPath;
        private final String attributeDictPath;
        private final FeatureSchema schema;
        private final ImmutableSet<String> numericFeatures;
        private final int start;
        private final int end;
        private static IntegerizedProductAttributesWrapper wrapper;

        public Integerize(Fields fields, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema,
                          int start, int end) {
            super(fields);
            this.integerizedAttributesPath = integerizedAttributesPath;
            this.attributeDictPath = attributeDictPath;
            this.schema = schema;
            numericFeatures = ImmutableSet.copyOf(schema.getFeaturesNamesForType(Feature.FeatureType.NUMERIC));
            this.start = start;
            this.end = end;
//            idDict = null;
//            wrapper = null;
        }

        private static synchronized void init(String integerizedAttributesPath, String attributeDictPath,
                                              int start, int end) throws IOException {
            if(idDict == null){
                String idDictPath = DictIntegerizerUtils.getAttributeDictPath(attributeDictPath, "productId");
                idDict = DictIntegerizerUtils.getDictIntegerizer(idDictPath);
                System.out.println("done reading attributes dict from path : " + attributeDictPath + ", " + idDict);
                wrapper = IntegerizedProductAttributesRepo.getWrapper(integerizedAttributesPath, "productId", start, end);
            }
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            try {
                HdfsUtils.setConfiguration((Configuration)flowProcess.getConfigCopy());
                init(integerizedAttributesPath, attributeDictPath, start, end);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            TupleEntry arguments = functionCall.getArguments();
            int numArguments = arguments.size();
            Tuple result = new Tuple();
            for (int i = 0; i < numArguments; i++) {
                List<Map<String, Object>> arg = (List<Map<String, Object>>) arguments.getObject(i);
                result.add(integerizeData(arg));
            }
            functionCall.getOutputCollector().add(result);

        }

        private List<Map<String, Object>> integerizeData(List<Map<String, Object>> productAttributes) {
            List<Map<String, Object>> integerAttributes = new ArrayList<>();
            for (Map<String, Object> productAttribute : productAttributes) {
                Map<String, Object> integerAttribute = new HashMap<>();
                Object productId = productAttribute.get("productId");
                if(productId instanceof Integer) {
                    integerAttributes.add(productAttribute);
                } else if(productId instanceof String) {
                    int idIndex = idDict.only_get((String)productId, DictIntegerizerUtils.MISSING_DATA_INDEX);
                    Map<String, Integer> idAttributes = wrapper.getIdAttributes(idIndex);
                    if(idAttributes == null) {
                        integerAttributes.add(productAttribute);
                    } else {
                        integerAttribute.putAll(idAttributes);
                        for (Map.Entry<String, Object> attributeToValue : productAttribute.entrySet()) {
                            String attribute = attributeToValue.getKey();
                            if(numericFeatures.contains(attribute)){
                                integerAttribute.put(attribute, attributeToValue.getValue());
                            }
                        }
                        integerAttributes.add(integerAttribute);
                    }
                } else {
                    throw new RuntimeException("Unkown type for productId : " + productId);
                }
            }
            return integerAttributes;
        }
    }

//    public static void flow(String input, String productsIntPath, String output) throws IOException {
//        flow(input, productsIntPath, output, 1);
//    }

    public static String flow(String input, String productsIntPath, String output, int numParts) throws IOException {
        String integerizedAttributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(productsIntPath);

        List<String> files = HdfsUtils.listFiles(integerizedAttributesPath, 1);
        int numLines = HdfsUtils.numLines(files);

        int linesPerPart = (int) Math.ceil(((float) numLines) / numParts);

        int start = 0;
        int end = start + linesPerPart;

        for (int i = 0; i < numParts; i++) {
            String loopOutput = output + "-" + i;
            System.out.println("starting flow process for start : " + start + " to end : " + end);
            flow(input, productsIntPath, loopOutput, start, end);
            start = end;
            end = start + linesPerPart;
            input = loopOutput;
        }
        return input;

    }

    public static void flow(String input, String productsIntPath, String output, int start, int stop) {
        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        String integerizedAttributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(productsIntPath);
        String attributeDictPath = IntegerizeProductAttributes.getAttributeDictsPath(productsIntPath);
        IntegerizeExplodedSession integerizer = new IntegerizeExplodedSession(integerizedAttributesPath, attributeDictPath, schema,
                start, stop);

        PipeRunner runner = new PipeRunner("explode-integerize");
        runner.setNumReducers(600);
        runner.executeHfs(integerizer, input, output, true);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/session-20180210.10000.explode",
                    "data/session-20180210.10000.explode.products-int",
                    "data/session-20180210.10000.explode.int"
            };
        }

        String input = args[0];
        String attributeDictPath = args[1];
        String output = args[2];

        try {
            flow(input, attributeDictPath, output, 2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
