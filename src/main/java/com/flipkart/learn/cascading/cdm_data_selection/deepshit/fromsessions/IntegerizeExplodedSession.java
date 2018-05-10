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
    private String integerizedAttributesPath;

    public IntegerizeExplodedSession(String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema) {
        this(new Pipe("session-integerizer"), integerizedAttributesPath, attributeDictPath, schema);
    }

    public IntegerizeExplodedSession(Pipe pipe, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema) {
        this(pipe, integerizedAttributesPath, attributeDictPath, schema,true);
    }

    public IntegerizeExplodedSession(Pipe pipe, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema, boolean jsonize) {
        this.attributeDictPath = attributeDictPath;
        this.integerizedAttributesPath = integerizedAttributesPath;
        this.schema = schema;
        this.jsonize = jsonize;
        setTails(modifyPipe(pipe));
    }

    private Pipe modifyPipe(Pipe pipe) {

        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS), Map.class);
        pipe = new JsonDecodeEach(pipe, new Fields(NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);

        pipe = new Each(pipe, new Fields(POSITIVE_PRODUCTS), new ToList(new Fields(POSITIVE_PRODUCTS)), Fields.SWAP);
        Fields integerizingFields = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS);
        pipe = new Each(pipe, integerizingFields, new Integerize(integerizingFields, integerizedAttributesPath, attributeDictPath, schema), Fields.SWAP);

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
        private static IntegerizedProductAttributesWrapper wrapper;

        public Integerize(Fields fields, String integerizedAttributesPath, String attributeDictPath, FeatureSchema schema) {
            super(fields);
            this.integerizedAttributesPath = integerizedAttributesPath;
            this.attributeDictPath = attributeDictPath;
            this.schema = schema;
            numericFeatures = ImmutableSet.copyOf(schema.getFeaturesNamesForType(Feature.FeatureType.NUMERIC));
        }

        private static synchronized void init(String integerizedAttributesPath, String attributeDictPath) throws IOException {
            if(idDict == null){
                String idDictPath = DictIntegerizerUtils.getAttributeDictPath(attributeDictPath, "productId");
                idDict = DictIntegerizerUtils.getDictIntegerizer(idDictPath);
                System.out.println("done reading attributes dict from path : " + attributeDictPath + ", " + idDict);
                wrapper = IntegerizedProductAttributesRepo.getWrapper(integerizedAttributesPath, "productId");
            }
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            try {
                HdfsUtils.setConfiguration((Configuration)flowProcess.getConfigCopy());
                init(integerizedAttributesPath, attributeDictPath);
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
                String productId = productAttribute.get("productId");
                int idIndex = idDict.only_get(productId, DictIntegerizerUtils.MISSING_DATA_INDEX);
                integerAttribute.putAll(wrapper.getIdAttributes(idIndex));
                for (Map.Entry<String, String> attributeToValue : productAttribute.entrySet()) {
                    String attribute = attributeToValue.getKey();
                    if(numericFeatures.contains(attribute)){
                        integerAttribute.put(attribute, attributeToValue.getValue());
                    }
                }
                integerAttributes.add(integerAttribute);
            }
            return integerAttributes;
        }
    }

    public static void flow(String input, String productsIntPath, String output) {
        IntegerizeExplodedSession integerizer = null;
        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        String integerizedAttributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(productsIntPath);
        String attributeDictPath = IntegerizeProductAttributes.getAttributeDictsPath(productsIntPath);
        integerizer = new IntegerizeExplodedSession(integerizedAttributesPath, attributeDictPath, schema);

        PipeRunner runner = new PipeRunner("explode-integerize");
        runner.setNumReducers(600);
        runner.executeHfs(integerizer, input, output, true);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000",
                    "data/product-attributes.MOB.int",
                    "data/sessionexplode-2017-0801.1000.int"
            };
        }

        String input = args[0];
        String attributeDictPath = args[1];
        String output = args[2];

        flow(input, attributeDictPath, output);


    }
}
