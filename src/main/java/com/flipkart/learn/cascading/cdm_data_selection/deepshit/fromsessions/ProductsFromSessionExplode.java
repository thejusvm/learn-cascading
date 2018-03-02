package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.cascading.GenerateFieldsFromMap;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;

import java.util.List;
import java.util.Map;

public class ProductsFromSessionExplode implements SimpleFlow {

    private FeatureSchema schema;

    public ProductsFromSessionExplode(FeatureSchema schema) {
        this.schema = schema;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("product-chain-pipe");
        Fields posProducts = new Fields(SessionExploder.POSITIVE_PRODUCTS);
        Fields negProducts = new Fields(SessionExploder.NEGATIVE_PRODUCTS);
        pipe = new JsonDecodeEach(pipe, posProducts, Map.class);
        pipe = new JsonDecodeEach(pipe, negProducts, List.class);
        Fields productAttributes = new Fields("ProductAttributes");
        pipe = new Each(pipe, Fields.merge(posProducts, negProducts), new FetchProductAttributes(productAttributes), Fields.RESULTS);

        Fields enumFields = new Fields(schema.getFeaturesNamesForType(Feature.FeatureType.enumeration).toArray(new String[0]));
        List<String> numericFeatures = schema.getFeaturesNamesForType(Feature.FeatureType.numeric);
        Fields numericFields = new Fields(numericFeatures.toArray(new String[0]));

        pipe = new Each(pipe, productAttributes, new GenerateFieldsFromMap(Fields.merge(enumFields, numericFields)), Fields.RESULTS);
        pipe = new GroupBy(pipe, enumFields);

        for (String numericFeature : numericFeatures) {
            Fields numericField = new Fields(numericFeature);
            pipe = new Every(pipe, numericField, new Average(numericField));
        }

        pipe = new Every(pipe, enumFields, new Count());

        for (String numericFeature : numericFeatures) {
            Fields numericField = new Fields(numericFeature);
            pipe = new TransformEach(pipe, numericField, x -> ((Number)x).intValue(), Fields.SWAP);
        }

        pipe = new TransformEach(pipe, new Fields("count"), x -> x, Fields.SWAP);


        return pipe;
    }

    private static class FetchProductAttributes extends BaseOperation implements Function {
        public FetchProductAttributes(Fields productAttributes) {
            super(productAttributes);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            Map pos = (Map) functionCall.getArguments().getObject(0);
            functionCall.getOutputCollector().add(new Tuple(pos));

            List<Map> negs = (List<Map>) functionCall.getArguments().getObject(1);
            for (Map neg : negs) {
                functionCall.getOutputCollector().add(new Tuple(neg));
            }
        }
    }

    private static void flow(String input, String output) {
        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);

        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        ProductsFromSessionExplode productsFromExplode = new ProductsFromSessionExplode(schema);

        runner.executeHfs(productsFromExplode.getPipe(), input, output, true);
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000.explode/part-*", "data/session-20180210.10000.explode.products"};
        }

        String input = args[0];
        String output = args[1];

        flow(input, output);

    }
}


