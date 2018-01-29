package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.commons.cascading.GenerateFieldsFromMap;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.ExtractCmsAttributes.FETCH_CONFIG;

public class ProductsFromSessionExplode implements SimpleFlow {

    private List<String> fields;

    public ProductsFromSessionExplode(List<String> fields) {

        this.fields = fields;
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
        pipe = new Each(pipe, productAttributes, new GenerateFieldsFromMap(new Fields(fields.toArray(new String[0]))), Fields.RESULTS);
        pipe = new GroupBy(pipe, Fields.ALL);
        pipe = new Every(pipe, new Count());
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

        List<String> fields = ImmutableList.copyOf(FETCH_CONFIG.keySet());

        ProductsFromSessionExplode productsFromExplode = new ProductsFromSessionExplode(fields);

        runner.executeHfs(productsFromExplode.getPipe(), input, output, true);
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000.aggsess/part-*", "data/products-from-sessions-explode.2017-0801.1000"};
        }

        String input = args[0];
        String output = args[1];

        flow(input, output);

    }
}


