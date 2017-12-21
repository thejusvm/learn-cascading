package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ProductObj;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.GenerateFieldsFromMap;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableSet;

import java.util.*;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields._ACCOUNTID;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

public class ProductsFromSession implements SimpleFlow {

    private List<String> fields;

    public ProductsFromSession(List<String> fields) {

        this.fields = fields;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("product-chain-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        Fields productAttributes = new Fields("ProductAttributes");
        pipe = new Each(pipe, userContext, new FetchProductAttributes(productAttributes), Fields.RESULTS);
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
            SearchSessions searchSessions = (SearchSessions) functionCall.getArguments().getObject(0);
            for (SearchSession searchSession : searchSessions.getSessions().values()) {
                for (ProductObj productObj : searchSession.getProducts()) {
                    if(Arrays.stream(lifeStylePrefixes).anyMatch(prefix -> productObj.getProductId().startsWith(prefix))) {
                        functionCall.getOutputCollector().add(new Tuple(productObj.getAttributes()));
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000/part-*", "data/products-from-sessions.2017-0801.1000"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);

        List<String> fields = new LinkedList<>();
        fields.add("productId");
        fields.add("brand");
        fields.add("ideal_for");
        fields.add("type");
        fields.add("color");
        fields.add("pattern");
        fields.add("occasion");
        fields.add("fit");
        fields.add("fabric");
        fields.add("vertical");

        ProductsFromSession productsFromExplode = new ProductsFromSession(fields);

        runner.executeHfs(productsFromExplode.getPipe(), args[0], args[1], true);

    }
}


