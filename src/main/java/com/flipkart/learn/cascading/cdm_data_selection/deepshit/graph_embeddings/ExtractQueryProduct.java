package com.flipkart.learn.cascading.cdm_data_selection.deepshit.graph_embeddings;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ProductObj;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.ExtractAttributePairs;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class ExtractQueryProduct implements SimpleFlow {


    public final static String COUNT = "count";
    public final static String PRODUCT_ID = "productId";
    public final static String QUERY = "query";

    private String attribute;

    public ExtractQueryProduct(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("query-chain-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        Fields qpFields = new Fields(QUERY, PRODUCT_ID);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new QueryProduct(qpFields), Fields.RESULTS);
        pipe = new GroupBy(pipe, qpFields);
        Fields count = new Fields(COUNT);
        pipe = new Every(pipe, qpFields, new Count(count));
        pipe = new GroupBy(pipe, Fields.NONE, count);
        return pipe;
    }

    private static class QueryProduct extends BaseOperation implements Function {

        public QueryProduct(Fields fieldDeclaration) {
            super(fieldDeclaration);
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions searchSessions = (SearchSessions)  functionCall.getArguments().getObject(0);
            searchSessions.getSessions().values().forEach(
                    searchSession -> {
                        String searchQuery = searchSession.getRequestContext().getSearchQuery();
                        if(searchQuery != null && !"".equals(searchQuery)) {
                            List<ProductObj> clickedProducts = searchSession.getClickedProduct();
                            for (ProductObj clickedProduct : clickedProducts) {
                                functionCall.getOutputCollector().add(new Tuple(searchQuery, clickedProduct.getProductId()));
                            }
                        }
                    }
            );
        }
    }


    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000/part-*", "data/session-20180210.10000.query_product", "productId"};
        }

        PipeRunner runner = new PipeRunner("query-chain");
        runner.setNumReducers(20);
        ExtractQueryProduct queryChains = new ExtractQueryProduct(args[2]);

        runner.executeHfs(queryChains.getPipe(), args[0], args[1], true);

    }


}
