package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SessionExploder implements SimpleFlow {

    public static final String PAST_CLICKED_PRODUCTS = "pastClickedProducts";
    public static final String PAST_BOUGHT_PRODUCTS = "pastBoughtProducts";
    public static final String POSITIVE_PRODUCTS = "positiveProducts";
    public static final String NEGATIVE_PRODUCTS = "negativeProducts";
    public static final String CONTEXT = "context";
    public static final String CONTEXT_CLICK = "context.click";
    public static final String RANDOM_ID = "random_id";

    private static int numProducts = 10;

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("session-exploder-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        Fields expodedFields = new Fields(RANDOM_ID, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS, POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, CONTEXT);
        pipe = new Each(pipe, userContext, new ExplodeSessions(expodedFields), Fields.RESULTS);
        pipe = new JsonEncodeEach(pipe, expodedFields);
        pipe = new GroupBy(pipe, new Fields(RANDOM_ID));
        return pipe;
    }

    private class ExplodeSessions extends BaseOperation implements Function, Serializable {

        public ExplodeSessions(Fields fields) {
            super(fields);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions sessionsContainer = (SearchSessions) functionCall.getArguments().getObject(0);
            Collection<SearchSession> sessions = sessionsContainer.getSessions().values();
            List<String> pastClick = Collections.emptyList();
            List<String> pastBought = Collections.emptyList();
            for (SearchSession session : sessions) {
                List<ProductObj> impressionProducts = session.getProducts();
                List<ProductObj> clickedProducts = session.getClickedProduct();
                List<ProductObj> boughtProducts = session.getBoughtProducts();

                for (ProductObj clickedProduct : clickedProducts) {
                    int clickedProductPos = clickedProduct.getPosition();
                    ArrayList<ProductObj> impressionsSorted = new ArrayList<>(impressionProducts);
                    impressionsSorted
                            .sort(Comparator.comparingInt(o -> Math.abs(o.getPosition() - clickedProductPos)));
                    List<String> finalPastClick = pastClick;
                    List<String> finalPastBought = pastBought;
                    List negativeForClicked = impressionsSorted
                            .stream()
                            .filter(x -> !clickedProduct.getProductId().equals(x.getProductId()))
                            .filter(x -> !finalPastClick.contains(x.getProductId()))
                            .filter(x -> !finalPastBought.contains(x.getProductId()))
                            .map(ProductObj::getProductId)
                            .limit(numProducts)
                            .collect(Collectors.toList());
                    pastClick = new ArrayList<>(pastClick);
                    functionCall.getOutputCollector().add(new Tuple(UUID.randomUUID().toString(), pastClick, pastBought, clickedProduct.getProductId(), negativeForClicked, CONTEXT_CLICK));
                }

                pastClick = new ArrayList<>(pastClick);
                for (ProductObj clickedProduct : clickedProducts) {
                    pastClick.add(clickedProduct.getProductId());
                }

                pastBought = new ArrayList<>(pastBought);
                for (ProductObj boughtProduct : boughtProducts) {
                    pastBought.add(boughtProduct.getProductId());
                }

            }
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/test-session.1", "data/test-session-exploded.1"};
        }

//        SimpleFlowRunner.execute(new SessionExploder(), args[0], args[1]);

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        runner.executeHfs(new SessionExploder().getPipe(), args[0], args[1], true);

    }

}
