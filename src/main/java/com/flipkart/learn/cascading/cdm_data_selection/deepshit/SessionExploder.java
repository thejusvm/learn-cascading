package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

public class SessionExploder implements SimpleFlow {

    public static final String PAST_CLICKED_PRODUCTS = "pastClickedProducts";
    public static final String PAST_BOUGHT_PRODUCTS = "pastBoughtProducts";
    public static final String POSITIVE_PRODUCTS = "positiveProducts";
    public static final String NEGATIVE_PRODUCTS = "negativeProducts";
    public static final String ACTION = "action";
    public static final String ACTION_CLICK = "action.click";
    public static final String RANDOM_ID = "random_id";

    private static int numProducts = 10;

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("session-exploder-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        Fields expodedFields = new Fields(RANDOM_ID, _FINDINGMETHOD, ACTION, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS, POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS);
        pipe = new Each(pipe, userContext, new ExplodeSessions(expodedFields), Fields.ALL);
        pipe = new Retain(pipe, Fields.merge(new Fields(_ACCOUNTID), expodedFields));
        pipe = new JsonEncodeEach(pipe, expodedFields);
        pipe = new GroupBy(pipe, new Fields(RANDOM_ID));
        pipe = new Discard(pipe, new Fields(RANDOM_ID));
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
                    String findingMethod = clickedProduct.getFindingmethod();
                    int clickedProductPos = clickedProduct.getPosition();
                    ArrayList<ProductObj> impressionsSorted = new ArrayList<>(impressionProducts);
                    impressionsSorted
                            .sort(Comparator.comparingInt(o -> Math.abs(clickedProductPos - o.getPosition())));
                    List<String> finalPastClick = pastClick;
                    List<String> finalPastBought = pastBought;
                    List negativeForClicked = impressionsSorted
                            .stream()
                            .filter(product -> !product.isClick()) // removing if the product has been clicked
                            .filter(product -> !clickedProduct.getProductId().equals(product.getProductId())) // removing the clicked product
                            .filter(product -> !finalPastClick.contains(product.getProductId())) // removed if the product had been clicked in the history
//                            .filter(product -> !finalPastBought.contains(product.getProductId()))
                            .filter(product -> clickedProduct.getPosition() + 2 >= product.getPosition()) // removing if the product's position is 2 greater than the clicked product
                            .map(ProductObj::getProductId)
                            .limit(numProducts)
                            .collect(Collectors.toList());
                    pastClick = new ArrayList<>(pastClick);
                    functionCall.getOutputCollector().add(new Tuple(UUID.randomUUID().toString(), findingMethod, ACTION_CLICK, pastClick, pastBought, clickedProduct.getProductId(), negativeForClicked));
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
