package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ProductObj;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
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
    public static final Fields EXPODED_FIELDS = new Fields(_SEARCHQUERYID, _TIMESTAMP, _FINDINGMETHOD, ACTION, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS, POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS);
    public static final Fields EXPLODER_TO_ENCODE_FIELDS = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS);


    private static int numProducts = 10;

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("session-exploder-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new ExplodeSessions(EXPODED_FIELDS), Fields.ALL);
        pipe = new Retain(pipe, Fields.merge(new Fields(_ACCOUNTID), EXPODED_FIELDS));
        pipe = new JsonEncodeEach(pipe, EXPLODER_TO_ENCODE_FIELDS);
        return pipe;
    }

    public static class ExplodeSessions extends BaseOperation implements Function, Serializable {

        public ExplodeSessions(Fields fields) {
            super(fields);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions sessionsContainer = (SearchSessions) functionCall.getArguments().getObject(0);
            Collection<SearchSession> sessions = sessionsContainer.getSessions().values();
            Map<String, Map<String, String>> pastClick = Collections.emptyMap();
            Map<String, Map<String, String>> pastBought = Collections.emptyMap();
            for (SearchSession session : sessions) {
                long timestamp = session.getTimestamp();
                String sqid = session.getSqid();
                List<ProductObj> impressionProducts = session.getProducts();
                List<ProductObj> clickedProducts = session.getClickedProduct();
                List<ProductObj> boughtProducts = session.getBoughtProducts();

                Set<String> dedupeNegativeProducts = new HashSet<>();
                for (ProductObj clickedProduct : clickedProducts) {
                    String findingMethod = clickedProduct.getFindingmethod();
                    int clickedProductPos = clickedProduct.getPosition();
                    ArrayList<ProductObj> impressionsSorted = new ArrayList<>(impressionProducts);
                    impressionsSorted
                            .sort(Comparator.comparingInt(o -> Math.abs(clickedProductPos - o.getPosition())));
                    List<ProductObj> negativeProducts = impressionsSorted
                            .stream()
                            .filter(product -> !product.isClick()) // removing if the product has been clicked
                            .filter(product -> !clickedProduct.getProductId().equals(product.getProductId())) // removing the clicked product
                            .filter(product -> clickedProduct.getPosition() + 2 >= product.getPosition()) // removing if the product's position is 2 greater than the clicked product
                            .filter(productObj -> !dedupeNegativeProducts.contains(productObj.getProductId()))
                            .limit(numProducts)
                            .collect(Collectors.toList());
                    List<Map<String, String>> negativeAttributes = negativeProducts
                            .stream()
                            .map(ProductObj::getAttributes)
                            .collect(Collectors.toList());
                    negativeProducts
                            .stream()
                            .map(ProductObj::getProductId)
                            .forEach(dedupeNegativeProducts::add);
                    functionCall.getOutputCollector().add(
                            new Tuple(sqid,
                                    timestamp,
                                    findingMethod,
                                    ACTION_CLICK,
                                    pastClick.values(),
                                    pastBought.values(),
                                    clickedProduct.getAttributes(),
                                    negativeAttributes));
                    pastClick = new LinkedHashMap<>(pastClick);
                    pastClick.put(clickedProduct.getProductId(), clickedProduct.getAttributes());
                }

                pastBought = new LinkedHashMap<>(pastBought);
                for (ProductObj boughtProduct : boughtProducts) {
                    pastBought.put(boughtProduct.getProductId(), boughtProduct.getAttributes());
                }

            }
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000.filter/part-*", "data/sessionexplode-2017-0801.1000"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        SessionExploder sessionExploder = new SessionExploder();

        runner.executeHfs(sessionExploder.getPipe(), args[0], args[1], true);

    }

}
