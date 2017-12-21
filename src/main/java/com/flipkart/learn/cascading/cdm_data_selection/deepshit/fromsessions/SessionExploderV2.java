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
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

public class SessionExploderV2 implements SimpleFlow {

    public static final String PAST_CLICKED_PRODUCTS = "pastClickedProducts";
    public static final String PAST_BOUGHT_PRODUCTS = "pastBoughtProducts";
    public static final String SCORABLE_PRODUCTS = "scorableProducts";
    public static final String TARGET = "target";
    public static final String ACTION = "action";
    public static final String ACTION_CLICK = "action.click";
    public static final String CLICK_ID = "clickId";

    private static int numProducts = 10;

    private Map<String, Set<String>> matchConfig = Collections.emptyMap();

    public void setMatchConfig(Map<String, Set<String>> matchConfig) {
        this.matchConfig = matchConfig;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("session-exploder-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        Fields expodedFields = new Fields(_SEARCHQUERYID, CLICK_ID,  _TIMESTAMP, _FINDINGMETHOD, ACTION, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS, SCORABLE_PRODUCTS, TARGET);
        pipe = new Each(pipe, userContext, new ExplodeSessions(expodedFields, matchConfig), Fields.ALL);
        pipe = new Retain(pipe, Fields.merge(new Fields(_ACCOUNTID), expodedFields));
        pipe = new JsonEncodeEach(pipe, new Fields(SCORABLE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS));
//        pipe = new GroupBy(pipe, new Fields(_SEARCHQUERYID));
        return pipe;
    }

    private class ExplodeSessions extends BaseOperation implements Function, Serializable {

        private final Map<String, Set<String>> matchConfig;

        public ExplodeSessions(Fields fields, Map<String, Set<String>> matchConfig) {
            super(fields);
            this.matchConfig = matchConfig;
        }

        private boolean match(ProductObj productObj) {

            return Arrays.stream(lifeStylePrefixes).anyMatch(prefix -> productObj.getProductId().startsWith(prefix));

//            if(matchConfig == null || matchConfig.isEmpty()) {
//                return true;
//            } else {
//                Map<String, String> attributes = productObj.getAttributes();
//                return matchConfig.entrySet().stream().allMatch(x -> x.getValue().contains(attributes.get(x.getKey())));
//            }
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

                clickedProducts = clickedProducts
                        .stream()
                        .filter(this::match)
                        .collect(Collectors.toList());

                Set<String> dedupeNegativeProducts = new HashSet<String>();
                for (ProductObj clickedProduct : clickedProducts) {
                    String findingMethod = clickedProduct.getFindingmethod();
                    int clickedProductPos = clickedProduct.getPosition();
                    ArrayList<ProductObj> impressionsSorted = new ArrayList<>(impressionProducts);
                    impressionsSorted
                            .sort(Comparator.comparingInt(o -> Math.abs(clickedProductPos - o.getPosition())));
                    List<ProductObj> negativeForClicked = impressionsSorted
                            .stream()
                            .filter(this::match)
                            .filter(product -> !product.isClick()) // removing if the product has been clicked
                            .filter(product -> !clickedProduct.getProductId().equals(product.getProductId())) // removing the clicked product
                            .filter(product -> clickedProduct.getPosition() + 2 >= product.getPosition()) // removing if the product's position is 2 greater than the clicked product
                            .limit(numProducts)
                            .collect(Collectors.toList());
                    String clickId = UUID.randomUUID().toString();
                    functionCall.getOutputCollector().add(
                            new Tuple(sqid,
                                    clickId,
                                    timestamp,
                                    findingMethod,
                                    ACTION_CLICK,
                                    pastClick.values(),
                                    pastBought.values(),
                                    clickedProduct.getAttributes(),
                                    1));
                    for (ProductObj negatives : negativeForClicked) {
                        if(!dedupeNegativeProducts.contains(negatives.getProductId())) {
                            functionCall.getOutputCollector().add(
                                    new Tuple(sqid,
                                            clickId,
                                            timestamp,
                                            findingMethod,
                                            ACTION_CLICK,
                                            pastClick.values(),
                                            pastBought.values(),
                                            negatives.getAttributes(),
                                            0));
                            dedupeNegativeProducts.add(negatives.getProductId());
                        }
                    }

                    pastClick = new LinkedHashMap<>(pastClick);
                    pastClick.put(clickedProduct.getProductId(), clickedProduct.getAttributes());
                }

                boughtProducts = boughtProducts.stream().filter(this::match).collect(Collectors.toList());
                pastBought = new LinkedHashMap<>(pastBought);
                for (ProductObj boughtProduct : boughtProducts) {
                    pastBought.put(boughtProduct.getProductId(), boughtProduct.getAttributes());
                }

            }
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000/part-*", "data/sessionexplodeV2-2017-0801.1000", "vertical:mobile"};
        }

        String matchConfigStr = args.length > 2 ? args[2] : null;

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        SessionExploderV2 sessionExploder = new SessionExploderV2();

        if(matchConfigStr != null) {
            Map<String, Set<String>> matchConfig = new HashMap<>();
            String[] matchConfigSplits = matchConfigStr.split("::");
            for (String matchConfigSplit : matchConfigSplits) {
                String[] perAttributeConf = matchConfigSplit.split(":", 2);
                String attributeKey = perAttributeConf[0];
                String attributeValue = perAttributeConf[1];
                ImmutableSet<String> attributeValues = ImmutableSet.copyOf(attributeValue.split(","));
                matchConfig.put(attributeKey, attributeValues);
            }
            sessionExploder.setMatchConfig(matchConfig);
        }
        runner.executeHfs(sessionExploder.getPipe(), args[0], args[1], true);

    }

}
