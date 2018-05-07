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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.*;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

public class SessionExploder implements SimpleFlow {

    public static final String PAST_CLICKED_SHORT_PRODUCTS = "pastClickedShortProducts";
    public static final String PAST_CLICKED_LONG_PRODUCTS = "pastClickedLongProducts";
    public static final String PAST_BOUGHT_PRODUCTS = "pastBoughtProducts";
    public static final String POSITIVE_PRODUCTS = "positiveProducts";
    public static final String NEGATIVE_PRODUCTS = "negativeProducts";
    public static final String NEGATIVE_WITH_RANDOM_PRODUCTS = "negativeWithRandomProducts";
    public static final String NEGATIVE_WITH_IMPRESSION_RANDOM_PRODUCTS = "negativeWithImpressionRandomProducts";
    public static final String IMPRESSIONS_DISTRIBUTED_NEGATIVE_SAMPLED_PRODUCTS = "impressionsDistributedNegativeSampledProducts";
    public static final String ACTION = "action";
    public static final String ACTION_CLICK = "action.click";
    public static final String REQ_CONTEXT = "reqContext";


    public static final Fields EXPODED_FIELDS = new Fields(_SEARCHQUERYID, _TIMESTAMP, _FINDINGMETHOD, ACTION, REQ_CONTEXT, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS, POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS);
    public static final Fields EXPLODER_TO_ENCODE_FIELDS = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS, PAST_BOUGHT_PRODUCTS);

    public static int defaultNumNegativeProduct = 10;
    public static int defaultLongShortThresholdInMin = 30;

    private int numNegativeProducts = defaultNumNegativeProduct;
    private int longShortThresholdInMinutes = defaultLongShortThresholdInMin;

    public void setLongShortThresholdInMinutes(int longShortThresholdInMinutes) {
        this.longShortThresholdInMinutes = longShortThresholdInMinutes;
    }

    public void setNumNegativeProducts(int numNegativeProducts) {
        this.numNegativeProducts = numNegativeProducts;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("session-exploder-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new ExplodeSessions(EXPODED_FIELDS, longShortThresholdInMinutes, numNegativeProducts), Fields.ALL);
        pipe = new Retain(pipe, Fields.merge(new Fields(_ACCOUNTID), EXPODED_FIELDS));
        pipe = new JsonEncodeEach(pipe, EXPLODER_TO_ENCODE_FIELDS);
        return pipe;
    }

    public static class ExplodeSessions extends BaseOperation implements Function, Serializable {

        private long longShortTimestampThreshold; //30 mins * 60 sec * 1000 millis * 1000 nano
        private final int numNegativeProducts;

        public ExplodeSessions(Fields fields, int longShortThresholdInMinutes, int numNegativeProducts) {
            super(fields);
            longShortTimestampThreshold = longShortThresholdInMinutes * 60 * 1000; //mins * 60 sec * 1000 millis
            this.numNegativeProducts = numNegativeProducts;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions sessionsContainer = (SearchSessions) functionCall.getArguments().getObject(0);
            Collection<SearchSession> sessions = sessionsContainer.getSessions().values();
            List<ProductObj> pastClick = Collections.emptyList();
            Map<String, Map<String, Object>> pastBought = Collections.emptyMap();
            for (SearchSession session : sessions) {
                RequestContext reqContext = session.getRequestContext();
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
                            .limit(numNegativeProducts)
                            .collect(Collectors.toList());
                    List<Map<String, Object>> negativeAttributes = negativeProducts
                            .stream()
                            .map(ProductObj::getAttributes)
                            .collect(Collectors.toList());
                    negativeProducts
                            .stream()
                            .map(ProductObj::getProductId)
                            .forEach(dedupeNegativeProducts::add);
                    Pair<List<ProductObj>, List<ProductObj>> shortLongPair = getShortTermLongTermClick(pastClick, timestamp);
                    List<ProductObj> shortTermClick = shortLongPair.getLeft();
                    List<ProductObj> longTermClick = shortLongPair.getRight();
                    functionCall.getOutputCollector().add(
                            new Tuple(sqid,
                                    timestamp,
                                    findingMethod,
                                    ACTION_CLICK,
                                    reqContext,
                                    shortTermClick.stream().map(ProductObj::getAttributes).collect(Collectors.toList()),
                                    longTermClick.stream().map(ProductObj::getAttributes).collect(Collectors.toList()),
                                    pastBought.values(),
                                    clickedProduct.getAttributes(),
                                    negativeAttributes));
                    pastClick = new ArrayList<>(pastClick);
                    pastClick.add(clickedProduct);
                }

                pastBought = new LinkedHashMap<>(pastBought);
                for (ProductObj boughtProduct : boughtProducts) {
                    pastBought.put(boughtProduct.getProductId(), boughtProduct.getAttributes());
                }

            }
        }

        private Pair<List<ProductObj>, List<ProductObj>> getShortTermLongTermClick(List<ProductObj> pastClick, long currentTimestamp) {
            List<ProductObj> shortTerm = new ArrayList<>();
            List<ProductObj> longTerm = new ArrayList<>();

            for (ProductObj productObj : pastClick) {
                if(currentTimestamp - productObj.getTimestamp() > longShortTimestampThreshold){
                    longTerm.add(productObj);
                } else {
                    shortTerm.add(productObj);
                }
            }

            return new MutablePair<>(shortTerm, longTerm);
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000", "data/session-20180210.10000.explode"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        SessionExploder sessionExploder = new SessionExploder();

        runner.executeHfs(sessionExploder.getPipe(), args[0], args[1], true);

    }

}
