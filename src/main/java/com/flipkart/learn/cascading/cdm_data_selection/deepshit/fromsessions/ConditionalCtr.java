package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Sum;
import cascading.pipe.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.flipkart.learn.cascading.commons.cascading.MultiInMultiOutFunction;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class ConditionalCtr extends SubAssembly {

    public final static String CONTEXT = "context";
    public final static String SCORABLE = "scorable";
    private static final String CLICK = "click";
    private static final String IMPRESSION = "impression";
    private static final String ctr = "ctr";

    private String attribute;

    public ConditionalCtr(Pipe pipe, String attribute) {
        this.attribute = attribute;
        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS), Map.class);
        pipe = new JsonDecodeEach(pipe, new Fields(NEGATIVE_PRODUCTS), List.class);
        pipe = new JsonDecodeEach(pipe, new Fields(PAST_CLICKED_SHORT_PRODUCTS), List.class);
        pipe = new JsonDecodeEach(pipe, new Fields(PAST_CLICKED_LONG_PRODUCTS), List.class);
        pipe = new Each(pipe,
                new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS),
                new PairWiseData(new Fields(CONTEXT, SCORABLE, CLICK, IMPRESSION), attribute),
                Fields.RESULTS);

        Fields groupByFields = new Fields(CONTEXT, SCORABLE);
        Fields clickField = new Fields(CLICK);
        Fields impField = new Fields(IMPRESSION);
        pipe = new GroupBy(pipe, groupByFields);
        pipe = new Every(pipe , clickField, new Sum(clickField));
        pipe = new Every(pipe, impField, new Sum(impField));
//        pipe = new GroupBy(pipe, Fields.NONE, impField, true);
        pipe = new TransformEach(pipe, new Fields(CLICK, IMPRESSION), new Fields(ctr),
                (MultiInMultiOutFunction) x -> new Object[]{((Double)x[0]).floatValue()/((Double)x[1]).floatValue()}, Fields.ALL);

        setTails(pipe);
    }

    private class PairWiseData extends BaseOperation implements Function {

        private final String attribute;

        public PairWiseData(Fields fields, String attribute) {
            super(fields);
            this.attribute = attribute;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry args = functionCall.getArguments();
            Map<String, Object> pos = (Map<String, Object>) args.getObject(0);
            List<Map<String, Object>> neg = (List<Map<String, Object>>)args.getObject(1);
            List<Map<String, Object>> shortC = (List<Map<String, Object>>) args.getObject(2);
            List<Map<String, Object>> longC = (List<Map<String, Object>>) args.getObject(3);

            List<Tuple> clickTuples = generateTuples(shortC, ImmutableList.of(pos), true, true);
            for (Tuple tuple : clickTuples) {
                functionCall.getOutputCollector().add(tuple);
            }


            List<Tuple> impressionTuples = generateTuples(shortC, neg, false, true);
            for (Tuple tuple : impressionTuples) {
                functionCall.getOutputCollector().add(tuple);
            }


        }

        private List<Tuple> generateTuples(List<Map<String, Object>> context, List<Map<String, Object>> product, boolean isClick, boolean isImpression) {
            int clickVal = isClick ? 1 : 0;
            int impVal = isImpression ? 1 : 0;
            List<Tuple> tuples = new ArrayList<>();
            for (Map<String, Object> contextObj : context) {
                for (Map<String, Object> productObj : product) {
                    tuples.add(new Tuple(contextObj.get(attribute), productObj.get(attribute), clickVal, impVal));
                }
            }
            return tuples;
        }
    }


    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000.explode", "data/session-20180210.10000.ctr", "productId"};
        }

        PipeRunner runner = new PipeRunner("contionalCtr");
        runner.setNumReducers(20);
        ConditionalCtr queryChains = new ConditionalCtr(new Pipe("contional-ctr"), args[2]);

        runner.executeHfs(queryChains, args[0], args[1], true);
    }


}
