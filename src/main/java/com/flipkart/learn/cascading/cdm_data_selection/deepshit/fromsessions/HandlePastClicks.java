package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.PAST_CLICKED_SHORT_PRODUCTS;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.PAST_CLICKED_LONG_PRODUCTS;

public class HandlePastClicks extends SubAssembly {

    private int numPastClicks;

    public HandlePastClicks(int numPastClicks) {
        this(new Pipe("negative_sampler"), numPastClicks);
    }

    public HandlePastClicks(Pipe pipe, int numPastClicks) {
        this(pipe, numPastClicks, true);
    }

    public HandlePastClicks(Pipe pipe, int numPastClicks, boolean jsonify) {

        this.numPastClicks = numPastClicks;

        Fields pastShort = new Fields(PAST_CLICKED_SHORT_PRODUCTS);
        Fields pastLong = new Fields(PAST_CLICKED_LONG_PRODUCTS);
        Fields pastField = Fields.merge(pastShort, pastLong);
        if(jsonify) {
            pipe = new JsonDecodeEach(pipe, pastField, List.class);
        }

        pipe = new Each(pipe, pastShort, new Handler(pastShort, numPastClicks), Fields.SWAP);
        pipe = new Each(pipe, pastLong, new Handler(pastLong, numPastClicks), Fields.SWAP);

        if(jsonify) {
            pipe = new JsonEncodeEach(pipe, pastField);
        }

        setTails(pipe);

    }

    private static class Handler extends BaseOperation implements Function {

        private final int numPastClicks;

        public Handler(Fields field, int numPastClicks) {
            super(field);
            this.numPastClicks = numPastClicks;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            List<Map<String, Integer>> pastClicks = (List<Map<String, Integer>>) functionCall.getArguments().getObject(0);
            List<Map<String, Integer>> reversedPastClicks = Lists.reverse(pastClicks);

            ArrayList<Map<String, Integer>> modifiedPastClicks = new ArrayList<>(numPastClicks);
            for (int i = 0; i < Math.min(reversedPastClicks.size(), numPastClicks); i++) {
                modifiedPastClicks.add(reversedPastClicks.get(i));
            }

            functionCall.getOutputCollector().add(new Tuple(modifiedPastClicks));

        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.int",
                    "data/sessionexplode-2017-0801.1000.past_click"
            };
        }

        HandlePastClicks pipe = new HandlePastClicks(2);
        PipeRunner runner = new PipeRunner("pastclickhandler");
        runner.setNumReducers(600);
        runner.executeHfs(pipe, args[0], args[1], true);


    }
}
