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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers.Sampler;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers.UniformRandomSampler;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.NEGATIVE_PRODUCTS;

public class NegativeSamplesGenerator extends SubAssembly {

    int numNegativeSamples = 20;

    public NegativeSamplesGenerator(String integerizedAttributesPath) {
        this(new Pipe("negative_sampler"), integerizedAttributesPath);
    }

    public NegativeSamplesGenerator(Pipe pipe, String integerizedAttributesPath) {
        this(pipe, integerizedAttributesPath, true);
    }

    public NegativeSamplesGenerator(Pipe pipe, String integerizedAttributesPath, boolean jsonify) {

        Fields negativeField = new Fields(NEGATIVE_PRODUCTS);
        if(jsonify) {
            pipe = new JsonDecodeEach(pipe, negativeField, List.class);
        }

        pipe = new Each(pipe, negativeField, new AddNegativeSamples(negativeField, numNegativeSamples, integerizedAttributesPath), Fields.SWAP);

        if(jsonify) {
            pipe = new JsonEncodeEach(pipe, negativeField);
        }

        setTails(pipe);

    }

    private static class AddNegativeSamples extends BaseOperation implements Function {

        private final int numNegativeSamples;
        private final String integerizedAttributesPath;
        private static Sampler sampler;

        public AddNegativeSamples(Fields negativeField, int numNegativeSamples, String integerizedAttributesPath) {
            super(negativeField);
            this.numNegativeSamples = numNegativeSamples;
            this.integerizedAttributesPath = integerizedAttributesPath;
        }

        public static synchronized void init(String attributesPath) {
            if(sampler == null) {
                try {
                    sampler = new UniformRandomSampler(attributesPath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            HdfsUtils.setConfiguration((Configuration) flowProcess.getConfigCopy());
            if(sampler == null) {
                init(integerizedAttributesPath);
            }

            List<Map<String, Integer>> originalNegativesList = (List<Map<String, Integer>>) functionCall.getArguments().getObject(0);
            List<Map<String, Integer>> negativesList = new ArrayList<>(numNegativeSamples);

            for (int i = 0; i < Math.min(numNegativeSamples, originalNegativesList.size()); i++) {
                Map<String, Integer> negative = originalNegativesList.get(i);
                negativesList.add(negative);
            }

            for (int i = 0; i < numNegativeSamples - negativesList.size(); i++) {
                negativesList.add(sampler.getNextSample());
            }

            functionCall.getOutputCollector().add(new Tuple(negativesList));

        }
    }
    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.int",
                    "data/product-attributes.MOB.int/integerized_attributes",
                    "data/sessionexplode-2017-0801.1000.neg"
            };
        }

        NegativeSamplesGenerator pipe = new NegativeSamplesGenerator(args[1]);
        PipeRunner runner = new PipeRunner("negative-sampler");
        runner.setNumReducers(600);
        runner.executeHfs(pipe, args[0], args[2], true);


    }
}
