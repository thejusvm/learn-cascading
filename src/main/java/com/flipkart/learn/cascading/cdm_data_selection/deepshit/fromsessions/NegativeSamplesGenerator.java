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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers.CountBasedSampler;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers.Sampler;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers.UniformRandomSampler;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.UNIFORM_RANDOM_NEGATIVE_PRODUCTS;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.IMPRESSIONS_DISTRIBUTED_NEGATIVE_SAMPLED_PRODUCTS;

public class NegativeSamplesGenerator extends SubAssembly {

    int numNegativeSamples = 20;

    public NegativeSamplesGenerator(String integerizedAttributesPath) {
        this(new Pipe("negative_sampler"), integerizedAttributesPath);
    }

    public NegativeSamplesGenerator(Pipe pipe, String integerizedAttributesPath) {
        this(pipe, integerizedAttributesPath, true);
    }

    public NegativeSamplesGenerator(Pipe pipe, String integerizedAttributesPath, boolean jsonify) {

        Fields uniformNegative = new Fields(UNIFORM_RANDOM_NEGATIVE_PRODUCTS);
        Fields zipfNegative = new Fields(IMPRESSIONS_DISTRIBUTED_NEGATIVE_SAMPLED_PRODUCTS);
        pipe = new Each(pipe, Fields.NONE, new AddNegativeSamples(uniformNegative, numNegativeSamples, new UniformRandomSampler(integerizedAttributesPath)), Fields.ALL);
        pipe = new Each(pipe, Fields.NONE, new AddNegativeSamples(zipfNegative, numNegativeSamples, new CountBasedSampler(integerizedAttributesPath)), Fields.ALL);

        if(jsonify) {
            pipe = new JsonEncodeEach(pipe, uniformNegative);
            pipe = new JsonEncodeEach(pipe, zipfNegative);
        }

        setTails(pipe);

    }

    private static class AddNegativeSamples extends BaseOperation implements Function {

        private final int numNegativeSamples;
        private Sampler sampler;

        public AddNegativeSamples(Fields negativeField, int numNegativeSamples, Sampler sampler) {
            super(negativeField);
            this.numNegativeSamples = numNegativeSamples;
            this.sampler = sampler;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            HdfsUtils.setConfiguration((Configuration) flowProcess.getConfigCopy());
            try {
                sampler.init();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            List<Map<String, Integer>> negativesList = new ArrayList<>(numNegativeSamples);
            for (int i = 0; i < numNegativeSamples; i++) {
                negativesList.add(sampler.getNextSample());
            }
            functionCall.getOutputCollector().add(new Tuple(negativesList));

        }
    }
    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.int",
                    "data/sessions-2017100.products-int.1/integerized_attributes",
                    "data/sessionexplode-2017-0801.1000.neg"
            };
        }

        NegativeSamplesGenerator pipe = new NegativeSamplesGenerator(args[1]);
        PipeRunner runner = new PipeRunner("negative-sampler");
        runner.setNumReducers(600);
        runner.executeHfs(pipe, args[0], args[2], true);


    }
}
