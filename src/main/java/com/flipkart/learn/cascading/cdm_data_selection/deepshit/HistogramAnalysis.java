package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;

import java.io.Serializable;

/**
 * Created by thejus on 18/9/17.
 */
@SimpleFlow.IrSimpleFlow(name = "histogram-analysis")
public class HistogramAnalysis implements SimpleFlow, Serializable {

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("analysis-pipe");
        Fields histogramFields = new Fields(SessionDataGenerator.NUM_DAYS);
        pipe = new Retain(pipe, histogramFields);
        pipe = new GroupBy(pipe, histogramFields);
        pipe = new Every(pipe, histogramFields, new Count());
        return pipe;
    }


    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[] {"data/session-2017-0801.1000/part-*", "data/session-histogram-2017-0801.1000"};
        }

        PipeRunner runner = new PipeRunner("histogram-analysis");
        runner.setNumReducers(600);
        runner.executeHfs(new HistogramAnalysis().getPipe(), args[0], args[1], true);
    }
}
