package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;

import java.util.ArrayList;
import java.util.List;

public class GroupByCount implements SimpleFlow {

    private Fields groupByField;

    public GroupByCount(String groupByField) {
        this.groupByField = new Fields(groupByField);
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("abc");
        pipe = new Retain(pipe, groupByField);
        pipe = new GroupBy(pipe, groupByField);
        pipe = new Every(pipe, new Count());
        return pipe;
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[] {
                    "data/session-20180210.10000.explode.split/test/part-00000",
                    "data/session-20180210.10000.explode.split.price_counts",
                    "positive_finalPrice"
            };
        }

        GroupByCount counter = new GroupByCount(args[2]);
        SimpleFlowRunner.execute(counter, args[0], args[1]);

    }


}
