package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;

import javax.xml.crypto.Data;

public class AccountStats {

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[] {
                    "data/session-2017-0801.1000.checkpoint",
                    "data/session-2017-0801.1000.accstats"
            };
        }

        String input = args[0];
        String output = args[1];

        Pipe pipe = new Pipe("aggregate_sessions");
//        Fields should_filter = new Fields("should_filter");
//        pipe = new TransformEach(pipe, new Fields(DataFields._PRODUCTID), should_filter,
//                pid -> SessionDataGenerator.isLifeStyle((String) pid) ? 1 : 0, Fields.ALL);
//        pipe = new Each(pipe, should_filter, new ExpressionFilter("(should_filter == 0)", Integer.class));
//        pipe = new Discard(pipe, should_filter);

        Fields acid = new Fields(DataFields._ACCOUNTID);
        Fields sqid = new Fields(DataFields._SEARCHQUERYID);
        Fields bothFields = Fields.merge(acid, sqid);
        Fields count1 = new Fields("count_1");
        Fields product_count = new Fields("product_count");

        pipe = new Retain(pipe, bothFields);

        pipe = new GroupBy(pipe, bothFields);
        pipe = new Every(pipe, bothFields, new Count(count1));

        pipe = new Discard(pipe, sqid);

        pipe = new GroupBy(pipe, acid);
        pipe = new Every(pipe, count1, new Sum(product_count));
        pipe = new Every(pipe, acid, new Count(new Fields("num_sessions")));


        pipe = new TransformEach(pipe, product_count, new Fields("filter_count"), x -> (Double)x > 3000 ? 1 : 0, Fields.ALL);
        pipe = new Each(pipe, Fields.ALL, new ExpressionFilter("(filter_count == 0)", Integer.class));


        PipeRunner runner = new PipeRunner("aggregate-sessions");
        runner.setNumReducers(2000);
        runner.executeHfs(pipe, input, output, true);



    }

}
