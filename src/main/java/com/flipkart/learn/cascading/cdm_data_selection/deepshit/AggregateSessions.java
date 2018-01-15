package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.pipe.Pipe;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;

public class AggregateSessions {

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[] {
                    "data/session-2017-0801.1000.checkpoint",
                    "data/product-attributes.MOB/part-00000",
                    "data/session-2017-0801.1000.aggsess"
            };
        }

        String input = args[0];
        String cmsInput = args[1];
        String output = args[2];
        Pipe pipe = new Pipe("aggregate_sessions");
        PipeRunner runner = new PipeRunner("aggregate-sessions");
        runner.setNumReducers(2000);
        runner.executeHfs(SessionDataGenerator.aggregateSessionsPipe(pipe, cmsInput), input, output, true);

    }

}
