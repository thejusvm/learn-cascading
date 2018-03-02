package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields._ACCOUNTID;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

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

        Fields should_filter = new Fields("should_filter");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);

        Pipe pipe = new Pipe("aggregate_sessions");
        pipe = new TransformEach(pipe, new Fields(DataFields._PRODUCTID), should_filter,
                pid -> SessionDataGenerator.isLifeStyle((String) pid) ? 1 : 0, Fields.ALL);
        pipe = new Each(pipe, should_filter, new ExpressionFilter("(should_filter == 0)", Integer.class));
        pipe = new Discard(pipe, should_filter);
        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        pipe = SessionDataGenerator.aggregateSessionsPipe(pipe, cmsInput, schema, false);
        pipe = new Each(pipe, userContext, new SessionExploder.ExplodeSessions(EXPODED_FIELDS, defaultLongShortThresholdInMin, defaultNumNegativeProduct), Fields.ALL);
        pipe = new Retain(pipe, Fields.merge(new Fields(_ACCOUNTID), EXPODED_FIELDS));
        pipe = new JsonEncodeEach(pipe, EXPLODER_TO_ENCODE_FIELDS);

        PipeRunner runner = new PipeRunner("aggregate-sessions");
        runner.setNumReducers(2000);
        runner.executeHfs(pipe, input, output, true);

    }

}
