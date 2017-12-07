package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlow;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.commons.CascadingRunner;
import com.flipkart.learn.cascading.commons.cascading.SingleFieldListAggregator;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.getCDMPipe;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

/**
 * Created by thejus on 11/9/17.
 */
@CascadingFlow(name = "simple-session-data")
public class SimpleSessionDataGenerator implements CascadingFlows, Serializable {


    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));

        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), options.get("output"), SinkMode.REPLACE);

        Pipe cdmRawPipe = getCDMPipe();
        cdmRawPipe = new Each(cdmRawPipe, new ExpressionFilter("(productCardClicks == 0)", Float.class));


        cdmRawPipe = new Retain(cdmRawPipe, new Fields(_ACCOUNTID, _PRODUCTID));
        cdmRawPipe = new Each(cdmRawPipe, new Fields(_PRODUCTID), new SessionDataGenerator.PrefixFilter(lifeStylePrefixes));
        cdmRawPipe = new GroupBy(cdmRawPipe, new Fields(_ACCOUNTID));



        Pipe sessionPipe = new Every(cdmRawPipe, new Fields(_PRODUCTID), new SingleFieldListAggregator(new Fields(_SESSIONID)));
        sessionPipe = new JsonEncodeEach(sessionPipe, new Fields(_SESSIONID));

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(cdmRawPipe, inputData)
                .addTailSink(sessionPipe, outputTap)
                .setAssertionLevel(AssertionLevel.VALID);
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[] {
                    "flowName=simple-session-data",
                    "input=data/cdm-2017-0801.1000.avro",
                    "output=data/sessionNoAttributes-2017-0801.1000",
            };
        }

        CascadingRunner.main(args);
    }


}
