package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import java.util.Map;

/**
 * Created by shubhranshu.shekhar on 02/06/17.
 */
public class CPRDataFlow implements CascadingFlows {
    //Arguments to run
    //java -cp com.flipkart.learn.cascading.commons.CascadingRunner
    //flowName=cpr-data input=/Users/shubhranshu.shekhar/Work/Data/impressionppv-r-00005.avro
    // output=/Users/shubhranshu.shekhar/Work/Data/temp/sample.output
    // cmsInput=/Users/shubhranshu.shekhar/Work/Data/cms.data

    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));
        Tap cmsData = new GlobHfs(new TextDelimited(new Fields(DataFields._FSN, DataFields._CMS), false, "\t"),
                options.get("cmsInput"));

        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, Fields.ALL,
                new VerticalFromCMSJson(new Fields(DataFields._FSN, DataFields._VERTICAL)));

        Pipe cdmPipe = new Pipe("cdmPipe");

        cdmPipe = new Each(cdmPipe, Fields.ALL,//new Fields("searchAttributes"),
                new CPRRow(DataFields.cdmOutputFields), Fields.RESULTS);

        Pipe cprRawPipe = new CoGroup(cdmPipe, new Fields(DataFields._PRODUCTID), cmsPipe,
                new Fields(DataFields._FSN),
                new InnerJoin());
        cprRawPipe = new Discard(cprRawPipe, Fields.merge(new Fields(DataFields._FSN)));

        DelimitedPartition partition = new DelimitedPartition(new Fields(DataFields._VERTICAL, String.class));


        PartitionTap cprVerticalSplitTap = new PartitionTap( new Hfs(new TextLine(), options.get("output")), partition, SinkMode.REPLACE );
        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(cmsPipe, cmsData)
                .addSource(cdmPipe, inputData)
                .addTailSink(cprRawPipe, cprVerticalSplitTap)
                .setAssertionLevel(AssertionLevel.VALID);
    }

}