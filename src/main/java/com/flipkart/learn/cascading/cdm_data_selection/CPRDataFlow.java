package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import cascading.avro.PackedAvroScheme;
import java.util.Map;

/**
 * Created by shubhranshu.shekhar on 02/06/17.
 */
public class CPRDataFlow implements CascadingFlows {

    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));

        Pipe cdmPipe = new Pipe("cdmPipe");
        Fields outputFields = new Fields(DataFields._LISTINGID,
                DataFields._ISSERVICEABLE,
                DataFields._AVAILABILITYSTATUS,
                DataFields._ISFLIPKARTADVANTAGE,
                DataFields._DELIVERYDATE,
                DataFields._MINDELIVERYDATEEPOCHMS,
                DataFields._MAXDELIVERYDATEEPOCHMS,
                DataFields._MRP,
                DataFields._FINALPRICE,
                DataFields._FSP,
                DataFields._SELLERID,
                DataFields._ISCODAVAILABLE,
                DataFields._DELIVERYSPEEDOPTIONS,
                DataFields._PREXOOFFERID,
                DataFields._OFFERIDS);
        cdmPipe = new Each(cdmPipe, Fields.ALL,//new Fields("searchAttributes"),
                new CPRRow(outputFields), Fields.RESULTS);

        Hfs sampleOutputSink = new Hfs(new TextLine(new Fields("searchAttributes")), options.get("output"), SinkMode.REPLACE);
        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(cdmPipe, inputData)
                .addTailSink(cdmPipe, sampleOutputSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }

}