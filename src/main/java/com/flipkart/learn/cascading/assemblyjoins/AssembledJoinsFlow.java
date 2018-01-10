package com.flipkart.learn.cascading.assemblyjoins;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 22/05/17.
 */
public class AssembledJoinsFlow implements CascadingFlows{
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {

        Tap incomingEmployeeInfoTap = new FileTap(new TextDelimited(new Fields("name","place", "salary"), ","), options.get("employeeInformationInput"));
        Tap incomingRegionAverageTap = new FileTap(new TextDelimited(new Fields("region", "average")), options.get("regionAverageInput"));

        Pipe incomingEmployeeInfoPipe = new Pipe("incomingEmployeeInfoPipe");
        Pipe incomingRegionAveragePipe = new Pipe("incomingRegionAveragePipe");

        SubAssembly joinerAssembly = new JoinerAssembly(incomingEmployeeInfoPipe, incomingRegionAveragePipe);

        Tap innerJoinedInformation = new FileTap(new TextDelimited(new Fields("name", "place", "salary", "average"), ","), options.get("innerJoinOutput"), SinkMode.REPLACE);
        Tap outerJoinedInformation = new FileTap(new TextDelimited(new Fields("name", "place", "salary", "average"), ","), options.get("outerJoinOutput"), SinkMode.REPLACE);

        return FlowDef.flowDef().addSource(incomingEmployeeInfoPipe, incomingEmployeeInfoTap)
                .addSource(incomingRegionAveragePipe, incomingRegionAverageTap )
                .addTailSink(joinerAssembly.getTails()[0], innerJoinedInformation)
                .addTailSink(joinerAssembly.getTails()[1], outerJoinedInformation);

    }

    @Override
    public void cleanup() {

    }
}
