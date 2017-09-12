package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.commons.CascadingRunner;

import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

/**
 * Created by thejus on 11/9/17.
 */
public class SimpleCDMFlow implements CascadingFlows {

    public static final Fields subFields = new Fields(
            _ACCOUNTID,
            _DEVICEID,
            _PLATFORM,
            _SESSIONID,
            _VISITORID,
//            _FETCHID,
            _FINDINGMETHOD,
            _TIMESTAMP,
            _PRODUCTID,
//            _ISVIDEOAVAILABLE,
//            _ISIMAGESAVAILABLE,
//            _FINALPRODUCTSTATE,
//            _ISSWATCHAVAILABLE,
//            _UGCREVIEWCOUNT,
//            _UGCAVGRATING,
//            _UGCRATINGCOUNT,
//            _LISTINGID,
//            _ISSERVICEABLE,
//            _AVAILABILITYSTATUS,
//            _STATE,
//            _ISFLIPKARTADVANTAGE,
//            _DELIVERYDATE,
//            _MINDELIVERYDATEEPOCHMS,
//            _MAXDELIVERYDATEEPOCHMS,
//            _MRP,
//            _FINALPRICE,
//            _FSP,
//            _ISCODAVAILABLE,
//            _DELIVERYSPEEDOPTIONS,
//            _PREXOOFFERID,
//            _OFFERIDS,
            _POSITION,
            _PRODUCTCARDCLICKS,
//            _PRODUCTPAGEVIEWS,
//            _PRODUCTPAGELISTINGINDEX,
            _ADDTOCARTCLICKS,
            _BUYNOWCLICKS,
            _PRODUCTCARDIMPRESSIONFILTER);

    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));

        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), options.get("output"), SinkMode.REPLACE);

        Pipe cdmPipe = new Pipe("cdmPipe");

        cdmPipe = new Each(cdmPipe, Fields.ALL, new CPRRow(DataFields.cdmOutputFields), Fields.RESULTS);
        cdmPipe = new Retain(cdmPipe, subFields);
//        cdmPipe = new Each(cdmPipe, Fields.ALL, new Limit(100));
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER), new RegexFilter("true"));
        cdmPipe = new Discard(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER));
        Fields buyIntentFields = new Fields(_BUYNOWCLICKS, _ADDTOCARTCLICKS);
        cdmPipe = new Each(cdmPipe, buyIntentFields, new ExpressionFunction(new Fields(_BUYINTENT), _BUYNOWCLICKS + "+" + _ADDTOCARTCLICKS, Float.class), Fields.ALL);
        cdmPipe = new Discard(cdmPipe, buyIntentFields);

        String _INVERTPRODUCTCARDCLICKS = "inv_productcardClicks";
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTCARDCLICKS), new ExpressionFunction(new Fields(_INVERTPRODUCTCARDCLICKS), "1 - " + _PRODUCTCARDCLICKS, Float.class), Fields.ALL);

        cdmPipe = new GroupBy(cdmPipe, new Fields(_ACCOUNTID, _VISITORID , _SESSIONID, _INVERTPRODUCTCARDCLICKS, _TIMESTAMP, _POSITION));
        cdmPipe = new Discard(cdmPipe, new Fields(_INVERTPRODUCTCARDCLICKS));

//        Fields mergeKeys = new Fields(_ACCOUNTID, _DEVICEID, _PLATFORM, _SESSIONID, _VISITORID);
//        Fields productFields = new Fields(_PRODUCTID,
//                _FINDINGMETHOD,
//                _TIMESTAMP,
//                _PRODUCTCARDCLICKS,
//                _PRODUCTPAGEVIEWS,
//                _ADDTOCARTCLICKS,
//                _BUYNOWCLICKS,
//                _POSITION);
//
//        Fields aggregatedProductData = new Fields("aggregatedProductData");
//        cdmPipe = new Every(cdmPipe, productFields, new MapAggregator(aggregatedProductData), Fields.merge(mergeKeys, aggregatedProductData));
//

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(cdmPipe, inputData)
                .addTailSink(cdmPipe, outputTap)
                .setAssertionLevel(AssertionLevel.VALID);
    }

    public static void main(String[] args) {
        args = new String[3];
        args[0] = "flowName=simple-cdm";
        args[1] = "input=data/cdm-2017-0801.1000.avro";
        args[2] = "output=data/simple-cdm-2017-0801.1000";
        CascadingRunner.main(args);
    }

}
