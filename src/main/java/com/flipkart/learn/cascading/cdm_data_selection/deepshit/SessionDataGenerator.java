package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.CPRRow;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.VerticalFromCMSJson;
import com.flipkart.learn.cascading.commons.CascadingFlow;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.commons.CascadingRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.*;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

/**
 * Created by thejus on 11/9/17.
 */
@CascadingFlow(name = "session-data")
public class SessionDataGenerator implements CascadingFlows, Serializable {

    public static final Fields subFields = new Fields(
            _ACCOUNTID,
            _DEVICEID,
            _PLATFORM,
            _SESSIONID,
            _VISITORID,
//            _FETCHID,
            _FINDINGMETHOD,
            _TIMESTAMP,
            _SEARCHQUERYID,
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
    public static final String USER_CONTEXT = "userContext";
    public static final String USER_STATS = "userStats";
    public static final String USER_DAY_STATS = "userDayStats";
    public static final String NUM_DAYS = "numDays";
    public static final String NUM_SESSIONS = "numSessions";
    public static final String NUM_IMPRESSIONS = "numImpressions";
    public static final String NUM_CLICKS = "numClicks";
    public static final String NUM_BUYS = "numBuys";




    private Pipe getCDMPipe() {
        Pipe cdmPipe = new Pipe("cdmPipe");

        cdmPipe = new Each(cdmPipe, Fields.ALL, new CPRRow(DataFields.cdmOutputFields), Fields.RESULTS);
        cdmPipe = new Retain(cdmPipe, subFields);
//        cdmPipe = new Each(cdmPipe, Fields.ALL, new Limit(100));
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER), new RegexFilter("true"));
        cdmPipe = new Discard(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER));
        Fields buyIntentFields = new Fields(_BUYNOWCLICKS, _ADDTOCARTCLICKS);
        cdmPipe = new Each(cdmPipe, buyIntentFields, new ExpressionFunction(new Fields(_BUYINTENT), _BUYNOWCLICKS + "+" + _ADDTOCARTCLICKS, Float.class), Fields.ALL);
        cdmPipe = new Discard(cdmPipe, buyIntentFields);
        return cdmPipe;
    }

    private Pipe getCmsPipe() {
        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, Fields.ALL,
                new VerticalFromCMSJson(new String[]{DataFields._FSN, DataFields._BRAND, DataFields._VERTICAL}));
        return cmsPipe;
    }

    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));
        Tap cmsData = new GlobHfs(new TextDelimited(new Fields(DataFields._FSN, DataFields._CMS), false, "\t"),
                options.get("cmsInput"));


        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), options.get("output"), SinkMode.REPLACE);

        Pipe cdmRawPipe = getCDMPipe();
        Pipe cmsPipe = getCmsPipe();

        Pipe cdmPipe = new CoGroup(cdmRawPipe, new Fields(DataFields._PRODUCTID), cmsPipe,
                new Fields(DataFields._FSN),
                new InnerJoin());

        cdmPipe = new GroupBy(cdmPipe, new Fields(_ACCOUNTID), new Fields(_VISITORID , _SESSIONID, _TIMESTAMP, _POSITION));
        Fields userContext = new Fields(USER_CONTEXT);
        Fields userStats = new Fields(USER_STATS);
        Fields userDayStats = new Fields(USER_DAY_STATS);

        cdmPipe = new Every(cdmPipe, new SessionDataAggregator(Fields.merge(userStats, userDayStats,userContext)), Fields.ALL);
        cdmPipe = new Each(cdmPipe, userStats, new ExplodeUserStats(new Fields(NUM_DAYS, NUM_SESSIONS, NUM_IMPRESSIONS, NUM_CLICKS, NUM_BUYS)), Fields.ALL);
        cdmPipe = new Each(cdmPipe, new Fields(NUM_CLICKS), new RegexFilter("^[^0]$"));
        cdmPipe = new JsonEncodeEach(cdmPipe, userStats);
        cdmPipe = new JsonEncodeEach(cdmPipe, userDayStats);
        cdmPipe = new JsonEncodeEach(cdmPipe, userContext);
//
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
                .addSource(cdmRawPipe, inputData)
                .addSource(cmsPipe, cmsData)
                .addTailSink(cdmPipe, outputTap)
                .setAssertionLevel(AssertionLevel.VALID);
    }

    public static void main(String[] args) {
        args = new String[3];
        args[0] = "flowName=session-data";
        args[1] = "input=data/cdm-2017-0801.1000.avro";
        args[2] = "output=data/session-2017-0801.1000";
        CascadingRunner.main(args);
    }

    private static class SessionDataAggregator extends BaseOperation<UserContext> implements Aggregator<UserContext>, Serializable {

        public SessionDataAggregator(Fields outputFields) {
            super(outputFields);
        }

        @Override
        public void start(FlowProcess flowProcess, AggregatorCall<UserContext> aggregatorCall) {
            UserContext cnxt = new UserContext();
            aggregatorCall.setContext(cnxt);
        }

        @Override
        public void aggregate(FlowProcess flowProcess, AggregatorCall<UserContext> aggregatorCall) {
            UserContext userContext = aggregatorCall.getContext();

            userContext.setAccountId(aggregatorCall.getArguments().getString(_ACCOUNTID));
            userContext.setDeviceId(aggregatorCall.getArguments().getString(_DEVICEID));
            userContext.setPlatform(aggregatorCall.getArguments().getString(_PLATFORM));

            String sqid = aggregatorCall.getArguments().getString(_SEARCHQUERYID);
            String findingmethod = aggregatorCall.getArguments().getString(_FINDINGMETHOD);
            String productId = aggregatorCall.getArguments().getString(_PRODUCTID);
            int pos = aggregatorCall.getArguments().getInteger(_POSITION);
            long timestamp = aggregatorCall.getArguments().getLong(_TIMESTAMP);
            float click = aggregatorCall.getArguments().getFloat(_PRODUCTCARDCLICKS);
            float buy = aggregatorCall.getArguments().getFloat(_BUYINTENT);


            userContext.addProduct(sqid, new ProductObj(productId, timestamp, pos, click, buy, findingmethod));
        }

        @Override
        public void complete(FlowProcess flowProcess, AggregatorCall<UserContext> aggregatorCall) {
            UserContext userContext = aggregatorCall.getContext();
            SearchSessions sessions = userContext.getProducts();
            Pair<SessionsStats, Map<String, SessionsStats>> stats = sessions.getStats();
            SessionsStats sessionsStats = stats.getFirst();
            aggregatorCall.getOutputCollector().add(new Tuple(sessionsStats, stats.getSecond(), sessions));
        }
    }

    private class ExplodeUserStats extends BaseOperation implements Function, Serializable {


        public ExplodeUserStats(Fields fields) {
            super(fields);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SessionsStats stats = (SessionsStats) functionCall.getArguments().getObject(0);
            functionCall.getOutputCollector().add(new Tuple(
                    stats.getNumDays(),
                    stats.getNumSessions(),
                    stats.getNumImpressions(),
                    stats.getNumClicks(),
                    stats.getNumBuys()
            ));
        }
    }
}
