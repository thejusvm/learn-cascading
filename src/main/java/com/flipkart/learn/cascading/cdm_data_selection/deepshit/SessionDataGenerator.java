package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
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
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SplitTrainTest;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.CascadingFlow;
import com.flipkart.learn.cascading.commons.CascadingFlows;
import com.flipkart.learn.cascading.commons.CascadingRunner;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

/**
 * Created by thejus on 11/9/17.
 */
@CascadingFlow(name = "session-data")
public class SessionDataGenerator implements CascadingFlows, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SplitTrainTest.class);


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
//            _ORIGINALSEARCHQUERY,
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

//    public static String[] lifeStylePrefixes = new String[]{"SAR"};
    public static String[] lifeStylePrefixes = new String[]{"ABQ", "ETE", "EKT", "7QX", "QBC", "ACB", "EFA", "W6C", "ENU", "WGZ", "APT", "EFD", "8QX", "ZQT", "ZHH", "ARM", "E8P", "6GY", "G72", "TYV", "BBO", "ESJ", "FUV", "TUP", "JP4", "BDA", "EYJ", "47D", "3HY", "SG2", "BRB", "EG3", "2GU", "7GN", "4MM", "BIB", "EZD", "RKZ", "FZX", "ZSZ", "BZR", "EFU", "KJW", "FMB", "YTE", "BLR", "EGV", "HAU", "JHR", "URQ", "BLO", "EM2", "DHB", "ZZA", "SEE", "BOL", "EEY", "JBU", "8DZ", "GGD", "BXR", "DTH", "9DT", "7Y9", "HFC", "BRA", "EUC", "8JS", "JHU", "QZ8", "BCO", "EET", "6F6", "UTN", "HQ3", "BRE", "EHQ", "FKN", "JXG", "ZKQ", "BPP", "EGF", "6QU", "CSU", "QJQ", "BRS", "ERY", "RHZ", "4TG", "KSW", "BCP", "E9G", "HPV", "ANZ", "HMZ", "BPC", "E8G", "N6Y", "PMH", "TD4", "BRF", "EAK", "4XG", "YYM", "26J", "CSP", "EPT", "RBN", "JQW", "JPA", "CAP", "ESU", "ZHH", "HAF", "BWB", "CPI", "EGZ", "BSN", "STW", "TZU", "CGN", "EXQ", "7TN", "XWQ", "7TB", "CRG", "DXQ", "RGU", "3H7", "UVQ", "CHO", "E87", "GRE", "6SU", "HVN", "CHU", "EUG", "GEY", "AYR", "FSZ", "CAT", "EZG", "N4D", "5M2", "UUX", "COR", "ENN", "B8W", "SGK", "8GB", "CRA", "EGG", "HGG", "2VW", "4FC", "CTP", "E2A", "69Q", "QH7", "MPC", "DHT", "EH3", "FUD", "YQY", "HJJ", "DRE", "DQR", "DHE", "WGK", "JSZ", "DRP", "EJA", "AQY", "ASY", "HKG", "DUP", "EHJ", "PHA", "AMC", "DBJ", "ETH", "E96", "SXP", "TQY", "HK9", "FAB", "EBT", "JZY", "DVG", "HAT", "FRO", "DYX", "XSB", "HAQ", "ADG", "GAT", "EBQ", "YRM", "EB6", "BZG", "GVE", "ESF", "ARA", "AAH", "EE3", "GWN", "E7Y", "K6H", "GGT", "PT2", "HKF", "ES8", "72R", "ZZF", "M55", "HAR", "E89", "S5Y", "RZP", "9MU", "HAT", "EST", "RGK", "XZG", "QYM", "JCK", "EM4", "D2W", "B94", "P4A", "JEA", "ETF", "BHH", "2Y2", "9U8", "JEG", "EWF", "M5M", "P8G", "6FJ", "JUM", "ESQ", "DYR", "RCQ", "4JN", "KAF", "E9F", "GYD", "VRA", "DBJ", "KCW", "ENB", "F3X", "VDX", "A6Y", "ACB", "ECF", "8XQ", "29E", "TJJ", "KAW", "EQD", "XGY", "QHR", "WYF", "BRF", "E6X", "U4H", "MVY", "WXZ", "CSP", "EFV", "3FA", "DKV", "YTJ", "CAP", "EG6", "27B", "GZ4", "DRX", "CPI", "E8H", "QMT", "H3U", "TYQ", "DRE", "EAJ", "XSZ", "A7B", "XVG", "DRP", "ECF", "YFS", "KJP", "HWW", "DUP", "EHK", "HRK", "ZU9", "QDB", "KET", "ETH", "3GP", "KFZ", "H27", "KIG", "EY4", "TUC", "96Y", "NGS", "LJG", "EAD", "5KA", "3AZ", "SUG", "LJG", "EH9", "DPC", "DYW", "XGG", "LCH", "EA5", "EGG", "TQR", "TQS", "KMI", "EZ6", "CAN", "Y4J", "2RW", "KMU", "EP2", "7RW", "RJ9", "CH9", "NST", "EH8", "HKA", "KH9", "RHT", "KPA", "ETK", "ZYU", "CUN", "KE4", "KPE", "EX3", "T9Z", "EAN", "8BX", "KSH", "ETF", "KSK", "H49", "SFU", "KTB", "ERN", "A4X", "NEB", "EHF", "KTE", "EUB", "W7H", "EEH", "ZQ8", "KTF", "EST", "GYA", "GDC", "FGH", "KTO", "ET2", "G23", "V2T", "JEG", "TKP", "EDS", "FRT", "AQT", "GYB", "VES", "EY2", "74G", "6PA", "S6Y", "KTA", "EGJ", "BGU", "D8Z", "GCZ", "KRT", "EPZ", "HRP", "DEH", "HVY", "LWM", "EEK", "SXZ", "ECU", "E3S", "LGG", "EZY", "XCX", "FNB", "44J", "LJG", "EFF", "DJY", "GYZ", "FEE", "LEH", "EF3", "ZYF", "WNW", "HV5", "LCH", "EEP", "9HH", "PAE", "8UH", "LFT", "EVW", "R8B", "HKS", "HPM", "LIN", "E88", "W4P", "DZE", "J39", "LWG", "EQT", "F5H", "2NW", "XUX", "LNG", "EN7", "WGG", "QGP", "E5N", "MTN", "EWF", "S7W", "F2V", "4V2", "MFL", "EZA", "V7Z", "JHM", "QPB", "NDN", "EK4", "GGM", "TBS", "PZB", "NST", "EKT", "ZU7", "KXG", "GYG", "PAN", "ESY", "EHP", "JQ4", "45H", "PTL", "ESK", "SMC", "MZF", "W2P", "PIA", "E9Z", "G2V", "XMU", "ZXD", "PSQ", "E8Z", "B7T", "DFQ", "UUF", "PON", "EXS", "TWH", "BZB", "Z6P", "PLO", "EDZ", "BZB", "FFV", "EWB", "PYJ", "EB9", "H68", "XBW", "24G", "RNC", "ETU", "F2E", "TWJ", "GZX", "SAL", "EDH", "HFA", "5XH", "6BZ", "SWD", "E9A", "PZF", "AWG", "KAC", "SFA", "EHP", "Z5Y", "BGJ", "8V8", "SAR", "EW3", "HTV", "4BH", "3T2", "SNG", "ETM", "HWM", "WZ8", "TWX", "SIY", "ENA", "BGV", "HJT", "BUW", "SCF", "EKZ", "RMG", "RJW", "V6K", "SPW", "EPJ", "PGV", "M4H", "6VR", "SWL", "EEA", "FSA", "DWB", "XPG", "SRW", "E5Q", "UFC", "WAA", "EYZ", "SHT", "EJM", "H6Q", "YAZ", "WJG", "SSD", "EF8", "XAH", "ABC", "TH4", "SRT", "ERP", "GUF", "XJE", "MPN", "RUG", "EWE", "ENT", "KDU", "98G", "SKI", "ET7", "CQQ", "HVN", "UXG", "SOC", "ER3", "QY7", "CJB", "U3D", "STO", "ENJ", "GNG", "CJY", "GKU", "SUI", "ERS", "2HM", "SPJ", "HBG", "SUS", "EHZ", "W9T", "DEG", "GJF", "SWT", "EHN", "WF9", "WWB", "ZAX", "SWS", "EMG", "Z6M", "DFZ", "7QK", "SWI", "EVQ", "3NM", "FVV", "HBE", "TSH", "E9C", "VZG", "TRX", "CYZ", "THA", "EHS", "8HQ", "HAC", "SGW", "TML", "EEV", "FEG", "H7F", "7ED", "TFH", "EGG", "4NK", "GF3", "TZC", "TIE", "E4Z", "QJ3", "YEX", "KGJ", "TGT", "EH4", "ZHQ", "93F", "DGJ", "TOP", "EFF", "BQK", "EWG", "BJ8", "TKP", "EKJ", "3FH", "7V2", "HKY", "TKS", "EH5", "VWW", "HZ2", "KWX", "TKT", "EZF", "U8Z", "SUU", "BNW", "TRO", "EHP", "G6N", "SVF", "8G3", "TRK", "EBH", "HDF", "U7U", "ZGJ", "TUN", "E8W", "PUW", "KK7", "RKH", "TRB", "EDB", "M7H", "CKR", "TPH", "VES", "EWF", "MMQ", "REK", "MJ5", "WSC", "ERS", "7SU", "ASQ", "UCK", "WCT", "EKA", "4YF", "KXF", "7AM", "WTB", "EA7", "MQK", "KCC", "5ZH"};

    public static boolean isLifeStyle(String pid){
        return Arrays.stream(lifeStylePrefixes).anyMatch(pid::startsWith);
    }

    private String checkpointFile;


    public static Pipe getCDMPipe(List<Feature> cdmFeatures) {
        Pipe cdmPipe = new Pipe("cdmPipe");

        cdmPipe = new Each(cdmPipe, Fields.ALL, new CPRRow(DataFields.cdmOutputFields), Fields.RESULTS);
        String[] featuresArray = cdmFeatures.stream().map(Feature::getSourceKey).collect(Collectors.toList()).toArray(new String[0]);
        Fields featureFields = new Fields(featuresArray);
        cdmPipe = new Retain(cdmPipe, Fields.merge(subFields, featureFields));
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER), new RegexFilter("true"));
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTID), new SessionDataGenerator.PrefixFilter(lifeStylePrefixes));
        cdmPipe = new Discard(cdmPipe, new Fields(_PRODUCTCARDIMPRESSIONFILTER));
        Fields buyIntentFields = new Fields(_BUYNOWCLICKS, _ADDTOCARTCLICKS);
        cdmPipe = new Each(cdmPipe, buyIntentFields, new ExpressionFunction(new Fields(_BUYINTENT), _BUYNOWCLICKS + "+" + _ADDTOCARTCLICKS, Float.class), Fields.ALL);
        cdmPipe = new Discard(cdmPipe, buyIntentFields);
        return cdmPipe;
    }

    private static String[] getAttributeFields(String cmsInput) {
        String[] attributes;
        try {
            String cmsInputFile = HdfsUtils.listFiles(cmsInput, 1).get(0);
            List<String> fields = HdfsUtils.nextLines(cmsInputFile, 1);
            attributes = fields.get(0).split("\t");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return attributes;
    }

    @Override
    public void cleanup() {
//        try {
//            HdfsUtils.delete(checkpointFile);
//        } catch (IOException e) {
//            LOG.error("error cleaning up checkpoint file", e);
//        }
    }
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputData = new GlobHfs( (Scheme)new AvroScheme(), options.get("input"));
        String cmsInput = options.get("cmsInput");
        Tap cmsData = new GlobHfs(new TextDelimited(Fields.ALL, true, "\t"), cmsInput);


        String runId = options.get("runId");
        if (runId == null) {
            runId = UUID.randomUUID().toString();
        }

        checkpointFile = options.get("output") + ".checkpoint";

        Tap checkpointTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), checkpointFile, SinkMode.REPLACE);
        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), options.get("output"), SinkMode.REPLACE);

        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);

        Pipe cdmRawPipe = getCDMPipe(schema.getFeaturesForSource(Feature.Source.CDM));

        Pipe cmsPipe = new Pipe("attributePipe");
        cmsPipe = new Rename(cmsPipe, new Fields(_PRODUCTID), new Fields(_FSN));
        cmsPipe = new Each(cmsPipe, new Fields(_FSN), new SessionDataGenerator.PrefixFilter(lifeStylePrefixes));


        Pipe cdmCmsPipe = new CoGroup(cdmRawPipe, new Fields(DataFields._PRODUCTID), cmsPipe,
                new Fields(DataFields._FSN),
                new LeftJoin());

        Checkpoint checkpointPipe = new Checkpoint("checkpoint", cdmCmsPipe);
        Pipe sessionPipe = aggregateSessionsPipe(checkpointPipe, cmsInput, schema);

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(cdmRawPipe, inputData)
                .addSource(cmsPipe, cmsData)
                .addCheckpoint(checkpointPipe, checkpointTap)
                .addTailSink(sessionPipe, outputTap)
                .setRunID(runId)
                .setAssertionLevel(AssertionLevel.VALID);

    }

    public static Pipe aggregateSessionsPipe(Pipe cmsCdmPipe, String cmsInput, FeatureSchema schema) {
        return aggregateSessionsPipe(cmsCdmPipe, cmsInput, schema,  true);
    }

    public static Pipe aggregateSessionsPipe(Pipe cmsCdmPipe, String cmsInput, FeatureSchema schema, boolean shouldSerialize) {
        Pipe sessionPipe = new GroupBy(cmsCdmPipe, new Fields(_ACCOUNTID), new Fields(_VISITORID , _SESSIONID, _TIMESTAMP, _POSITION));
        Fields userContext = new Fields(USER_CONTEXT);
        Fields userStats = new Fields(USER_STATS);
        Fields userDayStats = new Fields(USER_DAY_STATS);

        sessionPipe = new Every(sessionPipe, new SessionDataAggregator(Fields.merge(userStats, userDayStats,userContext), schema), Fields.ALL);
        sessionPipe = new Each(sessionPipe, userStats, new ExpandUserStats(new Fields(NUM_DAYS, NUM_SESSIONS, NUM_IMPRESSIONS, NUM_CLICKS, NUM_BUYS)), Fields.ALL);
        sessionPipe = new Each(sessionPipe, new ExpressionFilter("(numClicks == 0)", Float.class));

        if(shouldSerialize) {
            sessionPipe = new JsonEncodeEach(sessionPipe, userStats);
            sessionPipe = new JsonEncodeEach(sessionPipe, userDayStats);
            sessionPipe = new JsonEncodeEach(sessionPipe, userContext);
        }

        return sessionPipe;
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[] {
                    "flowName=session-data",
                    "input=data/impressionppv-20180210.10000",
                    "output=data/session-20180210.10000",
                    "cmsInput=data/product-attributes.MOB/part-00000",
                    "runId=sessions-20180210.10000"
            };
        }

        CascadingRunner.main(args);
    }

    public static class SessionDataAggregator extends BaseOperation<UserContext> implements Aggregator<UserContext>, Serializable {

        private final FeatureSchema schema;

        public SessionDataAggregator(Fields outputFields, FeatureSchema schema) {
            super(outputFields);
            this.schema = schema;
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
            String searchQuery = "ignore"; //aggregatorCall.getArguments().getString(_ORIGINALSEARCHQUERY);
            String findingmethod = aggregatorCall.getArguments().getString(_FINDINGMETHOD);
            String productId = aggregatorCall.getArguments().getString(_PRODUCTID);
            int pos = aggregatorCall.getArguments().getInteger(_POSITION);
            long timestamp = aggregatorCall.getArguments().getLong(_TIMESTAMP);
            float click = aggregatorCall.getArguments().getFloat(_PRODUCTCARDCLICKS);
            float buy = aggregatorCall.getArguments().getFloat(_BUYINTENT);

            Map<String, Object> productAttributes = new LinkedHashMap<>();
            List<Feature> enumFeatures = schema.getFeaturesForType(Feature.FeatureType.ENUMERATION);
            for (Feature feature : enumFeatures) {
                String featureValue = aggregatorCall.getArguments().getString(feature.getSourceKey());
                productAttributes.put(feature.getFeatureName(), feature.clean(featureValue));
            }

            List<Feature> numericFeatures = schema.getFeaturesForType(Feature.FeatureType.NUMERIC);
            for (Feature feature : numericFeatures) {
                double value = aggregatorCall.getArguments().getFloat(feature.getSourceKey());
                productAttributes.put(feature.getFeatureName(), feature.clean(value));
            }

            userContext.addToSession(sqid, searchQuery, new ProductObj(productId, timestamp, pos, click, buy, findingmethod, productAttributes));
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

    public static class PrefixFilter extends BaseOperation implements Filter {

        private final ImmutableSet<String> prefixes;

        public PrefixFilter(String[] prefixes) {
            this.prefixes = ImmutableSet.copyOf(prefixes);
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            String productId = (String) filterCall.getArguments().getObject(0);
            return !Iterables.any(prefixes, productId::startsWith);
        }
    }

    public static class ExpandUserStats extends BaseOperation implements Function, Serializable {


        public ExpandUserStats(Fields fields) {
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
