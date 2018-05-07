package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.operation.filter.FilterNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.RequestContext;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.MultiInMultiOutFunction;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SerializableFunction;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class UrlPidGeneratorFromSessions extends SubAssembly {

    private static final String STORE_PATH = "storePath";
    private int longShortThresholdInMinutes = defaultLongShortThresholdInMin;

    public UrlPidGeneratorFromSessions(Pipe pipe) {

        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new SessionExploder.ExplodeSessions(EXPODED_FIELDS, longShortThresholdInMinutes, 0), Fields.ALL);
        pipe = new TransformEach(pipe, new Fields(REQ_CONTEXT), new Fields(STORE_PATH), x -> ((RequestContext)x).getStorePath(), Fields.ALL);
        pipe = new Each(pipe, new Fields(STORE_PATH), new FilterNull());
        Fields uriFields = new Fields(REQ_CONTEXT, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS);
        Fields pidFields = new Fields(POSITIVE_PRODUCTS);
        pipe = new Retain(pipe, Fields.merge(uriFields, pidFields));
        Fields uri = new Fields("uri");
        pipe = new TransformEach(pipe, uriFields, uri, (MultiInMultiOutFunction) this::generateURI, Fields.SWAP);
        pipe = new TransformEach(pipe, pidFields, (SerializableFunction) x -> ((Map) x).get("productId"), Fields.SWAP);
        pipe = new Retain(pipe, Fields.merge(uri, pidFields));
        setTails(pipe);
    }

    private Object[] generateURI(Object[] x) {
        RequestContext requestContext = (RequestContext) x[0];
        List<String> shortClick = (List) x[1];
        List<String> longClick = (List) x[2];
        String uri = constructUri(requestContext, shortClick, longClick);
        return new String[] {uri};
    }

    public static String constructUri(RequestContext context,
                                      List<String> shortClick, List<String> longClick) {
        String searchQuery = context.getSearchQuery();
        String storePath = context.getStorePath();
        String filters = context.getFiltersApplied();
        int pincode = context.getPincode();

        String encodedQuery = null;
        if(searchQuery != null && !"".equals(searchQuery)) {
            try {
                encodedQuery = URLEncoder.encode( searchQuery, "UTF-8" );
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        String uri = "/sherlock/v1/stores/" + storePath + "/iterator?pincode="+pincode;
        if(encodedQuery != null) {
            uri += "&q="+encodedQuery;
        }
        if(filters != null) {
            uri += "&" + filters;
        }
        return uri;
    }

    public static void main(String[] args) {
        Pipe pipe = new Pipe("url-gen");

        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000", "data/session-20180210.10000.sessions.uri"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        pipe = new UrlPidGeneratorFromSessions(pipe);

        runner.executeHfs(pipe, args[0], args[1], true);

    }

}
