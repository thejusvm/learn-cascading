package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.operation.filter.FilterNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
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

    private int longShortThresholdInMinutes = defaultLongShortThresholdInMin;

    public UrlPidGeneratorFromSessions(Pipe pipe) {

        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new SessionExploder.ExplodeSessions(EXPODED_FIELDS, longShortThresholdInMinutes, 0), Fields.ALL);
        pipe = new Each(pipe, new Fields(STORE_PATH), new FilterNull());
        Fields uriFields = new Fields(SEARCH_QUERY, STORE_PATH, PAST_CLICKED_SHORT_PRODUCTS, PAST_CLICKED_LONG_PRODUCTS);
        Fields pidFields = new Fields(POSITIVE_PRODUCTS);
        pipe = new Retain(pipe, Fields.merge(uriFields, pidFields));
        Fields uri = new Fields("uri");
        pipe = new TransformEach(pipe, uriFields, uri, (MultiInMultiOutFunction) this::generateURI, Fields.SWAP);
        pipe = new TransformEach(pipe, pidFields, (SerializableFunction) x -> ((Map) x).get("productId"), Fields.SWAP);
        pipe = new Retain(pipe, Fields.merge(uri, pidFields));
        setTails(pipe);
    }

    private Object[] generateURI(Object[] x) {
        String searchQuery = (String) x[0];
        String storePath = (String) x[1];
        List<String> shortClick = (List) x[2];
        List<String> longClick = (List) x[3];

        String encodedQuery = null;
        if(searchQuery != null) {
            try {
                encodedQuery = URLEncoder.encode( searchQuery, "UTF-8" );
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        String uri = "/sherlock/stores/" + storePath + "/debug?";
        if(encodedQuery != null) {
            uri += "&q="+encodedQuery;
        }
        return new String[] {uri};
    }

    public static void main(String[] args) {
        Pipe pipe = new Pipe("url-gen");

        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000", "data/session-20180210.10000.uri"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        pipe = new UrlPidGeneratorFromSessions(pipe);

        runner.executeHfs(pipe, args[0], args[1], true);

    }

}
