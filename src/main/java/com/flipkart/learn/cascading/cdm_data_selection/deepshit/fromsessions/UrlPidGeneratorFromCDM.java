package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.avro.AvroScheme;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.FilterNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.MultiInMultiOutFunction;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SerializableFunction;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import org.apache.commons.lang3.tuple.Pair;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields.*;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.POSITIVE_PRODUCTS;

public class UrlPidGeneratorFromCDM extends SubAssembly {

    public UrlPidGeneratorFromCDM() {

        Pipe cdmPipe = SessionDataGenerator.getCDMPipe(Collections.emptyList());
        cdmPipe = new Each(cdmPipe, new Fields(_RESPONSESTOREPATH), new FilterNull());
        cdmPipe = new Each(cdmPipe, new Fields(_PRODUCTCARDCLICKS), new ExpressionFilter("(productCardClicks == 0)", Float.class));
        Fields uriFields = new Fields(_ORIGINALSEARCHQUERY, _RESPONSESTOREPATH, _FILTERSAPPLIED, _PINCODE, _SORT);
        Fields pidFields = new Fields(_PRODUCTID);
        cdmPipe = new Retain(cdmPipe, Fields.merge(uriFields, pidFields));
        cdmPipe = new TransformEach(cdmPipe, uriFields, new Fields("uri"),
                (MultiInMultiOutFunction) this::generateURI, Fields.SWAP);
        cdmPipe = new Retain(cdmPipe, new Fields("uri", _PRODUCTID));
        setTails(cdmPipe);
    }

    private Object[] generateURI(Object[] x) {
        String searchQuery = (String) x[0];
        String storePath = (String) x[1];
        List filters = (List) x[2];
        int pincode = (Integer) x[3];
        String sort = (String) x[4];

        String encodedQuery = null;
        if(searchQuery != null && !"".equals(searchQuery)) {
            try {
                encodedQuery = URLEncoder.encode( searchQuery, "UTF-8" );
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        String filtersString = constructFilterParams(filters);
        String uri = "/sherlock/stores/" + storePath + "/iterator?pincode="+pincode;
        if(encodedQuery != null) {
            uri += "&q="+encodedQuery;
        }
        if(filtersString != null) {
            uri += "&" + filtersString;
        }
        return new String[] {uri};
    }

    private String constructFilterParams(List<Pair<String, String>> filters) {
        if(filters.isEmpty()) {
            return null;
        } else {
            StringBuilder urlBuilder = new StringBuilder();
            for (Pair<String, String> filter : filters) {
                urlBuilder.append(filter.getKey())
                        .append("=")
                        .append(filter.getValue())
                        .append("&");
            }
            return urlBuilder.toString();
        }
    }

    public static void main(String[] args) {
        Pipe pipe = new Pipe("url-gen");

        if(args.length == 0) {
            args = new String[]{"data/cdm-2017-0801.1000.avro", "data/session-20180210.10000.uri"};
        }

        PipeRunner runner = new PipeRunner("session-explode");
        runner.setNumReducers(600);
        pipe = new UrlPidGeneratorFromCDM();

        Tap inputTap = new GlobHfs( (Scheme)new AvroScheme(), args[0]);
        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, false, "\t"), args[1], SinkMode.REPLACE);

        runner.addSource(pipe, inputTap);
        runner.addTailSink(pipe, outputTap);
        runner.execute();


    }


}
