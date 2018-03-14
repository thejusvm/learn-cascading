package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ProductObj;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

public class FilterSessions extends SubAssembly {


    private Map<String, Set<String>> matchConfig;

    public FilterSessions(Pipe pipe) {
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        Fields numProductsInSession = new Fields("numProductsInSession");
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new TransformEach(pipe, userContext, searchSession -> SearchSessions.filterSessions((SearchSessions) searchSession, this::match), Fields.SWAP);
        pipe = new TransformEach(pipe, userContext, numProductsInSession, searchSession -> ((SearchSessions) searchSession).getStats().getFirst().getNumImpressions(), Fields.ALL);
        pipe = new Each(pipe, numProductsInSession, new ExpressionFilter("(numProductsInSession == 0)", Integer.class));
        pipe = new Discard(pipe, numProductsInSession);
        pipe = new JsonEncodeEach(pipe, userContext);
        setTails(pipe);
    }

    public void setMatchConfig(Map<String,Set<String>> matchConfig) {
        this.matchConfig = matchConfig;
    }

    private boolean match(ProductObj productObj) {

        String[] lifeStylePrefixes = new String[]{"SAR"};
//        return productObj.getProductId().startsWith("MOB");
        return Arrays.stream(lifeStylePrefixes).anyMatch(prefix -> productObj.getProductId().startsWith(prefix));

//            if(matchConfig == null || matchConfig.isEmpty()) {
//                return true;
//            } else {
//                Map<String, String> attributes = productObj.getAttributes();
//                return matchConfig.entrySet().stream().allMatch(x -> x.getValue().contains(attributes.get(x.getKey())));
//            }
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000/part-*", "data/session-2017-0801.1000.filter", "vertical:mobile"};
        }

        String matchConfigStr = args.length > 2 ? args[2] : null;


        Pipe pipe = new Pipe("filter-session-pipe");
        FilterSessions filterSessions = new FilterSessions(pipe);

        if (matchConfigStr != null) {
            Map<String, Set<String>> matchConfig = new HashMap<>();
            String[] matchConfigSplits = matchConfigStr.split("::");
            for (String matchConfigSplit : matchConfigSplits) {
                String[] perAttributeConf = matchConfigSplit.split(":", 2);
                String attributeKey = perAttributeConf[0];
                String attributeValue = perAttributeConf[1];
                ImmutableSet<String> attributeValues = ImmutableSet.copyOf(attributeValue.split(","));
                matchConfig.put(attributeKey, attributeValues);
            }
            filterSessions.setMatchConfig(matchConfig);
        }

        PipeRunner runner = new PipeRunner("filtersessions");
        runner.setNumReducers(600);
        runner.executeHfs(filterSessions, args[0], args[1], true);

    }
}
